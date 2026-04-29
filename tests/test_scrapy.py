from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pytest

from meshagent.scrapy import scrapy
from meshagent.scrapy.scrapy import ScrapyImportProgress, _ScrapedPage


class _Headers:
    def __init__(self, values: dict[bytes, bytes]) -> None:
        self._values = values

    def get(self, key: bytes) -> bytes | None:
        return self._values.get(key)


class _Response:
    def __init__(self, *, url: str, content_type: str, body: bytes) -> None:
        self.url = url
        self.headers = _Headers({b"Content-Type": content_type.encode()})
        self.body = body


class _FakeDatasets:
    def __init__(self) -> None:
        self.schemas: dict[str, pa.Schema] = {}
        self.search_rows: dict[str, list[dict[str, Any]]] = {}
        self.create_calls: list[dict[str, Any]] = []
        self.add_columns_calls: list[dict[str, Any]] = []
        self.merge_calls: list[dict[str, Any]] = []
        self.search_stream_calls: list[dict[str, Any]] = []

    async def create_table_with_schema(
        self,
        *,
        name: str,
        schema: pa.Schema,
        mode: str,
        namespace: list[str] | None = None,
        branch: str | None = None,
    ) -> None:
        self.create_calls.append(
            {
                "name": name,
                "schema": schema,
                "mode": mode,
                "namespace": namespace,
                "branch": branch,
            }
        )
        self.schemas.setdefault(name, schema)

    async def inspect(
        self,
        *,
        table: str,
        namespace: list[str] | None = None,
        branch: str | None = None,
    ) -> pa.Schema:
        del namespace, branch
        return self.schemas.get(table, pa.schema([]))

    async def add_columns(
        self,
        *,
        table: str,
        new_columns: dict[str, pa.Field],
        namespace: list[str] | None = None,
        branch: str | None = None,
    ) -> None:
        self.add_columns_calls.append(
            {
                "table": table,
                "new_columns": new_columns,
                "namespace": namespace,
                "branch": branch,
            }
        )
        schema = self.schemas.get(table, pa.schema([]))
        for field in new_columns.values():
            schema = schema.append(field)
        self.schemas[table] = schema

    async def merge(
        self,
        *,
        table: str,
        on: str,
        records: pa.Table,
        namespace: list[str] | None = None,
        branch: str | None = None,
    ) -> None:
        self.merge_calls.append(
            {
                "table": table,
                "on": on,
                "records": records,
                "namespace": namespace,
                "branch": branch,
            }
        )
        existing_rows = self.search_rows.setdefault(table, [])
        by_url = {
            row["url"]: row for row in existing_rows if isinstance(row.get("url"), str)
        }
        for row in records.to_pylist():
            if isinstance(row.get("url"), str):
                by_url[row["url"]] = row
        self.search_rows[table] = list(by_url.values())

    async def search_stream(
        self,
        *,
        table: str,
        select: list[str] | None = None,
        namespace: list[str] | None = None,
        branch: str | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[pa.Table]:
        self.search_stream_calls.append(
            {
                "table": table,
                "select": select,
                "namespace": namespace,
                "branch": branch,
                "kwargs": kwargs,
            }
        )
        rows = self.search_rows.get(table, [])
        if select is not None:
            rows = [{key: row.get(key) for key in select} for row in rows]
        if rows:
            yield pa.Table.from_pylist(rows)


@dataclass
class _FakeRoom:
    datasets: _FakeDatasets


async def _fake_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    del kwargs
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/a",
            content_type="text/html; charset=utf-8",
            body=b"<html><head><script>x()</script></head><body>Hello <b>A</b></body></html>",
        ),  # type: ignore[arg-type]
        content=b"<html><head><script>x()</script></head><body>Hello <b>A</b></body></html>",
    )
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/b",
            content_type="text/plain",
            body=b"Plain B",
        ),  # type: ignore[arg-type]
        content=b"Plain B",
    )


async def _fake_scraped_page_with_discovery(
    **kwargs: Any,
) -> AsyncIterator[_ScrapedPage]:
    assert kwargs["start_urls"] == ["https://example.com"]
    assert kwargs["known_urls"] == {"https://example.com"}
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com",
            content_type="text/html; charset=utf-8",
            body=b"<a href='/next'>Next</a>",
        ),  # type: ignore[arg-type]
        content=b"<a href='/next'>Next</a>",
        discovered_urls=("https://example.com/next",),
    )


@pytest.mark.asyncio
async def test_import_domain_uses_default_columns_and_merge(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        table="pages",
        namespace=["crawls"],
        batch_size=1,
    )

    assert result.matched_records == 2
    assert result.imported_records == 2
    assert result.skipped_records == 0
    assert result.pages_read == 2
    assert room.datasets.create_calls[0]["mode"] == "create_if_not_exists"
    assert room.datasets.create_calls[0]["namespace"] == ["crawls"]
    assert room.datasets.merge_calls[0]["on"] == "url"
    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["url"] == (
        "https://example.com/a"
    )
    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["content_type"] == (
        "text/html; charset=utf-8"
    )
    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["text"] == "Hello A"


@pytest.mark.asyncio
async def test_extract_callback_can_filter_records(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    async def extract(response: _Response, content: bytes) -> dict[str, str] | None:
        del content
        if response.url.endswith("/b"):
            return None
        return {"url": response.url, "title": "A"}

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        extract=extract,  # type: ignore[arg-type]
    )

    assert result.matched_records == 2
    assert result.imported_records == 1
    assert result.skipped_records == 1
    assert room.datasets.merge_calls[0]["records"].to_pylist() == [
        {"url": "https://example.com/a", "title": "A"}
    ]


@pytest.mark.asyncio
async def test_import_domain_reports_progress(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())
    updates: list[ScrapyImportProgress] = []

    async def progress(update: ScrapyImportProgress) -> None:
        updates.append(update)

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=2,
        progress=progress,
    )

    assert result.imported_records == 2
    assert [update.stage for update in updates] == [
        "started",
        "page_scraped",
        "record_extracted",
        "page_scraped",
        "record_extracted",
        "batch_merged",
        "completed",
    ]
    assert updates[-1].matched_records == 2
    assert updates[-1].imported_records == 2
    assert updates[-1].pending_records == 0


def test_url_helpers() -> None:
    assert scrapy._start_url("example.com/docs") == "https://example.com/docs"
    assert scrapy._domain("https://Example.COM:443/docs") == "example.com"
    filters = scrapy._compiled_url_filters([r"/docs/", r"/blog/"])
    assert scrapy._matches_filters("https://example.com/docs/a", filters)
    assert not scrapy._matches_filters("https://example.com/about", filters)


@pytest.mark.asyncio
async def test_resume_persists_frontier_and_marks_imported_urls_done(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        scrapy, "_iter_scraped_pages", _fake_scraped_page_with_discovery
    )
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        table="pages",
        namespace=["crawls"],
        resume=True,
    )

    assert result.imported_records == 1
    assert result.discovered_urls == 1
    assert room.datasets.search_stream_calls == [
        {
            "table": "pages__frontier",
            "select": ["url", "status"],
            "namespace": ["crawls"],
            "branch": None,
            "kwargs": {},
        }
    ]
    frontier_merges = [
        call["records"].to_pylist()
        for call in room.datasets.merge_calls
        if call["table"] == "pages__frontier"
    ]
    assert frontier_merges[0][0]["status"] == "pending"
    assert frontier_merges[1][0]["url"] == "https://example.com/next"
    assert frontier_merges[1][0]["status"] == "pending"
    assert frontier_merges[2][0]["url"] == "https://example.com"
    assert frontier_merges[2][0]["status"] == "done"
