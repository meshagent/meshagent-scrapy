from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
import asyncio
from typing import Any

import pyarrow as pa
import pytest

from meshagent.api import RoomException
from meshagent.scrapy import scrapy
from scrapy.http import TextResponse

from meshagent.scrapy.scrapy import (
    ScrapyImportProgress,
    _ScrapedDiscovery,
    _ScrapedFailure,
    _ScrapedPage,
)


class _Headers:
    def __init__(self, values: dict[bytes, bytes]) -> None:
        self._values = values

    def get(self, key: bytes) -> bytes | None:
        return self._values.get(key)

    def to_unicode_dict(self) -> dict[str, str]:
        return {
            key.decode("latin-1", errors="replace"): value.decode(
                "latin-1",
                errors="replace",
            )
            for key, value in self._values.items()
        }


class _Request:
    def __init__(
        self,
        *,
        url: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.url = url
        self.meta = meta or {}


class _Response:
    def __init__(
        self,
        *,
        url: str,
        content_type: str,
        body: bytes,
        status: int = 200,
        request: _Request | None = None,
    ) -> None:
        self.url = url
        self.status = status
        self.request = request
        self.headers = _Headers({b"Content-Type": content_type.encode()})
        self.body = body


@dataclass
class _FakeIndex:
    name: str
    columns: list[str]
    type: str


class _FakeDatasets:
    def __init__(self) -> None:
        self.schemas: dict[str, pa.Schema] = {}
        self.search_rows: dict[str, list[dict[str, Any]]] = {}
        self.indexes: dict[str, list[_FakeIndex]] = {}
        self.create_calls: list[dict[str, Any]] = []
        self.add_columns_calls: list[dict[str, Any]] = []
        self.merge_calls: list[dict[str, Any]] = []
        self.search_stream_calls: list[dict[str, Any]] = []
        self.list_indexes_calls: list[dict[str, Any]] = []
        self.create_index_calls: list[dict[str, Any]] = []
        self.optimize_calls: list[dict[str, Any]] = []
        self.raise_existing_schema_conflict = False
        self.create_failures_remaining = 0
        self.merge_failures_remaining = 0

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
        if self.create_failures_remaining > 0:
            self.create_failures_remaining -= 1
            raise RoomException(
                "room connection closed before request completed: "
                "websocket closed with code 1006"
            )
        if self.raise_existing_schema_conflict and name in self.schemas:
            raise RoomException(
                f"Error creating table '{name}': Table '{name}' already exists "
                "with a different schema"
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
        if self.merge_failures_remaining > 0:
            self.merge_failures_remaining -= 1
            raise RoomException(
                "room connection closed before request completed: "
                "websocket closed with code 1006"
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

    async def list_indexes(
        self,
        *,
        table: str,
        namespace: list[str] | None = None,
        branch: str | None = None,
    ) -> list[_FakeIndex]:
        self.list_indexes_calls.append(
            {
                "table": table,
                "namespace": namespace,
                "branch": branch,
            }
        )
        return self.indexes.setdefault(table, [])

    async def create_index(
        self,
        *,
        table: str,
        config: Any,
        namespace: list[str] | None = None,
        branch: str | None = None,
    ) -> None:
        self.create_index_calls.append(
            {
                "table": table,
                "config": config,
                "namespace": namespace,
                "branch": branch,
            }
        )
        self.indexes.setdefault(table, []).append(
            _FakeIndex(
                name=config.name,
                columns=(
                    [config.column] if isinstance(config.column, str) else config.column
                ),
                type=config.index_type,
            )
        )

    async def optimize(
        self,
        *,
        table: str,
        namespace: list[str] | None = None,
        branch: str | None = None,
        config: Any = None,
    ) -> None:
        self.optimize_calls.append(
            {
                "table": table,
                "namespace": namespace,
                "branch": branch,
                "config": config,
            }
        )


@dataclass
class _FakeRoom:
    datasets: _FakeDatasets


def test_default_schema_marks_large_text_fields_for_lance_zstd() -> None:
    schema = scrapy._default_schema()

    assert pa.types.is_large_string(schema.field("text").type)
    assert schema.field("text").metadata == {b"lance-encoding:compression": b"zstd"}
    assert pa.types.is_large_string(
        schema.field("images").type.value_type.field("src").type
    )
    assert schema.field("images").type.value_type.field("src").metadata == {
        b"lance-encoding:compression": b"zstd"
    }
    assert pa.types.is_large_string(
        schema.field("images").type.value_type.field("alt").type
    )
    assert schema.field("images").type.value_type.field("alt").metadata == {
        b"lance-encoding:compression": b"zstd"
    }


async def _fake_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    del kwargs
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/a",
            content_type="text/html; charset=utf-8",
            body=b"<html><head><script>x()</script></head><body>Hello <b>A</b></body></html>",
        ),  # type: ignore[arg-type]
        content=b"<html><head><script>x()</script></head><body>Hello <b>A</b></body></html>",
        request_url="https://example.com/a",
    )
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/b",
            content_type="text/plain",
            body=b"Plain B",
        ),  # type: ignore[arg-type]
        content=b"Plain B",
        request_url="https://example.com/b",
    )


async def _fake_delayed_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    del kwargs
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/a",
            content_type="text/plain",
            body=b"Page A",
        ),  # type: ignore[arg-type]
        content=b"Page A",
        request_url="https://example.com/a",
    )
    await asyncio.sleep(0.03)
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/b",
            content_type="text/plain",
            body=b"Page B",
        ),  # type: ignore[arg-type]
        content=b"Page B",
        request_url="https://example.com/b",
    )


async def _fake_scraped_pages_with_binary(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    del kwargs
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/a",
            content_type="text/html; charset=utf-8",
            body=b"<html><body>Hello</body></html>",
        ),  # type: ignore[arg-type]
        content=b"<html><body>Hello</body></html>",
        request_url="https://example.com/a",
    )
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/file.step",
            content_type="application/octet-stream",
            body=b"\x00binary",
        ),  # type: ignore[arg-type]
        content=b"\x00binary",
        request_url="https://example.com/file.step",
    )


async def _fake_scraped_page_with_assets(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    del kwargs
    content = (
        b'<html><body><a href="/next" title="Next title" rel="nofollow" '
        b'data-id="1">Next <span>Page</span></a>'
        b'<img src="/hero.png" alt="Hero" width="10"></body></html>'
    )
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/a",
            content_type="text/html; charset=utf-8",
            body=content,
        ),  # type: ignore[arg-type]
        content=content,
        request_url="https://example.com/a",
    )


async def _fake_scraped_page_with_clean_noise(
    **kwargs: Any,
) -> AsyncIterator[_ScrapedPage]:
    del kwargs
    content = (
        b"<html><body>"
        b'<nav><a href="/nav">Navigation</a><img src="/nav.png" alt="Nav"></nav>'
        b'<article><h1>Main</h1><a href="/main">Main Link</a>'
        b'<img src="/main.png" alt="Main"></article>'
        b"</body></html>"
    )
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/a",
            content_type="text/html; charset=utf-8",
            body=content,
        ),  # type: ignore[arg-type]
        content=content,
        request_url="https://example.com/a",
    )


async def _fake_duplicate_primary_key_pages(
    **kwargs: Any,
) -> AsyncIterator[_ScrapedPage]:
    del kwargs
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/canonical",
            content_type="text/html; charset=utf-8",
            body=b"First",
        ),  # type: ignore[arg-type]
        content=b"First",
        request_url="https://example.com/a",
    )
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/canonical",
            content_type="text/html; charset=utf-8",
            body=b"Second",
        ),  # type: ignore[arg-type]
        content=b"Second",
        request_url="https://example.com/b",
    )


async def _fake_scraped_page_with_discovery(
    **kwargs: Any,
) -> AsyncIterator[_ScrapedPage]:
    assert kwargs["start_urls"] == ["https://example.com/"]
    assert kwargs["known_urls"] == {"https://example.com/"}
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/",
            content_type="text/html; charset=utf-8",
            body=b"<a href='/next'>Next</a>",
        ),  # type: ignore[arg-type]
        content=b"<a href='/next'>Next</a>",
        request_url="https://example.com/",
        discovered_urls=("https://example.com/next",),
    )


async def _fake_redirected_seed_page(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    assert kwargs["start_urls"] == ["https://example.com/"]
    yield _ScrapedPage(
        response=_Response(
            url="https://www.example.com/",
            content_type="text/html; charset=utf-8",
            body=b"Home",
        ),  # type: ignore[arg-type]
        content=b"Home",
        request_url="https://example.com/",
    )


async def _fake_redirect_chain_page(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    assert kwargs["start_urls"] == ["https://example.com/old.html"]
    yield _ScrapedPage(
        response=_Response(
            url="https://example.com/new",
            content_type="text/html; charset=utf-8",
            body=b"New",
            request=_Request(
                url="https://example.com/new",
                meta={"redirect_urls": ["https://example.com/old.html"]},
            ),
        ),  # type: ignore[arg-type]
        content=b"New",
        request_url="https://example.com/new",
        redirect_urls=("https://example.com/old.html",),
    )


async def _fake_no_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
    assert kwargs["start_urls"] == []
    if False:
        yield _ScrapedPage(  # pragma: no cover
            response=_Response(url="", content_type="", body=b""),  # type: ignore[arg-type]
            content=b"",
            request_url="",
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
        clean="none",
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
    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["text"] == (
        "Hello **A**\n"
    )
    assert [
        call["config"].model_dump()["column"]
        for call in room.datasets.create_index_calls
    ] == ["url"]


@pytest.mark.asyncio
async def test_import_domain_can_create_text_index_when_requested(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=1,
        index_columns=("text",),
    )

    assert [
        (
            call["config"].model_dump()["column"],
            call["config"].model_dump()["index_type"],
        )
        for call in room.datasets.create_index_calls
    ] == [("url", "BTREE"), ("text", "INVERTED")]


@pytest.mark.asyncio
async def test_import_domain_extracts_image_structs(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_page_with_assets)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        table="pages",
        clean="none",
    )

    row = room.datasets.merge_calls[0]["records"].to_pylist()[0]
    assert "link_urls" not in row
    assert "links" not in row
    assert "image_urls" not in row
    assert row["images"] == [
        {
            "src": "/hero.png",
            "alt": "Hero",
        }
    ]


@pytest.mark.asyncio
async def test_import_domain_clean_defaults_before_links(monkeypatch) -> None:
    monkeypatch.setattr(
        scrapy,
        "_iter_scraped_pages",
        _fake_scraped_page_with_clean_noise,
    )

    async def clean_content(**kwargs: Any) -> bytes:
        del kwargs
        return (
            b'<html><body><article><h1>Main</h1><a href="/main">Main Link</a>'
            b'<img src="/main.png" alt="Main"></article></body></html>'
        )

    monkeypatch.setattr(scrapy, "_trafilatura_content", clean_content)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        table="pages",
    )

    row = room.datasets.merge_calls[0]["records"].to_pylist()[0]
    assert row["images"] == [{"src": "/main.png", "alt": "Main"}]
    assert "Main" in row["text"]
    assert "Navigation" not in row["text"]


@pytest.mark.asyncio
async def test_import_domain_clean_can_run_after_links(monkeypatch) -> None:
    monkeypatch.setattr(
        scrapy,
        "_iter_scraped_pages",
        _fake_scraped_page_with_clean_noise,
    )

    async def clean_content(**kwargs: Any) -> bytes:
        del kwargs
        return b"<html><body><article><h1>Main</h1></article></body></html>"

    monkeypatch.setattr(scrapy, "_trafilatura_content", clean_content)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        table="pages",
        clean="after-links",
    )

    row = room.datasets.merge_calls[0]["records"].to_pylist()[0]
    assert row["images"] == [
        {"src": "/nav.png", "alt": "Nav"},
        {"src": "/main.png", "alt": "Main"},
    ]
    assert "Main" in row["text"]
    assert "Navigation" not in row["text"]


@pytest.mark.asyncio
async def test_import_domain_clean_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        scrapy,
        "_iter_scraped_pages",
        _fake_scraped_page_with_clean_noise,
    )

    async def clean_content(**kwargs: Any) -> bytes:
        raise AssertionError("cleaning should not run")

    monkeypatch.setattr(scrapy, "_trafilatura_content", clean_content)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        table="pages",
        clean="none",
    )

    row = room.datasets.merge_calls[0]["records"].to_pylist()[0]
    assert "link_urls" not in row
    assert "links" not in row
    assert "Navigation" in row["text"]


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
async def test_import_domain_can_strip_html_to_text(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=1,
        content_format="text",
        clean="none",
    )

    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["text"] == "Hello A"


@pytest.mark.asyncio
async def test_import_domain_can_keep_html(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=1,
        content_format="html",
        clean="none",
    )

    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["text"] == (
        "<html><head><script>x()</script></head><body>Hello <b>A</b></body></html>"
    )


@pytest.mark.asyncio
async def test_import_domain_html_defaults_to_strip_scripts(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=1,
        content_format="html",
    )

    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["text"] == (
        "<html><head></head><body>Hello <b>A</b></body></html>"
    )


@pytest.mark.asyncio
async def test_import_domain_html_defaults_to_strip_image_data_urls(
    monkeypatch,
) -> None:
    async def fake_scraped_page(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
        del kwargs
        content = (
            b"<html><body>"
            b'<img src="data:image/png;base64,AAAA" alt="Inline">'
            b'<img src="/hero.png" srcset="data:image/png;base64,BBBB 1x" '
            b'alt="Hero">'
            b"</body></html>"
        )
        yield _ScrapedPage(
            response=_Response(
                url="https://example.com/a",
                content_type="text/html; charset=utf-8",
                body=content,
            ),  # type: ignore[arg-type]
            content=content,
            request_url="https://example.com/a",
        )

    monkeypatch.setattr(scrapy, "_iter_scraped_pages", fake_scraped_page)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        content_format="html",
    )

    row = room.datasets.merge_calls[0]["records"].to_pylist()[0]
    assert "data:image" not in row["text"]
    assert "image_urls" not in row
    assert row["images"] == [
        {
            "src": "/hero.png",
            "alt": "Hero",
        }
    ]


@pytest.mark.asyncio
async def test_import_domain_html_strip_csv_controls_css_and_whitespace(
    monkeypatch,
) -> None:
    async def fake_scraped_page(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
        del kwargs
        content = (
            b"<html>\n  <head><style>.x { color: red; }</style></head>\n"
            b'  <body><p style="color: red">Hello</p></body>\n</html>'
        )
        yield _ScrapedPage(
            response=_Response(
                url="https://example.com/a",
                content_type="text/html; charset=utf-8",
                body=content,
            ),  # type: ignore[arg-type]
            content=content,
            request_url="https://example.com/a",
        )

    monkeypatch.setattr(scrapy, "_iter_scraped_pages", fake_scraped_page)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        content_format="html",
        strip="css,whitespace",
    )

    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["text"] == (
        "<html><head></head><body><p>Hello</p></body></html>"
    )


@pytest.mark.asyncio
async def test_import_domain_rejects_unknown_content_format(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    with pytest.raises(ValueError, match="content_format"):
        await scrapy.import_domain_with_scrapy(
            room,  # type: ignore[arg-type]
            url="https://example.com",
            content_format="json",  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_import_domain_rejects_unknown_clean(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    with pytest.raises(ValueError, match="clean"):
        await scrapy.import_domain_with_scrapy(
            room,  # type: ignore[arg-type]
            url="https://example.com",
            clean="always",  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_import_domain_rejects_unknown_strip(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    with pytest.raises(ValueError, match="strip"):
        await scrapy.import_domain_with_scrapy(
            room,  # type: ignore[arg-type]
            url="https://example.com",
            strip="scripts,nope",
        )


@pytest.mark.asyncio
async def test_import_domain_rejects_non_positive_concurrency(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    with pytest.raises(ValueError, match="concurrency"):
        await scrapy.import_domain_with_scrapy(
            room,  # type: ignore[arg-type]
            url="https://example.com",
            concurrency=0,
        )


@pytest.mark.asyncio
async def test_import_domain_rejects_non_positive_max_batch_bytes(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    with pytest.raises(ValueError, match="max_batch_bytes"):
        await scrapy.import_domain_with_scrapy(
            room,  # type: ignore[arg-type]
            url="https://example.com",
            max_batch_bytes=0,
        )


@pytest.mark.asyncio
async def test_import_domain_rejects_non_positive_max_batch_delay(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    with pytest.raises(ValueError, match="max_batch_delay"):
        await scrapy.import_domain_with_scrapy(
            room,  # type: ignore[arg-type]
            url="https://example.com",
            max_batch_delay=0,
        )


@pytest.mark.asyncio
async def test_import_domain_passes_concurrency_to_scrapy(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
        captured.update(kwargs)
        if False:
            yield

    monkeypatch.setattr(scrapy, "_iter_scraped_pages", fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        concurrency=5,
    )

    assert captured["concurrency"] == 5


@pytest.mark.asyncio
async def test_import_domain_passes_include_sitemap_to_scrapy(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
        captured.update(kwargs)
        if False:
            yield

    monkeypatch.setattr(scrapy, "_iter_scraped_pages", fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        include_sitemap=True,
    )

    assert captured["include_sitemap"] is True


@pytest.mark.asyncio
async def test_import_domain_dedupes_primary_keys_in_merge_batch(monkeypatch) -> None:
    monkeypatch.setattr(
        scrapy, "_iter_scraped_pages", _fake_duplicate_primary_key_pages
    )
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=2,
        clean="none",
    )

    assert result.matched_records == 2
    assert result.imported_records == 1
    assert room.datasets.merge_calls[0]["records"].to_pylist() == [
        {
            "url": "https://example.com/canonical",
            "date": room.datasets.merge_calls[0]["records"].to_pylist()[0]["date"],
            "content_type": "text/html; charset=utf-8",
            "text": "Second\n",
            "images": [],
        }
    ]


@pytest.mark.asyncio
async def test_import_domain_flushes_content_batch_by_estimated_bytes(
    monkeypatch,
) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=100,
        max_batch_bytes=1,
        clean="none",
    )

    assert result.imported_records == 2
    assert [call["records"].num_rows for call in room.datasets.merge_calls] == [1, 1]


@pytest.mark.asyncio
async def test_import_domain_retries_retryable_room_writes(monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(scrapy.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    datasets = _FakeDatasets()
    datasets.create_failures_remaining = 2
    room = _FakeRoom(datasets=datasets)

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=2,
        clean="none",
        create_indexes=False,
        max_batch_delay=None,
    )

    assert result.imported_records == 2
    assert result.skipped_records == 0
    assert sleeps == [1.0, 2.0]


@pytest.mark.asyncio
async def test_import_domain_skips_batch_after_retryable_write_failures(
    monkeypatch,
) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(scrapy.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    datasets = _FakeDatasets()
    datasets.merge_failures_remaining = 4
    room = _FakeRoom(datasets=datasets)

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=2,
        clean="none",
        create_indexes=False,
        max_batch_delay=None,
    )

    assert result.imported_records == 0
    assert result.skipped_records == 2
    assert len(datasets.search_rows.get("scrapy", [])) == 0
    assert sleeps == [1.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_import_domain_flushes_content_batch_by_delay(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_delayed_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=100,
        max_batch_bytes=None,
        max_batch_delay=0.01,
        clean="none",
    )

    assert result.imported_records == 2
    assert [call["records"].num_rows for call in room.datasets.merge_calls] == [1, 1]


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


@pytest.mark.asyncio
async def test_import_domain_can_skip_indexes(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        create_indexes=False,
    )

    assert room.datasets.create_index_calls == []


@pytest.mark.asyncio
async def test_response_filter_skips_non_matching_responses(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())
    updates: list[ScrapyImportProgress] = []

    async def progress(update: ScrapyImportProgress) -> None:
        updates.append(update)

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        response_filter="contains(headers.\"content-type\", 'text/html')",
        clean="none",
        progress=progress,
    )

    assert result.matched_records == 2
    assert result.imported_records == 1
    assert result.skipped_records == 1
    assert room.datasets.merge_calls[0]["records"].to_pylist() == [
        {
            "url": "https://example.com/a",
            "date": room.datasets.merge_calls[0]["records"].to_pylist()[0]["date"],
            "content_type": "text/html; charset=utf-8",
            "text": "Hello **A**\n",
            "images": [],
        }
    ]
    assert "response_filtered" in [update.stage for update in updates]


@pytest.mark.asyncio
async def test_default_response_filter_skips_binary_responses(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages_with_binary)
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        clean="none",
    )

    assert result.matched_records == 2
    assert result.imported_records == 1
    assert result.skipped_records == 1
    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["url"] == (
        "https://example.com/a"
    )


@pytest.mark.asyncio
async def test_custom_response_filter_replaces_default_text_filter(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages_with_binary)
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        response_filter="contains(url, '.step')",
        clean="none",
    )

    assert result.matched_records == 2
    assert result.imported_records == 1
    assert result.skipped_records == 1
    assert room.datasets.merge_calls[0]["records"].to_pylist()[0]["url"] == (
        "https://example.com/file.step"
    )


@pytest.mark.asyncio
async def test_import_domain_optimizes_periodically(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())
    updates: list[ScrapyImportProgress] = []

    async def progress(update: ScrapyImportProgress) -> None:
        updates.append(update)

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        batch_size=1,
        optimize_every=1,
        progress=progress,
    )

    assert [call["table"] for call in room.datasets.optimize_calls] == [
        "scrapy",
        "scrapy",
    ]
    assert "optimizing" in [update.stage for update in updates]
    assert "optimized" in [update.stage for update in updates]


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
    assert frontier_merges[2][0]["url"] == "https://example.com/"
    assert frontier_merges[2][0]["status"] == "done"
    assert [
        (call["table"], call["config"].model_dump()["column"])
        for call in room.datasets.create_index_calls
    ] == [
        ("pages", "url"),
        ("pages__frontier", "url"),
        ("pages__frontier", "status"),
    ]


@pytest.mark.asyncio
async def test_resume_marks_seed_request_url_done_when_response_url_differs(
    monkeypatch,
) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_redirected_seed_page)
    room = _FakeRoom(datasets=_FakeDatasets())
    room.datasets.search_rows["pages__frontier"] = [
        {
            "url": "https://example.com/",
            "status": "pending",
            "discovered_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "source_url": None,
            "error": None,
        }
    ]

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com",
        table="pages",
        resume=True,
    )

    assert result.imported_records == 1
    frontier_rows = {
        row["url"]: row["status"]
        for row in room.datasets.search_rows["pages__frontier"]
    }
    assert frontier_rows["https://example.com/"] == "done"
    assert frontier_rows["https://www.example.com/"] == "done"


@pytest.mark.asyncio
async def test_resume_marks_redirect_chain_urls_done(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_redirect_chain_page)
    room = _FakeRoom(datasets=_FakeDatasets())
    room.datasets.search_rows["pages__frontier"] = [
        {
            "url": "https://example.com/old.html",
            "status": "pending",
            "discovered_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "source_url": None,
            "error": None,
        }
    ]

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com/old.html",
        table="pages",
        resume=True,
    )

    assert result.imported_records == 1
    frontier_rows = {
        row["url"]: row["status"]
        for row in room.datasets.search_rows["pages__frontier"]
    }
    assert frontier_rows["https://example.com/old.html"] == "done"
    assert frontier_rows["https://example.com/new"] == "done"


@pytest.mark.asyncio
async def test_resume_reconciles_done_aliases_before_crawling(monkeypatch) -> None:
    monkeypatch.setattr(scrapy, "_iter_scraped_pages", _fake_no_pages)
    room = _FakeRoom(datasets=_FakeDatasets())
    room.datasets.search_rows["pages__frontier"] = [
        {"url": "https://example.com/page", "status": "done"},
        {"url": "https://example.com/page#main-content", "status": "pending"},
    ]

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com/page",
        table="pages",
        resume=True,
    )

    assert result.imported_records == 0
    frontier_rows = {
        row["url"]: row["status"]
        for row in room.datasets.search_rows["pages__frontier"]
    }
    assert frontier_rows["https://example.com/page"] == "done"
    assert frontier_rows["https://example.com/page#main-content"] == "done"


@pytest.mark.asyncio
async def test_resume_filters_retry_urls_to_current_crawl_filter(monkeypatch) -> None:
    captured_start_urls: list[str] = []

    async def fake_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
        captured_start_urls.extend(kwargs["start_urls"])
        if False:
            yield

    monkeypatch.setattr(scrapy, "_iter_scraped_pages", fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())
    room.datasets.search_rows["pages__frontier"] = [
        {"url": "https://www.pfiserfaucets.com/", "status": "failed"},
        {"url": "https://www.pfisterfaucets.com/", "status": "failed"},
    ]

    await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://www.pfisterfaucets.com/",
        table="pages",
        url_filter=r"^https?://www\.pfisterfaucets\.com(/.*)?$",
        resume=True,
        retry_failed=True,
    )

    assert captured_start_urls == ["https://www.pfisterfaucets.com/"]


@pytest.mark.asyncio
async def test_scrapy_request_defaults_do_not_send_accept_language(monkeypatch) -> None:
    captured_settings: dict[str, Any] = {}

    class FakeRunner:
        def __init__(self, settings: dict[str, Any]) -> None:
            captured_settings["runner"] = settings

        def crawl(self, spider_cls: Any) -> asyncio.Future[None]:
            captured_settings["spider"] = spider_cls.custom_settings
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    monkeypatch.setattr(scrapy, "AsyncCrawlerRunner", FakeRunner)

    events = [
        event
        async for event in scrapy._iter_scraped_pages(
            start_urls=["https://example.com/"],
            domain=None,
            url_filter=None,
            limit=1,
            concurrency=None,
            user_agent=None,
            respect_robots_txt=False,
            include_sitemap=False,
            known_urls=set(),
        )
    ]

    assert events == []
    headers = captured_settings["spider"]["DEFAULT_REQUEST_HEADERS"]
    assert (
        headers["Accept"]
        == "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    )
    assert "Accept-Language" not in headers
    assert captured_settings["spider"]["USER_AGENT"].startswith("Mozilla/5.0")


@pytest.mark.asyncio
async def test_scrapy_user_agent_override_sets_scrapy_user_agent(monkeypatch) -> None:
    captured_settings: dict[str, Any] = {}

    class FakeRunner:
        def __init__(self, settings: dict[str, Any]) -> None:
            captured_settings["runner"] = settings

        def crawl(self, spider_cls: Any) -> asyncio.Future[None]:
            captured_settings["spider"] = spider_cls.custom_settings
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    monkeypatch.setattr(scrapy, "AsyncCrawlerRunner", FakeRunner)

    events = [
        event
        async for event in scrapy._iter_scraped_pages(
            start_urls=["https://example.com/"],
            domain=None,
            url_filter=None,
            limit=1,
            concurrency=None,
            user_agent="meshagent-test",
            respect_robots_txt=False,
            include_sitemap=False,
            known_urls=set(),
        )
    ]

    assert events == []
    assert captured_settings["spider"]["USER_AGENT"] == "meshagent-test"


@pytest.mark.asyncio
async def test_scrapy_concurrency_sets_scrapy_concurrent_requests(monkeypatch) -> None:
    captured_settings: dict[str, Any] = {}

    class FakeRunner:
        def __init__(self, settings: dict[str, Any]) -> None:
            captured_settings["runner"] = settings

        def crawl(self, spider_cls: Any) -> asyncio.Future[None]:
            captured_settings["spider"] = spider_cls.custom_settings
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    monkeypatch.setattr(scrapy, "AsyncCrawlerRunner", FakeRunner)

    events = [
        event
        async for event in scrapy._iter_scraped_pages(
            start_urls=["https://example.com/"],
            domain=None,
            url_filter=None,
            limit=1,
            concurrency=5,
            user_agent=None,
            respect_robots_txt=False,
            include_sitemap=False,
            known_urls=set(),
        )
    ]

    assert events == []
    assert captured_settings["spider"]["CONCURRENT_REQUESTS"] == 5


@pytest.mark.asyncio
async def test_scrapy_include_sitemap_discovers_sitemap_urls(monkeypatch) -> None:
    captured_start_urls: list[str] = []
    sitemap_body = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/from-sitemap</loc></url>
  <url><loc>https://other.example.com/external</loc></url>
</urlset>
"""

    class FakeRunner:
        def __init__(self, settings: dict[str, Any]) -> None:
            del settings

        def crawl(self, spider_cls: Any) -> asyncio.Future[None]:
            async def run_spider() -> None:
                spider = spider_cls()
                async for request in spider.start():
                    captured_start_urls.append(request.url)
                    if request.url == "https://example.com/sitemap.xml":
                        response = TextResponse(
                            url=request.url,
                            body=sitemap_body,
                            encoding="utf-8",
                            request=request,
                        )
                        request.callback(response)

            return asyncio.create_task(run_spider())

    monkeypatch.setattr(scrapy, "AsyncCrawlerRunner", FakeRunner)

    events = [
        event
        async for event in scrapy._iter_scraped_pages(
            start_urls=["https://example.com/"],
            domain=None,
            url_filter=None,
            limit=1,
            concurrency=None,
            user_agent=None,
            respect_robots_txt=False,
            include_sitemap=True,
            known_urls=set(),
        )
    ]

    assert captured_start_urls == [
        "https://example.com/robots.txt",
        "https://example.com/sitemap.xml",
        "https://example.com/",
    ]
    assert events == [
        _ScrapedDiscovery(
            source_url="https://example.com/sitemap.xml",
            discovered_urls=("https://example.com/from-sitemap",),
        )
    ]


@pytest.mark.asyncio
async def test_resume_records_frontier_failure_details(monkeypatch) -> None:
    async def fake_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedFailure]:
        del kwargs
        yield _ScrapedFailure(
            url="https://example.com/",
            error="HTTP 403",
            failure_type="http_status",
            http_status=403,
            final_url="https://example.com/",
            content_type="text/html; charset=utf-8",
        )

    monkeypatch.setattr(scrapy, "_iter_scraped_pages", fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com/",
        table="pages",
        resume=True,
    )

    assert result.failed_urls == 1
    frontier_row = room.datasets.search_rows["pages__frontier"][0]
    assert frontier_row["status"] == "failed"
    assert frontier_row["error"] == "HTTP 403"
    assert frontier_row["failure_type"] == "http_status"
    assert frontier_row["http_status"] == 403
    assert frontier_row["final_url"] == "https://example.com/"
    assert frontier_row["content_type"] == "text/html; charset=utf-8"


@pytest.mark.asyncio
async def test_resume_records_unresolved_frontier_failure_type(monkeypatch) -> None:
    async def fake_scraped_pages(**kwargs: Any) -> AsyncIterator[_ScrapedPage]:
        del kwargs
        if False:
            yield

    monkeypatch.setattr(scrapy, "_iter_scraped_pages", fake_scraped_pages)
    room = _FakeRoom(datasets=_FakeDatasets())

    result = await scrapy.import_domain_with_scrapy(
        room,  # type: ignore[arg-type]
        url="https://example.com/",
        table="pages",
        resume=True,
    )

    assert result.failed_urls == 1
    frontier_row = room.datasets.search_rows["pages__frontier"][0]
    assert frontier_row["status"] == "failed"
    assert frontier_row["error"] == "request finished without response callback"
    assert frontier_row["failure_type"] == "crawler_no_callback"


@pytest.mark.asyncio
async def test_ensure_frontier_table_adds_columns_after_existing_schema_conflict() -> (
    None
):
    datasets = _FakeDatasets()
    datasets.schemas["pages__frontier"] = pa.schema(
        [
            pa.field("url", pa.string(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("discovered_at", pa.string()),
            pa.field("updated_at", pa.string()),
            pa.field("source_url", pa.string()),
            pa.field("error", pa.string()),
        ]
    )
    datasets.raise_existing_schema_conflict = True
    room = _FakeRoom(datasets=datasets)

    await scrapy._ensure_frontier_table(
        room=room,  # type: ignore[arg-type]
        table="pages__frontier",
        namespace=None,
        branch=None,
    )

    assert set(datasets.add_columns_calls[0]["new_columns"]) == {
        "failure_type",
        "http_status",
        "final_url",
        "content_type",
    }
    assert "failure_type" in datasets.schemas["pages__frontier"].names


@pytest.mark.asyncio
async def test_load_frontier_normalizes_urls_and_prefers_done_status() -> None:
    room = _FakeRoom(datasets=_FakeDatasets())
    room.datasets.search_rows["pages__frontier"] = [
        {"url": "https://example.com", "status": "pending"},
        {"url": "https://example.com/", "status": "done"},
    ]

    frontier = await scrapy._load_frontier(
        room=room,  # type: ignore[arg-type]
        table="pages__frontier",
        namespace=None,
        branch=None,
    )

    assert frontier == {"https://example.com/": "done"}
