from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import asyncio
import logging
import re
import warnings
from typing import Any, TypeAlias
from urllib.parse import urlparse

import pyarrow as pa

from meshagent.api import RoomClient

from scrapy import Request, Spider
from scrapy.crawler import AsyncCrawlerRunner
from scrapy.exceptions import CloseSpider
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor

logger = logging.getLogger(__name__)

warnings.filterwarnings(
    "ignore",
    message="HttpxDownloadHandler is experimental.*",
    module=r"scrapy\.core\.downloader\.handlers\._httpx",
)
logging.getLogger("scrapy.core.downloader.handlers._httpx").setLevel(logging.ERROR)

ExtractedRecord: TypeAlias = Mapping[str, Any]
ExtractCallback: TypeAlias = Callable[
    [Response, bytes],
    Awaitable[ExtractedRecord | None],
]


@dataclass(frozen=True)
class ScrapyImportResult:
    matched_records: int
    imported_records: int
    skipped_records: int
    pages_read: int
    discovered_urls: int = 0
    failed_urls: int = 0


@dataclass(frozen=True)
class ScrapyImportProgress:
    stage: str
    matched_records: int
    imported_records: int
    skipped_records: int
    pages_read: int
    pending_records: int
    current_url: str | None = None


ProgressCallback: TypeAlias = Callable[
    [ScrapyImportProgress],
    Awaitable[None],
]


@dataclass(frozen=True)
class _ScrapedPage:
    response: Response
    content: bytes
    discovered_urls: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ScrapedFailure:
    url: str
    error: str


_ScrapedEvent: TypeAlias = _ScrapedPage | _ScrapedFailure


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._hidden_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag.lower() in {"script", "style", "noscript"}:
            self._hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript"} and self._hidden_depth > 0:
            self._hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._hidden_depth == 0:
            stripped = data.strip()
            if stripped:
                self._parts.append(stripped)

    def text(self) -> str:
        return " ".join(self._parts)


async def import_domain_with_scrapy(
    room: RoomClient,
    *,
    url: str,
    domain: str | None = None,
    table: str = "scrapy",
    url_filter: str | Sequence[str] | None = None,
    extract: ExtractCallback | None = None,
    schema: pa.Schema | None = None,
    primary_key: str = "url",
    namespace: list[str] | None = None,
    branch: str | None = None,
    limit: int | None = None,
    batch_size: int = 100,
    user_agent: str | None = None,
    respect_robots_txt: bool = False,
    resume: bool = False,
    frontier_table: str | None = None,
    progress: ProgressCallback | None = None,
) -> ScrapyImportResult:
    """Spider a domain with Scrapy and import pages into a room dataset.

    The default extractor writes `url`, `date`, `content_type`, and `text`,
    merging on `url`. Pass `schema` when a custom extractor should create an
    empty table before the first row. Pass `progress` to receive async progress
    updates during the import.
    """

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if primary_key == "":
        raise ValueError("primary_key must be non-empty")
    if limit is not None and limit < 0:
        raise ValueError("limit must be greater than or equal to zero")

    extractor = extract or _default_extract
    schema = schema or (_default_schema() if extract is None else None)
    frontier_table_name = frontier_table or f"{table}__frontier"
    if schema is not None:
        await _ensure_table(
            room=room,
            table=table,
            schema=schema,
            namespace=namespace,
            branch=branch,
        )

    start_url = _start_url(url)
    frontier: dict[str, str] = {}
    start_urls = [start_url]
    if resume:
        await _ensure_frontier_table(
            room=room,
            table=frontier_table_name,
            namespace=namespace,
            branch=branch,
        )
        frontier = await _load_frontier(
            room=room,
            table=frontier_table_name,
            namespace=namespace,
            branch=branch,
        )
        if start_url not in frontier:
            await _merge_frontier_rows(
                room=room,
                table=frontier_table_name,
                rows=[_frontier_row(url=start_url, status="pending")],
                namespace=namespace,
                branch=branch,
            )
            frontier[start_url] = "pending"
        start_urls = [
            pending_url
            for pending_url, status in frontier.items()
            if status in {"pending", "failed"}
        ]
        if limit is not None:
            start_urls = start_urls[:limit]

    matched_records = 0
    imported_records = 0
    skipped_records = 0
    pages_read = 0
    discovered_urls = 0
    failed_urls = 0
    batch: list[dict[str, Any]] = []
    batch_urls: list[str] = []
    await _report_progress(
        progress,
        stage="started",
        matched_records=matched_records,
        imported_records=imported_records,
        skipped_records=skipped_records,
        pages_read=pages_read,
        pending_records=len(batch),
    )

    async for event in _iter_scraped_pages(
        start_urls=start_urls,
        domain=domain,
        url_filter=url_filter,
        limit=limit,
        user_agent=user_agent,
        respect_robots_txt=respect_robots_txt,
        known_urls=set(frontier) if resume else set(),
    ):
        if isinstance(event, _ScrapedFailure):
            failed_urls += 1
            if resume:
                frontier[event.url] = "failed"
                await _merge_frontier_rows(
                    room=room,
                    table=frontier_table_name,
                    rows=[
                        _frontier_row(
                            url=event.url,
                            status="failed",
                            error=event.error,
                        )
                    ],
                    namespace=namespace,
                    branch=branch,
                )
            await _report_progress(
                progress,
                stage="request_failed",
                matched_records=matched_records,
                imported_records=imported_records,
                skipped_records=skipped_records,
                pages_read=pages_read,
                pending_records=len(batch),
                current_url=event.url,
            )
            continue

        page = event
        if resume and page.discovered_urls:
            new_frontier_rows = []
            for discovered_url in page.discovered_urls:
                if discovered_url in frontier:
                    continue
                frontier[discovered_url] = "pending"
                discovered_urls += 1
                new_frontier_rows.append(
                    _frontier_row(
                        url=discovered_url,
                        status="pending",
                        source_url=page.response.url,
                    )
                )
            if new_frontier_rows:
                await _merge_frontier_rows(
                    room=room,
                    table=frontier_table_name,
                    rows=new_frontier_rows,
                    namespace=namespace,
                    branch=branch,
                )
                await _report_progress(
                    progress,
                    stage="frontier_discovered",
                    matched_records=matched_records,
                    imported_records=imported_records,
                    skipped_records=skipped_records,
                    pages_read=pages_read,
                    pending_records=len(batch),
                    current_url=page.response.url,
                )

        matched_records += 1
        pages_read += 1
        await _report_progress(
            progress,
            stage="page_scraped",
            matched_records=matched_records,
            imported_records=imported_records,
            skipped_records=skipped_records,
            pages_read=pages_read,
            pending_records=len(batch),
            current_url=page.response.url,
        )
        try:
            extracted = await extractor(page.response, page.content)
            if extracted is None:
                skipped_records += 1
                if resume:
                    frontier[page.response.url] = "skipped"
                    await _merge_frontier_rows(
                        room=room,
                        table=frontier_table_name,
                        rows=[
                            _frontier_row(
                                url=page.response.url,
                                status="skipped",
                            )
                        ],
                        namespace=namespace,
                        branch=branch,
                    )
                await _report_progress(
                    progress,
                    stage="record_skipped",
                    matched_records=matched_records,
                    imported_records=imported_records,
                    skipped_records=skipped_records,
                    pages_read=pages_read,
                    pending_records=len(batch),
                    current_url=page.response.url,
                )
                continue

            row = dict(extracted)
            if primary_key not in row:
                raise ValueError(
                    f"extract callback must return primary key column {primary_key!r}"
                )
            batch.append(row)
            batch_urls.append(page.response.url)
            await _report_progress(
                progress,
                stage="record_extracted",
                matched_records=matched_records,
                imported_records=imported_records,
                skipped_records=skipped_records,
                pages_read=pages_read,
                pending_records=len(batch),
                current_url=page.response.url,
            )
            if len(batch) >= batch_size:
                merged_urls = batch_urls
                schema = await _merge_batch(
                    room=room,
                    table=table,
                    rows=batch,
                    schema=schema,
                    primary_key=primary_key,
                    namespace=namespace,
                    branch=branch,
                )
                imported_records += len(batch)
                batch = []
                batch_urls = []
                if resume:
                    for merged_url in merged_urls:
                        frontier[merged_url] = "done"
                    await _merge_frontier_rows(
                        room=room,
                        table=frontier_table_name,
                        rows=[
                            _frontier_row(url=merged_url, status="done")
                            for merged_url in merged_urls
                        ],
                        namespace=namespace,
                        branch=branch,
                    )
                await _report_progress(
                    progress,
                    stage="batch_merged",
                    matched_records=matched_records,
                    imported_records=imported_records,
                    skipped_records=skipped_records,
                    pages_read=pages_read,
                    pending_records=len(batch),
                    current_url=page.response.url,
                )
        except Exception:
            logger.exception("failed to import Scrapy response %s", page.response.url)
            raise

    if batch:
        merged_urls = batch_urls
        await _merge_batch(
            room=room,
            table=table,
            rows=batch,
            schema=schema,
            primary_key=primary_key,
            namespace=namespace,
            branch=branch,
        )
        imported_records += len(batch)
        batch = []
        batch_urls = []
        if resume:
            for merged_url in merged_urls:
                frontier[merged_url] = "done"
            await _merge_frontier_rows(
                room=room,
                table=frontier_table_name,
                rows=[
                    _frontier_row(url=merged_url, status="done")
                    for merged_url in merged_urls
                ],
                namespace=namespace,
                branch=branch,
            )
        await _report_progress(
            progress,
            stage="batch_merged",
            matched_records=matched_records,
            imported_records=imported_records,
            skipped_records=skipped_records,
            pages_read=pages_read,
            pending_records=len(batch),
        )

    result = ScrapyImportResult(
        matched_records=matched_records,
        imported_records=imported_records,
        skipped_records=skipped_records,
        pages_read=pages_read,
        discovered_urls=discovered_urls,
        failed_urls=failed_urls,
    )
    await _report_progress(
        progress,
        stage="completed",
        matched_records=result.matched_records,
        imported_records=result.imported_records,
        skipped_records=result.skipped_records,
        pages_read=result.pages_read,
        pending_records=0,
    )
    return result


spider_domain_to_dataset = import_domain_with_scrapy


async def _iter_scraped_pages(
    *,
    start_urls: Sequence[str],
    domain: str | None,
    url_filter: str | Sequence[str] | None,
    limit: int | None,
    user_agent: str | None,
    respect_robots_txt: bool,
    known_urls: set[str],
) -> AsyncIterator[_ScrapedEvent]:
    if limit == 0 or len(start_urls) == 0:
        return

    start_url = start_urls[0]
    allowed_domain = _domain(start_url if domain is None else domain)
    filters = _compiled_url_filters(url_filter)
    queue: asyncio.Queue[_ScrapedEvent | None] = asyncio.Queue()
    spider_settings: dict[str, Any] = {
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": respect_robots_txt,
        "TELNETCONSOLE_ENABLED": False,
    }
    if user_agent is not None:
        spider_settings["USER_AGENT"] = user_agent

    class _MeshagentSpider(Spider):
        name = "meshagent_scrapy_domain"
        allowed_domains = [allowed_domain]
        custom_settings = spider_settings

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._link_extractor = LinkExtractor()
            self._sent_pages = 0
            self._seen_urls = set(known_urls)

        async def start(self) -> Any:
            for request_url in start_urls:
                self._seen_urls.add(request_url)
                yield Request(request_url, callback=self.parse, errback=self._errback)

        def parse(self, response: Response) -> Any:
            discovered_urls = []
            for link in self._link_extractor.extract_links(response):
                if _domain(link.url) != allowed_domain:
                    continue
                if not _matches_filters(link.url, filters):
                    continue
                if link.url in self._seen_urls:
                    continue
                self._seen_urls.add(link.url)
                discovered_urls.append(link.url)

            if _matches_filters(response.url, filters):
                self._sent_pages += 1
                queue.put_nowait(
                    _ScrapedPage(
                        response=response,
                        content=response.body,
                        discovered_urls=tuple(discovered_urls),
                    )
                )
                if limit is not None and self._sent_pages >= limit:
                    raise CloseSpider("limit reached")

            for discovered_url in discovered_urls:
                yield Request(
                    discovered_url,
                    callback=self.parse,
                    errback=self._errback,
                )

        def _errback(self, failure: Any) -> None:
            request = failure.request
            queue.put_nowait(
                _ScrapedFailure(
                    url=request.url,
                    error=str(failure.value),
                )
            )

    runner = AsyncCrawlerRunner({"TWISTED_REACTOR_ENABLED": False})
    crawl_future = runner.crawl(_MeshagentSpider)
    while True:
        if crawl_future.done() and queue.empty():
            await crawl_future
            break
        try:
            page = await asyncio.wait_for(queue.get(), timeout=0.1)
        except TimeoutError:
            continue
        if page is None:
            break
        yield page


async def _merge_batch(
    *,
    room: RoomClient,
    table: str,
    rows: list[dict[str, Any]],
    schema: pa.Schema | None,
    primary_key: str,
    namespace: list[str] | None,
    branch: str | None,
) -> pa.Schema:
    next_schema = _merge_schema(
        base=schema,
        rows=rows,
        primary_key=primary_key,
    )
    await _ensure_table(
        room=room,
        table=table,
        schema=next_schema,
        namespace=namespace,
        branch=branch,
    )
    await room.datasets.merge(
        table=table,
        on=primary_key,
        records=pa.Table.from_pylist(rows, schema=next_schema),
        namespace=namespace,
        branch=branch,
    )
    return next_schema


async def _ensure_table(
    *,
    room: RoomClient,
    table: str,
    schema: pa.Schema,
    namespace: list[str] | None,
    branch: str | None,
) -> None:
    await room.datasets.create_table_with_schema(
        name=table,
        schema=schema,
        mode="create_if_not_exists",
        namespace=namespace,
        branch=branch,
    )
    existing_schema = await room.datasets.inspect(
        table=table,
        namespace=namespace,
        branch=branch,
    )
    existing_names = set(existing_schema.names)
    missing_fields = {
        field.name: field for field in schema if field.name not in existing_names
    }
    if missing_fields:
        await room.datasets.add_columns(
            table=table,
            new_columns=missing_fields,
            namespace=namespace,
            branch=branch,
        )


async def _ensure_frontier_table(
    *,
    room: RoomClient,
    table: str,
    namespace: list[str] | None,
    branch: str | None,
) -> None:
    await _ensure_table(
        room=room,
        table=table,
        schema=_frontier_schema(),
        namespace=namespace,
        branch=branch,
    )


async def _load_frontier(
    *,
    room: RoomClient,
    table: str,
    namespace: list[str] | None,
    branch: str | None,
) -> dict[str, str]:
    frontier: dict[str, str] = {}
    async for batch in room.datasets.search_stream(
        table=table,
        select=["url", "status"],
        namespace=namespace,
        branch=branch,
    ):
        for row in batch.to_pylist():
            url = row["url"]
            status = row["status"]
            if isinstance(url, str) and isinstance(status, str):
                frontier[url] = status
    return frontier


async def _merge_frontier_rows(
    *,
    room: RoomClient,
    table: str,
    rows: list[dict[str, Any]],
    namespace: list[str] | None,
    branch: str | None,
) -> None:
    if not rows:
        return
    await _ensure_frontier_table(
        room=room,
        table=table,
        namespace=namespace,
        branch=branch,
    )
    await room.datasets.merge(
        table=table,
        on="url",
        records=pa.Table.from_pylist(rows, schema=_frontier_schema()),
        namespace=namespace,
        branch=branch,
    )


def _frontier_row(
    *,
    url: str,
    status: str,
    source_url: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "url": url,
        "status": status,
        "discovered_at": now,
        "updated_at": now,
        "source_url": source_url,
        "error": error,
    }


def _frontier_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("url", pa.string(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("discovered_at", pa.string()),
            pa.field("updated_at", pa.string()),
            pa.field("source_url", pa.string()),
            pa.field("error", pa.string()),
        ]
    )


async def _report_progress(
    progress: ProgressCallback | None,
    *,
    stage: str,
    matched_records: int,
    imported_records: int,
    skipped_records: int,
    pages_read: int,
    pending_records: int,
    current_url: str | None = None,
) -> None:
    if progress is None:
        return
    await progress(
        ScrapyImportProgress(
            stage=stage,
            matched_records=matched_records,
            imported_records=imported_records,
            skipped_records=skipped_records,
            pages_read=pages_read,
            pending_records=pending_records,
            current_url=current_url,
        )
    )


async def _default_extract(response: Response, content: bytes) -> dict[str, str]:
    content_type = _content_type(response)
    return {
        "url": response.url,
        "date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "content_type": content_type,
        "text": _content_text(content=content, content_type=content_type),
    }


def _content_type(response: Response) -> str:
    raw = response.headers.get(b"Content-Type")
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("latin-1", errors="replace")
    return str(raw)


def _content_text(*, content: bytes, content_type: str) -> str:
    decoded = content.decode(_charset(content_type), errors="replace")
    normalized_content_type = content_type.lower()
    if "html" not in normalized_content_type and "xml" not in normalized_content_type:
        return decoded

    parser = _TextExtractor()
    parser.feed(decoded)
    parser.close()
    return parser.text()


def _charset(content_type: str) -> str:
    for part in content_type.split(";"):
        key, separator, value = part.partition("=")
        if separator and key.strip().lower() == "charset" and value.strip() != "":
            return value.strip()
    return "utf-8"


def _default_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("url", pa.string(), nullable=False),
            pa.field("date", pa.string()),
            pa.field("content_type", pa.string()),
            pa.field("text", pa.string()),
        ]
    )


def _merge_schema(
    *,
    base: pa.Schema | None,
    rows: list[dict[str, Any]],
    primary_key: str,
) -> pa.Schema:
    inferred = pa.Table.from_pylist(rows).schema
    fields_by_name = {field.name: field for field in base} if base is not None else {}
    for field in inferred:
        if pa.types.is_null(field.type):
            field = pa.field(field.name, pa.string(), nullable=field.nullable)
        if field.name == primary_key:
            field = pa.field(field.name, field.type, nullable=False)
        fields_by_name.setdefault(field.name, field)

    if primary_key not in fields_by_name:
        raise ValueError(f"rows must include primary key column {primary_key!r}")
    return pa.schema(list(fields_by_name.values()))


def _start_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return value
    if parsed.scheme or parsed.netloc:
        raise ValueError("url must be an HTTP(S) URL or domain")
    host = parsed.path.split("/", maxsplit=1)[0]
    if host == "":
        raise ValueError("url must be non-empty")
    return f"https://{value}"


def _domain(value: str) -> str:
    parsed = urlparse(value)
    host = parsed.netloc or parsed.path.split("/", maxsplit=1)[0]
    host = host.split("@")[-1].split(":", maxsplit=1)[0].strip().strip(".").lower()
    if host == "":
        raise ValueError("domain must be non-empty")
    return host


def _url_filters(url_filter: str | Sequence[str] | None) -> list[str]:
    if url_filter is None:
        return []
    if isinstance(url_filter, str):
        return [url_filter]
    return list(url_filter)


def _compiled_url_filters(
    url_filter: str | Sequence[str] | None,
) -> list[re.Pattern[str]]:
    return [re.compile(value) for value in _url_filters(url_filter)]


def _matches_filters(url: str, filters: Sequence[re.Pattern[str]]) -> bool:
    if not filters:
        return True
    return any(pattern.search(url) is not None for pattern in filters)
