from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import asyncio
import logging
import re
import warnings
from typing import Any, Literal, TypeAlias, TypeVar
from urllib.parse import urlparse, urlunparse

from html_to_markdown import convert as html_to_markdown
import jmespath
import pyarrow as pa
from trafilatura import extract as trafilatura_extract

from meshagent.api import (
    DatasetIndexConfig,
    DatasetOptimizeConfig,
    LANCE_ZSTD_FIELD_METADATA,
    RoomClient,
    RoomException,
)

from scrapy import Request, Spider
from scrapy.crawler import AsyncCrawlerRunner
from scrapy.exceptions import CloseSpider
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots

logger = logging.getLogger(__name__)
_FAILURE_HTTP_STATUS_CODES = tuple(range(400, 600))
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
_DEFAULT_TEXT_RESPONSE_FILTER = (
    "contains(content_type_lower, 'text/') || "
    "contains(content_type_lower, 'html') || "
    "contains(content_type_lower, 'xml') || "
    "contains(content_type_lower, 'json')"
)
_DEFAULT_MAX_BATCH_BYTES = 16 * 1024 * 1024
_DEFAULT_BATCH_SIZE = 100
_DEFAULT_MAX_BATCH_DELAY = 60
_ROOM_WRITE_RETRIES = 3
_ROOM_WRITE_RETRY_BASE_DELAY_SECONDS = 1.0

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
ContentFormat: TypeAlias = Literal["md", "html", "text"]
StripKind: TypeAlias = Literal[
    "scripts", "css", "whitespace", "clean", "image-data-urls"
]
StripOrder: TypeAlias = Literal["before-links", "after-links"]
CleanMode: TypeAlias = Literal["before-links", "after-links", "none"]
StripInput: TypeAlias = str | Sequence[StripKind] | None
IndexColumn: TypeAlias = Literal["text"]
IndexInput: TypeAlias = Sequence[IndexColumn] | None
_STRIP_KINDS = frozenset({"scripts", "css", "whitespace", "clean", "image-data-urls"})
_INDEX_COLUMNS = frozenset({"text"})
_T = TypeVar("_T")


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
    request_url: str
    redirect_urls: tuple[str, ...] = ()
    discovered_urls: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ScrapedFailure:
    url: str
    error: str
    failure_type: str | None = None
    http_status: int | None = None
    final_url: str | None = None
    content_type: str | None = None


@dataclass(frozen=True)
class _ScrapedDiscovery:
    source_url: str
    discovered_urls: tuple[str, ...]


_ScrapedEvent: TypeAlias = _ScrapedPage | _ScrapedFailure | _ScrapedDiscovery


@dataclass(frozen=True)
class _FrontierState:
    statuses: dict[str, str]
    aliases: dict[str, dict[str, str]]


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


class _HtmlAssetExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.images: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "img":
            image = self._image_record(attrs)
            if image is not None:
                self.images.append(image)

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "img":
            image = self._image_record(attrs)
            if image is not None:
                self.images.append(image)

    def _image_record(
        self, attrs: list[tuple[str, str | None]]
    ) -> dict[str, Any] | None:
        src = _attribute_value(attrs, "src")
        if src is None or src.strip() == "" or _is_data_url(src):
            return None
        return {
            "src": src,
            "alt": _attribute_value(attrs, "alt"),
        }


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
    response_filter: str | None = None,
    content_format: ContentFormat = "md",
    strip: StripInput = None,
    strip_order: StripOrder = "before-links",
    clean: CleanMode | None = None,
    namespace: list[str] | None = None,
    branch: str | None = None,
    limit: int | None = None,
    concurrency: int | None = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    max_batch_bytes: int | None = _DEFAULT_MAX_BATCH_BYTES,
    max_batch_delay: float | None = _DEFAULT_MAX_BATCH_DELAY,
    frontier_batch_size: int = 500,
    user_agent: str | None = None,
    respect_robots_txt: bool = False,
    include_sitemap: bool = False,
    resume: bool = False,
    retry_failed: bool = False,
    frontier_table: str | None = None,
    create_indexes: bool = True,
    index_columns: IndexInput = None,
    optimize_every: int | None = 5000,
    progress: ProgressCallback | None = None,
) -> ScrapyImportResult:
    """Spider a domain with Scrapy and import pages into a room dataset.

    The default extractor writes `url`, `date`, `content_type`, and `text`,
    merging on `url`. Pass `schema` when a custom extractor should create an
    empty table before the first row. Pass `include_sitemap=True` to seed the
    crawl from robots.txt sitemap entries and `/sitemap.xml`. Pass `progress` to
    receive async progress updates during the import.
    """

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if max_batch_bytes is not None and max_batch_bytes <= 0:
        raise ValueError("max_batch_bytes must be greater than zero or None")
    if max_batch_delay is not None and max_batch_delay <= 0:
        raise ValueError("max_batch_delay must be greater than zero or None")
    if frontier_batch_size <= 0:
        raise ValueError("frontier_batch_size must be greater than zero")
    if primary_key == "":
        raise ValueError("primary_key must be non-empty")
    if limit is not None and limit < 0:
        raise ValueError("limit must be greater than or equal to zero")
    if concurrency is not None and concurrency <= 0:
        raise ValueError("concurrency must be greater than zero")
    if optimize_every is not None and optimize_every <= 0:
        raise ValueError("optimize_every must be greater than zero or None")
    if content_format not in {"md", "html", "text"}:
        raise ValueError("content_format must be 'md', 'html', or 'text'")
    strip_kinds = _resolve_strip_kinds(
        content_format=content_format,
        strip=strip,
        clean=clean,
    )
    strip_order = _resolve_strip_order(strip_order=strip_order, clean=clean)
    resolved_index_columns = _resolve_index_columns(index_columns)

    extractor = extract or _default_extractor(
        content_format=content_format,
        strip=strip_kinds,
        strip_order=strip_order,
    )
    response_filter_expression = jmespath.compile(
        response_filter or _DEFAULT_TEXT_RESPONSE_FILTER
    )
    schema = schema or (_default_schema() if extract is None else None)
    frontier_table_name = frontier_table or f"{table}__frontier"
    content_indexes_ready = not create_indexes
    frontier_indexes_ready = not create_indexes
    initial_content_table_available = False
    if schema is not None:
        initial_schema = schema
        initial_content_table_available = await _retry_room_write(
            description=f"ensure content table {table}",
            current_url=None,
            operation=lambda: _ensure_table(
                room=room,
                table=table,
                schema=initial_schema,
                namespace=namespace,
                branch=branch,
            ),
        )
        if create_indexes and initial_content_table_available:
            initial_content_indexes_ready = await _retry_room_write_value(
                description=f"ensure indexes for {table}",
                current_url=None,
                operation=lambda: _ensure_scrapy_indexes(
                    room=room,
                    table=table,
                    primary_key=primary_key,
                    schema=initial_schema,
                    index_columns=resolved_index_columns,
                    namespace=namespace,
                    branch=branch,
                ),
            )
            content_indexes_ready = bool(initial_content_indexes_ready)

    start_url = _frontier_url(_start_url(url))
    frontier: dict[str, str] = {}
    frontier_aliases: dict[str, dict[str, str]] = {}
    start_urls = [start_url]
    if resume:
        await _report_progress(
            progress,
            stage="loading_frontier",
            matched_records=0,
            imported_records=0,
            skipped_records=0,
            pages_read=0,
            pending_records=0,
        )
        frontier_table_available = await _retry_room_write(
            description=f"ensure frontier table {frontier_table_name}",
            current_url=None,
            operation=lambda: _ensure_frontier_table(
                room=room,
                table=frontier_table_name,
                namespace=namespace,
                branch=branch,
            ),
        )
        if create_indexes and frontier_table_available:
            loaded_frontier_indexes_ready = await _retry_room_write_value(
                description=f"ensure indexes for {frontier_table_name}",
                current_url=None,
                operation=lambda: _ensure_frontier_indexes(
                    room=room,
                    table=frontier_table_name,
                    namespace=namespace,
                    branch=branch,
                ),
            )
            frontier_indexes_ready = bool(loaded_frontier_indexes_ready)
        frontier_state = await _load_frontier_state(
            room=room,
            table=frontier_table_name,
            namespace=namespace,
            branch=branch,
        )
        frontier = frontier_state.statuses
        frontier_aliases = frontier_state.aliases
        await _reconcile_frontier_aliases(
            room=room,
            table=frontier_table_name,
            state=frontier_state,
            namespace=namespace,
            branch=branch,
        )
        if start_url not in frontier:
            start_row_merged = await _retry_room_write(
                description=f"merge start frontier row into {frontier_table_name}",
                current_url=start_url,
                operation=lambda: _merge_frontier_rows(
                    room=room,
                    table=frontier_table_name,
                    rows=[_frontier_row(url=start_url, status="pending")],
                    namespace=namespace,
                    branch=branch,
                    ensure=False,
                ),
            )
            if start_row_merged:
                frontier[start_url] = "pending"
                frontier_aliases.setdefault(start_url, {})[start_url] = "pending"
            if create_indexes and not frontier_indexes_ready and start_row_merged:
                start_frontier_indexes_ready = await _retry_room_write_value(
                    description=f"ensure indexes for {frontier_table_name}",
                    current_url=start_url,
                    operation=lambda: _ensure_frontier_indexes(
                        room=room,
                        table=frontier_table_name,
                        namespace=namespace,
                        branch=branch,
                    ),
                )
                frontier_indexes_ready = bool(start_frontier_indexes_ready)
        resume_filters = _compiled_url_filters(url_filter)
        start_urls = [
            pending_url
            for pending_url, status in frontier.items()
            if status == "pending" or (retry_failed and status == "failed")
            if _matches_filters(pending_url, resume_filters)
        ]
        if limit is not None:
            start_urls = start_urls[:limit]
        await _report_progress(
            progress,
            stage="frontier_loaded",
            matched_records=0,
            imported_records=0,
            skipped_records=0,
            pages_read=0,
            pending_records=len(start_urls),
        )

    matched_records = 0
    imported_records = 0
    skipped_records = 0
    pages_read = 0
    discovered_urls = 0
    failed_urls = 0
    batch: list[dict[str, Any]] = []
    batch_frontier_urls: list[str] = []
    batch_estimated_bytes = 0
    content_batch_lock = asyncio.Lock()
    content_batch_delay_task: asyncio.Task[None] | None = None
    frontier_batch: list[dict[str, Any]] = []
    unresolved_start_urls = set(start_urls) if resume else set()
    writes_since_optimize = 0
    content_table_available = initial_content_table_available

    async def maybe_optimize(current_url: str | None = None) -> None:
        nonlocal writes_since_optimize
        if optimize_every is None or writes_since_optimize < optimize_every:
            return
        tables = [table] if content_table_available else []
        if resume:
            tables.append(frontier_table_name)
        if not tables:
            return
        await _report_progress(
            progress,
            stage="optimizing",
            matched_records=matched_records,
            imported_records=imported_records,
            skipped_records=skipped_records,
            pages_read=pages_read,
            pending_records=len(batch),
            current_url=current_url,
        )
        optimized = await _retry_room_write(
            description="optimize Scrapy dataset tables",
            current_url=current_url,
            operation=lambda: _optimize_tables(
                room=room,
                tables=tables,
                namespace=namespace,
                branch=branch,
            ),
        )
        if not optimized:
            writes_since_optimize = 0
            return
        writes_since_optimize = 0
        await _report_progress(
            progress,
            stage="optimized",
            matched_records=matched_records,
            imported_records=imported_records,
            skipped_records=skipped_records,
            pages_read=pages_read,
            pending_records=len(batch),
            current_url=current_url,
        )

    async def flush_frontier_batch() -> None:
        nonlocal frontier_batch, frontier_indexes_ready, writes_since_optimize
        if not resume or not frontier_batch:
            return
        rows = frontier_batch
        frontier_batch = []
        merged = await _retry_room_write(
            description=f"merge frontier rows into {frontier_table_name}",
            current_url=None,
            operation=lambda: _merge_frontier_rows(
                room=room,
                table=frontier_table_name,
                rows=rows,
                namespace=namespace,
                branch=branch,
                ensure=False,
            ),
        )
        if not merged:
            return
        if create_indexes and not frontier_indexes_ready:
            merged_frontier_indexes_ready = await _retry_room_write_value(
                description=f"ensure indexes for {frontier_table_name}",
                current_url=None,
                operation=lambda: _ensure_frontier_indexes(
                    room=room,
                    table=frontier_table_name,
                    namespace=namespace,
                    branch=branch,
                ),
            )
            frontier_indexes_ready = bool(merged_frontier_indexes_ready)
        writes_since_optimize += len(rows)
        await maybe_optimize()

    async def append_frontier_rows(rows: list[dict[str, Any]]) -> None:
        if not resume or not rows:
            return
        frontier_batch.extend(rows)
        if len(frontier_batch) >= frontier_batch_size:
            await flush_frontier_batch()

    async def cancel_content_batch_delay_task() -> None:
        nonlocal content_batch_delay_task
        task = content_batch_delay_task
        if task is None or task is asyncio.current_task():
            return
        content_batch_delay_task = None
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    def schedule_content_batch_delay_task(current_url: str | None) -> None:
        nonlocal content_batch_delay_task
        if max_batch_delay is None or content_batch_delay_task is not None:
            return

        async def delayed_flush() -> None:
            nonlocal content_batch_delay_task
            try:
                await asyncio.sleep(max_batch_delay)
                await flush_content_batch(current_url=current_url)
            finally:
                if content_batch_delay_task is asyncio.current_task():
                    content_batch_delay_task = None

        content_batch_delay_task = asyncio.create_task(delayed_flush())

    async def flush_content_batch(current_url: str | None = None) -> None:
        async with content_batch_lock:
            await flush_content_batch_unlocked(current_url=current_url)

    async def flush_content_batch_unlocked(current_url: str | None = None) -> None:
        nonlocal batch
        nonlocal batch_estimated_bytes
        nonlocal batch_frontier_urls
        nonlocal content_indexes_ready
        nonlocal content_table_available
        nonlocal failed_urls
        nonlocal frontier_indexes_ready
        nonlocal imported_records
        nonlocal schema
        nonlocal skipped_records
        nonlocal writes_since_optimize
        if not batch:
            return

        merged_urls = _unique(batch_frontier_urls)
        rows_to_merge = _dedupe_rows_by_primary_key(
            rows=batch,
            primary_key=primary_key,
        )
        next_schema = await _retry_room_write_value(
            description=f"merge content batch into {table}",
            current_url=current_url,
            operation=lambda: _merge_batch(
                room=room,
                table=table,
                rows=rows_to_merge,
                schema=schema,
                primary_key=primary_key,
                namespace=namespace,
                branch=branch,
            ),
        )
        if next_schema is None:
            skipped_records += len(rows_to_merge)
            if resume:
                failed_frontier_urls = _expand_frontier_urls(
                    merged_urls,
                    aliases=frontier_aliases,
                )
                failed_urls += len(failed_frontier_urls)
                for failed_frontier_url in failed_frontier_urls:
                    _set_frontier_alias_status(
                        frontier=frontier,
                        aliases=frontier_aliases,
                        url=failed_frontier_url,
                        status="failed",
                    )
                await _retry_room_write(
                    description=f"mark failed frontier rows in {frontier_table_name}",
                    current_url=current_url,
                    operation=lambda: _merge_frontier_rows(
                        room=room,
                        table=frontier_table_name,
                        rows=[
                            _frontier_row(
                                url=failed_frontier_url,
                                status="failed",
                                error="dataset write failed after retries",
                                failure_type="dataset_write_failed",
                            )
                            for failed_frontier_url in failed_frontier_urls
                        ],
                        namespace=namespace,
                        branch=branch,
                        ensure=False,
                    ),
                )
            batch = []
            batch_frontier_urls = []
            batch_estimated_bytes = 0
            await cancel_content_batch_delay_task()
            await _report_progress(
                progress,
                stage="batch_write_failed",
                matched_records=matched_records,
                imported_records=imported_records,
                skipped_records=skipped_records,
                pages_read=pages_read,
                pending_records=0,
                current_url=current_url,
            )
            return
        schema = next_schema
        content_table_available = True
        if create_indexes and not content_indexes_ready:
            merged_content_indexes_ready = await _retry_room_write_value(
                description=f"ensure indexes for {table}",
                current_url=current_url,
                operation=lambda: _ensure_scrapy_indexes(
                    room=room,
                    table=table,
                    primary_key=primary_key,
                    schema=schema,
                    index_columns=resolved_index_columns,
                    namespace=namespace,
                    branch=branch,
                ),
            )
            content_indexes_ready = bool(merged_content_indexes_ready)
        imported_records += len(rows_to_merge)
        writes_since_optimize += len(rows_to_merge)
        batch = []
        batch_frontier_urls = []
        batch_estimated_bytes = 0
        await cancel_content_batch_delay_task()
        if resume:
            done_urls = _expand_frontier_urls(
                merged_urls,
                aliases=frontier_aliases,
            )
            for done_url in done_urls:
                _set_frontier_alias_status(
                    frontier=frontier,
                    aliases=frontier_aliases,
                    url=done_url,
                    status="done",
                )
            await flush_frontier_batch()
            done_rows_merged = await _retry_room_write(
                description=f"mark done frontier rows in {frontier_table_name}",
                current_url=current_url,
                operation=lambda: _merge_frontier_rows(
                    room=room,
                    table=frontier_table_name,
                    rows=[
                        _frontier_row(url=done_url, status="done")
                        for done_url in done_urls
                    ],
                    namespace=namespace,
                    branch=branch,
                    ensure=False,
                ),
            )
            if create_indexes and not frontier_indexes_ready and done_rows_merged:
                done_frontier_indexes_ready = await _retry_room_write_value(
                    description=f"ensure indexes for {frontier_table_name}",
                    current_url=current_url,
                    operation=lambda: _ensure_frontier_indexes(
                        room=room,
                        table=frontier_table_name,
                        namespace=namespace,
                        branch=branch,
                    ),
                )
                frontier_indexes_ready = bool(done_frontier_indexes_ready)
            if done_rows_merged:
                writes_since_optimize += len(done_urls)
        await _report_progress(
            progress,
            stage="batch_merged",
            matched_records=matched_records,
            imported_records=imported_records,
            skipped_records=skipped_records,
            pages_read=pages_read,
            pending_records=len(batch),
            current_url=current_url,
        )
        await maybe_optimize(current_url=current_url)

    def resolve_start_urls(urls: Sequence[str]) -> None:
        for resolved_url in urls:
            unresolved_start_urls.discard(_frontier_url(resolved_url))

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
        concurrency=concurrency,
        user_agent=user_agent,
        respect_robots_txt=respect_robots_txt,
        include_sitemap=include_sitemap,
        known_urls=set(frontier) if resume else set(),
    ):
        if isinstance(event, _ScrapedFailure):
            failed_urls += 1
            if resume:
                failed_frontier_urls = _expand_frontier_urls(
                    [event.url],
                    aliases=frontier_aliases,
                )
                for failed_frontier_url in failed_frontier_urls:
                    resolve_start_urls([failed_frontier_url])
                    _set_frontier_alias_status(
                        frontier=frontier,
                        aliases=frontier_aliases,
                        url=failed_frontier_url,
                        status="failed",
                    )
                await append_frontier_rows(
                    [
                        _frontier_row(
                            url=failed_frontier_url,
                            status="failed",
                            error=event.error,
                            failure_type=event.failure_type,
                            http_status=event.http_status,
                            final_url=event.final_url,
                            content_type=event.content_type,
                        )
                        for failed_frontier_url in failed_frontier_urls
                    ]
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

        if isinstance(event, _ScrapedDiscovery):
            if resume and event.discovered_urls:
                new_frontier_rows = []
                for discovered_url in event.discovered_urls:
                    frontier_url = _frontier_url(discovered_url)
                    if frontier_url in frontier:
                        continue
                    frontier[frontier_url] = "pending"
                    frontier_aliases.setdefault(frontier_url, {})[frontier_url] = (
                        "pending"
                    )
                    discovered_urls += 1
                    new_frontier_rows.append(
                        _frontier_row(
                            url=frontier_url,
                            status="pending",
                            source_url=event.source_url,
                        )
                    )
                if new_frontier_rows:
                    await append_frontier_rows(new_frontier_rows)
                    await _report_progress(
                        progress,
                        stage="frontier_discovered",
                        matched_records=matched_records,
                        imported_records=imported_records,
                        skipped_records=skipped_records,
                        pages_read=pages_read,
                        pending_records=len(batch),
                        current_url=event.source_url,
                    )
            continue

        page = event
        if resume:
            resolve_start_urls(
                _expand_frontier_urls(
                    _page_frontier_urls(page),
                    aliases=frontier_aliases,
                )
            )
        if resume and page.discovered_urls:
            new_frontier_rows = []
            for discovered_url in page.discovered_urls:
                frontier_url = _frontier_url(discovered_url)
                if frontier_url in frontier:
                    continue
                frontier[frontier_url] = "pending"
                frontier_aliases.setdefault(frontier_url, {})[frontier_url] = "pending"
                discovered_urls += 1
                new_frontier_rows.append(
                    _frontier_row(
                        url=frontier_url,
                        status="pending",
                        source_url=page.response.url,
                    )
                )
            if new_frontier_rows:
                await append_frontier_rows(new_frontier_rows)
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
        if response_filter_expression is not None and not bool(
            response_filter_expression.search(_response_filter_document(page.response))
        ):
            skipped_records += 1
            if resume:
                skipped_urls = _expand_frontier_urls(
                    _page_frontier_urls(page),
                    aliases=frontier_aliases,
                )
                for skipped_url in skipped_urls:
                    _set_frontier_alias_status(
                        frontier=frontier,
                        aliases=frontier_aliases,
                        url=skipped_url,
                        status="skipped",
                    )
                await append_frontier_rows(
                    [
                        _frontier_row(
                            url=skipped_url,
                            status="skipped",
                        )
                        for skipped_url in skipped_urls
                    ]
                )
            await _report_progress(
                progress,
                stage="response_filtered",
                matched_records=matched_records,
                imported_records=imported_records,
                skipped_records=skipped_records,
                pages_read=pages_read,
                pending_records=len(batch),
                current_url=page.response.url,
            )
            continue
        try:
            extracted = await extractor(page.response, page.content)
            if extracted is None:
                skipped_records += 1
                if resume:
                    skipped_urls = _expand_frontier_urls(
                        _page_frontier_urls(page),
                        aliases=frontier_aliases,
                    )
                    for skipped_url in skipped_urls:
                        _set_frontier_alias_status(
                            frontier=frontier,
                            aliases=frontier_aliases,
                            url=skipped_url,
                            status="skipped",
                        )
                    await append_frontier_rows(
                        [
                            _frontier_row(
                                url=skipped_url,
                                status="skipped",
                            )
                            for skipped_url in skipped_urls
                        ]
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
            row_estimated_bytes = _estimated_record_bytes(row)
            async with content_batch_lock:
                if (
                    max_batch_bytes is not None
                    and batch
                    and batch_estimated_bytes + row_estimated_bytes > max_batch_bytes
                ):
                    await flush_content_batch_unlocked(current_url=page.response.url)
                if not batch:
                    schedule_content_batch_delay_task(current_url=page.response.url)
                batch.append(row)
                batch_frontier_urls.extend(_page_frontier_urls(page))
                batch_estimated_bytes += row_estimated_bytes
                pending_records = len(batch)
                should_flush_batch = pending_records >= batch_size
            await _report_progress(
                progress,
                stage="record_extracted",
                matched_records=matched_records,
                imported_records=imported_records,
                skipped_records=skipped_records,
                pages_read=pages_read,
                pending_records=pending_records,
                current_url=page.response.url,
            )
            if should_flush_batch:
                await flush_content_batch(current_url=page.response.url)
        except Exception:
            logger.exception("failed to import Scrapy response %s", page.response.url)
            await cancel_content_batch_delay_task()
            raise

    await flush_content_batch()
    await cancel_content_batch_delay_task()

    await flush_frontier_batch()

    if resume and unresolved_start_urls:
        rows = [
            _frontier_row(
                url=unresolved_url,
                status="failed",
                error="request finished without response callback",
                failure_type="crawler_no_callback",
            )
            for unresolved_url in sorted(unresolved_start_urls)
        ]
        for row in rows:
            _set_frontier_alias_status(
                frontier=frontier,
                aliases=frontier_aliases,
                url=row["url"],
                status="failed",
            )
        await _retry_room_write(
            description=f"mark unresolved frontier rows in {frontier_table_name}",
            current_url=None,
            operation=lambda: _merge_frontier_rows(
                room=room,
                table=frontier_table_name,
                rows=rows,
                namespace=namespace,
                branch=branch,
                ensure=False,
            ),
        )
        failed_urls += len(rows)

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
    concurrency: int | None,
    user_agent: str | None,
    respect_robots_txt: bool,
    include_sitemap: bool,
    known_urls: set[str],
) -> AsyncIterator[_ScrapedEvent]:
    if limit == 0 or len(start_urls) == 0:
        return

    start_url = start_urls[0]
    allowed_domain = _domain(start_url if domain is None else domain)
    sitemap_urls = _sitemap_seed_urls(start_url) if include_sitemap else ()
    filters = _compiled_url_filters(url_filter)
    queue: asyncio.Queue[_ScrapedEvent | None] = asyncio.Queue()
    spider_settings: dict[str, Any] = {
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": respect_robots_txt,
        "TELNETCONSOLE_ENABLED": False,
        "USER_AGENT": user_agent or _DEFAULT_USER_AGENT,
    }
    if concurrency is not None:
        spider_settings["CONCURRENT_REQUESTS"] = concurrency

    class _MeshagentSpider(Spider):
        name = "meshagent_scrapy_domain"
        allowed_domains = [allowed_domain]
        custom_settings = spider_settings

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._link_extractor = LinkExtractor()
            self._sent_pages = 0
            self._seen_urls = set(known_urls)
            self._seen_sitemap_urls: set[str] = set()

        async def start(self) -> Any:
            for sitemap_url in sitemap_urls:
                self._seen_sitemap_urls.add(sitemap_url)
                yield Request(
                    sitemap_url,
                    callback=self.parse_sitemap,
                    errback=self._errback,
                    dont_filter=True,
                    meta={"handle_httpstatus_list": _FAILURE_HTTP_STATUS_CODES},
                )
            for request_url in start_urls:
                self._seen_urls.add(request_url)
                yield Request(
                    request_url,
                    callback=self.parse,
                    errback=self._errback,
                    dont_filter=True,
                    meta={"handle_httpstatus_list": _FAILURE_HTTP_STATUS_CODES},
                )

        def parse(self, response: Response) -> list[Request]:
            if response.status >= 400:
                headers = _response_headers(response)
                queue.put_nowait(
                    _ScrapedFailure(
                        url=response.url,
                        error=f"HTTP {response.status}",
                        failure_type="http_status",
                        http_status=response.status,
                        final_url=response.url,
                        content_type=headers.get("content-type"),
                    )
                )
                return []

            discovered_urls = []
            for link in self._link_extractor.extract_links(response):
                if _domain(link.url) != allowed_domain:
                    continue
                if not _matches_filters(link.url, filters):
                    continue
                discovered_url = _frontier_url(link.url)
                if discovered_url in self._seen_urls:
                    continue
                self._seen_urls.add(discovered_url)
                discovered_urls.append(discovered_url)

            if _matches_filters(response.url, filters):
                self._sent_pages += 1
                request = response.request
                request_url = response.url if request is None else request.url
                queue.put_nowait(
                    _ScrapedPage(
                        response=response,
                        content=response.body,
                        request_url=_frontier_url(request_url),
                        redirect_urls=_redirect_frontier_urls(request),
                        discovered_urls=tuple(discovered_urls),
                    )
                )
                if limit is not None and self._sent_pages >= limit:
                    raise CloseSpider("limit reached")

            return [
                Request(
                    discovered_url,
                    callback=self.parse,
                    errback=self._errback,
                    meta={"handle_httpstatus_list": _FAILURE_HTTP_STATUS_CODES},
                )
                for discovered_url in discovered_urls
            ]

        def parse_sitemap(self, response: Response) -> list[Request]:
            if response.status >= 400:
                return []

            if response.url.endswith("/robots.txt"):
                sitemap_urls = [
                    _frontier_url(sitemap_url)
                    for sitemap_url in sitemap_urls_from_robots(
                        response.body,
                        base_url=response.url,
                    )
                    if _domain(sitemap_url) == allowed_domain
                ]
                return self._sitemap_requests(sitemap_urls)

            try:
                sitemap = Sitemap(response.body)
            except Exception:
                logger.warning("ignoring invalid sitemap: %s", response.url)
                return []

            if sitemap.type == "sitemapindex":
                sitemap_urls = [
                    _frontier_url(entry["loc"])
                    for entry in sitemap
                    if _domain(entry["loc"]) == allowed_domain
                ]
                return self._sitemap_requests(sitemap_urls)

            if sitemap.type != "urlset":
                logger.warning("ignoring invalid sitemap: %s", response.url)
                return []

            discovered_urls = []
            for entry in sitemap:
                discovered_url = _frontier_url(entry["loc"])
                if _domain(discovered_url) != allowed_domain:
                    continue
                if not _matches_filters(discovered_url, filters):
                    continue
                if discovered_url in self._seen_urls:
                    continue
                self._seen_urls.add(discovered_url)
                discovered_urls.append(discovered_url)

            if discovered_urls:
                queue.put_nowait(
                    _ScrapedDiscovery(
                        source_url=response.url,
                        discovered_urls=tuple(discovered_urls),
                    )
                )

            return [
                Request(
                    discovered_url,
                    callback=self.parse,
                    errback=self._errback,
                    meta={"handle_httpstatus_list": _FAILURE_HTTP_STATUS_CODES},
                )
                for discovered_url in discovered_urls
            ]

        def _sitemap_requests(self, sitemap_urls: Sequence[str]) -> list[Request]:
            requests = []
            for sitemap_url in sitemap_urls:
                if sitemap_url in self._seen_sitemap_urls:
                    continue
                self._seen_sitemap_urls.add(sitemap_url)
                requests.append(
                    Request(
                        sitemap_url,
                        callback=self.parse_sitemap,
                        errback=self._errback,
                        meta={"handle_httpstatus_list": _FAILURE_HTTP_STATUS_CODES},
                    )
                )
            return requests

        def _errback(self, failure: Any) -> None:
            request = failure.request
            value = failure.value
            queue.put_nowait(
                _ScrapedFailure(
                    url=request.url,
                    error=str(value),
                    failure_type=type(value).__name__,
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
    try:
        await room.datasets.create_table_with_schema(
            name=table,
            schema=schema,
            mode="create_if_not_exists",
            namespace=namespace,
            branch=branch,
        )
    except RoomException as exc:
        if not _is_existing_table_schema_conflict(table=table, error=exc):
            raise
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


def _is_existing_table_schema_conflict(*, table: str, error: RoomException) -> bool:
    message = str(error)
    return (
        f"Table '{table}' already exists with a different schema" in message
        or f"Class `{table}` already exists with a different schema" in message
    )


def _is_retryable_room_write_error(error: RoomException) -> bool:
    message = str(error).lower()
    return "room connection closed" in message or "websocket closed" in message


async def _retry_room_write(
    *,
    description: str,
    current_url: str | None,
    operation: Callable[[], Awaitable[None]],
) -> bool:
    for retry_index in range(_ROOM_WRITE_RETRIES + 1):
        try:
            await operation()
            return True
        except RoomException as exc:
            if not _is_retryable_room_write_error(exc):
                raise
            if retry_index == _ROOM_WRITE_RETRIES:
                logger.warning(
                    "failed to %s after %s retries; moving on: %s",
                    description,
                    _ROOM_WRITE_RETRIES,
                    exc,
                    extra={"current_url": current_url},
                )
                return False
            delay = _ROOM_WRITE_RETRY_BASE_DELAY_SECONDS * (2**retry_index)
            logger.warning(
                "failed to %s; retrying in %.1fs: %s",
                description,
                delay,
                exc,
                extra={"current_url": current_url},
            )
            await asyncio.sleep(delay)
    return False


async def _retry_room_write_value(
    *,
    description: str,
    current_url: str | None,
    operation: Callable[[], Awaitable[_T]],
) -> _T | None:
    for retry_index in range(_ROOM_WRITE_RETRIES + 1):
        try:
            return await operation()
        except RoomException as exc:
            if not _is_retryable_room_write_error(exc):
                raise
            if retry_index == _ROOM_WRITE_RETRIES:
                logger.warning(
                    "failed to %s after %s retries; moving on: %s",
                    description,
                    _ROOM_WRITE_RETRIES,
                    exc,
                    extra={"current_url": current_url},
                )
                return None
            delay = _ROOM_WRITE_RETRY_BASE_DELAY_SECONDS * (2**retry_index)
            logger.warning(
                "failed to %s; retrying in %.1fs: %s",
                description,
                delay,
                exc,
                extra={"current_url": current_url},
            )
            await asyncio.sleep(delay)
    return None


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


async def _ensure_scrapy_indexes(
    *,
    room: RoomClient,
    table: str,
    primary_key: str,
    schema: pa.Schema | None,
    index_columns: Sequence[IndexColumn],
    namespace: list[str] | None,
    branch: str | None,
) -> bool:
    primary_key_ready = await _ensure_index(
        room=room,
        table=table,
        column=primary_key,
        index_type="BTREE",
        name=f"meshagent_scrapy_{primary_key}_btree",
        namespace=namespace,
        branch=branch,
    )
    schema_names = set(schema.names) if schema is not None else set()
    text_ready = True
    if "text" in index_columns:
        if "text" not in schema_names:
            raise ValueError(
                "cannot create text index because schema has no text column"
            )
        text_ready = await _ensure_index(
            room=room,
            table=table,
            column="text",
            index_type="INVERTED",
            name="meshagent_scrapy_text_inverted",
            namespace=namespace,
            branch=branch,
        )
    return primary_key_ready and text_ready


async def _ensure_frontier_indexes(
    *,
    room: RoomClient,
    table: str,
    namespace: list[str] | None,
    branch: str | None,
) -> bool:
    url_index_ready = await _ensure_index(
        room=room,
        table=table,
        column="url",
        index_type="BTREE",
        name="meshagent_scrapy_frontier_url_btree",
        namespace=namespace,
        branch=branch,
    )
    status_index_ready = await _ensure_index(
        room=room,
        table=table,
        column="status",
        index_type="BITMAP",
        name="meshagent_scrapy_frontier_status_bitmap",
        namespace=namespace,
        branch=branch,
    )
    return url_index_ready and status_index_ready


async def _ensure_index(
    *,
    room: RoomClient,
    table: str,
    column: str,
    index_type: str,
    name: str,
    namespace: list[str] | None,
    branch: str | None,
) -> bool:
    try:
        indexes = await room.datasets.list_indexes(
            table=table,
            namespace=namespace,
            branch=branch,
        )
        for index in indexes:
            if index.name == name:
                return True
            if index.columns == [column] and index.type.upper() == index_type:
                return True
        await room.datasets.create_index(
            table=table,
            config=DatasetIndexConfig(
                column=column,
                index_type=index_type,  # type: ignore[arg-type]
                name=name,
                replace=True,
            ),
            namespace=namespace,
            branch=branch,
        )
        return True
    except Exception as error:
        logger.warning(
            "unable to create %s index %s on %s.%s",
            index_type,
            name,
            "::".join(namespace or []),
            table,
            exc_info=error,
        )
        return False


async def _optimize_tables(
    *,
    room: RoomClient,
    tables: Sequence[str],
    namespace: list[str] | None,
    branch: str | None,
) -> None:
    for table in tables:
        try:
            await room.datasets.optimize(
                table=table,
                namespace=namespace,
                branch=branch,
                config=DatasetOptimizeConfig(),
            )
        except Exception as error:
            logger.warning(
                "unable to optimize table %s.%s",
                "::".join(namespace or []),
                table,
                exc_info=error,
            )


async def _load_frontier(
    *,
    room: RoomClient,
    table: str,
    namespace: list[str] | None,
    branch: str | None,
) -> dict[str, str]:
    return (
        await _load_frontier_state(
            room=room,
            table=table,
            namespace=namespace,
            branch=branch,
        )
    ).statuses


async def _load_frontier_state(
    *,
    room: RoomClient,
    table: str,
    namespace: list[str] | None,
    branch: str | None,
) -> _FrontierState:
    frontier: dict[str, str] = {}
    aliases: dict[str, dict[str, str]] = {}
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
                frontier_url = _frontier_url(url)
                aliases.setdefault(frontier_url, {})[url] = status
                existing_status = frontier.get(frontier_url)
                if existing_status is None or _frontier_status_rank(
                    status
                ) > _frontier_status_rank(existing_status):
                    frontier[frontier_url] = status
    return _FrontierState(statuses=frontier, aliases=aliases)


async def _reconcile_frontier_aliases(
    *,
    room: RoomClient,
    table: str,
    state: _FrontierState,
    namespace: list[str] | None,
    branch: str | None,
) -> None:
    rows = []
    for frontier_url, status in state.statuses.items():
        if status not in {"done", "skipped"}:
            continue
        aliases = state.aliases.get(frontier_url, {})
        for alias_url, alias_status in aliases.items():
            if alias_url == frontier_url:
                continue
            if _frontier_status_rank(alias_status) >= _frontier_status_rank(status):
                continue
            rows.append(_frontier_row(url=alias_url, status=status))
            aliases[alias_url] = status
    await _retry_room_write(
        description=f"reconcile frontier aliases in {table}",
        current_url=None,
        operation=lambda: _merge_frontier_rows(
            room=room,
            table=table,
            rows=rows,
            namespace=namespace,
            branch=branch,
            ensure=False,
        ),
    )


def _expand_frontier_urls(
    urls: Sequence[str],
    *,
    aliases: dict[str, dict[str, str]],
) -> list[str]:
    expanded = []
    for url in urls:
        frontier_url = _frontier_url(url)
        expanded.append(frontier_url)
        expanded.extend(aliases.get(frontier_url, {}))
    return _unique(expanded)


def _set_frontier_alias_status(
    *,
    frontier: dict[str, str],
    aliases: dict[str, dict[str, str]],
    url: str,
    status: str,
) -> None:
    frontier_url = _frontier_url(url)
    frontier[frontier_url] = status
    aliases.setdefault(frontier_url, {})[url] = status


async def _merge_frontier_rows(
    *,
    room: RoomClient,
    table: str,
    rows: list[dict[str, Any]],
    namespace: list[str] | None,
    branch: str | None,
    ensure: bool = True,
) -> None:
    if not rows:
        return
    rows = _dedupe_rows_by_primary_key(rows=rows, primary_key="url")
    if ensure:
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
    failure_type: str | None = None,
    http_status: int | None = None,
    final_url: str | None = None,
    content_type: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "url": url,
        "status": status,
        "discovered_at": now,
        "updated_at": now,
        "source_url": source_url,
        "error": error,
        "failure_type": failure_type,
        "http_status": http_status,
        "final_url": final_url,
        "content_type": content_type,
    }


def _page_frontier_urls(page: _ScrapedPage) -> list[str]:
    return _unique(
        [
            *page.redirect_urls,
            _frontier_url(page.request_url),
            _frontier_url(page.response.url),
        ]
    )


def _redirect_frontier_urls(request: Request | None) -> tuple[str, ...]:
    if request is None:
        return ()
    redirect_urls = request.meta.get("redirect_urls")
    if not isinstance(redirect_urls, Sequence) or isinstance(redirect_urls, str):
        return ()
    return tuple(_frontier_url(url) for url in redirect_urls if isinstance(url, str))


def _response_filter_document(response: Response) -> dict[str, Any]:
    headers = _response_headers(response)
    content_type = headers.get("content-type", "")
    return {
        "url": response.url,
        "status": response.status,
        "headers": headers,
        "content_type": content_type,
        "content_type_lower": content_type.lower(),
    }


def _response_headers(response: Response) -> dict[str, str]:
    return {
        name.lower(): value
        for name, value in response.headers.to_unicode_dict().items()
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
            pa.field("failure_type", pa.string()),
            pa.field("http_status", pa.int64()),
            pa.field("final_url", pa.string()),
            pa.field("content_type", pa.string()),
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


def _default_extractor(
    *,
    content_format: ContentFormat,
    strip: Sequence[StripKind],
    strip_order: StripOrder,
) -> ExtractCallback:
    async def extract(response: Response, content: bytes) -> dict[str, Any]:
        return await _default_extract(
            response=response,
            content=content,
            content_format=content_format,
            strip=strip,
            strip_order=strip_order,
        )

    return extract


async def _default_extract(
    *,
    response: Response,
    content: bytes,
    content_format: ContentFormat,
    strip: Sequence[StripKind],
    strip_order: StripOrder,
) -> dict[str, Any]:
    content_type = _content_type(response)
    stripped_content = await _strip_content(
        url=response.url,
        content=content,
        content_type=content_type,
        strip=strip,
    )
    asset_content = stripped_content if strip_order == "before-links" else content
    assets = _content_assets(
        content=asset_content,
        content_type=content_type,
    )
    return {
        "url": response.url,
        "date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "content_type": content_type,
        "text": _content_text(
            content=stripped_content,
            content_type=content_type,
            content_format=content_format,
        ),
        "images": assets["images"],
    }


def _content_type(response: Response) -> str:
    raw = response.headers.get(b"Content-Type")
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("latin-1", errors="replace")
    return str(raw)


def _content_text(
    *,
    content: bytes,
    content_type: str,
    content_format: ContentFormat,
) -> str:
    decoded = content.decode(_charset(content_type), errors="replace")
    if not _is_html_content_type(content_type):
        return decoded
    if content_format == "html":
        return decoded
    if content_format == "md":
        return html_to_markdown(decoded)

    parser = _TextExtractor()
    parser.feed(decoded)
    parser.close()
    return parser.text()


def _content_assets(
    *,
    content: bytes,
    content_type: str,
) -> dict[str, list[Any]]:
    if not _is_html_content_type(content_type):
        return {
            "images": [],
        }

    decoded = content.decode(_charset(content_type), errors="replace")
    parser = _HtmlAssetExtractor()
    parser.feed(decoded)
    parser.close()
    return {
        "images": parser.images,
    }


def _resolve_strip_kinds(
    *,
    content_format: ContentFormat,
    strip: StripInput,
    clean: CleanMode | None,
) -> tuple[StripKind, ...]:
    if strip is not None:
        return _parse_strip_kinds(strip)
    if clean is not None:
        if clean not in {"before-links", "after-links", "none"}:
            raise ValueError("clean must be 'before-links', 'after-links', or 'none'")
        return () if clean == "none" else ("clean",)
    if content_format == "html":
        return ("scripts", "image-data-urls")
    return ("clean",)


def _resolve_strip_order(
    *,
    strip_order: StripOrder,
    clean: CleanMode | None,
) -> StripOrder:
    if clean is not None:
        if clean not in {"before-links", "after-links", "none"}:
            raise ValueError("clean must be 'before-links', 'after-links', or 'none'")
        if clean != "none":
            return clean
    if strip_order not in {"before-links", "after-links"}:
        raise ValueError("strip_order must be 'before-links' or 'after-links'")
    return strip_order


def _parse_strip_kinds(strip: StripInput) -> tuple[StripKind, ...]:
    if strip is None:
        return ()
    if isinstance(strip, str):
        raw_values = [part.strip() for part in strip.split(",")]
    else:
        raw_values = [str(part).strip() for part in strip]

    values = tuple(value for value in raw_values if value != "")
    if not values:
        return ()
    if "none" in values:
        if len(values) > 1:
            raise ValueError("strip 'none' cannot be combined with other values")
        return ()

    unknown = [value for value in values if value not in _STRIP_KINDS]
    if unknown:
        expected = ", ".join(sorted(_STRIP_KINDS | {"none"}))
        raise ValueError(f"strip must contain only {expected}")

    unique_values: list[StripKind] = []
    for value in values:
        typed_value = value  # type: ignore[assignment]
        if typed_value not in unique_values:
            unique_values.append(typed_value)
    return tuple(unique_values)


def _resolve_index_columns(index_columns: IndexInput) -> tuple[IndexColumn, ...]:
    if index_columns is None:
        return ()

    values = tuple(str(value).strip() for value in index_columns if str(value).strip())
    unknown = [value for value in values if value not in _INDEX_COLUMNS]
    if unknown:
        expected = ", ".join(sorted(_INDEX_COLUMNS))
        raise ValueError(f"index_columns must contain only {expected}")

    unique_values: list[IndexColumn] = []
    for value in values:
        typed_value = value  # type: ignore[assignment]
        if typed_value not in unique_values:
            unique_values.append(typed_value)
    return tuple(unique_values)


async def _strip_content(
    *,
    url: str,
    content: bytes,
    content_type: str,
    strip: Sequence[StripKind],
) -> bytes:
    if not strip or not _is_html_content_type(content_type):
        return content

    stripped = content
    if (
        "scripts" in strip
        or "css" in strip
        or "whitespace" in strip
        or "image-data-urls" in strip
    ):
        stripped = await asyncio.to_thread(
            _strip_html_content_sync,
            content=stripped,
            content_type=content_type,
            strip=strip,
        )
    if "clean" in strip:
        stripped = await _trafilatura_content(
            url=url,
            content=stripped,
            content_type=content_type,
        )
    return stripped


_SCRIPT_RE = re.compile(r"<script\b[^>]*>.*?</script\s*>", re.IGNORECASE | re.DOTALL)
_STYLE_RE = re.compile(r"<style\b[^>]*>.*?</style\s*>", re.IGNORECASE | re.DOTALL)
_STYLESHEET_LINK_RE = re.compile(
    r"""<link\b(?=[^>]*\brel\s*=\s*["']?stylesheet\b)[^>]*>""",
    re.IGNORECASE | re.DOTALL,
)
_STYLE_ATTR_RE = re.compile(
    r"""\s+style\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)""",
    re.IGNORECASE | re.DOTALL,
)
_IMAGE_DATA_URL_ATTR_RE = re.compile(
    r"""\s+(?:src|srcset)\s*=\s*("[^"]*\bdata:image/[^"]*"|'[^']*\bdata:image/[^']*'|[^\s>]*\bdata:image/[^\s>]*)""",
    re.IGNORECASE | re.DOTALL,
)
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
_BETWEEN_TAG_WHITESPACE_RE = re.compile(r">\s+<")
_MULTI_WHITESPACE_RE = re.compile(r"[ \t\r\n]{2,}")


def _strip_html_content_sync(
    *,
    content: bytes,
    content_type: str,
    strip: Sequence[StripKind],
) -> bytes:
    charset = _charset(content_type)
    decoded = content.decode(charset, errors="replace")
    if "scripts" in strip:
        decoded = _SCRIPT_RE.sub("", decoded)
    if "css" in strip:
        decoded = _STYLE_RE.sub("", decoded)
        decoded = _STYLESHEET_LINK_RE.sub("", decoded)
        decoded = _STYLE_ATTR_RE.sub("", decoded)
    if "image-data-urls" in strip:
        decoded = _IMAGE_DATA_URL_ATTR_RE.sub("", decoded)
    if "whitespace" in strip:
        decoded = _HTML_COMMENT_RE.sub("", decoded)
        decoded = _BETWEEN_TAG_WHITESPACE_RE.sub("><", decoded)
        decoded = _MULTI_WHITESPACE_RE.sub(" ", decoded).strip()
    return decoded.encode(charset, errors="replace")


async def _trafilatura_content(
    *,
    url: str,
    content: bytes,
    content_type: str,
) -> bytes:
    if not _is_html_content_type(content_type):
        return content
    return await asyncio.to_thread(
        _trafilatura_content_sync,
        url=url,
        content=content,
        content_type=content_type,
    )


def _trafilatura_content_sync(
    *,
    url: str,
    content: bytes,
    content_type: str,
) -> bytes:
    decoded = content.decode(_charset(content_type), errors="replace")
    try:
        extracted = trafilatura_extract(
            decoded,
            url=url,
            output_format="html",
            include_comments=False,
            include_formatting=True,
            include_images=True,
            include_links=True,
            include_tables=True,
        )
    except Exception as error:
        logger.warning("unable to extract clean content from %s", url, exc_info=error)
        return content
    if extracted is None or extracted.strip() == "":
        return content
    return extracted.encode(_charset(content_type), errors="replace")


def _is_html_content_type(content_type: str) -> bool:
    normalized_content_type = content_type.lower()
    return "html" in normalized_content_type or "xml" in normalized_content_type


def _is_data_url(value: str | None) -> bool:
    return value is not None and value.strip().lower().startswith("data:")


def _estimated_record_bytes(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, bool):
        return 1
    if isinstance(value, int | float):
        return 8
    if isinstance(value, Mapping):
        return sum(
            _estimated_record_bytes(key) + _estimated_record_bytes(item)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return sum(_estimated_record_bytes(item) for item in value)
    return len(str(value).encode("utf-8"))


def _attribute_value(attrs: Sequence[tuple[str, str | None]], name: str) -> str | None:
    normalized_name = name.lower()
    for attr_name, value in attrs:
        if attr_name.lower() == normalized_name:
            return "" if value is None else value
    return None


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
            _compressed_string_field("text"),
            pa.field("images", pa.list_(_image_struct_type())),
        ]
    )


def _compressed_string_field(name: str) -> pa.Field:
    return pa.field(name, pa.large_string(), metadata=LANCE_ZSTD_FIELD_METADATA)


def _image_struct_type() -> pa.StructType:
    return pa.struct(
        [
            _compressed_string_field("src"),
            _compressed_string_field("alt"),
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


def _dedupe_rows_by_primary_key(
    *,
    rows: list[dict[str, Any]],
    primary_key: str,
) -> list[dict[str, Any]]:
    rows_by_key = {}
    ordered_keys = []
    for row in rows:
        key = row[primary_key]
        if key not in rows_by_key:
            ordered_keys.append(key)
        rows_by_key[key] = row
    return [rows_by_key[key] for key in ordered_keys]


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


def _frontier_url(value: str) -> str:
    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    return urlunparse(
        (
            scheme,
            netloc,
            path,
            parsed.params,
            parsed.query,
            "",
        )
    )


def _sitemap_seed_urls(start_url: str) -> tuple[str, str]:
    parsed = urlparse(start_url)
    origin = urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), "", "", "", ""))
    return (f"{origin}/robots.txt", f"{origin}/sitemap.xml")


def _frontier_status_rank(status: str) -> int:
    return {
        "pending": 0,
        "failed": 1,
        "skipped": 2,
        "done": 3,
    }.get(status, -1)


def _unique(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


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
