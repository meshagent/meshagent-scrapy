from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import asyncio
import logging
import re
import warnings
from typing import Any, Literal, TypeAlias
from urllib.parse import urljoin, urlparse, urlunparse

from html_to_markdown import convert as html_to_markdown
import jmespath
import pyarrow as pa
from trafilatura import extract as trafilatura_extract

from meshagent.api import (
    DatasetIndexConfig,
    DatasetOptimizeConfig,
    RoomClient,
    RoomException,
)

from scrapy import Request, Spider
from scrapy.crawler import AsyncCrawlerRunner
from scrapy.exceptions import CloseSpider
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor

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
_DEFAULT_BATCH_SIZE = 5000

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
CleanMode: TypeAlias = Literal["before-links", "after-links", "none"]


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


_ScrapedEvent: TypeAlias = _ScrapedPage | _ScrapedFailure


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
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self._base_url = base_url
        self._active_links: list[dict[str, Any]] = []
        self.links: list[dict[str, Any]] = []
        self.images: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "a":
            self._active_links.append(self._link_record(attrs))
        elif normalized_tag == "img":
            self.images.append(self._image_record(attrs))

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "a":
            link = self._link_record(attrs)
            link.pop("_parts")
            link["content"] = ""
            self.links.append(link)
        elif normalized_tag == "img":
            self.images.append(self._image_record(attrs))

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._active_links:
            return
        link = self._active_links.pop()
        link["content"] = " ".join(link.pop("_parts")).strip()
        self.links.append(link)

    def handle_data(self, data: str) -> None:
        stripped = data.strip()
        if not stripped:
            return
        for link in self._active_links:
            link["_parts"].append(stripped)

    def close(self) -> None:
        super().close()
        while self._active_links:
            link = self._active_links.pop()
            link["content"] = " ".join(link.pop("_parts")).strip()
            self.links.append(link)

    def _link_record(self, attrs: list[tuple[str, str | None]]) -> dict[str, Any]:
        attr_list = _attribute_list(attrs)
        href = _attribute_value(attrs, "href")
        return {
            "url": _absolute_url(self._base_url, href),
            "href": href,
            "content": "",
            "title": _attribute_value(attrs, "title"),
            "rel": _attribute_value(attrs, "rel"),
            "target": _attribute_value(attrs, "target"),
            "attributes": attr_list,
            "_parts": [],
        }

    def _image_record(self, attrs: list[tuple[str, str | None]]) -> dict[str, Any]:
        attr_list = _attribute_list(attrs)
        src = _attribute_value(attrs, "src")
        return {
            "url": _absolute_url(self._base_url, src),
            "src": src,
            "alt": _attribute_value(attrs, "alt"),
            "title": _attribute_value(attrs, "title"),
            "srcset": _attribute_value(attrs, "srcset"),
            "attributes": attr_list,
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
    clean: CleanMode = "before-links",
    namespace: list[str] | None = None,
    branch: str | None = None,
    limit: int | None = None,
    concurrency: int | None = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    max_batch_bytes: int | None = _DEFAULT_MAX_BATCH_BYTES,
    frontier_batch_size: int = 500,
    user_agent: str | None = None,
    respect_robots_txt: bool = False,
    resume: bool = False,
    retry_failed: bool = False,
    frontier_table: str | None = None,
    create_indexes: bool = True,
    optimize_every: int | None = 5000,
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
    if max_batch_bytes is not None and max_batch_bytes <= 0:
        raise ValueError("max_batch_bytes must be greater than zero or None")
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
    if clean not in {"before-links", "after-links", "none"}:
        raise ValueError("clean must be 'before-links', 'after-links', or 'none'")

    extractor = extract or _default_extractor(
        content_format=content_format,
        clean=clean,
    )
    response_filter_expression = jmespath.compile(
        response_filter or _DEFAULT_TEXT_RESPONSE_FILTER
    )
    schema = schema or (_default_schema() if extract is None else None)
    frontier_table_name = frontier_table or f"{table}__frontier"
    content_indexes_ready = not create_indexes
    frontier_indexes_ready = not create_indexes
    if schema is not None:
        await _ensure_table(
            room=room,
            table=table,
            schema=schema,
            namespace=namespace,
            branch=branch,
        )
        if create_indexes:
            content_indexes_ready = await _ensure_scrapy_indexes(
                room=room,
                table=table,
                primary_key=primary_key,
                schema=schema,
                namespace=namespace,
                branch=branch,
            )

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
        await _ensure_frontier_table(
            room=room,
            table=frontier_table_name,
            namespace=namespace,
            branch=branch,
        )
        if create_indexes:
            frontier_indexes_ready = await _ensure_frontier_indexes(
                room=room,
                table=frontier_table_name,
                namespace=namespace,
                branch=branch,
            )
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
            await _merge_frontier_rows(
                room=room,
                table=frontier_table_name,
                rows=[_frontier_row(url=start_url, status="pending")],
                namespace=namespace,
                branch=branch,
                ensure=False,
            )
            frontier[start_url] = "pending"
            frontier_aliases.setdefault(start_url, {})[start_url] = "pending"
            if create_indexes and not frontier_indexes_ready:
                frontier_indexes_ready = await _ensure_frontier_indexes(
                    room=room,
                    table=frontier_table_name,
                    namespace=namespace,
                    branch=branch,
                )
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
    frontier_batch: list[dict[str, Any]] = []
    unresolved_start_urls = set(start_urls) if resume else set()
    writes_since_optimize = 0
    content_table_available = schema is not None

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
        await _optimize_tables(
            room=room,
            tables=tables,
            namespace=namespace,
            branch=branch,
        )
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
        await _merge_frontier_rows(
            room=room,
            table=frontier_table_name,
            rows=rows,
            namespace=namespace,
            branch=branch,
            ensure=False,
        )
        if create_indexes and not frontier_indexes_ready:
            frontier_indexes_ready = await _ensure_frontier_indexes(
                room=room,
                table=frontier_table_name,
                namespace=namespace,
                branch=branch,
            )
        writes_since_optimize += len(rows)
        await maybe_optimize()

    async def append_frontier_rows(rows: list[dict[str, Any]]) -> None:
        if not resume or not rows:
            return
        frontier_batch.extend(rows)
        if len(frontier_batch) >= frontier_batch_size:
            await flush_frontier_batch()

    async def flush_content_batch(current_url: str | None = None) -> None:
        nonlocal batch
        nonlocal batch_estimated_bytes
        nonlocal batch_frontier_urls
        nonlocal content_indexes_ready
        nonlocal content_table_available
        nonlocal frontier_indexes_ready
        nonlocal imported_records
        nonlocal schema
        nonlocal writes_since_optimize
        if not batch:
            return

        merged_urls = _unique(batch_frontier_urls)
        rows_to_merge = _dedupe_rows_by_primary_key(
            rows=batch,
            primary_key=primary_key,
        )
        schema = await _merge_batch(
            room=room,
            table=table,
            rows=rows_to_merge,
            schema=schema,
            primary_key=primary_key,
            namespace=namespace,
            branch=branch,
        )
        content_table_available = True
        if create_indexes and not content_indexes_ready:
            content_indexes_ready = await _ensure_scrapy_indexes(
                room=room,
                table=table,
                primary_key=primary_key,
                schema=schema,
                namespace=namespace,
                branch=branch,
            )
        imported_records += len(rows_to_merge)
        writes_since_optimize += len(rows_to_merge)
        batch = []
        batch_frontier_urls = []
        batch_estimated_bytes = 0
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
            await _merge_frontier_rows(
                room=room,
                table=frontier_table_name,
                rows=[
                    _frontier_row(url=done_url, status="done") for done_url in done_urls
                ],
                namespace=namespace,
                branch=branch,
                ensure=False,
            )
            if create_indexes and not frontier_indexes_ready:
                frontier_indexes_ready = await _ensure_frontier_indexes(
                    room=room,
                    table=frontier_table_name,
                    namespace=namespace,
                    branch=branch,
                )
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
            if (
                max_batch_bytes is not None
                and batch
                and batch_estimated_bytes + row_estimated_bytes > max_batch_bytes
            ):
                await flush_content_batch(current_url=page.response.url)
            batch.append(row)
            batch_frontier_urls.extend(_page_frontier_urls(page))
            batch_estimated_bytes += row_estimated_bytes
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
                await flush_content_batch(current_url=page.response.url)
        except Exception:
            logger.exception("failed to import Scrapy response %s", page.response.url)
            raise

    await flush_content_batch()

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
        await _merge_frontier_rows(
            room=room,
            table=frontier_table_name,
            rows=rows,
            namespace=namespace,
            branch=branch,
            ensure=False,
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
    known_urls: set[str],
) -> AsyncIterator[_ScrapedEvent]:
    if limit == 0 or len(start_urls) == 0:
        return

    start_url = start_urls[0]
    allowed_domain = _domain(start_url if domain is None else domain)
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

        async def start(self) -> Any:
            for request_url in start_urls:
                self._seen_urls.add(request_url)
                yield Request(
                    request_url,
                    callback=self.parse,
                    errback=self._errback,
                    dont_filter=True,
                    meta={"handle_httpstatus_list": _FAILURE_HTTP_STATUS_CODES},
                )

        def parse(self, response: Response) -> Any:
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
                return

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

            for discovered_url in discovered_urls:
                yield Request(
                    discovered_url,
                    callback=self.parse,
                    errback=self._errback,
                    meta={"handle_httpstatus_list": _FAILURE_HTTP_STATUS_CODES},
                )

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
    namespace: list[str] | None,
    branch: str | None,
) -> bool:
    schema_names = set(schema.names) if schema is not None else set()
    primary_key_ready = await _ensure_index(
        room=room,
        table=table,
        column=primary_key,
        index_type="BTREE",
        name=f"meshagent_scrapy_{primary_key}_btree",
        namespace=namespace,
        branch=branch,
    )
    if "text" not in schema_names:
        return primary_key_ready
    text_ready = await _ensure_index(
        room=room,
        table=table,
        column="text",
        index_type="INVERTED",
        name="meshagent_scrapy_text_inverted",
        namespace=namespace,
        branch=branch,
    )
    if "link_urls" not in schema_names or "image_urls" not in schema_names:
        return primary_key_ready and text_ready
    link_urls_ready = await _ensure_index(
        room=room,
        table=table,
        column="link_urls",
        index_type="LABEL_LIST",
        name="meshagent_scrapy_link_urls_label_list",
        namespace=namespace,
        branch=branch,
    )
    image_urls_ready = await _ensure_index(
        room=room,
        table=table,
        column="image_urls",
        index_type="LABEL_LIST",
        name="meshagent_scrapy_image_urls_label_list",
        namespace=namespace,
        branch=branch,
    )
    return primary_key_ready and text_ready and link_urls_ready and image_urls_ready


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
    await _merge_frontier_rows(
        room=room,
        table=table,
        rows=rows,
        namespace=namespace,
        branch=branch,
        ensure=False,
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
    clean: CleanMode,
) -> ExtractCallback:
    async def extract(response: Response, content: bytes) -> dict[str, Any]:
        return await _default_extract(
            response=response,
            content=content,
            content_format=content_format,
            clean=clean,
        )

    return extract


async def _default_extract(
    *,
    response: Response,
    content: bytes,
    content_format: ContentFormat,
    clean: CleanMode,
) -> dict[str, Any]:
    content_type = _content_type(response)
    clean_content = (
        content
        if clean == "none"
        else await _trafilatura_content(
            url=response.url,
            content=content,
            content_type=content_type,
        )
    )
    asset_content = clean_content if clean == "before-links" else content
    assets = _content_assets(
        url=response.url,
        content=asset_content,
        content_type=content_type,
    )
    return {
        "url": response.url,
        "date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "content_type": content_type,
        "text": _content_text(
            content=clean_content,
            content_type=content_type,
            content_format=content_format,
        ),
        "links": assets["links"],
        "link_urls": assets["link_urls"],
        "images": assets["images"],
        "image_urls": assets["image_urls"],
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
    url: str,
    content: bytes,
    content_type: str,
) -> dict[str, list[Any]]:
    if not _is_html_content_type(content_type):
        return {
            "links": [],
            "link_urls": [],
            "images": [],
            "image_urls": [],
        }

    decoded = content.decode(_charset(content_type), errors="replace")
    parser = _HtmlAssetExtractor(url)
    parser.feed(decoded)
    parser.close()
    return {
        "links": parser.links,
        "link_urls": _record_urls(parser.links),
        "images": parser.images,
        "image_urls": _record_urls(parser.images),
    }


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


def _record_urls(records: Sequence[Mapping[str, Any]]) -> list[str]:
    urls = []
    for record in records:
        value = record.get("url")
        if isinstance(value, str) and value != "":
            urls.append(value)
    return _unique(urls)


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


def _attribute_list(attrs: Sequence[tuple[str, str | None]]) -> list[dict[str, str]]:
    return [
        {
            "name": name,
            "value": "" if value is None else value,
        }
        for name, value in attrs
    ]


def _attribute_value(attrs: Sequence[tuple[str, str | None]], name: str) -> str | None:
    normalized_name = name.lower()
    for attr_name, value in attrs:
        if attr_name.lower() == normalized_name:
            return "" if value is None else value
    return None


def _absolute_url(base_url: str, value: str | None) -> str | None:
    if value is None or value.strip() == "":
        return None
    return urljoin(base_url, value)


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
            pa.field("links", pa.list_(_link_struct_type())),
            pa.field("link_urls", pa.list_(pa.string())),
            pa.field("images", pa.list_(_image_struct_type())),
            pa.field("image_urls", pa.list_(pa.string())),
        ]
    )


def _attribute_struct_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("value", pa.string()),
        ]
    )


def _link_struct_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("url", pa.string()),
            pa.field("href", pa.string()),
            pa.field("content", pa.string()),
            pa.field("title", pa.string()),
            pa.field("rel", pa.string()),
            pa.field("target", pa.string()),
            pa.field("attributes", pa.list_(_attribute_struct_type())),
        ]
    )


def _image_struct_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("url", pa.string()),
            pa.field("src", pa.string()),
            pa.field("alt", pa.string()),
            pa.field("title", pa.string()),
            pa.field("srcset", pa.string()),
            pa.field("attributes", pa.list_(_attribute_struct_type())),
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
