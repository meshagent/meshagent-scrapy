from __future__ import annotations

import argparse
import asyncio
import re
import sys
import time
from urllib.parse import urlparse

from meshagent.api import RoomClient
from meshagent.api.http import new_client_session
from meshagent.scrapy import ScrapyImportProgress, import_domain_with_scrapy

_DEFAULT_BATCH_SIZE = 1000
_DEFAULT_MAX_BATCH_BYTES = 16 * 1024 * 1024
_DEFAULT_MAX_BATCH_DELAY = 5 * 60


def _namespace(value: str | None) -> list[str] | None:
    if value is None or value.strip() == "":
        return None
    return [part for part in value.split("::") if part != ""]


def _url_filter(value: str) -> str:
    parsed = urlparse(value)
    host = parsed.netloc or parsed.path.split("/", maxsplit=1)[0]
    if host == "":
        raise ValueError("url must be non-empty")
    path = parsed.path if parsed.netloc else parsed.path.removeprefix(host)
    path = path.rstrip("/")
    if path == "":
        return f"^https?://{re.escape(host)}(/.*)?$"
    return f"^https?://{re.escape(host)}{re.escape(path)}(/.*)?$"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Spider a domain with Scrapy into a Meshagent room dataset.",
    )
    parser.add_argument("url", help="Domain or URL to crawl, e.g. https://example.com")
    parser.add_argument("--table", default="scrapy", help="Dataset table name")
    parser.add_argument(
        "--namespace",
        default=None,
        help="Dataset namespace, using :: between nested segments",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of pages to import",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Maximum concurrent Scrapy requests; uses Scrapy's default when omitted",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Persist and resume a crawl frontier table",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry URLs previously marked failed in the frontier",
    )
    parser.add_argument(
        "--frontier-table",
        default=None,
        help="Dataset table to use for crawl frontier state",
    )
    parser.add_argument(
        "--frontier-batch-size",
        type=int,
        default=500,
        help="Number of frontier updates to buffer before writing",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=(
            f"Maximum page records to merge per content batch; defaults to "
            f"{_DEFAULT_BATCH_SIZE}"
        ),
    )
    parser.add_argument(
        "--max-batch-bytes",
        type=int,
        default=_DEFAULT_MAX_BATCH_BYTES,
        help=(
            "Estimated maximum content batch bytes before merging; "
            f"defaults to {_DEFAULT_MAX_BATCH_BYTES}; use 0 to disable"
        ),
    )
    parser.add_argument(
        "--max-batch-delay",
        type=float,
        default=_DEFAULT_MAX_BATCH_DELAY,
        help=(
            "Maximum seconds to buffer pending content rows before merging; "
            f"defaults to {_DEFAULT_MAX_BATCH_DELAY:g}; use 0 to disable"
        ),
    )
    parser.add_argument(
        "--response-filter",
        default=None,
        help="JMESPath expression over url, status, headers",
    )
    parser.add_argument(
        "--format",
        choices=["md", "html", "text"],
        default="md",
        help="Content format for the default text column",
    )
    parser.add_argument(
        "--clean",
        choices=["before-links", "after-links", "none"],
        default="before-links",
        help="Run Trafilatura cleanup before links/images, after links/images, or not at all",
    )
    parser.add_argument(
        "--indexes",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Create indexes for page and frontier tables",
    )
    parser.add_argument(
        "--optimize-every",
        type=int,
        default=5000,
        help="Run dataset optimize after this many writes; use 0 to disable",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help="User-Agent header Scrapy should send",
    )
    parser.add_argument(
        "--respect-robots-txt",
        action="store_true",
        help="Ask Scrapy to obey robots.txt",
    )
    parser.add_argument(
        "--silent",
        action="store_true",
        help="Suppress progress output",
    )
    return parser


class _ProgressReporter:
    def __init__(self) -> None:
        self._is_tty = sys.stderr.isatty()
        self._last_emit = 0.0
        self._needs_newline = False

    async def __call__(self, progress: ScrapyImportProgress) -> None:
        now = time.monotonic()
        if (
            progress.stage
            not in {"started", "batch_merged", "optimizing", "optimized", "completed"}
            and now - self._last_emit < 0.25
        ):
            return
        self._last_emit = now
        line = self._format(progress)
        if self._is_tty:
            sys.stderr.write(f"\r\033[2K{line}")
            self._needs_newline = progress.stage != "completed"
            if progress.stage == "completed":
                sys.stderr.write("\n")
        else:
            sys.stderr.write(f"{line}\n")
        sys.stderr.flush()

    def close(self) -> None:
        if self._needs_newline:
            sys.stderr.write("\n")
            sys.stderr.flush()
            self._needs_newline = False

    def _format(self, progress: ScrapyImportProgress) -> str:
        status = {
            "started": "starting",
            "loading_frontier": "loading frontier",
            "frontier_loaded": "frontier loaded",
            "frontier_discovered": "discovering",
            "page_scraped": "crawling",
            "response_filtered": "filtered",
            "request_failed": "failed",
            "record_extracted": "extracting",
            "record_skipped": "skipping",
            "batch_merged": "merged",
            "optimizing": "optimizing",
            "optimized": "optimized",
            "completed": "complete",
        }.get(progress.stage, progress.stage)
        parts = [
            f"{status}",
            f"matched={progress.matched_records}",
            f"imported={progress.imported_records}",
            f"skipped={progress.skipped_records}",
            f"pending={progress.pending_records}",
        ]
        if progress.current_url:
            parts.append(_ellipsize(progress.current_url, 88))
        return " | ".join(parts)


def _ellipsize(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return f"{value[: max_length - 1]}..."


async def _main() -> None:
    args = _parser().parse_args()
    reporter = None if args.silent else _ProgressReporter()

    try:
        async with new_client_session() as session:
            async with RoomClient(session=session) as room:
                result = await import_domain_with_scrapy(
                    room,
                    url=args.url,
                    table=args.table,
                    namespace=_namespace(args.namespace),
                    url_filter=_url_filter(args.url),
                    response_filter=args.response_filter,
                    content_format=args.format,
                    clean=args.clean,
                    limit=args.limit,
                    concurrency=args.concurrency,
                    batch_size=(
                        args.batch_size
                        if args.batch_size is not None
                        else _DEFAULT_BATCH_SIZE
                    ),
                    max_batch_bytes=(
                        args.max_batch_bytes if args.max_batch_bytes > 0 else None
                    ),
                    max_batch_delay=(
                        args.max_batch_delay if args.max_batch_delay > 0 else None
                    ),
                    user_agent=args.user_agent,
                    respect_robots_txt=args.respect_robots_txt,
                    resume=args.resume,
                    retry_failed=args.retry_failed,
                    frontier_table=args.frontier_table,
                    frontier_batch_size=args.frontier_batch_size,
                    create_indexes=args.indexes,
                    optimize_every=(
                        args.optimize_every if args.optimize_every > 0 else None
                    ),
                    progress=reporter,
                )
    finally:
        if reporter is not None:
            reporter.close()

    print(
        "imported "
        f"{result.imported_records}/{result.matched_records} records "
        f"into {args.namespace + '::' if args.namespace else ''}{args.table} "
        f"({result.skipped_records} skipped, "
        f"{result.discovered_urls} discovered, {result.failed_urls} failed)"
    )


if __name__ == "__main__":
    asyncio.run(_main())
