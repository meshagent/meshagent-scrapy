from __future__ import annotations

from importlib import util
from pathlib import Path


def _load_crawl_example():
    path = Path(__file__).parents[1] / "examples" / "crawl.py"
    spec = util.spec_from_file_location("meshagent_scrapy_crawl_example", path)
    assert spec is not None
    assert spec.loader is not None
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_example_crawler_uses_standard_count_batch_default() -> None:
    crawl = _load_crawl_example()

    assert crawl._DEFAULT_BATCH_SIZE == 100


def test_example_crawler_uses_size_batch_default() -> None:
    crawl = _load_crawl_example()

    assert crawl._DEFAULT_MAX_BATCH_BYTES == 16 * 1024 * 1024


def test_example_crawler_uses_batch_delay_default() -> None:
    crawl = _load_crawl_example()

    assert crawl._DEFAULT_MAX_BATCH_DELAY == 60


def test_batch_size_cli_argument_is_available() -> None:
    crawl = _load_crawl_example()

    args = crawl._parser().parse_args(
        [
            "https://example.com",
            "--format=html",
            "--clean=none",
            "--batch-size=5",
        ],
    )

    assert args.batch_size == 5


def test_max_batch_bytes_cli_argument_is_available() -> None:
    crawl = _load_crawl_example()

    args = crawl._parser().parse_args(
        [
            "https://example.com",
            "--max-batch-bytes=1048576",
        ],
    )

    assert args.max_batch_bytes == 1048576


def test_max_batch_delay_cli_argument_is_available() -> None:
    crawl = _load_crawl_example()

    args = crawl._parser().parse_args(
        [
            "https://example.com",
            "--max-batch-delay=30",
        ],
    )

    assert args.max_batch_delay == 30


def test_include_sitemap_cli_argument_is_available() -> None:
    crawl = _load_crawl_example()

    args = crawl._parser().parse_args(
        [
            "https://example.com",
            "--include-sitemap",
        ],
    )

    assert args.include_sitemap is True


def test_index_cli_argument_accepts_text_index() -> None:
    crawl = _load_crawl_example()

    args = crawl._parser().parse_args(
        [
            "https://example.com",
            "--index=text",
        ],
    )

    assert crawl._indexes(args.index) == ("text",)
