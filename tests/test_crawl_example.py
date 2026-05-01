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

    assert crawl._DEFAULT_BATCH_SIZE == 5000


def test_example_crawler_uses_size_batch_default() -> None:
    crawl = _load_crawl_example()

    assert crawl._DEFAULT_MAX_BATCH_BYTES == 16 * 1024 * 1024


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
