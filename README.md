# Meshagent Scrapy

Spider a website with Scrapy and import page content into a Meshagent room
dataset.

```python
from meshagent.scrapy import import_domain_with_scrapy

result = await import_domain_with_scrapy(
    room,
    url="https://example.com",
    table="pages",
    namespace=["crawls"],
    limit=100,
    concurrency=5,
)
```

To test it through `meshagent room connect`:

```bash
meshagent room connect --room=my-room --identity=scrapy -- \
  python meshagent-sdk/meshagent-scrapy/examples/crawl.py \
  https://www.meshagent.com --table=sample --namespace=crawls --limit=100 --concurrency=5
```

The sample command writes progress to stderr while it imports. TTY output uses a
single updating line; redirected output uses plain log lines. Pass `--silent` to
suppress progress output.

Pass `--concurrency` or `concurrency=` to tune Scrapy's maximum concurrent
requests.

Pass `--batch-size` or `batch_size=` to cap how many page records are merged
into the content table at once. The crawler also flushes content batches by
estimated payload size with `--max-batch-bytes` or `max_batch_bytes=`, which
defaults to 16 MiB, and by elapsed time with `--max-batch-delay` or
`max_batch_delay=`, which defaults to 60 seconds. The row-count cap defaults to
100. Raw HTML rows can be large, so prefer lowering the byte limit before
lowering the row count if the room server reports Lance/DataFusion merge memory
exhaustion while importing full HTML pages.

The crawler sends a browser-like User-Agent by default. Pass `--user-agent` or
`user_agent=` to override it for a specific crawl.

The default extractor writes page content as markdown in the `text` column. Use
`--format=html` to keep HTML, `--format=text` to strip markup to plain text, or
pass `content_format=` from library code.

By default, the crawler runs Trafilatura cleanup before converting markdown/text
content, which strips common navigation, footer, sidebar, and ad boilerplate.
For `--format=html`, the default is to strip scripts and inline image data URLs
while preserving the rest of the HTML. Use `--strip=` with comma-separated values
like `scripts`, `css`, `whitespace`, `image-data-urls`, or `clean` to choose the
HTML stripping steps, or `--strip=none` to process the raw response body.

The CLI persists crawl frontier state by default in `<table>__frontier`, so a
limited run can be resumed by running the same command again:

```bash
meshagent room connect --room=my-room --identity=scrapy -- \
  python meshagent-sdk/meshagent-scrapy/examples/crawl.py \
  https://www.meshagent.com --table=sample --namespace=crawls --limit=100
```

Pass `--frontier-table` to choose a different state table, or `--no-resume` to
run without frontier persistence. Library callers can opt in with
`resume=True`. Frontier updates are buffered before they are written; tune that
with `--frontier-batch-size` or the library `frontier_batch_size=` argument.
Failed URLs are not retried on resume unless you pass `--retry-failed` or
`retry_failed=True`.

The crawler creates indexes by default: a BTREE index on the page table primary
key, plus BTREE `url` and BITMAP `status` indexes on the frontier table. Pass
`--index=text` or `index_columns=("text",)` to also create an INVERTED index on
`text`. Pass `--no-indexes` or `create_indexes=False` to skip all automatic
index creation. It also runs dataset optimization periodically while importing
and shows `optimizing`/`optimized` in progress output. Tune that with
`--optimize-every` or `optimize_every=`, and use `0` on the CLI or `None` in
library code to disable automatic optimization.

By default, the crawler imports textual responses only, based on `Content-Type`
values containing `text/`, `html`, `xml`, or `json`. Pass `--response-filter`
or `response_filter=` to replace that default with a JMESPath expression over
`url`, `status`, `headers`, `content_type`, and `content_type_lower`. Header
names are lower-cased, so an HTML-only crawl can use:

```bash
--response-filter "contains(headers.\"content-type\", 'text/html')"
```

By default, records are merged on `url` with the columns `url`, `date`,
`content_type`, `text`, and `images`. `text` is markdown unless another content
format is selected. `images` is a struct array with `src` and `alt` only, and
inline image data URLs are excluded.

Pass an async `extract=` callback to derive custom columns from the Scrapy
response and content bytes. Return `None` from the callback to skip the record.
Pass an async `progress=` callback to observe import progress from library code.
