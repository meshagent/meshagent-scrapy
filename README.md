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
)
```

To test it through `meshagent room connect`:

```bash
meshagent room connect --room=my-room --identity=scrapy -- \
  python meshagent-sdk/meshagent-scrapy/examples/crawl.py \
  https://www.meshagent.com --table=sample --namespace=crawls --limit=100
```

The sample command writes progress to stderr while it imports. TTY output uses a
single updating line; redirected output uses plain log lines. Pass `--silent` to
suppress progress output.

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

The crawler creates scalar indexes by default: a BTREE index on the page table
primary key, plus BTREE `url` and BITMAP `status` indexes on the frontier table.
Pass `--no-indexes` or `create_indexes=False` to skip that. It also runs dataset
optimization periodically while importing and shows `optimizing`/`optimized` in
progress output. Tune that with `--optimize-every` or `optimize_every=`, and use
`0` on the CLI or `None` in library code to disable automatic optimization.

Pass `--response-filter` or `response_filter=` to skip responses with a JMESPath
expression over `url`, `status`, `headers`, and `content_type`. Header names are
lower-cased, so an HTML-only crawl can use:

```bash
--response-filter "contains(headers.\"content-type\", 'text/html')"
```

By default, records are merged on `url` with the columns `url`, `date`,
`content_type`, and `text`. Pass an async `extract=` callback to derive custom
columns from the Scrapy response and content bytes. Return `None` from the
callback to skip the record. Pass an async `progress=` callback to observe import
progress from library code.
