## [0.39.5]
- Added Scrapy crawler HTML/content stripping configuration via new `strip` and `strip_order` inputs (including support for stripping `scripts`, `css`, `whitespace`, `clean`, and `image-data-urls`)
- Changed default behavior for `content_format="html"` to strip `scripts` and inline image data URLs while preserving the rest of the HTML (and updated `--clean` CLI usage to map onto the new stripping configuration)
- Changed the default Scrapy content row-count batch cap from 1000 to 100.
- Changed the default Scrapy content max batch delay from 5 minutes to 60 seconds.
- Added retry handling for transient Scrapy dataset write disconnects before skipping the failed content batch.
- Broke Scrapy dataset output schema by removing `links`, `link_urls`, `image_urls`, and reducing `images` to `src`/`alt` only; inline image data URLs are excluded from extracted images
- Changed index creation defaults: automatic creation no longer includes inverted/label indexes for removed link/image URL columns; `text` inverted index creation is now opt-in via `index_columns=("text",)`
- Updated generated dataset schema to apply ZSTD compression metadata to large string fields (including `text` and image fields)

## [0.39.4]
- Added Scrapy import concurrency controls via `concurrency=` and the example crawler `--concurrency` flag.
- Changed Scrapy imports to send a browser-like User-Agent by default while keeping `user_agent=` and `--user-agent` overrides.
- Added size-aware Scrapy content merge batching with `max_batch_bytes=` and the example crawler `--max-batch-bytes` flag.
- Changed the default Scrapy content row-count batch cap to 1000 and added time-aware flushing with `max_batch_delay=` and `--max-batch-delay`, defaulting to 5 minutes.
- Changed Scrapy imports to skip non-text responses by default unless a custom response filter is supplied.
- Breaking: Python scheduled-task client and spec models now use a `ScheduledTaskSpec` contract (including queue/container targeting) instead of separate queue/schedule/payload parameters.
- Added Python scheduled-task run listing support with models/pages for runs and their status/attempt/timestamp fields.
- Updated scheduled-task client methods to support `room_id` filtering and the new spec-based request/response shapes.
- Added/updated CLI scheduled-task create/update flows to load the `ScheduledTaskSpec` from a YAML file and included new run-related CLI functionality.
- Removed generated CLI dataset functionality (including the previously available SQL-exec command).
- Added `croniter~=6.0` as a dependency to support cron parsing for scheduled tasks.

## [0.39.3]
- Added `meshagent-commoncrawl` package with Common Crawl import support (progress reporting, dataset record extraction/import utilities, and tests); includes dependencies such as `pyarrow~=21.0.0` and `warcio~=1.7`.
- Added `meshagent-scrapy` package with Scrapy-based dataset import support (scrapy import utilities, examples, and tests); includes dependencies such as `scrapy~=2.13`, `trafilatura~=2.0`, and `pyarrow~=21.0.0`.
- Updated OpenAI Responses adapter error handling to detect out-of-credits/`insufficient_quota` conditions and return a clearer non-retryable 402 response; also improved websocket error payload message extraction.
- Updated `meshagent-cli` default model selections from `gpt-5.4` to `gpt-5.5` across ask/chatbot/codex/task runner/mailbot/worker CLI flows.
- Updated `meshagent-cli` and `meshagent-python` packaging extras to include `meshagent-commoncrawl` and `meshagent-scrapy` (including dedicated `commoncrawl`/`scrapy` extras).
- Added/updated tests for the new OpenAI out-of-credits handling and for commoncrawl/scrapy importer functionality.

## [0.39.2]
- Added `meshagent-scrapy` with Scrapy domain imports into room datasets.
