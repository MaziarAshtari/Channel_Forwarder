# Forwarding

```bash
.venv/bin/channel-sync check-config
.venv/bin/channel-sync run
.venv/bin/channel-sync status          # offline, no API calls
```

`run` loads `.env`, checks both bots/channels and webhooks, then polls both
platforms in one asyncio process. Configure `TG_TO_BALE` and `BALE_TO_TG` as
`true` or `false`. The earlier `TELEGRAM_TO_BALE_ENABLED` and
`BALE_TO_TELEGRAM_ENABLED` names remain supported if the shorter names are
absent. Short names take precedence. Both platforms are always polled, including
disabled directions, so their expected echoes are consumed.

Telegram and TypeSafe jev use `TELEGRAM_PROXY_URL` when configured. Bale connects directly.
API and download clients use `trust_env=False`. Secrets are read only from `.env`.
Run reports, SQLite, and `.env` are gitignored; report JSON and logs redact tokens.
Bale file handles can embed bot tokens. Persisted operational handles replace
known bot tokens with references before encoding; decoding restores them from
the current `.env` only when needed for downloads.

`DRY_RUN=true`, or `run --dry-run`, still polls and acknowledges updates. It
records translated plans without downloading or sending. Those posts are marked
`dry_run` and will not be replayed when dry run is switched off. Disabled
directions similarly ignore their new source posts.

With `MODERATION_ENABLED=true`, the Telegram poller also receives `message`
updates and moderates real-user text/caption comments in the linked discussion
group before channel routing. Discussion comments never enter forwarding jobs.
Dry run still classifies and logs them, but suppresses deletions and review
alerts. Moderation decisions and deduplication have their own SQLite table and
do not consume forwarding circuit quotas. See
[README.md](README.md#comment-moderation) for setup, health checks, `moderate-test`,
and the documented Bale comment limitation.

Text and captions translate bold, italic, and links in both directions. Telegram
offsets and lengths use UTF-16, including emoji. Literal Markdown delimiters are
escaped on sends to Bale. Telegram receives plain text plus entities, never
`parse_mode`. Unsupported Telegram entity types retain their visible text;
crossing formatting spans fall back to escaped visible text. An explicit API
formatting rejection permits one plain-text attempt. Other HTTP 400 errors are
permanent unless they are the specific album rejection described below.

Photos, videos, and documents are downloaded from the source and uploaded to the
target. Platform file IDs are never reused across APIs. The largest photo by
pixel dimensions is selected. Transfer limits are 20 MiB downloads, 50 MiB
ordinary uploads, and 10 MiB photo uploads. Files over a limit are skipped with
a notice. An album can therefore be partial when an item exceeds the limit.

Album items are durably grouped by source chat and `media_group_id`, sorted by
source message ID, and held until `ALBUM_WAIT_SECONDS` (default 3) have elapsed
since the last received item. A later post waits behind that album to preserve
direction order. If Bale returns HTTP 400 `UNSUPPORTED_FILE` for an album, its
items are sent separately in one fallback pass. Completed items are not resent
after restart. Other album failures and uncertain outcomes do not permit this
fallback. An album item arriving after sending began pauses the direction for
reconciliation rather than silently splitting or duplicating it.

Before each network send, SQLite records a unique intent and one expected echo
signature per item, including identical items. A signature consists of normalized
visible text/caption (Unicode NFC, Markdown formatting removed, whitespace
collapsed), media kind, and stable properties: photo/video width and height,
or document filename and byte count. Photo sizes and all message/file/group IDs
are excluded. Bale's combined photo/document payload is treated as one photo.
Incoming updates first check own-bot authorship and consume a matching expected
signature. Matches are ignored before any job can be queued. Counts and the
default 600-second TTL persist across restart. Repeated update IDs or repeated
platform/chat/message IDs do not consume a second count.

Bale human posts can be indistinguishable from bot echoes. An identical human
post during the signature TTL can consume an expected count and be suppressed;
there is no API author field that resolves this. The count bounds suppression
to the number of expected items. Changed or missing media dimensions may prevent
a match; the live phases must establish those properties for video and other
media. The existing real album capture is included as an offline regression
fixture. This content-based policy is deliberately explicit rather than a claim
of universal identification.

`CIRCUIT_MAX_SENDS` defaults to 20 message items within
`CIRCUIT_WINDOW_SECONDS=60`, independently per direction. An album counts each
item. The worker pauses before exceeding the limit; the other direction can
continue. The latch persists across restart. Permanent forwarding failures and
uncertain sends also pause the affected direction. Inspect `status` and the
send log before manually clearing a circuit in SQLite. There is no automatic
reset command.

An entire polling batch, token-free operational payloads, and its next offset
commit together. Routing, update deduplication, signature consumption, and album
membership commit together. Send completion and mapping commit together, using
deterministic send keys. Restart resumes queued work and known-rejected fallback
paths without replaying completed sends. A network send accepted before a crash
or lost response remains `uncertain` and is never blindly retried. That direction
requires manual reconciliation; the APIs provide no client idempotency key.
Mappings record send-response IDs, which Bale's echo capture shows may differ
from echoed channel IDs, so mappings are never used for loop detection.

Normal runs retain a bounded recent report in `reports/run-*.json`, with full
durable history in SQLite. Live phases retain their complete run report.
Ctrl-C checkpoints and stops the process. `--stop-on-error` stops both pollers
and the worker on a forwarding error or circuit pause, for supervised tests.

## Supervised live phases

The assistant asks before **each** phase, then starts the corresponding command:

```bash
.venv/bin/channel-sync run --phase a --new-posts-only --stop-on-error --output reports/phase-a.json
.venv/bin/channel-sync run --phase b --new-posts-only --stop-on-error --output reports/phase-b.json
.venv/bin/channel-sync run --phase c --new-posts-only --stop-on-error --output reports/phase-c.json
```

Phase overrides are local to the process and do not edit `.env`. A enables only
Telegram → Bale, B only Bale → Telegram, C both. `--new-posts-only` saves and
ignores the initial API queue before announcing `Bridge ready`; normal `run`
resumes pending work and does not drain it. Existing durable jobs are retained.

After the assistant announces readiness, manually post each of these cases in
the enabled source channel, one at a time. For C, repeat them in both source
channels. Use unique text/caption labels such as `S2-A-plain`,
`S2-A-formatted`, `S2-A-photo`, `S2-A-video`, `S2-A-document`,
`S2-A-photo-album/1`, and `S2-A-mixed-album/1` (and `/2` for second items).

1. Plain text.
2. Formatted bold, italic, and a link; include an emoji before formatting.
3. Photo with a caption.
4. Video with a caption.
5. Document.
6. Two-photo album with distinct captions.
7. Mixed photo/video album with distinct captions.

Keep all files under 20 MiB and photos under 10 MiB. After each case, verify the
target channel has exactly one copy of every item, formatting/captions are
correct, and the source receives no returned post. Bot APIs do not provide a
complete read-back of channel history, so API send counts alone cannot prove
visual delivery or the absence of duplicates. The per-case report records API
evidence and the user's channel confirmation separately. Stop on any problem;
do not begin the next phase without fresh approval. The user can explicitly
authorize automatic batches instead of this manual protocol.

## Authorized automatic B/C batches

`live-batch` publishes labeled source fixtures and their mirrors in both test
channels. Invoke it only when this publication is intended. It runs B and then C
without individual prompts, keeping own-bot filtering, counted signatures, and
the configured circuit breaker active. Every permanent API error stops the
batch immediately; unlike normal forwarding, it attempts no formatting or album
fallback. It observes echoes for 60 seconds after each completed phase and then
stops the bridge. Reports contain successful and rejected cases separately.

Prepare a small 64×64 MP4 (FFmpeg is used only to create this optional test asset):

```bash
mkdir -p data/batch-fixtures
ffmpeg -hide_banner -loglevel error -f lavfi -i testsrc2=size=64x64:rate=12 -t 2 -c:v libx264 -pix_fmt yuv420p -movflags +faststart -y data/batch-fixtures/clip.mp4
.venv/bin/channel-sync live-batch --output reports/live-batch.json
```

Source posts use the configured bridge bots. Since normal polling must ignore
their own posts, the harness queues explicit durable test jobs from actual send
responses; it does not alter authors or manufacture polling updates. This tests
live conversion, media sending, mapping, and echo suppression, but human-post
admission remains covered separately by manual tests and offline tests. Bale
source album responses can omit file handles, so those cases reuse original
upload bytes and report that source-album downloads were not exercised. A normal
restart refuses an unfinished controlled fixture and pauses for reconciliation.
API observations and final visual channel inspection remain separate.
