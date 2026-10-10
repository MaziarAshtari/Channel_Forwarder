# channel-sync

Telegram ↔ Bale channel mirror with durable forwarding and echo diagnostics.
Stage two adds the `run` command. See [FORWARDING.md](FORWARDING.md) for direction
flags, album handling, restart safety, and the live test protocol. The original
diagnostics remain available below.

For explicitly authorized automatic test publication, `live-batch` runs phases
B and C with a strict stop on permanent errors. Its bot-fixture limitations and
setup are documented in [FORWARDING.md](FORWARDING.md#authorized-automatic-bc-batches).
The latest batch results and exact labels are in
[STAGE2_LIVE_BATCH_REPORT.md](STAGE2_LIVE_BATCH_REPORT.md).

Python 3.11+ on Linux (laptop or Ubuntu). One asyncio process uses HTTPX directly
for both APIs. SQLite runs in WAL mode with `synchronous=FULL`. No Telegram SDK.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -e . --no-deps
cp .env.example .env                 # only if .env does not already exist
chmod 600 .env
.venv/bin/channel-sync init-db
.venv/bin/python -m unittest discover -s tests -v
```

The venv and private `.env` have already been created in this workspace. Put tokens
only in `.env`; do not paste them into chat, command arguments, or tracked files.
Set `TELEGRAM_PROXY_URL` to your SOCKS5 proxy, or leave it blank. Telegram API and
file downloads use that proxy; Bale API and file downloads always connect directly.
Both clients set `trust_env=False` to ignore system/environment proxy variables.
The proxy needs to support SOCKS5 remote DNS when Telegram is blocked.

Test channels default to Telegram `-1004472119156` and Bale `5716375015`. Make each
bot an administrator in its channel with permission to post. Use dedicated test
bots without webhooks or another running poller.

```bash
.venv/bin/channel-sync check-config  # offline; must pass before live commands
.venv/bin/channel-sync health --output reports/health.json
```

`health` calls `getMe`, `getChat`, and `getChatMember` for each bot, concurrently
between platforms. It prints redacted JSON and fails if a bot cannot access its
configured channel, is not an administrator, or explicitly lacks posting rights.
Missing permission fields remain unknown; a read-only health call cannot prove a
send will succeed. Bale's returned chat type is reported as received, including
`group`. `DRY_RUN` does not block these read-only health calls.

For the live echo experiment, set `DRY_RUN=false` in `.env`, then run:

```bash
.venv/bin/channel-sync inspect-echo
```

Each run performs health and webhook checks, then starts **both pollers and waits
for a successful initial poll before sending anything**. Each bot posts a plain
text, bold/italic/link text (including an emoji to check UTF-16 offsets), a photo
with a caption, and a two-photo album with distinct captions to its own channel.
Fixtures are small local PNGs; no external image service is needed. Telegram uses
entities with UTF-16 offsets. Bale uses inline `*bold*`, `_italic_`, `[link](url)`
without assuming Telegram `parse_mode` support. Only a photo album is tested;
there is no mixed photo/video fallback.

Polling continues for **60 seconds after all send attempts finish**. The JSON
report under `reports/` contains full send envelopes (including intermediate
rate-limit responses on retried attempts), raw updates, polling
envelopes, errors, bot identity, timestamps, and evidence for each probe. It is
redacted and atomically checkpointed throughout, including on Ctrl-C. Pending
updates are saved before their offsets are advanced. Inspect consumes and
acknowledges the test bots' update queues; it does not delete old posts. Each run
uses its own SQLite offset scope; future forwarding offsets are separate.

With `DRY_RUN=true`, `inspect-echo` writes a plan and makes no API calls. Direction
flags control `run`; they do not control these diagnostic probes. `run` with
`DRY_RUN=true` polls both bots and records plans without downloads or sends.

The report compares `from.id` and `via_bot.id` to `getMe.id`, as well as content
signatures, message IDs, chat types, `sender_chat`, entities, captions, and album
IDs. Absence of an echo establishes no reliable identity field. A field present
on all captured echoes is evidence for these probes, not a universal guarantee;
human posts must also be checked in a later experiment. `sender_chat` identifies
the channel and cannot distinguish human posts from bot posts. `from.is_bot`
alone cannot identify *our* bot. Raw updates are retained for further analysis.

## Durable safety foundations

`schema.sql` defines offsets, raw/processed updates, source-to-target mappings,
send records, per-item content signatures, and a persistent circuit-breaker latch.
The store inserts an entire update batch and its next offset in one transaction;
malformed batches roll back. A send key is unique and can be claimed only once.
Successful send completion and any source-to-target mappings can commit together.
Interrupted `sending` records become `uncertain` when the next locked process starts.

The inspection helpers retain their stage-one signatures. The forwarding worker
uses the counted signatures in `signatures.py`: normalized visible text/caption,
media kind, and stable media properties. These exclude message IDs, file IDs,
chat type, album IDs, and unstable photo sizes. Matching updates consume one
expected item and are ignored before forwarding. See FORWARDING.md for the
unavoidable ambiguity of identical human content during the TTL.

Inspection has a global circuit breaker. Forwarding limits each direction
separately, defaulting to 20 message items per 60 seconds, and persists pauses
in SQLite. A second process with
the same database is rejected using a Linux file lock. Inspect the send log before
manually clearing the latch; this stage provides no automatic reset.

Reads retry only transport errors, HTTP 429, and HTTP 5xx with bounded attempts and
exponential backoff. HTTP 400 and other permanent failures are never retried.
Sends retry only explicit `ok:false` rate-limit rejections. Timeouts, connection
failures, malformed success responses, and HTTP 5xx on sends are uncertain and
are not retried. `run` permits a separate-message fallback only after Bale's
explicit HTTP 400 `UNSUPPORTED_FILE` album rejection, and permits plain text
only after an explicit formatting rejection. Inspection does not use fallbacks.

**Exactly-once delivery has an API limit:** neither API offers a client idempotency
key. A crash after remote acceptance but before the response is durably recorded
cannot be resolved from SQLite alone. This stage prevents blind resends and
preserves an uncertain send for manual reconciliation and pauses that direction.
It cannot truthfully guarantee both zero duplicates and zero missed deliveries
with these APIs alone.

Downloads are capped at 20 MiB, ordinary uploads at 50 MiB, photos at 10 MiB.
Download streams are bounded even if size metadata is missing. File and API URLs
always use the client's fixed platform host. Bot tokens are redacted from API
errors, formatted logs (including tracebacks), reports, and stored JSON. HTTPX and
httpcore debug logs are suppressed. Reports contain channel content and are
private, gitignored artifacts.

## Ubuntu deployment

The forwarding worker is implemented; live phase results are recorded separately
in STAGE2_REPORT.md. Server service configuration is not included yet. On Ubuntu, recreate
the venv from the same requirements, transfer `.env` privately, and put SQLite on
a persistent local disk and run `channel-sync run`. Do not share one bot with two
polling processes. Normal run reports retain the latest 1000 events; SQLite
retains the durable history. Back up the database and apply a storage retention
policy appropriate for your server before long-term deployment.

API references: [Telegram Bot API](https://core.telegram.org/bots/api),
[Bale bot documentation](https://docs.bale.ai/),
[HTTPX proxy configuration](https://www.python-httpx.org/advanced/proxies/), and
[HTTPX environment behavior](https://www.python-httpx.org/environment_variables/).
