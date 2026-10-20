# channel-sync

Telegram ↔ Bale channel mirror with durable forwarding and echo diagnostics.
An optional AI news curator reads X through headed Google Chrome, researches
AI/technology topics, and sends Persian drafts to both bots for admin approval.
See [CURATOR.md](CURATOR.md) for configuration, the approval workflow, exact
Ubuntu deployment steps and manual X login through Xvfb/VNC on a headless server.

The curator defaults to disabled. Its defaults use Asia/Tehran, 09:00–23:30,
400 captured posts/day, 10–25-minute sessions and 30–90-minute breaks. Topic
deduplication lasts 30 days, except explicit versioned release keys remain
permanent. `DRY_RUN=true` permits admin previews and prevents channel publication.
Browser crashes/hangs restart under a separate process watchdog; account
challenges persistently stop reading until an explicit resume.

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
Set `TELEGRAM_PROXY_URL` to your SOCKS5 proxy, or leave it blank. Telegram API,
file downloads, and TypeSafe jev evaluations use that proxy; Bale API and file
downloads always connect directly.
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
`group`. With moderation enabled, health also checks the Telegram discussion
group, requires explicit `can_delete_messages=true` for an administrator, and
performs one harmless jev evaluation to check reachability, authentication, the
configured model, and all six scores. This evaluation uses the Telegram proxy
and incurs normal TypeSafe API usage. `DRY_RUN` permits these checks. Bale
moderation is reported as unsupported and does not fail Telegram health.

## Comment moderation

1. In Telegram channel settings, link a discussion group. Add the Telegram bot
   to that group as an administrator with **Delete messages** enabled. Admin bots
   receive group messages even with Telegram's default bot privacy mode.
2. Add the moderation keys from [.env.example](.env.example) to your private
   `.env`. Set `TYPESAFE_API_KEY` there and keep `MODERATION_ENABLED=true`.
   Shell environment variables do not supply secrets. `TELEGRAM_DISCUSSION_GROUP_ID`
   may be the negative numeric group ID, or blank to discover
   `getChat(TELEGRAM_CHANNEL_ID).linked_chat_id` on startup.
3. Set the topic and thresholds, test classification, and run health:

   ```bash
   .venv/bin/channel-sync moderate-test "Thank you for the tutorial."
   .venv/bin/channel-sync health --output reports/health.json
   .venv/bin/channel-sync run --dry-run
   ```

4. Inspect the moderation logs and SQLite decisions. Set `DRY_RUN=false` in
   `.env` and start `channel-sync run` to enable automatic deletion.

| Key | Default / purpose |
| --- | --- |
| `MODERATION_ENABLED` | `true`; set `false` to disable moderation |
| `TYPESAFE_API_KEY` | Empty; set your private TypeSafe key in `.env` |
| `JEV_MODEL` | `jev-latest` |
| `MODERATION_THRESHOLD` | `0.55`; delete when `should_delete >= threshold` |
| `MODERATION_REVIEW_LOW` | `0.45`; review when `review_low <= should_delete < threshold` |
| `MODERATION_CHANNEL_TOPIC` | `Technology news and tutorials` |
| `TELEGRAM_DISCUSSION_GROUP_ID` | Empty; discover the linked group |
| `BALE_DISCUSSION_GROUP_ID` | Empty; reserved and currently ignored |
| `MODERATION_SKIP_ADMINS` | `true`; exempt discussion admins and owners |
| `MODERATION_ALERT_CHAT_ID` | Empty; optional Telegram chat for review alerts |

All six questions are in the editable
[moderation_questions.json](src/channel_sync/moderation_questions.json). Keep
their IDs and `noul` types; restart after editing instructions or criteria.
Each evaluation sends the original text/caption, configured topic, and
`comment_language="Persian or English"` to
`POST https://api.typesafe.ai/v1/systemone`. Only `should_delete` controls
deletion. All other scores are stored and logged as supporting reasons.

Moderation handles new real-user text/caption messages in the configured
discussion group, including replies and ordinary group messages. It skips
automatic channel forwards, chat/channel senders (including anonymous admins),
bots, missing user identities, media without text/caption, and admins when
configured. Edited messages are outside this feature. Discussion comments never
become forwarding jobs, even when both sync directions are enabled or paused.
Both features share the existing pollers; Telegram requests `channel_post` and
`message` updates when moderation is enabled. No additional `getUpdates`
consumer is started. `--new-posts-only` still moderates discussion comments in
its initial batch while skipping historical channel forwarding.

Transport failures, HTTP 429/5xx, and malformed jev answers retry with the
existing timeout, attempt count, and exponential backoff settings. Permanent
rejections such as invalid keys do not retry. Exhausted errors keep the comment
and log a warning; unknown admin status also keeps it. `run` continues syncing
during jev failures. Missing discussion setup or delete rights produces a
startup warning; no deletion occurs until the setup is corrected and the bot
is restarted. `health` exits unsuccessfully for these setup or jev failures.

`DRY_RUN=true` still classifies comments in `run` and logs decisions, but sends
neither deletions nor review alerts. `moderate-test` prints all six scores and
never polls, deletes, sends alerts, or writes decisions. An alert chat must
allow the bot to send messages. Review alerts use plain English labels with
the original comment and scores.

SQLite's `moderation_decisions` records platform, chat/message/user IDs, text,
all scores, action, reason, alert status, and timestamps. Its primary key
prevents repeat processing across update redelivery and restarts. Skips and
fail-open outcomes are recorded too; unavailable scores are stored as `{}`.
`status` reports counts by moderation action. Decisions made during dry run
or fail-open are final and are not replayed after changing settings. An
interrupted classification keeps the comment; an interrupted deletion is
recorded as uncertain and is not blindly repeated. Moderation does not consume
the forwarding circuit's quota or pause a sync direction on an API failure.

**Bale limitation:** the official [Bale docs](https://docs.bale.ai/) and
[Swagger spec used by tapi.bale.ai/docs](https://tapi.bale.ai/swagger/swagger.json)
document ordinary message updates and `deleteMessage`, but do not document
channel-comment delivery, a linked discussion group, or deletion of native
channel comments. This implementation therefore makes Bale comment moderation
a no-op and reports a clear warning at startup and in health. This is a
documentation limitation, not a claim that every Bale client lacks comments.
References: [Telegram Bot API](https://core.telegram.org/bots/api),
[Telegram bot privacy FAQ](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get),
and [TypeSafe API](https://docs.typesafe.ai/api).

## Echo diagnostics

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
acknowledges the test bots' update queues. With moderation enabled, its existing
pollers also moderate discussion comments; channel posts remain diagnostic
inputs. Each run
uses its own SQLite offset scope; future forwarding offsets are separate.

With `DRY_RUN=true`, `inspect-echo` writes a plan and makes no API calls. Direction
flags control `run`; they do not control these diagnostic probes. `run` with
`DRY_RUN=true` polls both bots, records plans, and classifies discussion comments
without downloads, sends, or deletions.

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
always use the client's fixed platform host. Bot tokens and the TypeSafe key are redacted from API
errors, formatted logs (including tracebacks), reports, and stored JSON. HTTPX and
httpcore debug logs are suppressed. Reports contain channel content and are
private, gitignored artifacts.

## Ubuntu deployment

The forwarding worker is implemented; live phase results are recorded separately
in STAGE2_REPORT.md. The [curator deployment guide](CURATOR.md#ubuntu-deployment-with-existing-channel-syncservice)
and [systemd drop-in](deploy/channel-sync-curator.conf) cover the existing
`channel-sync.service`, Chrome and Xvfb. On Ubuntu, recreate
the venv from the same requirements, transfer `.env` privately, and put SQLite on
a persistent local disk and run `channel-sync run`. Do not share one bot with two
polling processes. Normal run reports retain the latest 1000 events; SQLite
retains the durable history. Back up the database and apply a storage retention
policy appropriate for your server before long-term deployment.

API references: [Telegram Bot API](https://core.telegram.org/bots/api),
[Bale bot documentation](https://docs.bale.ai/),
[HTTPX proxy configuration](https://www.python-httpx.org/advanced/proxies/), and
[HTTPX environment behavior](https://www.python-httpx.org/environment_variables/).
