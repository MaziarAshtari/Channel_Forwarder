# Stage-one implementation report

Built on 2026-10-07 in the current workspace as the Python project `channel-sync`.
The package is installed in `.venv` using Python 3.14.4. A local Git repository
is initialized on `main`; no commit was created. No forwarding worker exists.

Created files:

- `.gitignore`, `.env.example`, private `.env` (now populated by the user)
- `pyproject.toml`, `requirements.txt`, `README.md`, this report
- `src/channel_sync/__init__.py`, `__main__.py`, `cli.py`, `config.py`
- `src/channel_sync/clients.py`, `health.py`, `inspection.py`
- `src/channel_sync/redaction.py`, `reports.py`, `messages.py`
- `src/channel_sync/schema.sql`, `store.py`
- `src/channel_sync/assets/blue.png`, `orange.png`
- `tests/helpers.py`, `test_clients.py`, `test_config_redaction.py`
- `tests/test_inspection_health.py`, `test_store_messages.py`
- Ignored runtime artifacts: `.venv/`, `data/channel-sync.sqlite3`, process lock

Offline validation:

- `python -m unittest discover -s tests -v`: **50 tests passed**.
- Dependency validation: `pip check` passed.
- Editable installation and wheel build passed. The wheel includes the schema
  and both PNG fixtures, and excludes `.env`.
- Both the console command and `python -m channel_sync` work.
- SQLite initialization passed, including a repeated initialization.
- Git ignore checks passed for `.env`, `.venv`, SQLite, and reports.

Tests cover proxy separation, secret redaction, UTF-16 entity offsets, polling
readiness, raw capture, intermediate HTTP 429 responses, permanent album errors,
dry run, webhook conflicts, administrator checks, update deduplication, atomic
transactions, changed Bale message/file IDs, signature expiration, interrupted
sends, persistent circuit pauses, and the process lock. HTTPX MockTransport
handled every API request in tests; no test contacted a bot API.

The user filled `.env` and authorized live diagnostics by replying `ready`.
Configuration validation passed with `DRY_RUN=false` and no Telegram proxy.
Both direction flags are enabled in the user's configuration; there is still
no forwarding worker in this stage.

Health: **passed** on 2026-10-07. Saved output: [health.json](reports/health.json).

| Platform | Bot username | Bot ID | Channel ID | Membership | Can post |
| --- | --- | --- | --- | --- | --- |
| Telegram | AGP_Test_Bot_Telegram_bot | 8620351679 | -1004472119156 | administrator | true |
| Bale | agp_test_two_bot | 1226649196 | 5716375015 | administrator | true |

`getMe`, `getChat`, and `getChatMember` succeeded for both platforms. Each
`getChat` returned the configured ID and `type=channel`. Telegram connected
directly in this experiment; the optional SOCKS5 route remains untested live.
Bale connected directly.

Inspect-echo: **complete**, run `ec1d9b787fd9`. Full redacted send responses,
raw updates, polling responses, and per-probe evidence:
[inspect-echo JSON](reports/inspect-echo-2026-10-07T05-57-33.290764+00-00.json).

Both initial polls completed before the first send. Each bot successfully sent
all four probes: plain text, formatted text, captioned photo, and a two-photo
album. This was eight API send calls and ten channel messages total. The
observation window began at `2026-10-07T05:57:51.790951+00:00` and continued for
60 seconds. There were no poll errors, retries, uncertain sends, or circuit
pauses. All eight sends were durably recorded as `sent`.

| Platform | Plain echoes | Formatted echoes | Photo echoes | Album-item echoes | Own-bot author field established |
| --- | --- | --- | --- | --- | --- |
| Telegram | 0 | 0 | 0 | 0 | None; no echoes observed |
| Bale | 0 | 0 | 0 | 2 | None; both author IDs were zero |

Telegram send responses contained the requested bold, italic, and text-link
entities with UTF-16 offsets after the emoji. Photo captions and album grouping
were present in send responses. No own-bot updates arrived during this run, so
no update field can be established as a reliable own-bot identifier from it.

Bale returned both album items under `message` with `chat.type=channel`.
Both had `from.id=0`, `from.is_bot=false`, the channel in `sender_chat`, and no
`via_bot`. These fields do not distinguish our bot from the earlier supplied
human-post observation. Send responses instead had our actual bot ID in `from`
and `chat.type=group`.

Bale album send responses had message IDs **51 and 54**, whereas echoed items
had IDs **52 and 53**. The two album updates arrived in reverse item order:
`album/2` first (update 30), then `album/1` (update 31). Both carried the same
`media_group_id` as the send response, preserved their distinct captions, and
included both `document` and `photo` fields. Album handling in the later worker
must account for these multiple media fields and out-of-order delivery.

The log's caption/media content signature matched both Bale echoes despite
changed message IDs and misleading author metadata. This fallback identified
all echoes captured here; the unique probe captions make this evidence narrower
than a guarantee for arbitrary production content. No single Bale update field
was demonstrated to reliably identify our own bot's posts. Neither
`sender_chat`, `from.is_bot`, nor send-response message IDs can serve that role.

Bale's formatted send response preserved the inline Markdown; there was no
formatted-text echo, so client rendering has not been independently verified.
The successful two-photo album does not establish mixed photo/video support.

After the live experiment, **all 50 offline tests passed again**. Exact bot
tokens were absent from every saved report and SQLite artifact checked.
Token-like Bale file identifiers are redacted in the raw capture as well.

Commands executed:

```bash
.venv/bin/channel-sync health --output reports/health.json
.venv/bin/channel-sync inspect-echo
.venv/bin/python -m unittest discover -s tests
```

Ambiguous remote sends are recorded as uncertain and never blindly retried.
Exactly-once delivery across a lost send response needs a future reconciliation
policy because the Bot APIs do not provide client idempotency keys. This limit
and the scope of the preliminary content-signature helper are documented in
`README.md`.
