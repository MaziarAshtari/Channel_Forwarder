# AI news curator

The optional curator runs inside `channel-sync run`, using the existing bot
pollers and exclusive process lock. Browser reads, research, admin actions and
publication have separate supervised async tasks and durable SQLite queues.
Curator writes use independent HTTP connections and send locks; only the existing
main bot clients poll updates, so slow curator API retries cannot hold a bridge send lock.
The browser has a separate Python/Playwright driver/Chrome process tree.
Curator errors do not pause bridge directions or stop moderation. No second
`getUpdates` consumer or separate curator service is required.

## Configuration

Merge the curator section of `.env.example` into your private `.env`; do not
replace your existing bot tokens, channel IDs or direction flags. All settings
and credentials come from this file, with environment interpolation disabled.
The implementation added missing settings to the workspace `.env` without
changing existing values. `CURATOR_ENABLED=false` is the default.

Required before enabling:

- `LLM_BASE_URL`: API base, including `/v1` if your provider uses it. The code
  appends `/chat/completions`. The configured `LLM_MODEL` must support Chat
  Completions function tool calls. Set `LLM_API_KEY` privately.
- `TYPESAFE_API_KEY`: the same jev account/model/proxy used by moderation.
- `ADMIN_TELEGRAM_USER_IDS`, `ADMIN_BALE_USER_IDS`: comma-separated positive
  numeric **user** IDs, not channel IDs. Each admin must first open/start the
  corresponding bot privately. Group messages and unlisted users are ignored.
- At least one working search/research server in `mcp_servers.json`.
- Installed Google Chrome, a persistent profile authenticated with `x-login`,
  and a reachable `X_PROXY_URL` (blank inherits `TELEGRAM_PROXY_URL`).

Defaults: `X_TIMEZONE=Asia/Tehran`, `X_ACTIVE_HOURS=09:00-23:30`,
`X_MAX_POSTS_PER_DAY=400`, sessions 10–25 minutes, breaks 30–90 minutes,
`CURATOR_RELEVANCE_THRESHOLD=0.7`, `TOPIC_DEDUP_DAYS=30`,
`TOPIC_DEDUP_CANDIDATES=8`, `TOPIC_SAME_THRESHOLD=0.8`,
`LLM_MAX_TOOL_ROUNDS=8`, `CHANNEL_SIGNATURE='🆔 @asrgooyeshpardaz'`.
Hours are inclusive at the start and exclusive at the end; overnight ranges
are supported. Reading stops outside the hours, at the daily capture quota,
or when paused. Daily counts use the configured timezone and survive restarts.
Explicit `curate-test` input can bypass the automatic reader quota and hours.

`X_BROWSER_MEMORY_MB=1536` is a conservative summed-RSS watchdog for the whole
browser process tree, checked every two seconds. It can count shared pages
more than once. Set `0` to disable it. This is a watchdog budget, not a kernel
cgroup hard limit; short peaks between checks remain possible. Do not place a
small `MemoryMax` on the whole bridge service, since that can kill the bridge.
`X_BROWSER_WATCHDOG_SECONDS=90` kills a child that stops reporting progress.
Crashes/hangs/memory overruns restart with exponential backoff, starting at
15 seconds with jitter and capped around 30 minutes. Normal sessions use the
configured random breaks. Chrome remains headed and uses `channel="chrome"`,
with no custom user agent, stealth plugins or fingerprint overrides.

Chrome cannot use authenticated SOCKS proxies. If necessary, provide a local
unauthenticated SOCKS relay through `X_PROXY_URL`. HTTP proxy credentials are
supported. LLM and MCP HTTP use `TELEGRAM_PROXY_URL` only when
`LLM_USE_PROXY=true`; system proxy variables are ignored. For stdio servers,
this setting supplies explicit proxy environment variables, which the server
must support. Bale always connects directly. Media downloads use the X proxy.

## MCP servers and research

`mcp_servers.json` contains an initially empty `servers` object and inactive
examples. Copy the desired example **into `servers`** and replace tool names
with your server's actual search/read tool names. Install any stdio server's
own dependencies separately. Example:

```json
{
  "servers": {
    "research": {
      "type": "streamable_http",
      "url": "${SEARCH_MCP_URL}",
      "headers": {"Authorization": "Bearer ${SEARCH_MCP_API_KEY}"},
      "tools": ["search", "fetch"]
    }
  }
}
```

Or use `type: "stdio"`, `command`, `args`, and an `env` object such as
`{"SEARCH_API_KEY":"${SEARCH_API_KEY}"}`. Use absolute command/script paths
for systemd. `${NAME}` references are resolved exclusively from `.env`, never
from ambient shell variables. Missing references fail without printing values.
Keep keys out of the JSON file. Server stderr is discarded to avoid accidental
credential leakage. Only explicitly allowlisted tools, or tools annotated by
the server as read-only when no allowlist is supplied, are exposed to the LLM.

The official `mcp` SDK is pinned to the tested 1.26 transport API. Both stdio
and streamable HTTP support tool pagination and JSON Schema argument validation.
Research needs both search and document-reading capability: snippets alone are
not sufficient. The LLM must cite primary documents and supply supporting
verbatim evidence. The code rejects facts whose quote and URL do not occur in
tool output, rejects invented draft URLs/numerical claims, and withholds a
draft if no evidence is verified. These checks supplement editorial approval;
the LLM still makes judgments about whether a source is primary and whether a
claim follows from a passage.

Edit `src/channel_sync/curator_questions.json` to adjust the three relevance
questions, topic comparison and new-information scoring. Logs retain all jev
scores; SQLite retains per-post relevance scores. Persian style examples stay
in `curator/examples/*.md` and are treated as style examples, never evidence.
Restart after changing configuration or questions.

## Reader stops and deduplication

The reader observes the page's own timeline GraphQL responses and rendered DOM.
It issues no X API requests independently. Its automatic actions are limited
to navigating home/an explicitly supplied post URL, reading and scrolling. It
does not click reactions, follow, reply, compose posts or navigate to DMs.
Original post/quote media URLs and visible thread context are retained.
Only observed `pbs.twimg.com`/`video.twimg.com` media is downloaded, without
cookies. The first ten unique media items are selected for an album; every
source URL remains stored. Transfer limits follow the existing bridge: 10 MiB photos and 20 MiB
downloads; oversize/inaccessible media can be disabled in the preview.

Login walls, CAPTCHA/account challenges, authentication/rate-limit responses,
and unconfirmed signed-in state latch a persistent stop and alert both admin
lists. The child is killed before alert delivery. Restarting systemd does not
clear a challenge. Resolve it with `x-login`, then run `curator-resume` or use
`/curator resume`/the Resume button. Layout/schema changes may require updating
the parser; uncertain login detection stops conservatively.

Every captured ID is unique in `curator_posts`. Source arrival never creates a
second draft for a reserved topic. Exact keys for explicit **versioned release**
events remain permanent; other exact keys and semantic comparisons expire after
30 days by default, including rejected/pending/published topics. Expiration is
measured from topic creation. Retained audit rows are not deleted on expiration.
Topic reservation and post-research rechecking use a serialized semantic check
and short `BEGIN IMMEDIATE` transactions. No SQL transaction spans an await.
Additional sources attach to the existing topic, with a regeneration offer
when jev detects substantive new information. Their media enters the reviewed
draft through regeneration; attaching a source never silently changes its preview.

## Approval and CLI

```bash
.venv/bin/channel-sync x-login
.venv/bin/channel-sync curate-test 'https://x.com/author/status/123456789'
.venv/bin/channel-sync curate-test 'An AI release announcement to research'
.venv/bin/channel-sync curator-stats
.venv/bin/channel-sync curator-resume
```

`curate-test` sends previews only to admins. With the service running, it
enqueues its input for that service; otherwise it processes the durable queue
locally and sends previews without starting any bot poller. Run the service
to receive button actions. Reusing a seen URL/identical manual input does not
create another draft; use Regenerate on its existing topic instead.

In either bot, `/curator`, `/curator pause`, `/curator resume`, and
`/curator pending` show status, today's counts and pending drafts. Preview
messages contain the full text/media plus a separate control message with:
Approve, Reject, Regenerate, Edit and Toggle media. Controls in both bots use
the same SQLite state. Old revisions cannot approve a changed draft. Edit
asks you to reply with complete corrected text. Regenerate asks for an optional
instruction; reply `/skip` for none. Turning media off leaves the original
source media stored and generates a text-only preview for publication.

Approval freezes the reviewed text, revision, media choice and source list for
both destinations. Later source discoveries cannot change an in-progress send.
Published-topic regeneration updates the admin preview only; it does not
republish an already published topic. Known partial failures expose Retry
publish for only the failed destination. Completed message parts are skipped
after restart/retry. Explicit rate-limit rejections use bounded client retries;
ambiguous timeouts/server responses become `uncertain` and require manual
reconciliation, following the bridge's existing send journal. There is no
blind retry button for an uncertain outcome.

`DRY_RUN=true` still reads, filters, researches, sends admin previews and accepts
decisions. It publishes nothing to channels. An approval in dry run is recorded
as `dry_run` and is not replayed after changing the setting. Use a new topic for
the first live approval. Existing bridge/moderation dry-run behavior is unchanged.
`health` checks admin configuration, proxy TCP connectivity, a small LLM chat,
MCP tool listing and a recent/fresh browser login observation. It makes no
channel publication. A fresh browser probe may detect/latch an X challenge.

## Ubuntu deployment with existing channel-sync.service

Run these steps in your deployed project directory, as the existing service's
non-root user. Confirm the directory/user using:

```bash
systemctl show channel-sync.service -p User -p WorkingDirectory -p ExecStart
```

Keep that service's existing `ExecStart=.../.venv/bin/channel-sync run` and
`WorkingDirectory`. Chrome requires its normal sandbox and a non-root user;
do not add `--no-sandbox`. Deploy this source tree while preserving private
`.env`, `data/` and your existing profile. Then:

```bash
sudo systemctl stop channel-sync.service
# Back up the existing database, including its WAL, using SQLite's backup API.
.venv/bin/python - <<'PY'
from datetime import datetime
from pathlib import Path
import sqlite3
from channel_sync.config import load_config
p = load_config(Path('.env')).database_path
backup = p.with_name(p.name + '.backup-' + datetime.now().strftime('%Y%m%d%H%M%S'))
with sqlite3.connect(p) as source, sqlite3.connect(backup) as target:
    source.backup(target)
backup.chmod(0o600)
PY
sudo apt-get update
sudo apt-get install -y xvfb xauth x11vnc ca-certificates wget tzdata
# Ubuntu amd64: install the real Google Chrome if it is not already installed.
wget -O /tmp/channel-sync-google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt-get install -y /tmp/channel-sync-google-chrome.deb
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -e . --no-deps
.venv/bin/python -m playwright install-deps chrome
.venv/bin/python -m unittest discover -s tests -q
chmod 600 .env
```

The Playwright dependency installer may request sudo for operating-system
packages. Chrome's official Linux package here is amd64. There is no bundled
Chromium or headless fallback. Fill `.env` and `mcp_servers.json` as described
above; set `CURATOR_ENABLED=true`, initially `DRY_RUN=true`. Settings paths are
relative to the `.env` directory. The default profile stays under gitignored
`data/`; if you choose another location, exclude it from source control too.
Keep `X_PROXY_URL` reachable on the server itself.

Install the supplied drop-in (it preserves the existing executable/user/path):

```bash
sudo mkdir -p /etc/systemd/system/channel-sync.service.d
sudo install -m 644 deploy/channel-sync-curator.conf /etc/systemd/system/channel-sync.service.d/curator.conf
sudo systemctl daemon-reload
```

Log in using the headless-server instructions below, then:

```bash
.venv/bin/channel-sync curator-resume
.venv/bin/channel-sync check-config
.venv/bin/channel-sync health --output reports/health-curator.json
sudo systemctl start channel-sync.service
sudo journalctl -u channel-sync.service -f
```

Test with `curate-test` and inspect previews in both bots. When satisfied, set
`DRY_RUN=false` in `.env` and run `sudo systemctl restart channel-sync.service`.
Each channel publication still requires a bot approval. No deployment command
automatically publishes a test post to either channel.

## Manual x-login on a headless server

Stop the service to release the profile, and run login as **the same Unix user
that runs the service**. Xvfb provides a display but cannot itself show you the
login form. Use a localhost-only VNC server over an SSH tunnel:

1. On your local computer, keep this tunnel open in a terminal:

   ```bash
   ssh -N -L 5901:127.0.0.1:5901 YOUR_SERVICE_USER@YOUR_SERVER
   ```

2. In a separate SSH session on the server, in the project directory:

   ```bash
   sudo systemctl stop channel-sync.service
   mkdir -p ~/.vnc
   x11vnc -storepasswd ~/.vnc/passwd
   chmod 600 ~/.vnc/passwd
   xvfb-run -a -s '-screen 0 1280x900x24 -nolisten tcp' bash -c '
     x11vnc -display "$DISPLAY" -localhost -rfbport 5901 -rfbauth "$HOME/.vnc/passwd" -forever -shared >/tmp/channel-sync-vnc.log 2>&1 &
     vnc_pid=$!
     trap "kill $vnc_pid 2>/dev/null || true" EXIT
     .venv/bin/channel-sync x-login
   '
   ```

   Choose a separate VNC password when prompted; this is not your X password.

3. Connect your local VNC viewer to `127.0.0.1:5901` (some viewers use
   `127.0.0.1::5901`). Log into X manually in the visible Chrome window and
   finish any verification. Decline any password-saving prompt. Close **all Chrome windows** to save the profile
   and finish `x-login`. Passwords are never collected by channel-sync; the
   Chrome profile contains the session login and must remain private.

4. On the server run `curator-resume`, then `health`, then start the service
   as shown above. Close the VNC viewer and SSH tunnel. Normal service sessions
   automatically get their own headed Xvfb display and reuse the saved profile.

An alternative is SSH X11 forwarding (`ssh -Y`) with a working local X server;
then run `.venv/bin/channel-sync x-login` directly in that forwarded session.

## API references

- [Telegram Bot API](https://core.telegram.org/bots/api#sendphoto): 1,024-character
  captions, 4,096-character messages, and media group limits.
- [Bale Bot API](https://docs.bale.ai/): `sendPhoto`/`sendVideo` allow 4,096-character
  captions; `InputMediaPhoto`/`InputMediaVideo` specify 1,024 for album captions.
  `sendMessage` specifies 4,096. The sender conservatively counts UTF-16 units
  so emoji never split or overflow the Telegram limits.
- [Bale official Swagger](https://tapi.bale.ai/swagger/swagger.json): callback
  updates, inline controls and `editMessageText` request shapes.
- [Playwright persistent contexts](https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch-persistent-context).
- [Official MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk/tree/v1.26.0).
- [OpenAI function calling](https://developers.openai.com/api/docs/guides/function-calling).
