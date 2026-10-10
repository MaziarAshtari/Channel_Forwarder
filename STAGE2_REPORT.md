# Stage-two implementation and supervised test report

Implemented on 2026-10-07. **107 offline tests passed. All seven phase-A cases
passed with visual confirmation. The user authorized automatic batches for B
and C: 20/21 cases passed API checks, and the final Bale-source mixed album was
rejected with HTTP 400 `UNSUPPORTED_FILE`. The bridge stopped immediately;
visual confirmation for B and C is pending.**

New modules: `formatting.py`, `media.py`, `signatures.py`, `bridge_store.py`,
and `bridge.py`. Updated the API clients, SQLite schema, CLI, configuration,
example environment, redaction, and README. Added FORWARDING.md, three test
modules, and the redacted real Bale album capture in
`tests/fixtures/bale_album_echo.json`.

Implemented behavior:

- Bidirectional bold/italic/link conversion with UTF-16 offsets, literal Markdown
  escaping, Telegram entities, and one plain-text fallback for explicit format
  rejection.
- Text, photo, video, document, and ordered album forwarding. Albums wait three
  seconds after the last item. Bale's explicit `UNSUPPORTED_FILE` album rejection
  permits one separate-message fallback pass.
- Before sending, persist counted expected signatures for every item. Normalize
  visible text/caption and whitespace, media type, and stable media properties;
  exclude message/file IDs. Consume matches before queueing any forward. Own-bot
  authors are ignored too. The real anonymous Bale album echoes are regression
  fixtures and both are suppressed.
- Independent persistent circuit breakers, durable inbox/album jobs, deterministic
  send intents, atomic send completion/mapping, and safe restart behavior.
- `run`, offline `status`, phase-specific direction overrides, dry run, private
  redacted run reports, and stop-on-error mode for supervised tests.

`.env` direction names were upgraded to `TG_TO_BALE` and `BALE_TO_TG` while
preserving their existing values. Signature TTL is now 600 seconds and album
wait is 3 seconds. Legacy direction names remain accepted. Normal `.env` has
both directions enabled, but phase A overrides that locally to Telegram → Bale
only. The approved phase-A worker is now running.

Offline results:

- **95 tests passed**: converters, escaping, signatures and counts, actual Bale
  capture, album ordering, unsupported mixed-album fallback, HTTP request shapes,
  oversized files, own authors, both directions with anonymous echoes, dry run,
  initial backlog isolation, circuit persistence/isolation, operational file-handle
  recovery, crash after send completion, and crash between fallback items.
- `pip check` and source compilation passed.
- Wheel build passed; all forwarding modules, schema, and photo assets were
  included, with `.env` excluded.
- Configuration validation passed. Both new CLI help and offline status work.
- Real bot tokens were absent from source, tests, reports, and SQLite artifacts
  checked. Existing stage-one reports remain intact.
- Every API request in offline tests used a stub or HTTPX MockTransport. The
  supervised live phase is recorded separately below.

## Live results

API evidence and visual confirmation will be recorded separately. A successful
send response alone cannot prove the channel shows exactly one copy with correct
rendering. Wait for `Bridge ready` before posting; test one case at a time and
stop on any issue.

Phase A was approved and started on 2026-10-07. Health passed for both bots, and
both pollers were ready at `2026-10-07T06:29:35Z`. Runtime flags are
`TG_TO_BALE=true`, `BALE_TO_TG=false`, `DRY_RUN=false`. There were no startup
errors or send attempts at readiness. Live evidence is checkpointed in
`reports/phase-a.json`; the worker stops on the first forwarding problem.

After the laptop restarted, the user authorized resuming phase A. SQLite retained
all three completed jobs, with no interrupted sends or paused directions. The
bridge resumed from its saved offsets, passed both health checks, and became
ready at `2026-10-07T07:17:28Z` with the same phase-A flags. New evidence is saved
in `reports/phase-a-resumed-2026-10-07-07-17-18.json`; the original report is
preserved.

**A / plain text: passed.** The user posted `S2-A-plain` and confirmed exactly
one Bale copy with no extra returned Telegram copy. Telegram message 53 produced
one successful `sendMessage` attempt (Bale response ID 55). Bale echoed it as
message 56 / update 32 with `from.id=0`, `is_bot=false`; the expected signature
was consumed and the echo was ignored before queueing. The mapping was persisted,
the forward job is `sent`, the echo count is zero, and neither circuit is paused.
The phase-A worker remains running without errors. Per-case evidence and user
confirmation are saved in `reports/phase-a-cases.json`.

**A / formatted text: passed.** The user posted
`S2-A-formatted 😀 bold italic link` and confirmed correct formatting, exactly
one Bale copy, and no extra returned Telegram copy. Telegram message 54 contained
bold, italic, and text-link entities; their UTF-16 ranges were verified after
the emoji. One successful send returned Bale message ID 57 with the expected
Markdown. No plain-text fallback was used. No formatted echo was captured at
confirmation; its expected signature remains available until the TTL expires.

**A / photo with caption: passed.** Telegram message 55 contained `S2-A-photo`.
The largest photo (756×398, reported size 32,114 bytes) was forwarded with one
successful `sendPhoto` attempt, returning Bale message ID 58. Caption and
dimensions were verified in the send response. The user confirmed one correctly
shown photo with the same caption and no extra returned Telegram copy. No photo
echo was captured at confirmation. The job and mapping are persisted, and both
circuits remain unpaused.

**A / video with caption: passed.** A queued Telegram
post (message 56, `S2-A-video`) was recovered after restart and forwarded in one
successful `sendVideo` attempt (Bale response ID 59). The response preserves the
caption, 854×432 dimensions, and reported size 14,241,138 bytes. Bale echoed it
as message 60 / update 33; its expected signature was consumed and ignored. The
job and mapping are persisted, and both circuits remain unpaused. The user
confirmed one correctly shown, playable Bale video with the same caption and no
extra returned Telegram copy.

**A / document: passed.** Telegram message 57 contained a 328,605-byte PDF with
caption `S2-A-document`. One successful `sendDocument` attempt returned Bale
message ID 61, preserving the caption, filename, MIME type, and reported size.
The user confirmed one correctly shown document that opens, with the same
caption and no extra returned Telegram copy. No document echo was captured at
confirmation; its expected signature was recorded before sending. The job and
mapping are persisted, and both circuits remain unpaused.

**A / photo album: passed.** Telegram messages 58 and 59 shared one album ID;
the first caption was `S2-A-photo-album`. The durable buffer waited at least
three seconds before processing, and one successful `sendMediaGroup` attempt
returned Bale message IDs 62 and 63 in source order. No fallback was needed.
Bale's response contained message IDs and its album ID but omitted media and
captions; the persisted request and user confirmation verify the caption. The
user confirmed both photos once, in order, with no extra returned Telegram
copies. Only the second item echoed at confirmation (message 64 / update 34,
with both photo and document fields and `from.id=0`, `is_bot=false`); its
signature was consumed. Both mappings are persisted and neither circuit is
paused.

**A / mixed photo/video album: passed.** Telegram messages 60 and 61 contained
a photo followed by a video, sharing one album ID, with `S2-A-mixed-album` on
the first item. One successful `sendMediaGroup` attempt returned Bale message
IDs 65 and 66 in source order. Bale accepted this mixed album directly; the
`UNSUPPORTED_FILE` fallback was not exercised live. The user confirmed each
item once, in order, with the caption preserved and no extra returned Telegram
copies. The video echoed as message 67 / update 35 with `from.id=0`,
`is_bot=false` and both video and document fields. Its dimensions stayed
854×432 while duration changed from 120 to 119 seconds. The content signature
matched and was consumed before the send response was recorded, confirming
that suppression was active during the request. The first item did not echo
at confirmation. Both mappings are persisted and neither circuit is paused.

**Phase A completed: 7/7 cases passed.** There were seven successful forwarding
requests and nine mapped items, with four observed Bale echoes suppressed and
zero return forwards. The laptop restart did not replay any of the three
earlier completed jobs. The bridge stopped at `2026-10-07T07:30:22Z`, with
seven `sent` jobs, no pending or unresolved sends, no forwarding errors, and
both circuits unpaused. Per-case evidence and user confirmations are saved in
`reports/phase-a-cases.json`; raw send responses and updates remain in the two
phase-A session reports. Phase B will enable only Bale → Telegram after the
user approves it.

| Case | A: Telegram → Bale | B: Bale → Telegram | C: both directions |
| --- | --- | --- | --- |
| Plain text | Passed: one send, echo consumed, user confirmed | API passed; visual pending | API passed from both sources; visual pending |
| Bold, italic, link, emoji | Passed: formatting verified, one send, user confirmed | API passed; entities verified; visual pending | API passed from both sources; formatting verified; visual pending |
| Photo + caption | Passed: one photo, caption/dimensions verified, user confirmed | API passed; visual pending | API passed from both sources; visual pending |
| Video + caption | Passed: one send, echo consumed, user confirmed | API passed; visual pending | API passed from both sources; visual pending |
| Document | Passed: one send, file properties verified, user confirmed | API passed; visual pending | API passed from both sources; visual pending |
| Photo album | Passed: one album send, ordered items, echo consumed, user confirmed | API passed; visual pending | API passed from both sources; visual pending |
| Mixed photo/video album | Passed: one mixed album send, ordered items, echo consumed, user confirmed | API passed; visual pending | Telegram source API passed; Bale source publication HTTP 400 UNSUPPORTED_FILE |

The user later authorized both remaining phases and automatic fixture publication
without intermediate confirmations. The batch run `CS-20261007T075748` published
25 source items and 25 mirror items in 20 successful cases, with one publication
and one mirror request per case. It ignored 16 anonymous Bale echoes and
created no ordinary or return forwarding jobs. Both health checks passed for
both phases, and the configured circuit breaker stayed active without tripping.
The final Bale-source mixed album failed during source publication, before
queueing a forward. Its only request returned HTTP 400 `UNSUPPORTED_FILE`.
The batch attempted no fallback and stopped at `2026-10-07T08:00:52Z`, with no
pending jobs or unresolved sends. Phase B completed its final 60-second echo
observation; phase C stopped before that final observation window.

All fixtures were bot-published, with explicit durable test jobs using actual
source response IDs. Own-bot filtering and signature suppression remained
enabled, so these automated batches did not exercise human-post admission.
Bale-source album responses omit attachment metadata; their three successful
album cases reused the known uploaded bytes rather than downloading source
album files. Single-media cases and Telegram-source albums used API downloads.
These distinctions and the exact labels, source channels, message mappings,
formatting observations, and rejected case are recorded in
[STAGE2_LIVE_BATCH_REPORT.md](STAGE2_LIVE_BATCH_REPORT.md). B/C channel rendering
and visual copy counts remain for the user's final inspection.

Prepared commands:

```bash
.venv/bin/channel-sync run --phase a --new-posts-only --stop-on-error --output reports/phase-a.json
.venv/bin/channel-sync run --phase b --new-posts-only --stop-on-error --output reports/phase-b.json
.venv/bin/channel-sync run --phase c --new-posts-only --stop-on-error --output reports/phase-c.json
```

For A, post manually in Telegram `-1004472119156`, using unique `S2-A-*`
text/caption labels. For B, post in Bale `5716375015` with `S2-B-*` labels.
For C, test each case from each platform using distinct labels. The full checklist
and expected confirmation are in FORWARDING.md.

## Material limits

Lost send responses are recorded as uncertain and pause their direction;
they are never blindly retried. This prevents automatic duplicate replay but
may leave a delivery needing manual reconciliation. Identical human content
during the echo TTL can be indistinguishable from an expected bot echo and be
suppressed. Phase A validated photo and video dimensions as stable signature
properties in live echoes; video duration changed and is excluded. No document
echo was observed in phase A, so document echo matching remains verified offline.
Source-to-target mappings retain API send-response IDs and are not loop-detection
keys, because Bale can use different IDs in its echoes.
