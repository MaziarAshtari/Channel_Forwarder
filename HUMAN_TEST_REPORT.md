# Human bidirectional polling test

**Bridge stopped after the complete three-minute observation. All five observed human posts (six items) forwarded once. No echo was forwarded back, no loop or circuit trip was detected, and no errors occurred.**

The process used normal polling with both directions enabled, no automatic source publication, and no controlled fixture jobs. Human source updates followed the normal inbox, album buffer, conversion, download, send, and mapping path. Own-bot filtering, counted content signatures, the configured circuit breaker, and immediate repeat-signature detection remained active.

The user reported: “Thanks, I tested it and I think it works well!” This is positive visual feedback for the posts tested; it does not supply evidence for cases absent from the captured updates.

| Real source | Observed case | Source message IDs | Destination response IDs | Method | Requests / result |
| --- | --- | --- | --- | --- | --- |
| Telegram | text | 87 | Bale: 109 | `sendMessage` | 1 / HTTP 200 |
| Bale | text | 111 | Telegram: 88 | `sendMessage` | 1 / HTTP 200 |
| Bale | text | 112 | Telegram: 89 | `sendMessage` | 1 / HTTP 200 |
| Telegram | photo | 90 | Bale: 113 | `sendPhoto` | 1 / HTTP 200 |
| Telegram | mixed album | 91, 92 | Bale: 114, 117 | `sendMediaGroup` | 1 / HTTP 200 |

## Observation and loop checks

- Bridge ready: `2026-10-07T08:17:42.235446+00:00`.
- Last accepted source update: `2026-10-07T08:20:45.390149+00:00`. The last source API message timestamp was `2026-10-07T08:20:45Z`.
- Observation completed: `2026-10-07T08:23:45.530317+00:00`; duration after the last source update: **180.14 seconds**.
- Process stopped: `2026-10-07T08:23:45.545100+00:00`. No bridge process remains running.
- Each source post has one successful send request and one persisted target mapping per item. The mixed album was sent once, with source video 91 and photo 92 mapped in request order to response IDs 114 and 117.
- Bale echoed the Telegram text as message 110 and both mixed-album items as messages 116 and 115. Their response IDs were 109, 114, and 117. Every echo matched a registered content signature and was ignored before queueing; all three arrived before their send completion was recorded.
- No photo echo or Telegram echo was observed. Unconsumed expected counts are not additional forwards.
- No extra forwarding job or successful send was observed for any echo. API message mappings and request counts cannot independently read channel history; the user supplied positive visual feedback.
- Both directions remain unpaused. No circuit trip occurred. The configured limit remained 20 forwarded items per direction per 60 seconds; observed rolling peaks were 3 Telegram → Bale items and 2 Bale → Telegram items.
- No temporary or permanent forwarding errors, retries, formatting fallbacks, or album fallbacks occurred. There are no pending jobs or unresolved sends.

## Coverage limits

The capture contains Telegram text, a photo, and a mixed video/photo album, plus two Bale text posts. No formatting entities or Bale Markdown were present in those source texts. The following planned cases were absent and are **not claimed as validated by this human test**:

- Telegram formatted text.
- Bale formatted text.
- Standalone video in either direction.
- Photo album in either direction.
- Human Bale-app photo/video posts and mixed album.

Earlier automated batches and offline tests have separate coverage and should not be treated as human Bale-app evidence. The earlier automated batch visual inspection was confirmed by the user; its rejected source-publication case remains rejected.

## Evidence

- Raw responses and real updates: `reports/human-bidirectional-2026-10-07-08-17-31.json`.
- Per-post audit: `reports/human-bidirectional-2026-10-07-08-17-31-audit.json`.
- SQLite retains processed updates, polling offsets, jobs, sent-message signatures, and all six item mappings.
- Offline verification: 113 tests passed after adding the supervised stop checks and observation timer.
