# Automated live batch: CS-20261007T075748

**Bridge stopped. 20/21 cases passed the API checks; the final source publication was rejected. Visual review is pending.**

The user authorized automatic publication for B and C without individual confirmations. B enabled only Bale → Telegram; C enabled both directions. All source fixtures and mirrored messages were published by the bridge bots. Explicit durable test jobs used actual source send-response IDs, with no exception to the own-bot filter or echo suppression. Human-post admission through polling was not exercised in these automated batches.

Telegram source channel: `-1004472119156` (`AGP_Test_Channel`). Bale source channel: `5716375015` (`agp_test_two`). Successful labels should appear once in each channel, subject to visual confirmation. Album items use `/1` and `/2`.

| Phase | Source | Exact case label | Source response IDs | Mirror response IDs | API result |
| --- | --- | --- | --- | --- | --- |
| B | Bale | `CS-20261007T075748-B-BT-plain` | 68 | 62 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| B | Bale | `CS-20261007T075748-B-BT-formatted` | 70 | 63 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| B | Bale | `CS-20261007T075748-B-BT-photo` | 72 | 64 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| B | Bale | `CS-20261007T075748-B-BT-video` | 73 | 65 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| B | Bale | `CS-20261007T075748-B-BT-document` | 75 | 66 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| B | Bale | `CS-20261007T075748-B-BT-photo-album` | 76, 79 | 67, 68 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| B | Bale | `CS-20261007T075748-B-BT-mixed-album` | 80, 81 | 69, 70 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Telegram | `CS-20261007T075748-C-TB-plain` | 71 | 83 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Telegram | `CS-20261007T075748-C-TB-formatted` | 72 | 84 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Telegram | `CS-20261007T075748-C-TB-photo` | 73 | 86 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Telegram | `CS-20261007T075748-C-TB-video` | 74 | 87 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Telegram | `CS-20261007T075748-C-TB-document` | 75 | 89 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Telegram | `CS-20261007T075748-C-TB-photo-album` | 76, 77 | 90, 93 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Telegram | `CS-20261007T075748-C-TB-mixed-album` | 78, 79 | 94, 97 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Bale | `CS-20261007T075748-C-BT-plain` | 98 | 80 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Bale | `CS-20261007T075748-C-BT-formatted` | 100 | 81 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Bale | `CS-20261007T075748-C-BT-photo` | 101 | 82 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Bale | `CS-20261007T075748-C-BT-video` | 102 | 83 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Bale | `CS-20261007T075748-C-BT-document` | 104 | 84 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Bale | `CS-20261007T075748-C-BT-photo-album` | 106, 107 | 85, 86 | One HTTP 200 publication and one HTTP 200 mirror; ordered mappings |
| C | Bale | `CS-20261007T075748-C-BT-mixed-album` | None recorded | None recorded | Source HTTP 400 UNSUPPORTED_FILE; no mirror attempt |

## Every published source item and its exact label

| Source channel | Exact text/caption label | Source response ID | Mirror response ID |
| --- | --- | --- | --- |
| Bale | `CS-20261007T075748-B-BT-plain` | 68 | 62 |
| Bale | `CS-20261007T075748-B-BT-formatted` | 70 | 63 |
| Bale | `CS-20261007T075748-B-BT-photo` | 72 | 64 |
| Bale | `CS-20261007T075748-B-BT-video` | 73 | 65 |
| Bale | `CS-20261007T075748-B-BT-document` | 75 | 66 |
| Bale | `CS-20261007T075748-B-BT-photo-album/1` | 76 | 67 |
| Bale | `CS-20261007T075748-B-BT-photo-album/2` | 79 | 68 |
| Bale | `CS-20261007T075748-B-BT-mixed-album/1` | 80 | 69 |
| Bale | `CS-20261007T075748-B-BT-mixed-album/2` | 81 | 70 |
| Telegram | `CS-20261007T075748-C-TB-plain` | 71 | 83 |
| Telegram | `CS-20261007T075748-C-TB-formatted` | 72 | 84 |
| Telegram | `CS-20261007T075748-C-TB-photo` | 73 | 86 |
| Telegram | `CS-20261007T075748-C-TB-video` | 74 | 87 |
| Telegram | `CS-20261007T075748-C-TB-document` | 75 | 89 |
| Telegram | `CS-20261007T075748-C-TB-photo-album/1` | 76 | 90 |
| Telegram | `CS-20261007T075748-C-TB-photo-album/2` | 77 | 93 |
| Telegram | `CS-20261007T075748-C-TB-mixed-album/1` | 78 | 94 |
| Telegram | `CS-20261007T075748-C-TB-mixed-album/2` | 79 | 97 |
| Bale | `CS-20261007T075748-C-BT-plain` | 98 | 80 |
| Bale | `CS-20261007T075748-C-BT-formatted` | 100 | 81 |
| Bale | `CS-20261007T075748-C-BT-photo` | 101 | 82 |
| Bale | `CS-20261007T075748-C-BT-video` | 102 | 83 |
| Bale | `CS-20261007T075748-C-BT-document` | 104 | 84 |
| Bale | `CS-20261007T075748-C-BT-photo-album/1` | 106 | 85 |
| Bale | `CS-20261007T075748-C-BT-photo-album/2` | 107 | 86 |

## Observations and limits

- Both health checks passed in both phases. B completed 60 seconds of observation after publishing; C stopped immediately on the final source API rejection and did not complete its final observation window.
- 25 source items and 25 mirror items have successful send responses: 20 source requests plus 20 mirror requests. Each successful case has one successful mirror request and one persisted target mapping per item. These responses cannot independently prove channel history contains no visual duplicates.
- All 16 observed Bale updates for these fixtures were consumed by expected content signatures; no Telegram echoes were observed. No ordinary forward job, return forward, circuit pause, or unresolved send occurred. The configured breaker stayed at 20 forwarded items per direction per 60 seconds.
- The rejected label is `CS-20261007T075748-C-BT-mixed-album`, with planned captions ending `/1` and `/2`. Its single Bale `sendMediaGroup` call returned HTTP 400 `UNSUPPORTED_FILE`; no source response IDs or destination sends were recorded. API observations cannot rule out a partial server-side write, so include this label in the final channel inspection.
- No plain-text or separate-item fallback was attempted, in accordance with the strict stop-on-permanent-error instruction. Both earlier mixed-album cases succeeded.
- Both Bale → Telegram formatted responses contain bold, italic, and text-link entities with correct UTF-16 slices after an emoji. The Telegram → Bale response retains the expected Markdown. No formatting rejection occurred.
- Bale source album send responses omit media/file metadata. These three successful album cases reused the known original upload bytes for forwarding, so source-album downloads were not exercised. All nine successful single-media cases used actual source API downloads; both Telegram-source albums downloaded every source item.
- Album responses from Bale also omit captions; their outgoing request captions are retained in SQLite, while visual caption rendering remains for user review.
- API response message IDs differ from anonymous Bale echo IDs. The manifest records response IDs; signatures rather than IDs suppressed echoes.

## Evidence

- Batch report: `reports/live-batch-2026-10-07-07-57-48.json`.
- Per-case audit: `reports/live-batch-2026-10-07-07-57-48-audit.json`.
- Raw phase responses and updates: `reports/live-batch-2026-10-07-07-57-48-phase-b.json`.
- Raw phase responses and updates: `reports/live-batch-2026-10-07-07-57-48-phase-c.json`.
- Offline suite: 107 tests. Video fixture: 2-second 64×64 H.264 MP4, 11,122 bytes; photos: distinct blue/orange 128×128 PNGs; documents: uniquely labeled UTF-8 text files.
