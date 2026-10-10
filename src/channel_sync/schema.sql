PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY CHECK (version = 1)
);
INSERT OR IGNORE INTO schema_version VALUES (1);

CREATE TABLE IF NOT EXISTS poll_offsets (
    platform TEXT NOT NULL CHECK (platform IN ('telegram', 'bale')),
    scope TEXT NOT NULL,
    next_offset INTEGER NOT NULL CHECK (next_offset >= 0),
    PRIMARY KEY (platform, scope)
);
CREATE TABLE IF NOT EXISTS processed_updates (
    platform TEXT NOT NULL CHECK (platform IN ('telegram', 'bale')),
    scope TEXT NOT NULL,
    update_id INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'received'
        CHECK (status IN ('received', 'ignored', 'processed')),
    received_at REAL NOT NULL,
    PRIMARY KEY (platform, scope, update_id)
);
CREATE TABLE IF NOT EXISTS sent_messages (
    send_key TEXT PRIMARY KEY,
    platform TEXT NOT NULL CHECK (platform IN ('telegram', 'bale')),
    chat_id INTEGER NOT NULL,
    method TEXT NOT NULL,
    request_json TEXT NOT NULL,
    signature TEXT NOT NULL,
    signature_expires_at REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('prepared', 'sending', 'sent', 'failed', 'uncertain')),
    created_at REAL NOT NULL,
    attempted_at REAL,
    response_json TEXT,
    error_json TEXT
);
CREATE INDEX IF NOT EXISTS sent_signature_idx
    ON sent_messages (platform, chat_id, signature, signature_expires_at);
CREATE TABLE IF NOT EXISTS send_attempts (
    send_key TEXT NOT NULL REFERENCES sent_messages(send_key),
    attempt INTEGER NOT NULL,
    recorded_at REAL NOT NULL,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (send_key, attempt)
);
CREATE TABLE IF NOT EXISTS sent_signatures (
    send_key TEXT NOT NULL REFERENCES sent_messages(send_key),
    signature TEXT NOT NULL,
    expires_at REAL NOT NULL,
    PRIMARY KEY (send_key, signature)
);
CREATE INDEX IF NOT EXISTS individual_signature_idx ON sent_signatures(signature, expires_at);
CREATE TABLE IF NOT EXISTS message_mapping (
    source_platform TEXT NOT NULL CHECK (source_platform IN ('telegram', 'bale')),
    source_chat_id INTEGER NOT NULL,
    source_message_id INTEGER NOT NULL,
    source_media_group_id TEXT,
    target_platform TEXT NOT NULL CHECK (target_platform IN ('telegram', 'bale')),
    target_chat_id INTEGER NOT NULL,
    target_message_id INTEGER NOT NULL,
    send_key TEXT NOT NULL REFERENCES sent_messages(send_key),
    PRIMARY KEY (source_platform, source_chat_id, source_message_id, target_platform, target_message_id)
);
CREATE TABLE IF NOT EXISTS circuit_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    reason TEXT,
    paused_at REAL
);
INSERT OR IGNORE INTO circuit_state(singleton) VALUES (1);

-- Additive stage-two migration. Legacy diagnostics and offsets stay intact.
CREATE TABLE IF NOT EXISTS bridge_schema(version INTEGER PRIMARY KEY CHECK(version=2));
INSERT OR IGNORE INTO bridge_schema VALUES(2);
CREATE TABLE IF NOT EXISTS inbox_payloads (
    platform TEXT NOT NULL, scope TEXT NOT NULL, update_id INTEGER NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(platform,scope,update_id),
    FOREIGN KEY(platform,scope,update_id) REFERENCES processed_updates(platform,scope,update_id)
);
CREATE TABLE IF NOT EXISTS bridge_jobs (
    job_key TEXT PRIMARY KEY,
    source_platform TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('tg_to_bale','bale_to_tg')),
    state TEXT NOT NULL DEFAULT 'pending'
      CHECK(state IN ('pending','working','sent','failed','uncertain','skipped','dry_run')),
    created_at REAL NOT NULL, due_at REAL NOT NULL, finished_at REAL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS bridge_job_due ON bridge_jobs(state,direction,created_at);
CREATE TABLE IF NOT EXISTS bridge_items (
    platform TEXT NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
    update_id INTEGER NOT NULL, media_group_id TEXT,
    job_key TEXT NOT NULL REFERENCES bridge_jobs(job_key), message_json TEXT NOT NULL,
    PRIMARY KEY(platform,chat_id,message_id)
);
CREATE TABLE IF NOT EXISTS bridge_seen (
    platform TEXT NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
    update_id INTEGER NOT NULL, reason TEXT NOT NULL,
    PRIMARY KEY(platform,chat_id,message_id)
);
CREATE TABLE IF NOT EXISTS pending_echoes (
    send_key TEXT NOT NULL REFERENCES sent_messages(send_key),
    item_index INTEGER NOT NULL, platform TEXT NOT NULL, chat_id INTEGER NOT NULL,
    signature TEXT NOT NULL, remaining INTEGER NOT NULL DEFAULT 1 CHECK(remaining IN (0,1)),
    expires_at REAL NOT NULL,
    PRIMARY KEY(send_key,item_index)
);
CREATE INDEX IF NOT EXISTS echo_lookup ON pending_echoes(platform,chat_id,signature,expires_at,remaining);
CREATE TABLE IF NOT EXISTS direction_circuits (
    direction TEXT PRIMARY KEY CHECK(direction IN ('tg_to_bale','bale_to_tg')),
    paused INTEGER NOT NULL DEFAULT 0, reason TEXT, paused_at REAL
);
INSERT OR IGNORE INTO direction_circuits(direction) VALUES('tg_to_bale'),('bale_to_tg');
CREATE TABLE IF NOT EXISTS forward_events (
    job_key TEXT PRIMARY KEY REFERENCES bridge_jobs(job_key),
    direction TEXT NOT NULL, occurred_at REAL NOT NULL, item_count INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS forward_rate ON forward_events(direction,occurred_at);
