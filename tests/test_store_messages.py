from pathlib import Path
import sqlite3
import tempfile
import unittest

from channel_sync.cli import process_lock
from channel_sync.inspection import probe_specs
from channel_sync.messages import content_signature, loop_reason, own_author, utf16_length, utf16_slice
from channel_sync.redaction import Redactor
from channel_sync.store import CircuitOpen, Store
from helpers import TELEGRAM_TOKEN


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.sqlite3"
        self.store = Store(self.path, Redactor(TELEGRAM_TOKEN))
        self.addCleanup(self.store.close)

    def prepare(self, key="probe", *, now=100, signature="signature"):
        self.store.prepare_send(send_key=key, platform="bale", chat_id=5716375015,
                                method="sendMessage", request={"text": "hello", "debug": TELEGRAM_TOKEN},
                                signatures=[signature], ttl=120, now=now)

    def claim(self, key="probe", *, now=100, max_sends=20):
        self.store.claim_send(key, window=60, max_sends=max_sends, ttl=120, now=now)

    def test_update_batch_and_offset_are_atomic_and_deduplicated(self):
        updates = [{"update_id": 7, "message": {"text": TELEGRAM_TOKEN}}, {"update_id": 3}]
        self.assertEqual(self.store.ingest("bale", "sync", updates), 2)
        self.assertEqual(self.store.ingest("bale", "sync", updates), 0)
        self.assertEqual(self.store.offset("bale", "sync"), 8)
        self.store.ingest("bale", "sync", [{"update_id": 2}])
        self.assertEqual(self.store.offset("bale", "sync"), 8)
        row = self.store.connection.execute("SELECT raw_json FROM processed_updates WHERE update_id=7").fetchone()[0]
        self.assertNotIn(TELEGRAM_TOKEN, row)

    def test_malformed_update_rolls_back_whole_batch(self):
        with self.assertRaises(ValueError):
            self.store.ingest("telegram", "sync", [{"update_id": 5}, {"update_id": "invalid"}])
        self.assertIsNone(self.store.offset("telegram", "sync"))
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM processed_updates").fetchone()[0], 0)

    def test_scopes_do_not_mix_offsets(self):
        self.store.ingest("telegram", "inspect:one", [{"update_id": 42}])
        self.assertIsNone(self.store.offset("telegram", "sync"))
        self.assertIsNone(self.store.offset("bale", "inspect:one"))

    def test_send_claim_and_recovery_prevent_replay(self):
        self.prepare()
        with self.assertRaises(sqlite3.IntegrityError):
            self.prepare()
        self.claim()
        self.assertEqual(self.store.recover_uncertain(), 1)
        with self.assertRaises(ValueError):
            self.claim()
        row = self.store.connection.execute("SELECT status,request_json FROM sent_messages").fetchone()
        self.assertEqual(row["status"], "uncertain")
        self.assertNotIn(TELEGRAM_TOKEN, row["request_json"])

    def test_successful_completion_and_mapping_commit_together(self):
        self.prepare()
        self.claim()
        mapping = {"source_platform": "telegram", "source_chat_id": -1004472119156,
                   "source_message_id": 1, "target_platform": "bale", "target_chat_id": 5716375015,
                   "target_message_id": 25}
        self.store.finish_send("probe", status="sent", response={"ok": True}, mappings=[mapping])
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM message_mapping").fetchone()[0], 1)
        with self.assertRaises(ValueError):
            self.claim()

    def test_mapping_failure_rolls_back_send_completion(self):
        self.prepare()
        self.claim()
        mapping = {"source_platform": "INVALID", "source_chat_id": 1,
                   "source_message_id": 1, "target_platform": "bale", "target_chat_id": 2,
                   "target_message_id": 3}
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.finish_send("probe", status="sent", mappings=[mapping])
        self.assertEqual(self.store.connection.execute("SELECT status FROM sent_messages").fetchone()[0], "sending")

    def test_signature_ignores_failed_sends_and_expires(self):
        self.prepare()
        self.assertFalse(self.store.matches_sent("bale", 5716375015, "signature", now=100))
        self.claim()
        self.assertTrue(self.store.matches_sent("bale", 5716375015, "signature", now=110))
        self.assertFalse(self.store.matches_sent("bale", 5716375015, "signature", now=221))
        self.assertFalse(self.store.matches_sent("telegram", 5716375015, "signature", now=110))
        self.store.finish_send("probe", status="failed", error={"code": 400})
        self.assertFalse(self.store.matches_sent("bale", 5716375015, "signature", now=110))

    def test_individual_album_signatures_are_logged(self):
        self.store.prepare_send(send_key="album", platform="bale", chat_id=1, method="sendMediaGroup",
                                request={}, signatures=["photo1", "photo2"], ttl=120, now=100)
        self.store.claim_send("album", window=60, max_sends=20, ttl=120, now=100)
        for signature in ("photo1", "photo2"):
            self.assertTrue(self.store.matches_sent("bale", 1, signature, now=101))

    def test_circuit_breaker_latches_and_survives_reopen(self):
        self.prepare("one")
        self.claim("one", max_sends=1)
        self.prepare("two")
        with self.assertRaises(CircuitOpen):
            self.claim("two", now=101, max_sends=1)
        second = Store(self.path)
        try:
            with self.assertRaises(CircuitOpen):
                second.claim_send("two", window=60, max_sends=1, ttl=120, now=1000)
        finally:
            second.close()

    def test_second_process_lock_is_rejected(self):
        with process_lock(self.path):
            with self.assertRaises(RuntimeError):
                with process_lock(self.path):
                    self.fail("Should not acquire second lock")

    def test_changed_ids_still_trigger_logged_signature_layer(self):
        original = {"photo": [{"file_id": "old"}], "caption": "unique probe", "message_id": 25}
        echo = {"photo": [{"file_id": "new"}], "caption": "unique probe", "message_id": 26,
                "from": {"id": 0, "is_bot": False}, "sender_chat": {"id": 5716375015}}
        self.prepare(signature=content_signature(original, "bale"))
        self.claim()
        self.assertEqual(loop_reason(self.store, "bale", 5716375015, echo, 123, now=101),
                         "recent sent content signature")


class MessageTests(unittest.TestCase):
    def test_utf16_entity_offsets_after_emoji(self):
        text = "😀 bold"
        self.assertEqual(utf16_length(text), 7)
        self.assertEqual(utf16_slice(text, 3, 4), "bold")
        with self.assertRaises((ValueError, UnicodeDecodeError)):
            utf16_slice(text, 1, 1)

    def test_formatted_probe_uses_correct_utf16_offsets(self):
        probe = probe_specs("test", "telegram")[1]
        self.assertEqual([utf16_slice(probe["text"], e["offset"], e["length"]) for e in probe["entities"]],
                         ["bold", "italic", "link"])

    def test_signature_normalizes_observed_markdown(self):
        telegram = {"text": "😀 bold italic link", "entities": [{"type": "bold"}]}
        bale = {"text": "😀 *bold* _italic_ [link](https://example.com/)"}
        self.assertEqual(content_signature(telegram, "telegram"), content_signature(bale, "bale"))

    def test_sender_chat_and_is_bot_do_not_identify_our_bot(self):
        human = {"from": {"id": 0, "is_bot": False}, "sender_chat": {"id": 1}}
        other = {"from": {"id": 2, "is_bot": True}}
        self.assertIsNone(own_author(human, 123))
        self.assertIsNone(own_author(other, 123))
        self.assertEqual(own_author({"from": {"id": 123}}, 123), "from.id")
        self.assertEqual(own_author({"via_bot": {"id": 123}}, 123), "via_bot.id")
