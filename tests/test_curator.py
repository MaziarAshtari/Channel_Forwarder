import asyncio
from contextlib import asynccontextmanager, redirect_stdout
from dataclasses import replace
from datetime import datetime
import json
import io
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import httpx

from channel_sync.bridge import Bridge
from channel_sync.bridge_store import SCOPE
from channel_sync.clients import ApiError, TelegramClient
from channel_sync.config import ConfigError, load_config
from channel_sync.cli import main, process_lock
from channel_sync.curator.mcp import ResearchMCP, load_servers
from channel_sync.curator.research import http_client
from channel_sync.curator.publisher import publication_parts, split_text
from channel_sync.curator.reader import (
    active_now,
    challenge_reason,
    parse_timeline,
    run_browser,
    browser_child,
    tweet_url,
)
from channel_sync.curator.research import LLM, research, verify_facts
from channel_sync.curator.service import Curator
from channel_sync.curator.store import CuratorStore
from channel_sync.curator.topics import find_duplicate, normalize_topic
from channel_sync.messages import utf16_length
from channel_sync.redaction import Redactor, config_secrets
from channel_sync.signatures import echo_signature
from helpers import config
from test_inspection_health import FakeAPIs


def topic(key="anthropic|claude-opus-5.5|release", **values):
    return normalize_topic(
        {
            "entity": "Anthropic Claude",
            "event": "release",
            "topic_key": key,
            "summary": "Anthropic released Claude Opus 5.5.",
            "specific_version": True,
            **values,
        }
    )


def post(ident="101", author="account_a", text="Claude Opus 5.5 released"):
    return {
        "id": ident,
        "url": f"https://x.com/{author}/status/{ident}",
        "author": author,
        "text": text,
        "created_at": "2026-10-20",
        "media": [],
        "external_links": [],
    }


class FakeBot:
    def __init__(self, platform, conf):
        self.platform, self.config = platform, conf
        self.redactor = Redactor(*config_secrets(conf))
        self.attempt_recorder = None
        self.calls = []
        self.fail_channel = False
        self.uncertain_channel = False
        self.next_id = 10

    async def send_content(self, chat_id, items):
        self.calls.append(("send_content", chat_id, items))
        if chat_id in {self.config.telegram_channel_id, self.config.bale_channel_id}:
            if self.fail_channel:
                raise ApiError(self.platform, "sendMessage", "Rejected", code=403)
            if self.uncertain_channel:
                raise ApiError(
                    self.platform, "sendMessage", "Response lost", uncertain=True
                )
        result = []
        for item in items:
            self.next_id += 1
            result.append({"message_id": self.next_id})
        return {"ok": True, "result": result if len(items) > 1 else result[0]}

    async def call(self, method, params, **kwargs):
        self.calls.append((method, params, kwargs))
        self.next_id += 1
        return {"ok": True, "result": {"message_id": self.next_id}}

    async def aclose(self):
        pass

    async def get_chat(self, chat_id):
        return {"ok": True, "result": {"id": chat_id, "type": "private"}}


class CuratorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.conf = config(
            self.root,
            curator_enabled=True,
            dry_run=False,
            admin_telegram_user_ids=(111,),
            admin_bale_user_ids=(222,),
            llm_base_url="https://llm.example/v1",
            llm_api_key="llm-test-key",
            llm_model="model",
            typesafe_api_key="jev-test-key",
            mcp_servers_path=self.root / "mcp.json",
            curator_examples_dir=self.root / "examples",
        )
        self.conf.mcp_servers_path.write_text(
            '{"servers":{"test":{"type":"http","url":"https://mcp.example","tools":["search","fetch"]}}}'
        )
        self.conf.curator_examples_dir.mkdir()
        (self.conf.curator_examples_dir / "1.md").write_text(
            "🧠 عنوان\n\nمتن فارسی\n🔗 منبع\n#هوش_مصنوعی"
        )
        self.store = CuratorStore(
            self.conf.database_path, Redactor(*config_secrets(self.conf))
        )
        self.addCleanup(self.store.close)
        self.bots = {p: FakeBot(p, self.conf) for p in ("telegram", "bale")}
        self.jev = AsyncMock()

        async def evaluate(state, questions):
            return {q: (0 if q == "is_same_topic" else 0.9) for q in questions}

        self.jev.evaluate.side_effect = evaluate
        self.llm = AsyncMock()
        self.llm.topic.return_value = topic()
        self.researcher = AsyncMock(
            return_value={
                "text": "🧠 خبر مهم\n\nمتن پیش‌نویس\n🔗 https://example.com\n#AI\n"
                + self.conf.channel_signature,
                "facts": [
                    {
                        "claim": "Verified fact",
                        "url": "https://example.com",
                        "quote": "Verified primary-source quote",
                    }
                ],
                "sources": ["https://example.com"],
            }
        )
        self.curator = Curator(
            self.conf,
            self.bots,
            self.store,
            jev=self.jev,
            llm=self.llm,
            researcher=self.researcher,
        )
        self.addAsyncCleanup(self.curator.aclose)

    def pending(self, ident="101", **changes):
        self.store.add_post(post(ident), self.conf)
        ident, _ = self.store.reserve(topic(**changes), ident, 30)
        self.store.save_draft(ident, topic(**changes), self.researcher.return_value)
        return self.store.topic(ident)

    def callback(self, platform, topic, action, *, user=None, update_id=1):
        chat = user or (111 if platform == "telegram" else 222)
        preview = self.store.connection.execute(
            "SELECT message_id FROM curator_previews WHERE topic_id=? AND platform=? AND chat_id=?",
            (topic["id"], platform, chat),
        ).fetchone()
        if action in {"replace", "rewrite", "back"}:
            preview = self.store.connection.execute(
                "SELECT message_id FROM curator_replies WHERE topic_id=? AND platform=? AND chat_id=?",
                (topic["id"], platform, chat),
            ).fetchone()
        return {
            "update_id": update_id,
            "callback_query": {
                "id": str(update_id),
                "from": {"id": chat},
                "data": f"c:{topic['id']}:{topic['revision']}:{action}",
                "message": {
                    "message_id": preview[0] if preview else 999,
                    "chat": {"id": chat, "type": "private"},
                },
            },
        }

    def channel_calls(self, platform):
        return [
            c
            for c in self.bots[platform].calls
            if c[0] == "send_content"
            and c[1] in {self.conf.telegram_channel_id, self.conf.bale_channel_id}
        ]

    def private_text(self, platform, text):
        chat = 111 if platform == "telegram" else 222
        return {
            "message": {
                "message_id": 999,
                "chat": {"id": chat, "type": "private"},
                "from": {"id": chat},
                "text": text,
            }
        }

    def assert_persian_lines(self, text, *, post=False):
        for line in text.splitlines():
            if post and line.lstrip().startswith(("#", "🔗")):
                continue
            first = next((char for char in line if char.isalpha()), None)
            if first:
                self.assertTrue("\u0600" <= first <= "\u06ff", line)

    async def test_duplicate_skip_is_silent_in_every_topic_state(self):
        existing = self.pending()
        await self.curator.approvals.sync_topic(existing["id"])
        calls_before = {p: len(bot.calls) for p, bot in self.bots.items()}
        states = (
            "researching",
            "pending",
            "approved",
            "published",
            "rejected",
            "error",
            "partial",
            "dry_run",
        )
        for index, state in enumerate(states, 200):
            with self.subTest(state=state):
                self.store.connection.execute(
                    "UPDATE curator_topics SET state=? WHERE id=?",
                    (state, existing["id"]),
                )
                candidate = post(str(index), "another_account")
                self.store.add_post(candidate, self.conf)
                with self.assertLogs(
                    "channel_sync.curator.service", level="INFO"
                ) as logs:
                    await self.curator.process_post(candidate)
                self.assertIn(
                    f"duplicate skipped topic={existing['id']} post={index}",
                    "\n".join(logs.output),
                )
                self.assertEqual(
                    [p["id"] for p in self.store.sources(existing["id"])], ["101"]
                )
        self.assertEqual(self.store.stats(self.conf)["today"]["duplicate"], len(states))
        self.assertEqual(
            calls_before, {p: len(bot.calls) for p, bot in self.bots.items()}
        )
        self.assertFalse(
            self.store.connection.execute(
                "SELECT 1 FROM curator_tasks WHERE kind='alert'"
            ).fetchone()
        )
        self.assertEqual(self.researcher.await_count, 0)
        self.assertTrue(
            all(
                set(c.args[1]) == {"is_ai_tech_news", "is_useful", "should_curate"}
                for c in self.jev.evaluate.call_args_list
            )
        )

    async def test_semantic_duplicate_also_skips_silently(self):
        existing = self.pending()
        self.store.decide(existing["id"], existing["revision"], "reject", "مدیر")
        self.llm.topic.return_value = topic("anthropic|opus-version-5.5|release")

        async def scores(state, questions):
            return {key: 0.95 for key in questions}

        self.jev.evaluate.side_effect = scores
        candidate = post("102", "another_account")
        self.store.add_post(candidate, self.conf)
        await self.curator.process_post(candidate)
        self.assertEqual(len(self.store.sources(existing["id"])), 1)
        self.assertEqual(self.store.stats(self.conf)["today"]["duplicate"], 1)
        self.assertFalse(any(bot.calls for bot in self.bots.values()))
        self.researcher.assert_not_awaited()

    async def test_edit_menu_replaces_next_message_in_either_bot(self):
        current = self.pending()
        await self.curator.approvals.sync_topic(current["id"])
        for platform in self.bots:
            with self.subTest(platform=platform):
                await self.curator.approvals.handle(
                    platform, self.callback(platform, current, "edit")
                )
                menu = [
                    c[1] for c in self.bots[platform].calls if c[0] == "sendMessage"
                ][-1]
                self.assertEqual(
                    [
                        b["text"]
                        for row in menu["reply_markup"]["inline_keyboard"]
                        for b in row
                    ],
                    ["✍️ ارسال متن جدید", "🤖 دستور بازنویسی", "↩️ بازگشت"],
                )
                await self.curator.approvals.handle(
                    platform, self.callback(platform, current, "replace")
                )
                await self.curator.approvals.handle(
                    platform, self.private_text(platform, "🧠 متن تازهٔ فارسی")
                )
                old = current
                current = self.store.topic(current["id"])
                self.assertIn("متن تازهٔ فارسی", current["text"])
                self.assertGreater(current["revision"], old["revision"])
                self.assertEqual(
                    self.store.connection.execute(
                        "SELECT COUNT(*) FROM curator_previews WHERE revision=?",
                        (current["revision"],),
                    ).fetchone()[0],
                    2,
                )
                other = "bale" if platform == "telegram" else "telegram"
                await self.curator.approvals.handle(
                    other, self.callback(other, old, "approve")
                )
                self.assertEqual(self.store.topic(current["id"])["state"], "pending")
                self.assertFalse(
                    self.store.connection.execute(
                        "SELECT 1 FROM curator_publications"
                    ).fetchone()
                )

    async def test_rewrite_instruction_produces_new_preview_and_requires_approval(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        await self.curator.approvals.handle(
            "bale", self.callback("bale", pending, "edit")
        )
        await self.curator.approvals.handle(
            "bale", self.callback("bale", pending, "rewrite")
        )
        instruction = "متن را کوتاه‌تر و ساده‌تر بنویس"
        await self.curator.approvals.handle(
            "bale", self.private_text("bale", instruction)
        )
        self.assertEqual(self.store.topic(pending["id"])["state"], "researching")
        for platform in self.bots:
            edits = [
                c[1] for c in self.bots[platform].calls if c[0] == "editMessageText"
            ]
            self.assertIn("در حال پژوهش و نگارش", edits[-1]["text"])
            self.assertEqual(edits[-1]["reply_markup"]["inline_keyboard"], [])
        await self.curator.approvals.handle(
            "bale", self.private_text("bale", instruction)
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_tasks WHERE kind='rewrite'"
            ).fetchone()[0],
            1,
        )
        self.researcher.return_value = {
            **self.researcher.return_value,
            "text": "خبر بازنویسی‌شده\nشناسه: " + self.conf.channel_signature,
        }
        await self.curator.pipeline_once()
        await self.curator.outbox_once()
        self.assertEqual(self.researcher.call_args.kwargs["instruction"], instruction)
        current = self.store.topic(pending["id"])
        self.assertEqual(current["state"], "pending")
        self.assertIn("بازنویسی‌شده", current["text"])
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_previews WHERE revision=?",
                (current["revision"],),
            ).fetchone()[0],
            2,
        )
        self.assertFalse(self.channel_calls("telegram") or self.channel_calls("bale"))

    async def test_edit_back_cancels_next_message_capture(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        for platform in self.bots:
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "edit")
            )
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "replace")
            )
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "back")
            )
            await self.curator.approvals.handle(
                platform, self.private_text(platform, "متن ناخواسته")
            )
        self.assertEqual(self.store.topic(pending["id"])["text"], pending["text"])
        self.assertFalse(
            self.store.connection.execute("SELECT 1 FROM curator_replies").fetchone()
        )

    async def test_cross_bot_edits_and_approval_cannot_reuse_old_revision(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        for platform in self.bots:
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "edit")
            )
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "replace")
            )
        await asyncio.gather(
            self.curator.approvals.handle(
                "telegram", self.private_text("telegram", "خبر نخست")
            ),
            self.curator.approvals.handle("bale", self.private_text("bale", "خبر دوم")),
        )
        current = self.store.topic(pending["id"])
        self.assertEqual(current["revision"], pending["revision"] + 1)
        self.assertIn("خبر نخست", current["text"])
        await self.curator.approvals.handle(
            "bale", self.callback("bale", pending, "approve")
        )
        self.assertEqual(self.store.topic(pending["id"])["state"], "pending")

    async def test_media_is_automatic_even_when_legacy_flag_was_disabled(self):
        pending = self.pending()
        self.store.connection.execute(
            "UPDATE curator_topics SET media_enabled=0 WHERE id=?", (pending["id"],)
        )
        media = [
            {
                "kind": "photo",
                "content": b"photo",
                "properties": {"width": 10, "height": 10},
            }
        ]
        with patch.object(
            self.curator.publisher, "media", new=AsyncMock(return_value=media)
        ):
            await self.curator.approvals.sync_topic(pending["id"])
            for bot in self.bots.values():
                self.assertTrue(
                    any(
                        c[0] == "send_content" and c[2][0]["kind"] == "photo"
                        for c in bot.calls
                    )
                )
            self.store.decide(pending["id"], pending["revision"], "approve", "مدیر")
            await self.curator.outbox_once()
        for platform in self.bots:
            self.assertEqual(self.channel_calls(platform)[0][2][0]["kind"], "photo")

    async def test_all_admin_ui_lines_and_buttons_start_with_persian(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        for platform in self.bots:
            chat = 111 if platform == "telegram" else 222
            await self.curator.approvals.status(platform, chat)
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "edit")
            )
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "replace")
            )
            await self.curator.approvals.handle(
                platform, self.private_text(platform, "متن " * 1500)
            )
            await self.curator.approvals.handle(
                platform, self.callback(platform, pending, "back")
            )
        await self.curator.event({"kind": "challenge", "reason": "rate limit exceeded"})
        await self.curator.admin_once()
        for bot in self.bots.values():
            for event in bot.calls:
                if event[0] == "send_content":
                    for item in event[2]:
                        self.assert_persian_lines(item["text"], post=True)
                elif event[0] in {"sendMessage", "editMessageText"}:
                    self.assert_persian_lines(event[1]["text"])
                    for row in (
                        event[1].get("reply_markup", {}).get("inline_keyboard", [])
                    ):
                        for button in row:
                            self.assert_persian_lines(button["text"])

    async def test_recover_refreshes_old_controls_and_discards_removed_workflow(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        self.store.connection.execute(
            "INSERT INTO curator_replies VALUES('telegram',111,999,?,?,?)",
            (pending["id"], pending["revision"], "regen"),
        )
        self.store.enqueue("regenerate", {"topic_id": pending["id"]})
        self.store.connection.execute(
            "UPDATE curator_topics SET state='researching' WHERE id=?", (pending["id"],)
        )
        self.store.recover()
        self.assertEqual(self.store.topic(pending["id"])["state"], "pending")
        self.assertFalse(
            self.store.connection.execute("SELECT 1 FROM curator_replies").fetchone()
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT state FROM curator_tasks WHERE kind='regenerate'"
            ).fetchone()[0],
            "cancelled",
        )
        await self.curator.outbox_once()
        markup = self.curator.approvals.buttons(self.store.topic(pending["id"]))
        self.assertEqual(len([b for row in markup["inline_keyboard"] for b in row]), 3)

    async def test_same_topic_from_different_accounts_creates_one_draft(self):
        for p in (post(), post("102", "different_account")):
            self.store.add_post(p, self.conf)
            await self.curator.process_post(p)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_topics"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(len(self.store.sources(1)), 1)
        self.assertEqual(self.researcher.await_count, 1)
        self.assertEqual(
            json.loads(
                self.store.connection.execute(
                    "SELECT scores_json FROM curator_posts WHERE id='102'"
                ).fetchone()[0]
            )["should_curate"],
            0.9,
        )

    async def test_semantic_dedup_includes_rejected_topics(self):
        existing = self.pending()
        self.store.decide(existing["id"], existing["revision"], "reject", "Admin")
        self.jev.evaluate.side_effect = None
        self.jev.evaluate.return_value = {"is_same_topic": 0.95}
        candidate = topic("anthropic|opus-v5.5|release")
        match = await find_duplicate(self.store, self.jev, self.conf, candidate)
        self.assertEqual(match["id"], existing["id"])

    async def test_nonversioned_key_expires_but_versioned_release_stays(self):
        persistent = self.pending()
        expiring = self.pending(
            "102",
            key="anthropic|claude-api|pricing",
            event="pricing",
            specific_version=False,
        )
        self.store.connection.execute(
            "UPDATE curator_topics SET created_at=?", (time.time() - 31 * 86400,)
        )
        self.assertIsNotNone(self.store.exact(persistent["topic_key"], 30))
        self.assertIsNone(self.store.exact(expiring["topic_key"], 30))
        new_id, duplicate = self.store.reserve(
            topic(
                "anthropic|claude-api|pricing", event="pricing", specific_version=False
            ),
            "102",
            30,
        )
        self.assertFalse(duplicate)
        self.assertNotEqual(new_id, expiring["id"])

    async def test_generic_release_key_is_not_permanent(self):
        self.assertFalse(topic("anthropic|claude|release")["permanent"])
        self.assertFalse(
            topic("anthropic|claude-opus-5.5|pricing", event="pricing")["permanent"]
        )

    async def test_merged_versioned_alias_remains_reserved_after_window(self):
        canonical = self.pending()
        other = self.pending("102", key="anthropic|opus-version-5.5|release")
        self.store.merge(other["id"], canonical["id"])
        self.store.connection.execute(
            "UPDATE curator_topics SET created_at=?", (time.time() - 50 * 86400,)
        )
        alias = self.store.exact(other["topic_key"], 30)
        self.assertEqual(alias["id"], canonical["id"])

    async def test_concurrent_semantic_topics_reserve_only_one_draft(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def research_with_pause(*args, **kwargs):
            started.set()
            await release.wait()
            return self.researcher.return_value

        async def extract(value):
            return (
                topic("anthropic|opus-version-5.5|release")
                if value.get("id") == "102"
                else topic()
            )

        async def evaluate(state, questions):
            return {key: (0.95 if key == "is_same_topic" else 0.9) for key in questions}

        self.curator.researcher = research_with_pause
        self.llm.topic.side_effect = extract
        self.jev.evaluate.side_effect = evaluate
        for ident in ("101", "102"):
            self.store.add_post(post(ident), self.conf)
        first = asyncio.create_task(self.curator.process_post(post()))
        try:
            await asyncio.wait_for(started.wait(), 1)
            await self.curator.process_post(post("102"))
        finally:
            release.set()
            await first
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_topics"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(len(self.store.sources(1)), 1)

    async def test_research_topic_rechecked_and_merged_before_preview(self):
        existing = self.pending()
        await self.curator.approvals.sync_topic(existing["id"])
        calls_before = {p: len(bot.calls) for p, bot in self.bots.items()}
        candidate = post("102", text="Different initial interpretation")
        self.store.add_post(candidate, self.conf)
        self.llm.topic.side_effect = [
            topic("other|product-2|release", summary="Different initial topic"),
            topic(),
        ]
        await self.curator.process_post(candidate)
        self.assertEqual(self.store.topic(2)["state"], "merged")
        self.assertEqual(self.store.topic(2)["merged_into"], existing["id"])
        self.assertEqual(len(self.store.sources(existing["id"])), 1)
        self.assertEqual(self.store.stats(self.conf)["today"]["duplicate"], 1)
        await self.curator.outbox_once()
        self.assertEqual(
            calls_before, {p: len(bot.calls) for p, bot in self.bots.items()}
        )
        self.assertFalse(
            self.store.connection.execute(
                "SELECT 1 FROM curator_previews WHERE topic_id=2"
            ).fetchone()
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_topics WHERE state='pending'"
            ).fetchone()[0],
            1,
        )

    async def test_concurrent_exact_reservations_converge(self):
        for ident in ("101", "102"):
            self.store.add_post(post(ident), self.conf)
        second = CuratorStore(self.conf.database_path)
        try:

            async def reserve(store, ident):
                await asyncio.sleep(0)
                return store.reserve(topic(), ident, 30)[0]

            ids = await asyncio.gather(
                reserve(self.store, "101"), reserve(second, "102")
            )
            self.assertEqual(ids[0], ids[1])
            self.assertEqual(len(self.store.sources(ids[0])), 1)
        finally:
            second.close()

    async def test_idempotent_approval_and_cross_bot_status_sync(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        await self.curator.approvals.handle(
            "telegram", self.callback("telegram", pending, "approve")
        )
        await self.curator.approvals.handle(
            "bale", self.callback("bale", pending, "approve", update_id=2)
        )
        await self.curator.outbox_once()
        await self.curator.outbox_once()
        for platform in self.bots:
            self.assertEqual(len(self.channel_calls(platform)), 1)
            edits = [c for c in self.bots[platform].calls if c[0] == "editMessageText"]
            self.assertTrue(
                any("تأیید توسط کاربر 111 در تلگرام" in c[1]["text"] for c in edits)
            )
        self.assertEqual(self.store.topic(pending["id"])["state"], "published")

    async def test_partial_failure_retry_only_failed_platform(self):
        pending = self.pending()
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        self.bots["bale"].fail_channel = True
        await self.curator.outbox_once()
        self.assertEqual(self.store.topic(pending["id"])["state"], "partial")
        await self.curator.approvals.sync_topic(pending["id"])
        current = self.store.topic(pending["id"])
        self.assertEqual(self.curator.approvals.buttons(current)["inline_keyboard"], [])
        await self.curator.approvals.status("telegram", 111)
        status = [c[1] for c in self.bots["telegram"].calls if c[0] == "sendMessage"][
            -1
        ]
        retry_buttons = [
            b
            for row in status["reply_markup"]["inline_keyboard"]
            for b in row
            if "retry_" in b["callback_data"]
        ]
        self.assertEqual(len(retry_buttons), 1)
        self.assertTrue(retry_buttons[0]["callback_data"].endswith(":retry_bale"))
        self.assert_persian_lines(retry_buttons[0]["text"])
        self.bots["bale"].fail_channel = False
        await self.curator.approvals.handle(
            "telegram", self.callback("telegram", current, "retry_bale")
        )
        await self.curator.outbox_once()
        self.assertEqual(len(self.channel_calls("telegram")), 1)
        self.assertEqual(len(self.channel_calls("bale")), 2)
        self.assertEqual(self.store.topic(pending["id"])["state"], "published")

    async def test_published_topic_cannot_be_edited_or_rewritten(self):
        pending = self.pending()
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        await self.curator.outbox_once()
        current = self.store.topic(pending["id"])
        for action in ("edit", "regen", "rewrite"):
            await self.curator.approvals.handle(
                "telegram", self.callback("telegram", current, action)
            )
        self.assertEqual(self.curator.approvals.buttons(current)["inline_keyboard"], [])
        self.assertFalse(
            self.store.connection.execute("SELECT 1 FROM curator_replies").fetchone()
        )
        await self.curator.pipeline_once()
        await self.curator.outbox_once()
        self.assertEqual(self.store.topic(pending["id"])["state"], "published")
        self.assertEqual(
            self.store.topic(pending["id"])["revision"], current["revision"]
        )
        self.assertEqual(len(self.channel_calls("telegram")), 1)
        self.assertEqual(len(self.channel_calls("bale")), 1)

    async def test_uncertain_publication_is_never_retried(self):
        pending = self.pending()
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        self.bots["bale"].uncertain_channel = True
        await self.curator.outbox_once()
        self.store.recover()
        await self.curator.outbox_once()
        await self.curator.publisher.publish(pending["id"], "bale", retry_failed=True)
        self.assertEqual(len(self.channel_calls("bale")), 1)

    async def test_dry_run_sends_previews_but_never_channels(self):
        self.curator.publisher.config = replace(self.conf, dry_run=True)
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        await self.curator.outbox_once()
        self.assertEqual(self.store.topic(pending["id"])["state"], "dry_run")
        self.assertTrue(
            any(
                c[0] == "send_content" and c[1] == 111
                for c in self.bots["telegram"].calls
            )
        )
        self.assertFalse(self.channel_calls("telegram") or self.channel_calls("bale"))

    async def test_freezes_sources_at_approval(self):
        pending = self.pending()
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        extra = post("102")
        extra["media"] = [
            {"kind": "photo", "url": "https://pbs.twimg.com/media/extra.jpg"}
        ]
        self.store.add_post(extra, self.conf)
        self.store.attach(pending["id"], "102")
        with patch.object(
            self.curator.publisher, "media", new=AsyncMock(return_value=[])
        ) as media:
            await self.curator.publisher.publish(pending["id"], "telegram")
        self.assertEqual(len(media.call_args.args[0]), 1)

    async def test_no_unauthorized_or_group_actions(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        await self.curator.approvals.handle(
            "telegram", self.callback("telegram", pending, "approve", user=777)
        )
        update = self.callback("telegram", pending, "approve")
        update["callback_query"]["message"]["chat"]["type"] = "group"
        await self.curator.approvals.handle("telegram", update)
        self.assertEqual(self.store.topic(pending["id"])["state"], "pending")

    async def test_admin_preview_rejects_nonprivate_targets(self):
        self.bots["bale"].get_chat = AsyncMock(
            return_value={"result": {"id": 222, "type": "channel"}}
        )
        with self.assertRaisesRegex(ValueError, "private chat"):
            await self.curator.approvals.message("bale", 222, "Draft")
        self.assertFalse(any(c[0] == "sendMessage" for c in self.bots["bale"].calls))

    async def test_three_persian_buttons_and_removed_controls_are_ignored(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        for platform in self.bots:
            messages = [
                c[1] for c in self.bots[platform].calls if c[0] == "sendMessage"
            ]
            labels = [
                b["text"]
                for row in messages[-1]["reply_markup"]["inline_keyboard"]
                for b in row
            ]
            self.assertEqual(labels, ["✅ تأیید", "❌ لغو", "✏️ ویرایش"])
            for action in ("media", "regen"):
                await self.curator.approvals.handle(
                    platform, self.callback(platform, pending, action)
                )
        self.assertTrue(self.store.topic(pending["id"])["media_enabled"])
        self.assertEqual(
            self.store.topic(pending["id"])["revision"], pending["revision"]
        )
        self.assertEqual(self.store.topic(pending["id"])["state"], "pending")

    async def test_edit_reply_updates_both_previews(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        await self.curator.approvals.handle(
            "telegram", self.callback("telegram", pending, "edit")
        )
        await self.curator.approvals.handle(
            "telegram", self.callback("telegram", pending, "replace")
        )
        prompt = self.store.connection.execute(
            "SELECT message_id FROM curator_replies"
        ).fetchone()[0]
        await self.curator.approvals.handle(
            "telegram",
            {
                "message": {
                    "message_id": 100,
                    "chat": {"id": 111, "type": "private"},
                    "from": {"id": 111},
                    "reply_to_message": {"message_id": prompt},
                    "text": "🧠 متن اصلاح شده",
                }
            },
        )
        await self.curator.approvals.sync_topic(pending["id"])
        current = self.store.topic(pending["id"])
        self.assertIn("متن اصلاح شده", current["text"])
        self.assertTrue(current["text"].endswith(self.conf.channel_signature))
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_previews WHERE revision=?",
                (current["revision"],),
            ).fetchone()[0],
            2,
        )

    async def test_challenge_latches_and_alerts_both_bots(self):
        self.assertFalse(
            await self.curator.event(
                {"kind": "challenge", "reason": "unusual activity"}
            )
        )
        self.assertTrue(self.store.reader()["challenge"])
        self.store.recover()
        self.assertTrue(self.store.reader()["paused"])
        await self.curator.admin_once()
        for bot in self.bots.values():
            self.assertTrue(
                any(
                    c[0] == "sendMessage" and "فعالیت یا ورود غیرعادی" in c[1]["text"]
                    for c in bot.calls
                )
            )
        self.store.resume_reader()
        self.assertFalse(self.store.reader()["challenge"])

    async def test_action_during_second_bot_preview_keeps_sync_dirty(self):
        pending = self.pending()
        original = self.bots["bale"].call

        async def approve_during_preview(method, params, **kwargs):
            if method == "sendMessage" and params["text"].startswith("پیش‌نویس شمارهٔ"):
                self.store.decide(
                    pending["id"],
                    pending["revision"],
                    "approve",
                    "تأیید توسط کاربر 111 در تلگرام",
                )
            return await original(method, params, **kwargs)

        self.bots["bale"].call = approve_during_preview
        await self.curator.approvals.sync_topic(pending["id"])
        dirty = self.store.connection.execute(
            "SELECT dirty FROM curator_previews WHERE platform='bale'"
        ).fetchone()[0]
        self.assertTrue(dirty)
        await self.curator.approvals.sync_topic(pending["id"])
        edits = [c for c in self.bots["bale"].calls if c[0] == "editMessageText"]
        self.assertTrue(
            any("تأیید توسط کاربر 111 در تلگرام" in c[1]["text"] for c in edits)
        )

    async def test_duplicate_media_never_enters_review_or_approval_snapshot(self):
        pending = self.pending()
        self.store.add_post(post("102"), self.conf)
        await self.curator.process_post(post("102"))
        self.assertEqual(len(self.store.sources(pending["id"])), 1)
        self.assertEqual(len(self.store.draft_sources(pending["id"])), 1)
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        snapshot = json.loads(
            self.store.connection.execute(
                "SELECT snapshot_json FROM curator_publications LIMIT 1"
            ).fetchone()[0]
        )
        self.assertEqual(len(snapshot["posts"]), 1)

    async def test_seen_id_and_daily_quota_persist(self):
        conf = replace(self.conf, x_max_posts_per_day=1)
        self.assertTrue(self.store.add_post(post(), conf))
        self.assertFalse(self.store.add_post(post(), conf))
        self.assertFalse(self.store.add_post(post("102"), conf))
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_posts"
            ).fetchone()[0],
            2,
        )

    async def test_admin_updates_share_existing_inbox_and_are_deduplicated(self):
        pending = self.pending()
        await self.curator.approvals.sync_topic(pending["id"])
        update = self.callback("telegram", pending, "approve", update_id=555)
        self.store.ingest("telegram", SCOPE, [update], encoder=lambda x: x)
        self.curator.route_inbox("telegram")
        self.curator.route_inbox("telegram")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM curator_tasks WHERE kind='admin'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(self.store.pending_updates("telegram"), [])

    async def test_publication_registers_bridge_echo_signature(self):
        pending = self.pending()
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        await self.curator.publisher.publish(pending["id"], "bale")
        item = self.channel_calls("bale")[0][2][0]
        signature = echo_signature({"text": item["text"]}, "bale")
        self.assertIsNotNone(
            self.store.connection.execute(
                "SELECT 1 FROM pending_echoes WHERE signature=? AND remaining=1",
                (signature,),
            ).fetchone()
        )

    async def test_publication_real_client_records_safe_rate_limit_retry(self):
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(
                    429,
                    json={
                        "ok": False,
                        "error_code": 429,
                        "parameters": {"retry_after": 0},
                    },
                )
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 44}})

        client = TelegramClient(
            self.conf.telegram_token,
            self.conf,
            transport=httpx.MockTransport(handler),
            sleeper=AsyncMock(),
        )
        self.addAsyncCleanup(client.aclose)
        self.bots["telegram"] = client
        pending = self.pending()
        self.store.decide(pending["id"], pending["revision"], "approve", "Admin")
        await self.curator.publisher.publish(pending["id"], "telegram")
        attempts = self.store.connection.execute(
            "SELECT attempt FROM send_attempts WHERE send_key LIKE 'curator:%:telegram:0'"
        ).fetchall()
        self.assertEqual([r[0] for r in attempts], [1, 2])
        self.assertEqual(len(calls), 2)

    async def test_jev_failure_does_not_pause_forwarding(self):
        self.jev.evaluate.side_effect = RuntimeError("jev offline")
        self.store.add_post(post(), self.conf)
        self.assertTrue(await self.curator.pipeline_once())
        self.assertEqual(
            self.store.connection.execute("SELECT state FROM curator_posts").fetchone()[
                0
            ],
            "error",
        )
        self.assertFalse(self.store.paused("tg_to_bale"))

    async def test_browser_crash_schedules_backoff(self):
        self.curator.browser = AsyncMock(side_effect=RuntimeError("browser crash"))
        with patch("channel_sync.curator.service.active_now", return_value=True):
            task = asyncio.create_task(self.curator.read_loop())
            try:
                async with asyncio.timeout(4):
                    while not self.store.reader()["last_error"]:
                        await asyncio.sleep(0.01)
                self.assertGreater(self.store.reader()["next_run"], time.time() + 10)
                self.assertEqual(self.curator.browser.await_count, 1)
            finally:
                self.curator.stop.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_shutdown_cancels_browser_without_swallowing_cancellation(self):
        started, finished = asyncio.Event(), asyncio.Event()

        async def browser(*args, **kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finished.set()

        self.curator.browser = browser
        with patch("channel_sync.curator.service.active_now", return_value=True):
            task = asyncio.create_task(self.curator.run())
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        self.assertTrue(finished.is_set())

    async def test_bridge_continues_when_curator_task_fails(self):
        from channel_sync.clients import BaleClient

        conf = replace(self.conf, telegram_to_bale_enabled=True)
        api = FakeAPIs(conf)
        api.queues["telegram"] = [
            {
                "update_id": 1,
                "channel_post": {
                    "message_id": 7,
                    "chat": {"id": conf.telegram_channel_id, "type": "channel"},
                    "text": "Bridge post",
                    "from": {"id": 999},
                },
            }
        ]
        transport = httpx.MockTransport(api.handle)
        clients = {
            "telegram": TelegramClient(conf.telegram_token, conf, transport=transport),
            "bale": BaleClient(conf.bale_token, conf, transport=transport),
        }
        bridge = Bridge(conf, clients, self.store)
        bridge.curator.run = AsyncMock(side_effect=RuntimeError("Curator failure"))
        task = asyncio.create_task(bridge.run())
        try:
            async with asyncio.timeout(3):
                while not any(method == "sendMessage" for _, method in api.events):
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
            self.assertFalse(task.done())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.gather(*(c.aclose() for c in clients.values()))

    async def test_curator_health_with_mock_llm_mcp_and_browser(self):
        from channel_sync.curator.health import curator_health

        class MCP:
            tools = [{"function": {"name": "search"}}, {"function": {"name": "fetch"}}]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        async def browser(conf, callback, **kwargs):
            await callback({"kind": "heartbeat", "logged_in": True})

        with (
            patch("channel_sync.curator.health.LLM", return_value=self.llm),
            patch("channel_sync.curator.health.ResearchMCP", return_value=MCP()),
            patch("channel_sync.curator.health.run_browser", side_effect=browser),
        ):
            report = await curator_health(self.conf, self.bots)
        self.assertTrue(report["ok"])
        self.assertEqual(report["mcp"]["tools"], ["search", "fetch"])
        self.assertTrue(report["browser"]["logged_in"])


class ReaderFormattingTests(unittest.TestCase):
    def test_caption_limits_and_media_first_for_long_posts(self):
        media = [{"kind": "photo", "properties": {}, "content": b"photo"}]
        text = "😀" * 600
        telegram = publication_parts(text, media, "telegram")
        bale = publication_parts(text, media, "bale")
        self.assertEqual(telegram[0][0]["text"], "")
        self.assertEqual(telegram[1][0]["text"], text)
        self.assertEqual(len(bale), 1)
        self.assertEqual(bale[0][0]["text"], text)
        album = publication_parts(text, media * 2, "bale")
        self.assertEqual(len(album), 2)
        self.assertTrue(all(i["text"] == "" for i in album[0]))

    def test_split_preserves_unicode_and_boundaries(self):
        text = "😀 فارسی\n" * 900
        parts = split_text(text)
        self.assertEqual("".join(parts), text)
        self.assertTrue(all(utf16_length(p) <= 4096 for p in parts))

    def test_challenges_and_read_only_url_validation(self):
        for url, text in [
            ("https://x.com/i/flow/login", ""),
            ("https://x.com/account/access", ""),
            ("https://x.com/home", "Unusual activity"),
            ("https://x.com/home", "Rate limit exceeded"),
        ]:
            self.assertIsNotNone(challenge_reason(url, text))
        for value in [
            "https://x.com/messages",
            "https://evil.example/user/status/123",
            "file:///tmp/key",
        ]:
            with self.assertRaises(ValueError):
                tweet_url(value)
        self.assertEqual(tweet_url("https://x.com/author/status/123?s=20")[1], "123")

    def test_tehran_schedule_including_overnight(self):
        conf = config(Path("/tmp"))
        for hour, minute, expected in [
            (8, 59, False),
            (9, 0, True),
            (23, 29, True),
            (23, 30, False),
        ]:
            self.assertEqual(
                active_now(
                    conf,
                    datetime(
                        2026, 10, 20, hour, minute, tzinfo=ZoneInfo("Asia/Tehran")
                    ),
                ),
                expected,
            )
        self.assertTrue(
            active_now(
                replace(conf, x_active_hours="22:00-03:00"),
                datetime(2026, 10, 20, 1, 0),
            )
        )

    def test_passive_graphql_extracts_quotes_thread_links_best_video(self):
        def tweet(ident, text, reply=None):
            return {
                "__typename": "Tweet",
                "rest_id": ident,
                "core": {
                    "user_results": {"result": {"legacy": {"screen_name": "author"}}}
                },
                "legacy": {
                    "full_text": text,
                    "created_at": "date",
                    "in_reply_to_status_id_str": reply,
                    "entities": {
                        "urls": [{"expanded_url": "https://official.example/release"}]
                    },
                },
            }

        root = tweet("1", "Root")
        root["quoted_status_result"] = {"result": tweet("3", "Quoted")}
        root["legacy"]["extended_entities"] = {
            "media": [
                {
                    "type": "video",
                    "original_info": {"width": 128, "height": 64},
                    "video_info": {
                        "variants": [
                            {"content_type": "video/mp4", "bitrate": 1, "url": "low"},
                            {"content_type": "video/mp4", "bitrate": 9, "url": "high"},
                        ]
                    },
                }
            ]
        }
        posts = parse_timeline({"data": [root, tweet("2", "Thread continuation", "1")]})
        p = next(p for p in posts if p["id"] == "1")
        self.assertIn("Quoted", p["text"])
        self.assertEqual(p["media"][0]["url"], "high")
        self.assertEqual(p["visible_thread"][0]["text"], "Thread continuation")


class ResearchTests(unittest.IsolatedAsyncioTestCase):
    async def test_official_sdk_http_and_stdio_transports_with_mocks(self):
        from mcp.types import Tool

        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "mcp.json"
            path.write_text(
                json.dumps(
                    {
                        "servers": {
                            "stdio": {
                                "type": "stdio",
                                "command": "test-search",
                                "env": {"KEY": "${SEARCH_KEY}"},
                                "tools": ["search"],
                            },
                            "http": {
                                "type": "streamable_http",
                                "url": "https://mcp.example",
                                "tools": ["search"],
                            },
                        }
                    }
                )
            )
            conf = config(
                Path(root),
                mcp_servers_path=path,
                env_values={"SEARCH_KEY": "secret-search-key"},
            )
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=False)
            session.initialize = AsyncMock()
            listing = MagicMock()
            listing.tools = [
                Tool(
                    name="search",
                    inputSchema={
                        "type": "object",
                        "properties": {"q": {"type": "string"}},
                        "required": ["q"],
                    },
                )
            ]
            listing.nextCursor = None
            session.list_tools = AsyncMock(return_value=listing)
            result = MagicMock(isError=False)
            result.model_dump.return_value = {"content": [{"text": "Found source"}]}
            session.call_tool = AsyncMock(return_value=result)
            transports = []

            @asynccontextmanager
            async def stdio(params, **kwargs):
                transports.append(("stdio", params))
                yield ("read", "write")

            @asynccontextmanager
            async def http(url, **kwargs):
                transports.append(("http", url))
                yield ("read", "write", None)

            with (
                patch("mcp.ClientSession", return_value=session),
                patch("mcp.client.stdio.stdio_client", side_effect=stdio),
                patch(
                    "mcp.client.streamable_http.streamable_http_client",
                    side_effect=http,
                ),
            ):
                async with ResearchMCP(conf) as mcp:
                    self.assertEqual(len(mcp.tools), 2)
                    self.assertIn(
                        "Found source", await mcp.call("s1_search", {"q": "AI"})
                    )
                    with self.assertRaises(Exception):
                        await mcp.call("s0_search", {"q": 7})
            self.assertEqual([t[0] for t in transports], ["stdio", "http"])
            self.assertEqual(transports[0][1].env["KEY"], "secret-search-key")

    async def test_tool_round_trip_with_mock_mcp_and_grounded_writer(self):
        with tempfile.TemporaryDirectory() as root:
            conf = config(
                Path(root), llm_max_tool_rounds=2, curator_examples_dir=Path(root)
            )
            (Path(root) / "1.md").write_text("🧠 عنوان فارسی\n#AI")
            url = "https://official.example/release"
            quote = "The new model supports a context of one million tokens."
            fact = {"claim": quote, "url": url, "quote": quote}
            original = post()
            llm = AsyncMock()
            llm.chat.side_effect = [
                {
                    "tool_calls": [
                        {
                            "id": "call1",
                            "type": "function",
                            "function": {
                                "name": "search",
                                "arguments": '{"query":"release"}',
                            },
                        }
                    ]
                },
                {"content": json.dumps({"facts": [fact]})},
                {
                    "content": json.dumps(
                        {
                            "text": f"🧠 خبر مدل\n\nمدل تازه منتشر شد.\n🔗 {url}\n🔗 {original['url']}\n#AI\n{conf.channel_signature}"
                        },
                        ensure_ascii=False,
                    )
                },
            ]

            class MCP:
                tools = [{"type": "function", "function": {"name": "search"}}]

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

                async def call(self, name, arguments):
                    self_called.append((name, arguments))
                    return url + "\n" + quote

            self_called = []
            with patch(
                "channel_sync.curator.research.tehran_date",
                return_value="2026-10-20 (۱۴۰۵/۰۷/۲۸)",
            ):
                result = await research(
                    conf, llm, [original], mcp_factory=lambda c: MCP()
                )
            self.assertEqual(result["facts"], [fact])
            self.assertEqual(self_called, [("search", {"query": "release"})])
            writer_system = llm.chat.call_args_list[-1].args[0][0]["content"]
            self.assertTrue(
                writer_system.endswith("Current date: 2026-10-20 (۱۴۰۵/۰۷/۲۸)\n---")
            )
            self.assertIn("## Persian (RTL) Formatting Rule", writer_system)
            self.assertIn(
                "The hashtag line and the 🔗 link lines from the examples are allowed exceptions.",
                writer_system,
            )
            self.assertNotIn("{{date}}", writer_system)
            history = llm.chat.call_args_list[1].args[0]
            self.assertTrue(
                any(
                    m["role"] == "tool" and m["tool_call_id"] == "call1"
                    for m in history
                )
            )

    async def test_unverified_facts_are_withheld(self):
        with self.assertRaises(ValueError):
            verify_facts(
                [
                    {
                        "claim": "Fake",
                        "url": "https://official.example",
                        "quote": "An invented quote long enough",
                    }
                ],
                ["search found nothing"],
            )

    async def test_numerical_claim_must_be_supported_by_actual_quote(self):
        quote = "The official benchmark score is 91.5 percent."
        with self.assertRaises(ValueError):
            verify_facts(
                [
                    {
                        "claim": "Benchmark score is 99.0 percent",
                        "url": "https://official.example",
                        "quote": quote,
                    }
                ],
                ["https://official.example\n" + quote],
            )

    async def test_mcp_env_references_do_not_use_ambient_secrets(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "mcp.json"
            path.write_text(
                json.dumps(
                    {
                        "servers": {
                            "search": {
                                "type": "http",
                                "url": "${URL}",
                                "headers": {"Authorization": "Bearer ${KEY}"},
                            }
                        }
                    }
                )
            )
            conf = config(
                Path(root),
                mcp_servers_path=path,
                env_values={"URL": "https://mcp.example", "KEY": "private"},
            )
            self.assertEqual(
                load_servers(conf)["search"]["headers"]["Authorization"],
                "Bearer private",
            )
            with (
                patch.dict("os.environ", {"KEY": "ambient"}),
                self.assertRaises(ConfigError),
            ):
                load_servers(replace(conf, env_values={"URL": "https://mcp.example"}))

    async def test_llm_compatible_http_contract_and_proxy_policy(self):
        with tempfile.TemporaryDirectory() as root:
            conf = config(
                Path(root),
                llm_base_url="https://llm.example/v1",
                llm_api_key="private",
                llm_model="configured",
            )

            def handler(request):
                self.assertEqual(request.url.path, "/v1/chat/completions")
                body = json.loads(request.content)
                self.assertEqual(body["model"], "configured")
                self.assertIn("tools", body)
                return httpx.Response(
                    200,
                    json={
                        "choices": [{"message": {"role": "assistant", "content": "OK"}}]
                    },
                )

            llm = LLM(conf, transport=httpx.MockTransport(handler))
            try:
                result = await llm.chat(
                    [{"role": "user", "content": "Hi"}],
                    [{"type": "function", "function": {"name": "test"}}],
                )
                self.assertEqual(result["content"], "OK")
            finally:
                await llm.aclose()
            with patch(
                "channel_sync.curator.research.httpx.AsyncClient"
            ) as constructor:
                http_client(
                    replace(
                        conf,
                        llm_use_proxy=True,
                        telegram_proxy_url="socks5://127.0.0.1:10808",
                    )
                )
                self.assertEqual(
                    constructor.call_args.kwargs["proxy"], "socks5://127.0.0.1:10808"
                )
                self.assertFalse(constructor.call_args.kwargs["trust_env"])


class BrowserProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_chrome_launch_contract_is_headed_and_unspoofed(self):
        with tempfile.TemporaryDirectory() as folder:
            conf = config(Path(folder), x_browser_profile_dir=Path(folder) / "profile")
            page = MagicMock()
            page.url = "https://x.com/home"
            page.goto = AsyncMock()
            page.evaluate = AsyncMock(return_value="")
            page.locator.return_value.count = AsyncMock(return_value=1)
            context = MagicMock(pages=[page])
            context.close = AsyncMock()
            playwright = MagicMock()
            playwright.chromium.launch_persistent_context = AsyncMock(
                return_value=context
            )
            manager = MagicMock()
            manager.__aenter__ = AsyncMock(return_value=playwright)
            manager.__aexit__ = AsyncMock(return_value=False)
            with (
                patch("playwright.async_api.async_playwright", return_value=manager),
                patch("channel_sync.curator.reader.emit"),
            ):
                await browser_child(conf, "probe")
            options = playwright.chromium.launch_persistent_context.call_args.kwargs
            self.assertEqual(options["channel"], "chrome")
            self.assertFalse(options["headless"])
            self.assertTrue(options["chromium_sandbox"])
            self.assertNotIn("user_agent", options)
            self.assertNotIn("args", options)
            context.close.assert_awaited_once()

    async def test_memory_budget_kills_only_browser_child(self):
        conf = config(Path("/tmp"), x_browser_memory_mb=1)
        original_create = asyncio.create_subprocess_exec
        processes = []

        async def spawn(*args, **kwargs):
            process = await original_create(
                sys.executable,
                "-c",
                "import time; data=bytearray(4000000); time.sleep(60)",
                **kwargs,
            )
            processes.append(process)
            return process

        with patch(
            "channel_sync.curator.reader.asyncio.create_subprocess_exec",
            side_effect=spawn,
        ):
            with self.assertRaisesRegex(RuntimeError, "memory budget"):
                await run_browser(conf, AsyncMock())
        self.assertIsNotNone(processes[0].returncode)

    async def test_stop_on_challenge_kills_process_before_alert_callback(self):
        conf = config(Path("/tmp"), x_browser_memory_mb=0)
        original_create = asyncio.create_subprocess_exec
        processes = []

        async def spawn(*args, **kwargs):
            process = await original_create(
                sys.executable,
                "-c",
                'import time; print(\'{"kind":"challenge","reason":"captcha"}\',flush=True); time.sleep(60)',
                **kwargs,
            )
            processes.append(process)
            return process

        events = []

        async def callback(event):
            self.assertIsNotNone(processes[0].returncode)
            events.append(event)

        with patch(
            "channel_sync.curator.reader.asyncio.create_subprocess_exec",
            side_effect=spawn,
        ):
            await run_browser(conf, callback)
        self.assertEqual(events[0]["reason"], "captcha")

    async def test_hung_child_is_killed_by_watchdog(self):
        conf = config(Path("/tmp"), x_browser_watchdog_seconds=1, x_browser_memory_mb=0)
        original_create = asyncio.create_subprocess_exec
        processes = []

        async def spawn(*args, **kwargs):
            process = await original_create(
                sys.executable, "-c", "import time; time.sleep(60)", **kwargs
            )
            processes.append(process)
            return process

        with patch(
            "channel_sync.curator.reader.asyncio.create_subprocess_exec",
            side_effect=spawn,
        ):
            with self.assertRaises(TimeoutError):
                await run_browser(conf, AsyncMock())
        self.assertIsNotNone(processes[0].returncode)


class CuratorConfigCLITests(unittest.TestCase):
    def test_defaults_validation_and_secret_redaction(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / ".env"
            template = (
                Path(".env.example")
                .read_text()
                .replace(
                    "TELEGRAM_BOT_TOKEN=",
                    "TELEGRAM_BOT_TOKEN=123456:long_enough_test_token",
                )
                .replace(
                    "BALE_BOT_TOKEN=", "BALE_BOT_TOKEN=654321:long_enough_bale_token"
                )
            )
            path.write_text(template)
            conf = load_config(path)
            self.assertFalse(conf.curator_enabled)
            self.assertEqual(conf.topic_dedup_days, 30)
            self.assertEqual(conf.x_timezone, "Asia/Tehran")
            self.assertEqual(conf.x_max_posts_per_day, 400)
            self.assertEqual(conf.x_proxy_url, conf.telegram_proxy_url)
            path.write_text(
                template.replace(
                    "ADMIN_TELEGRAM_USER_IDS=", "ADMIN_TELEGRAM_USER_IDS=111, 112"
                ).replace("LLM_API_KEY=", "LLM_API_KEY=private-test-key")
            )
            conf = load_config(path)
            self.assertEqual(conf.admin_telegram_user_ids, (111, 112))
            self.assertNotIn("private-test-key", repr(conf))
            self.assertNotIn(
                "private-test-key",
                Redactor(*config_secrets(conf)).text("bad private-test-key"),
            )
            for old, new in [
                ("ADMIN_BALE_USER_IDS=", "ADMIN_BALE_USER_IDS=-1"),
                ("X_TIMEZONE=Asia/Tehran", "X_TIMEZONE=Invalid/City"),
                ("X_ACTIVE_HOURS=09:00-23:30", "X_ACTIVE_HOURS=25:00-26:00"),
                ("X_SESSION_MINUTES_MIN=10", "X_SESSION_MINUTES_MIN=99"),
            ]:
                path.write_text(template.replace(old, new))
                with self.assertRaises(ConfigError):
                    load_config(path)

    def test_stats_and_resume_work_while_main_process_lock_is_held(self):
        with tempfile.TemporaryDirectory() as root:
            conf = config(Path(root))
            store = CuratorStore(conf.database_path)
            store.pause_reader("captcha", challenge=True)
            try:
                with (
                    process_lock(conf.database_path),
                    patch("channel_sync.cli.load_config", return_value=conf),
                ):
                    with redirect_stdout(io.StringIO()) as output:
                        self.assertEqual(main(["curator-stats"]), 0)
                    self.assertTrue(
                        json.loads(output.getvalue())["reader"]["challenge"]
                    )
                    with redirect_stdout(io.StringIO()):
                        self.assertEqual(main(["curator-resume"]), 0)
                self.assertFalse(store.reader()["challenge"])
            finally:
                store.close()
