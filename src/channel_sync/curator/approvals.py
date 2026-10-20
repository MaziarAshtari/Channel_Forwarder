import json
import asyncio
import logging
import time

from ..clients import ApiError, client_write_lock
from ..formatting import escape_bale
from ..messages import utf16_length
from .publisher import publication_parts

log = logging.getLogger(__name__)


class Approvals:
    def __init__(self, config, clients, store, publisher):
        self.config, self.clients, self.store, self.publisher = (
            config,
            clients,
            store,
            publisher,
        )
        self.preview_locks = {}
        self.private_chats = set()

    def admins(self, platform):
        return (
            self.config.admin_telegram_user_ids
            if platform == "telegram"
            else self.config.admin_bale_user_ids
        )

    def authorized(self, platform, update):
        query = update.get("callback_query")
        message = (
            query.get("message") if isinstance(query, dict) else update.get("message")
        )
        user = (
            query.get("from")
            if isinstance(query, dict)
            else (message or {}).get("from")
        )
        return (
            isinstance(message, dict)
            and message.get("chat", {}).get("type") == "private"
            and isinstance(user, dict)
            and user.get("id") in self.admins(platform)
            and message["chat"].get("id") == user["id"]
            and not user.get("is_bot", False)
        )

    async def call(self, platform, method, params):
        async with client_write_lock(self.clients[platform]):
            return await self.clients[platform].call(
                method, params, record_attempts=False
            )

    async def message(self, platform, chat, text, markup=None):
        await self.ensure_private(platform, chat)
        params = {
            "chat_id": chat,
            "text": escape_bale(text) if platform == "bale" else text,
        }
        if markup is not None:
            params["reply_markup"] = markup
        return await self.call(platform, "sendMessage", params)

    async def ensure_private(self, platform, chat):
        if chat not in self.admins(platform) or chat in {
            self.config.telegram_channel_id,
            self.config.bale_channel_id,
        }:
            raise ValueError(
                "Curator previews require an allowlisted private admin chat"
            )
        if (platform, chat) not in self.private_chats:
            result = (await self.clients[platform].get_chat(chat))["result"]
            if result.get("id") != chat or result.get("type") != "private":
                raise ValueError(
                    "Configured admin target is not a private chat; open/start the bot first"
                )
            self.private_chats.add((platform, chat))

    async def alert(self, text, markup=None):
        # One inaccessible admin never prevents delivery to the other platform.
        async def send(platform, chat):
            try:
                await self.message(platform, chat, text, markup)
            except Exception as exc:
                log.warning(
                    "Curator admin notification failed platform=%s error=%s",
                    platform,
                    type(exc).__name__,
                )

        await asyncio.gather(
            *(send(p, chat) for p in self.clients for chat in self.admins(p))
        )

    async def source_notice(self, topic):
        for platform in self.clients:
            for chat in self.admins(platform):
                try:
                    response = await self.message(
                        platform,
                        chat,
                        f"New information attached to topic #{topic['id']}: {topic['summary']}\n"
                        + (
                            "Already published; regeneration updates the admin preview only."
                            if topic["state"] == "published"
                            else ""
                        ),
                        {
                            "inline_keyboard": [
                                [
                                    {
                                        "text": "🔄 Regenerate with new sources",
                                        "callback_data": f"c:{topic['id']}:{topic['revision']}:regen",
                                    }
                                ]
                            ]
                        },
                    )
                    self.store.connection.execute(
                        "INSERT OR REPLACE INTO curator_replies VALUES(?,?,?,?,?,?)",
                        (
                            platform,
                            chat,
                            response["result"]["message_id"],
                            topic["id"],
                            topic["revision"],
                            "source_offer",
                        ),
                    )
                except Exception as exc:
                    log.warning(
                        "Curator new-source alert failed platform=%s error=%s",
                        platform,
                        type(exc).__name__,
                    )

    def buttons(self, topic):
        ident, rev = topic["id"], topic["revision"]

        def button(label, action):
            return {"text": label, "callback_data": f"c:{ident}:{rev}:{action}"}

        if topic["state"] == "pending":
            rows = [
                [button("✅ Approve", "approve"), button("❌ Reject", "reject")],
                [button("🔄 Regenerate", "regen"), button("✏️ Edit", "edit")],
                [
                    button(
                        "🖼 Media: " + ("on" if topic["media_enabled"] else "off"),
                        "media",
                    )
                ],
            ]
        elif topic["state"] in {"rejected", "error"}:
            rows = [[button("🔄 Regenerate", "regen")]]
        else:
            rows = []
            if topic["state"] == "published":
                rows.append([button("🔄 Regenerate preview", "regen")])
            for row in self.store.connection.execute(
                "SELECT platform FROM curator_publications WHERE topic_id=? AND state='failed'",
                (ident,),
            ):
                rows.append([button("Retry publish: " + row[0], "retry_" + row[0])])
        return {"inline_keyboard": rows}

    def control_text(self, topic):
        deliveries = [
            f"{r[0]}: {r[1]}" + (f" ({r[2][:240]})" if r[2] else "")
            for r in self.store.connection.execute(
                "SELECT platform,state,error FROM curator_publications WHERE topic_id=?",
                (topic["id"],),
            )
        ]
        header = (
            f"Draft #{topic['id']} · {topic['state']} · revision {topic['revision']}\n"
        )
        if topic.get("actor"):
            header += topic["actor"] + "\n"
        if deliveries:
            header += " | ".join(deliveries) + "\n"
        header += "Media: " + ("on" if topic["media_enabled"] else "off") + "\n\n"
        # Keep full draft in its preview messages; control text fits both APIs.
        return header + topic["summary"][:1300]

    async def sync_topic(self, topic_id, *, force=False):
        async with self.preview_locks.setdefault(topic_id, asyncio.Lock()):
            await self._sync_topic(topic_id, force=force)

    def preview_changed(self, topic_id, text, markup):
        current = self.store.topic(topic_id)
        return int(
            self.control_text(current) != text or self.buttons(current) != markup
        )

    async def _sync_topic(self, topic_id, *, force=False):
        topic = self.store.topic(topic_id)
        if not topic or topic["state"] == "researching":
            return
        for platform in self.clients:
            for chat in self.admins(platform):
                row = self.store.connection.execute(
                    "SELECT * FROM curator_previews WHERE topic_id=? AND platform=? AND chat_id=?",
                    (topic_id, platform, chat),
                ).fetchone()
                try:
                    await self.ensure_private(platform, chat)
                    text, markup = self.control_text(topic), self.buttons(topic)
                    if row and not row["dirty"] and not force:
                        continue
                    if row and row["revision"] == topic["revision"]:
                        params = {
                            "chat_id": chat,
                            "message_id": row["message_id"],
                            "text": text,
                            "reply_markup": markup,
                        }
                        if platform == "bale":
                            params["text"] = escape_bale(params["text"])
                        try:
                            await self.call(platform, "editMessageText", params)
                        except ApiError as exc:
                            if "not modified" not in str(exc).lower():
                                raise
                        self.store.connection.execute(
                            "UPDATE curator_previews SET dirty=? WHERE topic_id=? AND platform=? AND chat_id=?",
                            (
                                self.preview_changed(topic_id, text, markup),
                                topic_id,
                                platform,
                                chat,
                            ),
                        )
                        continue
                    if row:
                        await self.call(
                            platform,
                            "editMessageText",
                            {
                                "chat_id": chat,
                                "message_id": row["message_id"],
                                "text": f"Draft #{topic_id}: superseded by revision {topic['revision']}",
                                "reply_markup": {"inline_keyboard": []},
                            },
                        )
                    if topic["text"]:
                        try:
                            media = (
                                await self.publisher.media(
                                    self.store.draft_sources(topic_id)
                                )
                                if topic["media_enabled"]
                                else []
                            )
                        except Exception as exc:
                            media = []
                            await self.message(
                                platform,
                                chat,
                                "Media preview unavailable: "
                                + type(exc).__name__
                                + ". The stored source media remains selected; use Toggle media to publish text only.",
                            )
                        for index, items in enumerate(
                            publication_parts(topic["text"], media, platform)
                        ):
                            await self.publisher.send_part(
                                f"curator:preview:{topic_id}:{topic['revision']}:{platform}:{chat}:{index}",
                                platform,
                                chat,
                                items,
                            )
                    text, markup = self.control_text(topic), self.buttons(topic)
                    response = await self.message(platform, chat, text, markup)
                    self.store.connection.execute(
                        "INSERT INTO curator_previews(topic_id,platform,chat_id,message_id,revision,dirty) "
                        "VALUES(?,?,?,?,?,?) ON CONFLICT(topic_id,platform,chat_id) DO UPDATE SET message_id=excluded.message_id,revision=excluded.revision,dirty=excluded.dirty",
                        (
                            topic_id,
                            platform,
                            chat,
                            response["result"]["message_id"],
                            topic["revision"],
                            self.preview_changed(topic_id, text, markup),
                        ),
                    )
                except Exception as exc:
                    log.warning(
                        "Curator preview sync failed topic=%s platform=%s error=%s",
                        topic_id,
                        platform,
                        type(exc).__name__,
                    )
                    # Keep a missing/dirty preview eligible for the next outbox pass.

    async def status(self, platform, chat, command="status"):
        if command == "pause":
            self.store.pause_reader("Paused by admin")
        elif command == "resume":
            self.store.resume_reader()
        pending = [
            f"#{r[0]}: {r[1][:130]}"
            for r in self.store.connection.execute(
                "SELECT id,summary FROM curator_topics WHERE state='pending' ORDER BY id LIMIT 20"
            )
        ]
        data = self.store.stats(self.config)
        text = (
            "Curator\n"
            + json.dumps(data, ensure_ascii=False, indent=2)
            + "\nPending drafts:\n"
            + "\n".join(pending)
        )
        markup = {
            "inline_keyboard": [
                [
                    {"text": "Pause", "callback_data": "c:pause"},
                    {"text": "Resume", "callback_data": "c:resume"},
                    {"text": "Pending", "callback_data": "c:pending"},
                ]
            ]
        }
        await self.message(platform, chat, text[:3600], markup)
        if command == "pending":
            for row in self.store.connection.execute(
                "SELECT id FROM curator_topics WHERE state='pending' ORDER BY id LIMIT 20"
            ):
                await self.sync_topic(row[0], force=True)

    async def handle(self, platform, update):
        if not self.authorized(platform, update):
            return
        query = update.get("callback_query")
        message = query["message"] if query else update["message"]
        chat = message["chat"]["id"]
        if query:
            data = query.get("data", "")
            # Bale documents answerCallbackQuery only in some API revisions;
            # acknowledgment failure must never reject an otherwise valid action.
            try:
                await self.call(
                    platform, "answerCallbackQuery", {"callback_query_id": query["id"]}
                )
            except Exception:
                pass
            if data in {"c:pause", "c:resume", "c:pending"}:
                await self.status(platform, chat, data.split(":")[1])
                return
            fields = data.split(":")
            if len(fields) != 4 or fields[0] != "c":
                return
            try:
                ident, revision = int(fields[1]), int(fields[2])
            except ValueError:
                return
            action = fields[3]
            preview = self.store.connection.execute(
                "SELECT message_id FROM curator_previews WHERE topic_id=? AND platform=? AND chat_id=?",
                (ident, platform, chat),
            ).fetchone()
            offer = self.store.connection.execute(
                "SELECT 1 FROM curator_replies WHERE platform=? AND chat_id=? AND message_id=? "
                "AND topic_id=? AND revision=? AND mode='source_offer'",
                (platform, chat, message.get("message_id"), ident, revision),
            ).fetchone()
            if (not preview or preview[0] != message.get("message_id")) and not (
                offer and action == "regen"
            ):
                return
            topic = self.store.topic(ident)
            if not topic or topic["revision"] != revision:
                await self.message(
                    platform, chat, "This preview is stale. Open /curator pending."
                )
                return
            verb = {
                "approve": "Approved",
                "reject": "Rejected",
                "media": "Media changed",
            }.get(action, action.capitalize())
            actor = f"{verb} by {query['from']['id']} in {platform.title()}"
            if action in {"approve", "reject", "media"}:
                self.store.decide(ident, revision, action, actor)
            elif action.startswith("retry_") and action[6:] in {"telegram", "bale"}:
                target = action[6:]
                self.store.connection.execute(
                    "UPDATE curator_publications SET state='queued' WHERE topic_id=? AND platform=? AND state='failed'",
                    (ident, target),
                )
                self.store.dirty(ident)
            elif action in {"regen", "edit"} and topic["state"] in {
                "pending",
                "rejected",
                "error",
                "published",
            }:
                if topic["state"] == "published" and action == "edit":
                    return
                prompt = (
                    "Reply to this message with corrected text."
                    if action == "edit"
                    else "Reply with regeneration instructions, or /skip for no instruction."
                )
                response = await self.message(platform, chat, prompt)
                self.store.connection.execute(
                    "INSERT OR REPLACE INTO curator_replies VALUES(?,?,?,?,?,?)",
                    (
                        platform,
                        chat,
                        response["result"]["message_id"],
                        ident,
                        revision,
                        action,
                    ),
                )
            return
        text = message.get("text", "").strip()
        if text.startswith("/curator"):
            command = text.split(maxsplit=1)[1] if " " in text else "status"
            await self.status(platform, chat, command)
            return
        reply_id = message.get("reply_to_message", {}).get("message_id")
        reply = self.store.connection.execute(
            "SELECT * FROM curator_replies WHERE platform=? AND chat_id=? AND message_id=?",
            (platform, chat, reply_id),
        ).fetchone()
        if not reply or not text:
            return
        topic = self.store.topic(reply["topic_id"])
        if (
            not topic
            or topic["revision"] != reply["revision"]
            or topic["state"] not in {"pending", "rejected", "error", "published"}
        ):
            await self.message(
                platform, chat, "This edit/regeneration request is stale."
            )
            return
        if reply["mode"] == "edit":
            if utf16_length(text) > 3900:
                await self.message(
                    platform,
                    chat,
                    "Keep the corrected post within 3900 UTF-16 units, including signature.",
                )
                return
            if not text.endswith(self.config.channel_signature):
                text += "\n\n" + self.config.channel_signature
            if utf16_length(text) > 3900:
                await self.message(
                    platform,
                    chat,
                    "The corrected post plus signature exceeds 3900 UTF-16 units.",
                )
                return
            self.store.connection.execute(
                "UPDATE curator_topics SET text=?,state='pending',revision=revision+1,updated_at=? WHERE id=?",
                (text, time.time(), topic["id"]),
            )
        else:
            self.store.connection.execute(
                "UPDATE curator_topics SET state='researching',revision=revision+1 WHERE id=?",
                (topic["id"],),
            )
            self.store.enqueue(
                "regenerate",
                {
                    "topic_id": topic["id"],
                    "instruction": "" if text == "/skip" else text,
                    "preview_only": topic["state"] == "published",
                },
            )
        self.store.connection.execute(
            "DELETE FROM curator_replies WHERE platform=? AND chat_id=? AND message_id=?",
            (platform, chat, reply_id),
        )
        self.store.dirty(topic["id"])
