import asyncio
import logging
import re
import time

from ..clients import ApiError, client_write_lock
from ..formatting import escape_bale
from ..messages import utf16_length
from ..persian import challenge_description, persian_lines, platform_name, state_name
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
        text = persian_lines(text)
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

    @staticmethod
    def button(topic, label, action):
        return {
            "text": label,
            "callback_data": f"c:{topic['id']}:{topic['revision']}:{action}",
        }

    def buttons(self, topic):
        rows = []
        if topic["state"] == "pending":
            rows = [
                [
                    self.button(topic, "✅ تأیید", "approve"),
                    self.button(topic, "❌ لغو", "reject"),
                    self.button(topic, "✏️ ویرایش", "edit"),
                ],
            ]
        return {"inline_keyboard": rows}

    def control_text(self, topic):
        deliveries = [
            f"انتشار در {platform_name(r[0])}: {state_name(r[1])}"
            for r in self.store.connection.execute(
                "SELECT platform,state FROM curator_publications WHERE topic_id=?",
                (topic["id"],),
            )
        ]
        header = f"پیش‌نویس شمارهٔ {topic['id']} · {state_name(topic['state'])} · نسخهٔ {topic['revision']}\n"
        if topic.get("actor"):
            actor = topic["actor"]
            legacy = re.fullmatch(
                r"(Approved|Rejected) by (\d+) in (Telegram|Bale)", actor
            )
            if legacy:
                verb = "تأیید" if legacy[1] == "Approved" else "لغو"
                actor = f"{verb} توسط کاربر {legacy[2]} در {platform_name(legacy[3].lower())}"
            if not re.search(r"[\u0600-\u06ff]", actor):
                actor = "اقدام آخر توسط مدیر انجام شد."
            header += actor + "\n"
        if deliveries:
            header += "\n".join(deliveries) + "\n"
        if topic["state"] == "partial":
            header += "بازیابی انتشار ناموفق از فرمان /curator در دسترس است.\n"
        header += "رسانهٔ پست اصلی در صورت وجود به‌طور خودکار همراه متن ارسال می‌شود.\n\n"
        # Keep full draft in its preview messages; control text fits both APIs.
        return header + "موضوع: " + topic["entity"][:300]

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
        if not topic or topic["state"] == "merged":
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
                    if topic["state"] == "researching":
                        # Rewriting invalidates both old controls immediately, but
                        # must not resend the previous draft as a new preview.
                        if row:
                            await self.call(
                                platform,
                                "editMessageText",
                                {
                                    "chat_id": chat,
                                    "message_id": row["message_id"],
                                    "text": escape_bale(text)
                                    if platform == "bale"
                                    else text,
                                    "reply_markup": markup,
                                },
                            )
                            self.store.connection.execute(
                                "UPDATE curator_previews SET revision=?,dirty=? WHERE topic_id=? AND platform=? AND chat_id=?",
                                (
                                    topic["revision"],
                                    self.preview_changed(topic_id, text, markup),
                                    topic_id,
                                    platform,
                                    chat,
                                ),
                            )
                        continue
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
                                "text": f"پیش‌نویس شمارهٔ {topic_id} با نسخهٔ {topic['revision']} جایگزین شد.",
                                "reply_markup": {"inline_keyboard": []},
                            },
                        )
                    if topic["text"]:
                        try:
                            media = await self.publisher.media(
                                self.store.draft_sources(topic_id)
                            )
                        except Exception as exc:
                            media = []
                            log.warning(
                                "Curator media preview failed topic=%s platform=%s error=%s",
                                topic_id,
                                platform,
                                type(exc).__name__,
                            )
                            await self.message(
                                platform,
                                chat,
                                "نمایش رسانه در پیش‌نمایش ممکن نشد؛ هنگام انتشار، دریافت رسانهٔ اصلی دوباره امتحان می‌شود.",
                            )
                        for index, items in enumerate(
                            publication_parts(
                                persian_lines(topic["text"], post=True), media, platform
                            )
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
            f"پیش‌نویس شمارهٔ {r[0]}: موضوع {r[1][:100]}"
            for r in self.store.connection.execute(
                "SELECT id,entity FROM curator_topics WHERE state='pending' ORDER BY id LIMIT 20"
            )
        ]
        data = self.store.stats(self.config)
        reader = data["reader"]
        lines = [
            "وضعیت خبرگزین",
            "خواندن تایم‌لاین: " + ("متوقف" if reader["paused"] else "فعال"),
            "ورود به حساب: " + ("تأییدشده" if reader["logged_in"] else "نیازمند بررسی"),
            f"تاریخ آمار: {data['day']}",
            f"پست‌های دریافت‌شدهٔ امروز: {sum(data['today'].values())}",
            f"تکراری‌های امروز: {data['today'].get('duplicate', 0)}",
        ]
        if reader["challenge"]:
            lines.append("دلیل توقف: " + challenge_description(reader["reason"]))
        if reader["last_error"]:
            lines.append(
                "مرورگر با خطا روبه‌رو شده است؛ جزئیات در گزارش سرویس ثبت شده است."
            )
        for label, key in (
            ("پست", "today"),
            ("موضوع", "topics"),
            ("انتشار", "publications"),
        ):
            lines.extend(
                f"آمار {label}، {state_name(state)}: {count}"
                for state, count in data[key].items()
            )
        lines += [
            "پیش‌نویس‌های در انتظار تأیید:",
            *(pending or ["پیش‌نویسی در انتظار تأیید نیست."]),
        ]
        lines.append(
            "راهنما: فرمان‌های /curator pause و /curator resume و /curator pending در دسترس‌اند."
        )
        markup = {
            "inline_keyboard": [
                [
                    {"text": "توقف", "callback_data": "c:pause"},
                    {"text": "ادامه", "callback_data": "c:resume"},
                    {"text": "پیش‌نویس‌ها", "callback_data": "c:pending"},
                ]
            ]
        }
        # Recovery controls belong to status, not to the three-button draft UI.
        for row in self.store.connection.execute(
            "SELECT t.id,t.revision,p.platform FROM curator_publications p "
            "JOIN curator_topics t ON t.id=p.topic_id WHERE p.state='failed' ORDER BY t.id LIMIT 20"
        ):
            markup["inline_keyboard"].append(
                [
                    self.button(
                        row,
                        f"تلاش دوباره برای انتشار شمارهٔ {row['id']} در {platform_name(row['platform'])}",
                        "retry_" + row["platform"],
                    )
                ]
            )
        await self.message(platform, chat, "\n".join(lines)[:3600], markup)
        if command == "pending":
            for row in self.store.connection.execute(
                "SELECT id FROM curator_topics WHERE state='pending' ORDER BY id LIMIT 20"
            ):
                await self.sync_topic(row[0], force=True)

    async def edit_menu(self, platform, chat, topic):
        markup = {
            "inline_keyboard": [
                [self.button(topic, "✍️ ارسال متن جدید", "replace")],
                [self.button(topic, "🤖 دستور بازنویسی", "rewrite")],
                [self.button(topic, "↩️ بازگشت", "back")],
            ]
        }
        response = await self.message(
            platform,
            chat,
            f"ویرایش پیش‌نویس شمارهٔ {topic['id']}؛ روش ویرایش را انتخاب کنید.",
            markup,
        )
        with self.store.transaction() as db:
            db.execute(
                "DELETE FROM curator_replies WHERE platform=? AND chat_id=?",
                (platform, chat),
            )
            db.execute(
                "INSERT INTO curator_replies VALUES(?,?,?,?,?,?)",
                (
                    platform,
                    chat,
                    response["result"]["message_id"],
                    topic["id"],
                    topic["revision"],
                    "menu",
                ),
            )

    async def menu_action(self, platform, chat, message_id, topic, action):
        if action == "back":
            text = "بازگشت به پیش‌نمایش انجام شد؛ برای تغییر متن دوباره ویرایش را بزنید."
            markup = {"inline_keyboard": []}
        else:
            text = (
                "متن کامل جدید را در پیام بعدی بفرستید؛ این متن جایگزین پیش‌نویس می‌شود."
                if action == "replace"
                else "دستور بازنویسی را در پیام بعدی بفرستید؛ نسخهٔ تازه برای تأیید به هر دو ربات ارسال می‌شود."
            )
            markup = {"inline_keyboard": [[self.button(topic, "↩️ بازگشت", "back")]]}
        await self.call(
            platform,
            "editMessageText",
            {
                "chat_id": chat,
                "message_id": message_id,
                "text": escape_bale(text) if platform == "bale" else text,
                "reply_markup": markup,
            },
        )
        if action == "back":
            self.store.connection.execute(
                "DELETE FROM curator_replies WHERE platform=? AND chat_id=? AND message_id=?",
                (platform, chat, message_id),
            )
        else:
            self.store.connection.execute(
                "UPDATE curator_replies SET mode=? WHERE platform=? AND chat_id=? AND message_id=?",
                (action, platform, chat, message_id),
            )

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
            menu = self.store.connection.execute(
                "SELECT mode FROM curator_replies WHERE platform=? AND chat_id=? AND message_id=? "
                "AND topic_id=? AND revision=? AND mode IN ('menu','replace','rewrite')",
                (platform, chat, message.get("message_id"), ident, revision),
            ).fetchone()
            retry = action in {"retry_telegram", "retry_bale"}
            if action in {"replace", "rewrite", "back"}:
                valid = bool(menu)
            else:
                valid = (
                    bool(preview and preview[0] == message.get("message_id")) or retry
                )
            if not valid:
                return
            topic = self.store.topic(ident)
            if not topic or topic["revision"] != revision:
                await self.message(
                    platform,
                    chat,
                    "نسخهٔ این پیش‌نمایش قدیمی است؛ با فرمان /curator pending نسخهٔ تازه را باز کنید.",
                )
                return
            if action in {"approve", "reject"}:
                verb = "تأیید" if action == "approve" else "لغو"
                actor = f"{verb} توسط کاربر {query['from']['id']} در {platform_name(platform)}"
                if self.store.decide(ident, revision, action, actor):
                    self.store.connection.execute(
                        "DELETE FROM curator_replies WHERE topic_id=?", (ident,)
                    )
                    await self.sync_topic(ident)
            elif retry and topic["state"] == "partial":
                target = action[6:]
                self.store.connection.execute(
                    "UPDATE curator_publications SET state='queued' WHERE topic_id=? AND platform=? AND state='failed'",
                    (ident, target),
                )
                self.store.dirty(ident)
                await self.message(
                    platform,
                    chat,
                    f"انتشار ناموفق در {platform_name(target)} دوباره در صف قرار گرفت.",
                )
            elif action == "edit" and topic["state"] == "pending":
                await self.edit_menu(platform, chat, topic)
            elif (
                action in {"replace", "rewrite", "back"} and topic["state"] == "pending"
            ):
                await self.menu_action(
                    platform, chat, message["message_id"], topic, action
                )
            return
        text = message.get("text", "").strip()
        if text.startswith("/curator"):
            command = text.split(maxsplit=1)[1] if " " in text else "status"
            await self.status(platform, chat, command)
            return
        reply_id = message.get("reply_to_message", {}).get("message_id")
        if reply_id is not None:
            reply = self.store.connection.execute(
                "SELECT * FROM curator_replies WHERE platform=? AND chat_id=? AND message_id=? AND mode IN ('replace','rewrite')",
                (platform, chat, reply_id),
            ).fetchone()
        else:
            # The next private text message is sufficient; a Telegram-style reply
            # is optional, so the same interaction works in both bots.
            reply = self.store.connection.execute(
                "SELECT * FROM curator_replies WHERE platform=? AND chat_id=? AND mode IN ('replace','rewrite') ORDER BY message_id DESC LIMIT 1",
                (platform, chat),
            ).fetchone()
        if not reply or not text:
            return
        topic = self.store.topic(reply["topic_id"])
        if (
            not topic
            or topic["revision"] != reply["revision"]
            or topic["state"] != "pending"
        ):
            await self.message(
                platform,
                chat,
                "درخواست ویرایش قدیمی است؛ نسخهٔ تازهٔ پیش‌نویس را باز کنید.",
            )
            return
        if reply["mode"] == "replace":
            if not text.endswith(self.config.channel_signature):
                text += "\n\n" + self.config.channel_signature
            text = persian_lines(text, post=True)
            if utf16_length(text) > 3900:
                await self.message(
                    platform,
                    chat,
                    "متن جدید همراه امضا باید حداکثر ۳۹۰۰ واحد نوشتاری باشد؛ متن کوتاه‌تری بفرستید.",
                )
                return
        # Atomic revision/state guards keep cross-bot edits and approval exclusive.
        with self.store.transaction() as db:
            if reply["mode"] == "replace":
                changed = db.execute(
                    "UPDATE curator_topics SET text=?,revision=revision+1,updated_at=? WHERE id=? AND revision=? AND state='pending'",
                    (text, time.time(), topic["id"], reply["revision"]),
                ).rowcount
            else:
                changed = db.execute(
                    "UPDATE curator_topics SET state='researching',revision=revision+1,updated_at=? WHERE id=? AND revision=? AND state='pending'",
                    (time.time(), topic["id"], reply["revision"]),
                ).rowcount
                if changed:
                    self.store.enqueue(
                        "rewrite", {"topic_id": topic["id"], "instruction": text}
                    )
            if changed:
                db.execute(
                    "DELETE FROM curator_replies WHERE topic_id=?", (topic["id"],)
                )
                self.store.dirty(topic["id"])
        if changed:
            if reply["mode"] == "rewrite":
                await self.sync_topic(topic["id"])
                await self.message(
                    platform,
                    chat,
                    "بازنویسی آغاز شد؛ پیش‌نمایش تازه برای تأیید به هر دو ربات ارسال می‌شود.",
                )
            else:
                await self.sync_topic(topic["id"])
