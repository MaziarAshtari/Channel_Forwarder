import asyncio
import json
import logging

from .clients import ApiError
from .health import BALE_MODERATION_WARNING, discussion_health
from .jev import JevClient
from .redaction import Redactor, config_secrets

log = logging.getLogger(__name__)


class Moderator:
    def __init__(self, config, telegram, store, *, jev=None):
        self.config, self.telegram, self.store = config, telegram, store
        self.jev, self._owns_jev = jev, jev is None
        self.group_id = config.telegram_discussion_group_id
        self.bot_id = None
        self.can_delete = False
        self.redactor = Redactor(*config_secrets(config))

    async def prepare(self, channel_health) -> dict:
        if not self.config.moderation_enabled:
            return {"enabled": False}
        log.warning("%s", BALE_MODERATION_WARNING)
        if not self.config.typesafe_api_key:
            log.warning("TYPESAFE_API_KEY is empty; moderation will fail open and keep comments.")
        group = await discussion_health(self.config, self.telegram, channel_health)
        self.group_id = group["chat_id"]
        self.bot_id = group.get("bot_id") or channel_health.get("getMe", {}).get("result", {}).get("id")
        self.can_delete = group["ok"]
        if not group["ok"]:
            log.warning("Telegram moderation unavailable: %s", self.redactor.text(group.get("error", "Discussion check failed")))
        if self.jev is None:
            self.jev = JevClient(self.config)
        return {"enabled": True, "telegram": group,
                "bale": {"supported": False, "status": "unsupported", "warning": BALE_MODERATION_WARNING}}

    async def aclose(self):
        if self._owns_jev and self.jev is not None:
            await self.jev.aclose()

    def is_discussion_message(self, platform, update) -> bool:
        # Bale native comments have no documented delivery path. Never guess one.
        message = update.get("message")
        return (self.config.moderation_enabled and platform == "telegram" and self.group_id is not None
                and isinstance(message, dict) and isinstance(message.get("chat"), dict)
                and message["chat"].get("id") == self.group_id
                and message["chat"].get("type") in {"group", "supergroup"}
                and type(message.get("message_id")) is int and message["message_id"] > 0)

    def skip_reason(self, message) -> str | None:
        if message.get("is_automatic_forward"):
            return "automatic channel forward"
        sender_chat = message.get("sender_chat")
        if sender_chat:
            # sender_chat also represents anonymous admins; from may be a fake user.
            return "sent as a chat rather than a real user"
        user = message.get("from")
        if not isinstance(user, dict) or type(user.get("id")) is not int or user["id"] <= 0:
            return "no real user identity"
        if user.get("is_bot") or user["id"] == self.bot_id:
            return "bot author"
        if not any(isinstance(message.get(key), str) and message[key].strip() for key in ("text", "caption")):
            return "no text or caption"
        return None

    async def handle(self, platform, update) -> dict | None:
        if not self.is_discussion_message(platform, update):
            return None
        message = update["message"]
        chat_id, message_id = message["chat"]["id"], message["message_id"]
        text = next((message[key] for key in ("text", "caption")
                     if isinstance(message.get(key), str) and message[key].strip()), "")
        user = message.get("from") or {}
        user_id = user.get("id") if isinstance(user, dict) and type(user.get("id")) is int else None
        if not self.store.claim_moderation(platform, chat_id, message_id, user_id, text):
            log.info("%s moderation chat=%s message=%s: duplicate comment", platform, chat_id, message_id)
            return {"action": "duplicate", "chat_id": chat_id, "message_id": message_id}
        scores = {}

        def finish(action, reason=None):
            self.store.finish_moderation(platform, chat_id, message_id, action=action, scores=scores, reason=reason)
            log.info("%s moderation chat=%s message=%s user=%s action=%s scores=%s reason=%s",
                     platform, chat_id, message_id, user_id, action, json.dumps(scores, sort_keys=True),
                     self.redactor.text(reason or ""))
            return {"action": action, "chat_id": chat_id, "message_id": message_id,
                    "scores": scores, "reason": reason}

        reason = self.skip_reason(message)
        if reason:
            return finish("skipped", reason)
        try:
            if self.config.moderation_skip_admins:
                member = (await self.telegram.get_chat_member(chat_id, user_id))["result"]
                status = member.get("status")
                if status in {"creator", "owner", "administrator"}:
                    return finish("skipped", "group administrator")
                if status not in {"member", "restricted", "left", "kicked"}:
                    raise ApiError("telegram", "getChatMember", "Unknown comment author membership")
            scores = await self.jev.classify(text)
        except Exception as exc:
            # Cancellation propagates; the durable claim is recovered on restart.
            reason = self.redactor.text(str(exc))
            log.warning("%s moderation chat=%s message=%s failed open; keeping comment: %s",
                        platform, chat_id, message_id, reason)
            return finish("kept_error", reason)
        if scores["should_delete"] >= self.config.moderation_threshold:
            if self.config.dry_run:
                return finish("dry_run", "would delete at or above threshold")
            if not self.can_delete:
                log.warning("Telegram moderation chat=%s message=%s: keeping comment because delete rights are unavailable",
                            chat_id, message_id)
                return finish("kept_no_delete_rights", "discussion admin/delete rights check failed")
            finish("deleting", "should_delete at or above threshold")
            try:
                response = await self.telegram.delete_message(chat_id, message_id)
                if response.get("result") is not True:
                    raise ApiError("telegram", "deleteMessage", "Invalid deletion response", uncertain=True)
            except asyncio.CancelledError:
                finish("delete_uncertain", "Interrupted during deletion; refusing replay")
                raise
            except Exception as exc:
                reason = self.redactor.text(str(exc))
                log.warning("Telegram moderation chat=%s message=%s deletion failed: %s", chat_id, message_id, reason)
                return finish("delete_uncertain" if getattr(exc, "uncertain", True) else "delete_failed", reason)
            return finish("deleted", "should_delete at or above threshold")
        review = scores["should_delete"] >= self.config.moderation_review_low
        result = finish("review" if review else "kept", "below deletion threshold")
        if review and self.config.moderation_alert_chat_id is not None:
            if self.config.dry_run:
                self.store.moderation_alert(platform, chat_id, message_id, "dry_run")
                log.info("Telegram moderation chat=%s message=%s: would send review alert", chat_id, message_id)
            else:
                await self.alert(chat_id, message_id, user_id, text, scores)
        return result

    async def alert(self, chat_id, message_id, user_id, text, scores):
        self.store.moderation_alert("telegram", chat_id, message_id, "sending")
        body = (f"Moderation review: Telegram chat {chat_id}, message {message_id}, user {user_id}\n"
                f"Scores: {json.dumps(scores, sort_keys=True)}\nComment: {text[:2500]}")
        try:
            # A concurrent forwarding/probe send may own this client's recorder.
            # Alert outcomes belong in the moderation audit, not its send journal.
            await self.telegram.send_message(self.config.moderation_alert_chat_id, self.redactor.text(body),
                                             record_attempts=False)
        except asyncio.CancelledError:
            self.store.moderation_alert("telegram", chat_id, message_id, "uncertain")
            raise
        except Exception as exc:
            self.store.moderation_alert("telegram", chat_id, message_id,
                                        "uncertain" if getattr(exc, "uncertain", True) else "failed")
            log.warning("Moderation review alert failed: %s", self.redactor.text(str(exc)))
        else:
            self.store.moderation_alert("telegram", chat_id, message_id, "sent")
