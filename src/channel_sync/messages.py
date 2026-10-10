"""Inspection helpers, not a cross-platform formatting converter."""
import hashlib
import json
import re
import unicodedata

from .store import Store


def utf16_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def utf16_slice(text: str, offset: int, length: int) -> str:
    if offset < 0 or length < 0:
        raise ValueError("UTF-16 offsets cannot be negative.")
    encoded = text.encode("utf-16-le")
    if 2 * (offset + length) > len(encoded):
        raise ValueError("Entity extends beyond text.")
    return encoded[2 * offset:2 * (offset + length)].decode("utf-16-le")


def extract_message(update: dict, platform: str) -> dict | None:
    message = update.get("channel_post" if platform == "telegram" else "message")
    return message if isinstance(message, dict) else None


def visible_text(text: str, platform: str) -> str:
    if platform == "bale":
        # Only the observed bold/italic/link subset, for signature comparison.
        text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
        text = re.sub(r"(?<!\\)([*_])(.+?)(?<!\\)\1", r"\2", text)
        text = re.sub(r"\\([*_\[\]\\])", r"\1", text)
    return unicodedata.normalize("NFC", text).replace("\r\n", "\n")


def content_signature(message: dict, platform: str) -> str:
    kind = next((key for key in ("photo", "video", "document", "audio", "animation", "voice")
                 if key in message), "text")
    payload = {"kind": kind, "text": visible_text(message.get("text", message.get("caption", "")), platform)}
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def own_author(message: dict, bot_id: int) -> str | None:
    for key in ("from", "via_bot"):
        if isinstance(message.get(key), dict) and message[key].get("id") == bot_id:
            return f"{key}.id"
    # sender_chat is the channel for human posts too, so it proves nothing here.
    return None


def loop_reason(store: Store, platform: str, chat_id: int, message: dict, bot_id: int,
                *, now: float | None = None) -> str | None:
    author = own_author(message, bot_id)
    if author:
        return f"own bot: {author}"
    if store.matches_sent(platform, chat_id, content_signature(message, platform), now):
        return "recent sent content signature"
    return None
