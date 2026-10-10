import base64
import hashlib
import json
import unicodedata

from .formatting import bale_to_telegram
from .media import media_kind, stable_properties


def normalized_text(text: str, platform: str) -> str:
    if platform == "bale":
        text = bale_to_telegram(text).text
    return " ".join(unicodedata.normalize("NFC", text).split())


def echo_signature(message: dict, platform: str) -> str:
    value = {"text": normalized_text(message.get("text", message.get("caption", "")), platform),
             "kind": media_kind(message), "properties": stable_properties(message)}
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class PayloadCodec:
    """Persist usable file handles without persisting bot tokens embedded by Bale."""
    def __init__(self, config):
        self.tokens = {"telegram": config.telegram_token, "bale": config.bale_token}

    def encode(self, value):
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if key in {"file_id", "file_unique_id"} and isinstance(item, str):
                    for platform, token in self.tokens.items():
                        item = item.replace(token, f"<CHANNEL_SYNC_TOKEN_{platform}>")
                    result[key] = {"file_ref": base64.urlsafe_b64encode(item.encode()).decode()}
                else:
                    result[key] = self.encode(item)
            return result
        if isinstance(value, list):
            return [self.encode(item) for item in value]
        return value

    def decode(self, value):
        if isinstance(value, dict):
            if set(value) == {"file_ref"}:
                item = base64.urlsafe_b64decode(value["file_ref"]).decode()
                for platform, token in self.tokens.items():
                    item = item.replace(f"<CHANNEL_SYNC_TOKEN_{platform}>", token)
                return item
            return {key: self.decode(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.decode(item) for item in value]
        return value
