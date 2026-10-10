import asyncio
import json
import logging
import random
import time
from typing import Any
from urllib.parse import quote

import httpx

from .config import Config
from .redaction import Redactor

log = logging.getLogger(__name__)
DOWNLOAD_LIMIT = 20 * 1024 * 1024
UPLOAD_LIMIT = 50 * 1024 * 1024
PHOTO_LIMIT = 10 * 1024 * 1024
READ_METHODS = {"getMe", "getChat", "getChatMember", "getFile", "getWebhookInfo", "getUpdates"}


class ApiError(RuntimeError):
    def __init__(self, platform: str, method: str, description: str, *, code: int | None = None,
                 temporary: bool = False, uncertain: bool = False, response=None):
        super().__init__(f"{platform} {method}: {description}")
        self.platform, self.method, self.code = platform, method, code
        self.temporary, self.uncertain, self.response = temporary, uncertain, response

    def as_dict(self):
        return {"error": str(self), "code": self.code, "temporary": self.temporary,
                "uncertain": self.uncertain, "response": self.response}


def make_http_client(config: Config, platform: str, *, transport=None) -> httpx.AsyncClient:
    if platform not in {"telegram", "bale"}:
        raise ValueError("Unknown platform.")
    options = {"trust_env": False, "follow_redirects": False,
               "timeout": httpx.Timeout(config.request_timeout), "transport": transport}
    if platform == "telegram" and config.telegram_proxy_url:
        options["proxy"] = config.telegram_proxy_url
    return httpx.AsyncClient(**options)


class BotClient:
    platform: str
    host: str

    def __init__(self, token: str, config: Config, *, transport=None, sleeper=asyncio.sleep):
        self.config = config
        self.redactor = Redactor(config.telegram_token, config.bale_token, config.telegram_proxy_url,
                                 config.typesafe_api_key)
        self._base_url = f"{self.host}/bot{token}"
        self._file_url = f"{self.host}/file/bot{token}"
        self.http = make_http_client(config, self.platform, transport=transport)
        self._sleep = sleeper
        self.attempt_recorder = None

    def _record_attempt(self, method: str, attempt: int, **details):
        if self.attempt_recorder is not None:
            self.attempt_recorder(self.redactor.data({"platform": self.platform, "method": method,
                                                     "attempt": attempt + 1, "recorded_at": time.time(), **details}))

    async def aclose(self):
        await self.http.aclose()

    async def call(self, method: str, params: dict | None = None, *, files=None, record_attempts=True) -> dict:
        """Return the complete API envelope; never retry ambiguous writes."""
        if not method.isascii() or not method.isalnum():
            raise ValueError("Invalid API method.")
        params = params or {}
        is_read = method in READ_METHODS
        for attempt in range(self.config.retry_attempts):
            retry_after = 0.0
            try:
                kwargs: dict[str, Any] = {}
                if method == "getUpdates":
                    kwargs["timeout"] = httpx.Timeout(
                        max(self.config.request_timeout, params.get("timeout", 0) + 10)
                    )
                if files:
                    kwargs["data"] = {
                        key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict, bool))
                        else str(value) for key, value in params.items()
                    }
                    kwargs["files"] = files
                else:
                    kwargs["json"] = params
                response = await self.http.post(f"{self._base_url}/{method}", **kwargs)
            except httpx.TransportError as exc:
                if record_attempts:
                    self._record_attempt(method, attempt, transport_error=type(exc).__name__)
                error = ApiError(self.platform, method, f"Transport failure ({type(exc).__name__})",
                                 temporary=True, uncertain=not is_read)
                retry = is_read
            else:
                try:
                    envelope = response.json()
                except ValueError:
                    envelope = None
                if record_attempts:
                    self._record_attempt(method, attempt, http_status=response.status_code,
                                         response=envelope if envelope is not None else {"body": response.text})
                valid = isinstance(envelope, dict) and isinstance(envelope.get("ok"), bool)
                if response.is_success and valid and envelope["ok"] and "result" in envelope:
                    return envelope
                code = response.status_code
                if valid and isinstance(envelope.get("error_code"), int):
                    code = envelope["error_code"]
                temporary = code == 429 or 500 <= code <= 599
                # Only an explicit rate-limit rejection is safe to retry for sends.
                safe_rate_limit = valid and envelope["ok"] is False and code == 429
                retry = temporary and (is_read or safe_rate_limit)
                uncertain = not is_read and (code >= 500 or not valid or envelope.get("ok") is True)
                description = str(envelope.get("description", "Invalid API response")) if valid else "Invalid API response"
                error = ApiError(self.platform, method, self.redactor.text(description), code=code,
                                 temporary=temporary, uncertain=uncertain,
                                 response=self.redactor.data(envelope if envelope is not None else
                                                            {"http_status": response.status_code, "body": response.text}))
                if valid:
                    parameters = envelope.get("parameters") or {}
                    try:
                        retry_after = max(0.0, float(parameters.get("retry_after", 0)))
                    except (ValueError, TypeError, AttributeError):
                        retry_after = 0.0
            if not retry or attempt + 1 == self.config.retry_attempts:
                raise error from None
            delay = max(retry_after, self.config.retry_base * 2 ** attempt + random.uniform(0, 0.2))
            log.warning("%s %s temporary failure; retry %s/%s in %.1fs",
                        self.platform, method, attempt + 2, self.config.retry_attempts, delay)
            await self._sleep(delay)
        raise AssertionError("Unreachable retry state.")

    async def get_me(self):
        return await self.call("getMe")

    async def get_chat(self, chat_id: int):
        return await self.call("getChat", {"chat_id": chat_id})

    async def get_chat_member(self, chat_id: int, user_id: int):
        return await self.call("getChatMember", {"chat_id": chat_id, "user_id": user_id})

    async def get_updates(self, offset: int | None, *, timeout: int | None = None):
        params = {"timeout": self.config.poll_timeout if timeout is None else timeout, "limit": 100}
        if offset is not None:
            params["offset"] = offset
        if self.platform == "telegram":
            params["allowed_updates"] = ["channel_post"]
            if self.config.moderation_enabled:
                params["allowed_updates"].append("message")
        # Bale's allowed_updates is undocumented; do not send it.
        return await self.call("getUpdates", params)

    async def send_message(self, chat_id: int, text: str, *, entities: list | None = None, record_attempts=True):
        params = {"chat_id": chat_id, "text": text}
        if entities:
            params["entities"] = entities
        return await self.call("sendMessage", params, record_attempts=record_attempts)

    async def delete_message(self, chat_id: int, message_id: int):
        # Use the existing conservative write retries: ambiguous deletes are audited.
        return await self.call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

    @staticmethod
    def validate_upload(content: bytes, *, photo: bool = False):
        if len(content) > (PHOTO_LIMIT if photo else UPLOAD_LIMIT):
            raise ValueError("Upload exceeds configured Bot API file limit.")

    async def send_photo(self, chat_id: int, content: bytes, caption: str):
        self.validate_upload(content, photo=True)
        return await self.call("sendPhoto", {"chat_id": chat_id, "caption": caption},
                               files={"photo": ("probe.png", content, "image/png")})

    async def send_photo_album(self, chat_id: int, photos: list[tuple[bytes, str]]):
        if not 2 <= len(photos) <= 10:
            raise ValueError("A photo album requires 2–10 photos.")
        media, files = [], {}
        for index, (content, caption) in enumerate(photos):
            self.validate_upload(content, photo=True)
            name = f"photo{index}"
            media.append({"type": "photo", "media": f"attach://{name}", "caption": caption})
            files[name] = (f"probe-{index}.png", content, "image/png")
        return await self.call("sendMediaGroup", {"chat_id": chat_id, "media": media}, files=files)

    async def send_content(self, chat_id: int, items: list[dict]):
        """Send translated text or uploaded media; never reuse cross-platform IDs."""
        if not items or len(items) > 10:
            raise ValueError("A send requires 1–10 items")
        if len(items) == 1:
            item = items[0]
            if item["kind"] == "text":
                params = {"chat_id": chat_id, "text": item["text"]}
                if item.get("entities"):
                    params["entities"] = item["entities"]
                return await self.call("sendMessage", params)
            kind = item["kind"]
            self.validate_upload(item["content"], photo=kind == "photo")
            params = {"chat_id": chat_id, "caption": item["text"]}
            if item.get("entities"):
                params["caption_entities"] = item["entities"]
            return await self.call({"photo": "sendPhoto", "video": "sendVideo", "document": "sendDocument"}[kind], params,
                                   files={kind: (item["filename"], item["content"], item["mime_type"])})
        media, uploads = [], {}
        for index, item in enumerate(items):
            self.validate_upload(item["content"], photo=item["kind"] == "photo")
            name = f"item{index}"
            entry = {"type": item["kind"], "media": f"attach://{name}", "caption": item["text"]}
            if item.get("entities"):
                entry["caption_entities"] = item["entities"]
            media.append(entry)
            uploads[name] = (item["filename"], item["content"], item["mime_type"])
        return await self.call("sendMediaGroup", {"chat_id": chat_id, "media": media}, files=uploads)

    async def download_file(self, file_id: str) -> bytes:
        file = (await self.call("getFile", {"file_id": file_id}))["result"]
        if file.get("file_size", 0) > DOWNLOAD_LIMIT:
            raise ValueError("Download exceeds 20 MB.")
        path = file.get("file_path")
        if not isinstance(path, str) or not path or any(part in {".", ".."} for part in path.split("/")) or path.startswith("/"):
            raise ValueError("Invalid API file path.")
        # Always use our fixed host and client: proxy policy applies to file traffic too.
        url = f"{self._file_url}/{quote(path, safe='/')}"
        for attempt in range(self.config.retry_attempts):
            try:
                async with self.http.stream("GET", url) as response:
                    if not response.is_success:
                        raise ApiError(self.platform, "downloadFile", "File download rejected",
                                       code=response.status_code,
                                       temporary=response.status_code == 429 or response.status_code >= 500)
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > DOWNLOAD_LIMIT:
                            raise ValueError("Download exceeds 20 MB.")
                    return bytes(data)
            except httpx.TransportError as exc:
                error = ApiError(self.platform, "downloadFile", f"Transport failure ({type(exc).__name__})",
                                 temporary=True)
            except ApiError as exc:
                error = exc
            if not error.temporary or attempt + 1 == self.config.retry_attempts:
                raise error from None
            await self._sleep(self.config.retry_base * 2 ** attempt)
        raise AssertionError("Unreachable retry state.")


class TelegramClient(BotClient):
    platform = "telegram"
    host = "https://api.telegram.org"


class BaleClient(BotClient):
    platform = "bale"
    host = "https://tapi.bale.ai"

    async def send_message(self, chat_id: int, text: str, *, entities: list | None = None, record_attempts=True):
        if entities:
            raise ValueError("Bale text uses inline Markdown; Telegram entities are unsupported here.")
        return await super().send_message(chat_id, text, record_attempts=record_attempts)
