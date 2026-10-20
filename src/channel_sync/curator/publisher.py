import asyncio
import hashlib
import json
import os
import tempfile
from urllib.parse import urlsplit

import httpx

from ..clients import ApiError, DOWNLOAD_LIMIT, PHOTO_LIMIT, client_write_lock
from ..formatting import escape_bale
from ..messages import utf16_length
from ..signatures import echo_signature


def split_text(text, limit=4096):
    parts = []
    while text:
        units = 0
        end = 0
        for index, char in enumerate(text):
            units += utf16_length(char)
            if units > limit:
                break
            end = index + 1
        if end == 0:
            raise ValueError("Text splitting limit too small")
        if end < len(text):
            boundary = text.rfind("\n", 0, end)
            if boundary > end // 2:
                end = boundary + 1
        parts.append(text[:end])
        text = text[end:]
    return parts


def publication_parts(text, media, platform):
    # Official Telegram: 1024 caption / 4096 text. Bale single photo/video:
    # 4096 caption; InputMedia album captions: 1024. See CURATOR.md references.
    caption_limit = 4096 if platform == "bale" and len(media) == 1 else 1024
    caption = text if media and utf16_length(text) <= caption_limit else ""
    parts = []
    for offset in range(0, len(media), 10):
        group = [
            {**m, "text": caption if offset + i == 0 else "", "entities": []}
            for i, m in enumerate(media[offset : offset + 10])
        ]
        parts.append(group)
    if not media or not caption:
        parts += [
            [{"kind": "text", "text": chunk, "entities": []}]
            for chunk in split_text(text)
        ]
    if platform == "bale":
        parts = [
            [{**item, "text": escape_bale(item["text"])} for item in group]
            for group in parts
        ]
    return parts


def expected_message(item):
    if item["kind"] == "text":
        return {"text": item["text"]}
    info = item.get("properties", {})
    return {
        "caption": item["text"],
        item["kind"]: [info] if item["kind"] == "photo" else info,
    }


class Publisher:
    def __init__(self, config, clients, store):
        self.config, self.clients, self.store = config, clients, store
        self.media_http = httpx.AsyncClient(
            trust_env=False,
            follow_redirects=False,
            proxy=config.x_proxy_url,
            timeout=config.request_timeout,
        )
        self.cache = config.database_path.parent / "curator-media"

    async def aclose(self):
        await self.media_http.aclose()

    @staticmethod
    def cache_bytes(path, content):
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".media-")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    async def media(self, posts):
        result, seen = [], set()
        self.cache.mkdir(parents=True, exist_ok=True, mode=0o700)
        for post in posts:
            originals = [post, post.get("quoted_post", {})]
            for source in originals:
                for item in source.get("media", []):
                    # Retain every URL in SQLite, while keeping publication
                    # bounded to one album and at most 200 MiB of transfer bytes.
                    if len(result) >= 10:
                        return result
                    url = item.get("url", "")
                    host = urlsplit(url).hostname
                    if (
                        urlsplit(url).scheme != "https"
                        or host not in {"pbs.twimg.com", "video.twimg.com"}
                        or url in seen
                    ):
                        continue
                    seen.add(url)
                    if item.get("kind") not in {"photo", "video"}:
                        continue
                    path = self.cache / (
                        hashlib.sha256(url.encode()).hexdigest()
                        + (".jpg" if item["kind"] == "photo" else ".mp4")
                    )
                    limit = PHOTO_LIMIT if item["kind"] == "photo" else DOWNLOAD_LIMIT
                    if path.exists():
                        content = await asyncio.to_thread(path.read_bytes)
                    else:
                        async with self.media_http.stream("GET", url) as response:
                            if not response.is_success:
                                raise ValueError(
                                    f"Original media download rejected (HTTP {response.status_code})"
                                )
                            data = bytearray()
                            async for chunk in response.aiter_bytes():
                                data.extend(chunk)
                                if len(data) > limit:
                                    raise ValueError(
                                        "Original media exceeds transfer limit"
                                    )
                            content = bytes(data)
                        await asyncio.to_thread(self.cache_bytes, path, content)
                        path.chmod(0o600)
                    if len(content) > limit:
                        raise ValueError("Cached media exceeds transfer limit")
                    filename, mime = (
                        path.name,
                        ("image/jpeg" if item["kind"] == "photo" else "video/mp4"),
                    )
                    if item["kind"] == "photo" and content.startswith(
                        b"\x89PNG\r\n\x1a\n"
                    ):
                        filename, mime = path.stem + ".png", "image/png"
                    result.append(
                        {
                            "kind": item["kind"],
                            "content": content,
                            "filename": filename,
                            "mime_type": mime,
                            "properties": {
                                k: int(item[k])
                                for k in ("width", "height")
                                if item.get(k)
                            },
                        }
                    )
        return result

    async def send_part(self, key, platform, chat_id, items, *, retry_failed=False):
        async with client_write_lock(self.clients[platform]):
            row = self.store.send(key)
            if row and row["status"] == "sent":
                return json.loads(row["response_json"])
            if row and row["status"] in {"sending", "uncertain"}:
                raise ApiError(
                    platform,
                    row["method"],
                    "Uncertain send requires manual reconciliation",
                    uncertain=True,
                )
            if row and row["status"] == "failed":
                if not retry_failed:
                    error = json.loads(row["error_json"])
                    raise ApiError(
                        platform,
                        row["method"],
                        error.get("error", "Rejected"),
                        code=error.get("code"),
                    )
                # A known rejection can reuse its identity; accepted/ambiguous sends cannot.
                with self.store.transaction() as db:
                    db.execute(
                        "UPDATE sent_messages SET status='prepared' WHERE send_key=? AND status='failed'",
                        (key,),
                    )
                    db.execute(
                        "UPDATE pending_echoes SET remaining=1 WHERE send_key=?", (key,)
                    )
            method = (
                "sendMediaGroup"
                if len(items) > 1
                else {
                    "text": "sendMessage",
                    "photo": "sendPhoto",
                    "video": "sendVideo",
                }[items[0]["kind"]]
            )
            if not row:
                self.store.prepare_forward_send(
                    send_key=key,
                    platform=platform,
                    chat_id=chat_id,
                    method=method,
                    request={
                        "curator": True,
                        "items": [
                            {k: v for k, v in i.items() if k != "content"}
                            for i in items
                        ],
                    },
                    signatures=[
                        echo_signature(expected_message(i), platform) for i in items
                    ],
                    ttl=self.config.signature_ttl,
                )
            self.store.claim_forward_send(key, self.config.signature_ttl)
            client = self.clients[platform]
            previous = client.attempt_recorder
            base_attempt = self.store.connection.execute(
                "SELECT COALESCE(MAX(attempt),0) FROM send_attempts WHERE send_key=?",
                (key,),
            ).fetchone()[0]

            def record(attempt):
                if attempt["method"] == method:
                    self.store.record_send_attempt(
                        key, base_attempt + attempt["attempt"], attempt
                    )

            client.attempt_recorder = record
            try:
                response = await client.send_content(chat_id, items)
                results = response.get("result")
                results = results if isinstance(results, list) else [results]
                if len(results) != len(items) or any(
                    not isinstance(m, dict) or type(m.get("message_id")) is not int
                    for m in results
                ):
                    raise ApiError(
                        platform,
                        method,
                        "Incomplete publication response",
                        uncertain=True,
                    )
            except BaseException as exc:
                uncertain = not isinstance(exc, ApiError) or exc.uncertain
                self.store.finish_forward_send(
                    key,
                    status="uncertain" if uncertain else "failed",
                    error={
                        "error": self.store.redactor.text(str(exc)),
                        "code": getattr(exc, "code", None),
                        "uncertain": uncertain,
                    },
                    ttl=self.config.signature_ttl,
                )
                raise
            else:
                self.store.finish_forward_send(
                    key, status="sent", response=response, ttl=self.config.signature_ttl
                )
                return response
            finally:
                client.attempt_recorder = previous

    async def publish(self, topic_id, platform, *, retry_failed=False):
        topic = self.store.topic(topic_id)
        if not topic or topic["state"] not in {
            "approved",
            "published",
            "partial",
            "dry_run",
        }:
            return
        row = self.store.connection.execute(
            "SELECT state,snapshot_json FROM curator_publications WHERE topic_id=? AND platform=?",
            (topic_id, platform),
        ).fetchone()
        if not row or row[0] in {"sent", "dry_run"} or row[0] == "uncertain":
            return
        snapshot = json.loads(row["snapshot_json"])
        topic = {**topic, **snapshot}
        if self.config.dry_run:
            state, error = "dry_run", None
        else:
            try:
                media = await self.media(snapshot["posts"])
                parts = publication_parts(topic["text"], media, platform)
                chat_id = (
                    self.config.telegram_channel_id
                    if platform == "telegram"
                    else self.config.bale_channel_id
                )
                for index, items in enumerate(parts):
                    key = f"curator:{topic_id}:{topic['revision']}:{platform}:{index}"
                    try:
                        await self.send_part(
                            key, platform, chat_id, items, retry_failed=retry_failed
                        )
                    except ApiError as exc:
                        if (
                            len(items) < 2
                            or platform != "bale"
                            or exc.uncertain
                            or exc.code != 400
                            or "UNSUPPORTED_FILE" not in str(exc).upper()
                        ):
                            raise
                        for item_index, item in enumerate(items):
                            await self.send_part(
                                f"{key}:single:{item_index}",
                                platform,
                                chat_id,
                                [item],
                                retry_failed=retry_failed,
                            )
                state, error = "sent", None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state = "uncertain" if getattr(exc, "uncertain", False) else "failed"
                error = self.store.redactor.text(str(exc))
        with self.store.transaction() as db:
            db.execute(
                "UPDATE curator_publications SET state=?,error=? WHERE topic_id=? AND platform=?",
                (state, error, topic_id, platform),
            )
            states = [
                r[0]
                for r in db.execute(
                    "SELECT state FROM curator_publications WHERE topic_id=?",
                    (topic_id,),
                )
            ]
            overall = (
                "published"
                if states == ["sent", "sent"]
                else "dry_run"
                if states == ["dry_run", "dry_run"]
                else "partial"
            )
            db.execute(
                "UPDATE curator_topics SET state=? WHERE id=?", (overall, topic_id)
            )
            self.store.dirty(topic_id)
