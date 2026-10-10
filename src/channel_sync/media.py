from dataclasses import dataclass
import mimetypes
from pathlib import PurePath

from .clients import DOWNLOAD_LIMIT, PHOTO_LIMIT, UPLOAD_LIMIT


class FileTooLarge(ValueError):
    pass


@dataclass
class Attachment:
    kind: str
    content: bytes
    filename: str
    mime_type: str
    properties: dict


def media_kind(message: dict) -> str:
    # Bale album echoes contain BOTH document and photo. Treat each as one photo.
    return next((kind for kind in ("photo", "video", "document") if message.get(kind)), "text")


def media_info(message: dict) -> tuple[str, dict]:
    kind = media_kind(message)
    if kind == "photo":
        return kind, max(message["photo"], key=lambda p: (p.get("width", 0) * p.get("height", 0), p.get("file_size", 0)))
    return kind, message.get(kind, {})


def stable_properties(message: dict) -> dict:
    kind, info = media_info(message)
    if kind in {"photo", "video"}:
        # File size and platform file IDs change during image processing.
        return {key: int(info[key]) for key in ("width", "height") if info.get(key)}
    if kind == "document":
        return {key: info[key] for key in ("file_name", "file_size") if info.get(key) is not None}
    return {}


async def download_attachment(client, message: dict) -> Attachment | None:
    kind, info = media_info(message)
    if kind == "text":
        return None
    limit = min(DOWNLOAD_LIMIT, PHOTO_LIMIT if kind == "photo" else UPLOAD_LIMIT)
    if info.get("file_size", 0) > limit:
        raise FileTooLarge(f"{kind} exceeds the {limit // 1024 // 1024} MiB transfer limit")
    try:
        content = await client.download_file(info["file_id"])
    except ValueError as exc:
        if "exceeds" in str(exc):
            raise FileTooLarge(str(exc)) from None
        raise
    if len(content) > limit:
        raise FileTooLarge(f"{kind} exceeds the transfer limit")
    filename = PurePath(info.get("file_name") or ("photo.jpg" if kind == "photo" else "video.mp4")).name
    # Detect actual PNG uploads, including the captured PNG album fixtures.
    if kind == "photo" and content.startswith(b"\x89PNG\r\n\x1a\n"):
        filename = "photo.png"
    mime = info.get("mime_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    properties = stable_properties(message)
    if kind == "document":
        properties = {"file_name": filename, "file_size": len(content)}
    return Attachment(kind, content, filename, mime, properties)
