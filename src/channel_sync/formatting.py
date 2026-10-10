"""The bold/italic/link subset shared by Telegram and Bale."""
from dataclasses import dataclass

from .messages import utf16_length, utf16_slice


@dataclass(frozen=True)
class RichText:
    text: str
    entities: list[dict]


def _find_unescaped(text: str, needle: str, start: int) -> int:
    while start < len(text):
        if text[start] == "\\":
            start += 2
        elif text.startswith(needle, start):
            return start
        else:
            start += 1
    return -1


def bale_to_telegram(markdown: str) -> RichText:
    def parse(text: str) -> RichText:
        output, entities = "", []
        index = 0
        while index < len(text):
            char = text[index]
            if char == "\\" and index + 1 < len(text) and text[index + 1] in "\\*_[]()":
                output += text[index + 1]
                index += 2
                continue
            if char in "*_":
                end = _find_unescaped(text, char, index + 1)
                if end > index + 1:
                    inner = parse(text[index + 1:end])
                    offset = utf16_length(output)
                    output += inner.text
                    entities.extend({**e, "offset": e["offset"] + offset} for e in inner.entities)
                    entities.append({"type": "bold" if char == "*" else "italic",
                                     "offset": offset, "length": utf16_length(inner.text)})
                    index = end + 1
                    continue
            if char == "[":
                label_end = _find_unescaped(text, "]", index + 1)
                if label_end != -1 and text[label_end:label_end + 2] == "](":
                    url_end = _find_unescaped(text, ")", label_end + 2)
                    if url_end != -1:
                        inner = parse(text[index + 1:label_end])
                        url = text[label_end + 2:url_end]
                        for escaped in ("*", "_", "[", "]", "(", ")"):
                            url = url.replace("\\" + escaped, escaped)
                        url = url.replace("\\\\", "\\")
                        offset = utf16_length(output)
                        output += inner.text
                        entities.extend({**e, "offset": e["offset"] + offset} for e in inner.entities)
                        if inner.text:
                            entities.append({"type": "text_link", "offset": offset,
                                             "length": utf16_length(inner.text), "url": url})
                        index = url_end + 1
                        continue
            output += char
            index += 1
        return RichText(output, sorted(entities, key=lambda e: (e["offset"], -e["length"], e["type"])))
    return parse(markdown)


def escape_bale(text: str) -> str:
    return "".join("\\" + char if char in "\\*_[]()" else char for char in text)


def telegram_to_bale(text: str, entities: list[dict] | None) -> str:
    # Render an entity tree. Crossing intervals cannot be expressed by Markdown;
    # retain visible text rather than emitting malformed formatting.
    boundaries = {utf16_length(text[:index]): index for index in range(len(text) + 1)}
    spans = []
    for entity in entities or []:
        if entity.get("type") not in {"bold", "italic", "text_link"}:
            continue
        start, length = entity.get("offset", -1), entity.get("length", -1)
        if start not in boundaries or start + length not in boundaries or length <= 0:
            raise ValueError("Invalid UTF-16 entity boundary.")
        utf16_slice(text, start, length)
        spans.append((boundaries[start], boundaries[start + length], entity))
    spans.sort(key=lambda span: (span[0], -span[1], span[2]["type"]))
    for index, (start, end, _) in enumerate(spans):
        if any(start < other_start < end < other_end for other_start, other_end, _ in spans[index + 1:]):
            return escape_bale(text)

    def render(start: int, end: int, children: list) -> str:
        result, cursor, index = "", start, 0
        while index < len(children):
            left, right, entity = children[index]
            result += escape_bale(text[cursor:left])
            nested = []
            index += 1
            while index < len(children) and children[index][0] >= left and children[index][1] <= right:
                nested.append(children[index])
                index += 1
            body = render(left, right, nested)
            if entity["type"] == "text_link":
                url = escape_bale(str(entity.get("url", "")))
                body = f"[{body}]({url})"
            else:
                marker = "*" if entity["type"] == "bold" else "_"
                body = marker + body + marker
            result += body
            cursor = right
        return result + escape_bale(text[cursor:end])
    return render(0, len(text), spans)


def convert(text: str, entities: list[dict] | None, source: str, target: str) -> RichText:
    plain = bale_to_telegram(text) if source == "bale" else RichText(text, entities or [])
    if target == "bale":
        return RichText(telegram_to_bale(plain.text, plain.entities), [])
    return plain
