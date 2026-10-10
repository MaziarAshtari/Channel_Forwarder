from pathlib import Path
import json
import tempfile
import unittest

from channel_sync.formatting import bale_to_telegram, telegram_to_bale, convert
from channel_sync.messages import utf16_slice
from channel_sync.signatures import PayloadCodec, echo_signature
from helpers import config, TELEGRAM_TOKEN, BALE_TOKEN


class ConverterTests(unittest.TestCase):
    def test_bold_italic_link_emoji_offsets(self):
        rich = bale_to_telegram("😀 *bold* _italic_ [link](https://example.com/)")
        self.assertEqual(rich.text, "😀 bold italic link")
        self.assertEqual([(e["type"], utf16_slice(rich.text, e["offset"], e["length"])) for e in rich.entities],
                         [("bold", "bold"), ("italic", "italic"), ("text_link", "link")])
        self.assertEqual(rich.entities[0]["offset"], 3)

    def test_literal_markdown_characters_are_escaped_and_round_trip(self):
        text = "literal *bold* _italic_ [brackets](parentheses) \\ tail 😀"
        rendered = telegram_to_bale(text, [])
        self.assertIn("\\*bold\\*", rendered)
        restored = bale_to_telegram(rendered)
        self.assertEqual(restored.text, text)
        self.assertEqual(restored.entities, [])

    def test_nested_bold_italic_link_round_trip(self):
        rich = bale_to_telegram("[*bold _italic_*](https://example.com/a\\(b\\))")
        self.assertEqual(rich.text, "bold italic")
        self.assertEqual({e["type"] for e in rich.entities}, {"bold", "italic", "text_link"})
        self.assertEqual(next(e["url"] for e in rich.entities if e["type"] == "text_link"), "https://example.com/a(b)")
        again = bale_to_telegram(telegram_to_bale(rich.text, rich.entities))
        self.assertEqual(again.text, rich.text)
        self.assertEqual(again.entities, rich.entities)

    def test_adjacent_styles_and_same_span_styles(self):
        rich = bale_to_telegram("*a*_b_ *_😀_*")
        self.assertEqual(rich.text, "ab 😀")
        self.assertEqual(bale_to_telegram(telegram_to_bale(rich.text, rich.entities)), rich)

    def test_unclosed_markers_remain_visible(self):
        text = "an unmatched * and [link](broken"
        self.assertEqual(bale_to_telegram(text).text, text)

    def test_crossing_entities_fall_back_to_visible_text(self):
        text = "abcdef"
        rendered = telegram_to_bale(text, [{"type": "bold", "offset": 0, "length": 4},
                                           {"type": "italic", "offset": 2, "length": 4}])
        self.assertEqual(bale_to_telegram(rendered).text, text)
        self.assertEqual(bale_to_telegram(rendered).entities, [])

    def test_split_surrogate_entity_is_rejected(self):
        with self.assertRaises(ValueError):
            telegram_to_bale("😀 text", [{"type": "bold", "offset": 1, "length": 2}])

    def test_unsupported_entity_retains_text(self):
        rendered = telegram_to_bale("code", [{"type": "code", "offset": 0, "length": 4}])
        self.assertEqual(rendered, "code")

    def test_bale_to_telegram_uses_entities(self):
        rich = convert("caption *bold*", [], "bale", "telegram")
        self.assertEqual(rich.text, "caption bold")
        self.assertEqual(rich.entities, [{"type": "bold", "offset": 8, "length": 4}])

    def test_formatting_around_link_with_reserved_url_characters(self):
        text = "link"
        url = "https://example.com/a*_b[c](d)"
        entities = [{"type": "bold", "offset": 0, "length": 4},
                    {"type": "text_link", "offset": 0, "length": 4, "url": url}]
        result = bale_to_telegram(telegram_to_bale(text, entities))
        self.assertEqual(result.text, text)
        self.assertEqual(next(e["url"] for e in result.entities if e["type"] == "text_link"), url)


class SignatureTests(unittest.TestCase):
    def test_whitespace_and_markdown_normalization(self):
        a = {"text": "😀 bold\n italic   link"}
        b = {"text": "😀 *bold*\t_italic_ [link](https://example.com/)"}
        self.assertEqual(echo_signature(a, "telegram"), echo_signature(b, "bale"))

    def test_ids_file_sizes_album_ids_authors_are_excluded_for_photos(self):
        a = {"message_id": 25, "media_group_id": "old", "photo": [{"width": 128, "height": 128, "file_size": 100, "file_id": "a"}], "caption": "hello"}
        b = {"message_id": 26, "media_group_id": "new", "from": {"id": 0},
             "document": {"file_name": "probe.png", "file_size": 300},
             "photo": [{"width": 128, "height": 128, "file_size": 300, "file_id": "b"}], "caption": "hello"}
        self.assertEqual(echo_signature(a, "telegram"), echo_signature(b, "bale"))

    def test_stable_media_properties_and_kind_differentiate_content(self):
        a = {"photo": [{"width": 128, "height": 128}], "caption": "caption"}
        b = {"photo": [{"width": 256, "height": 128}], "caption": "caption"}
        video = {"video": {"width": 128, "height": 128}, "caption": "caption"}
        self.assertNotEqual(echo_signature(a, "bale"), echo_signature(b, "bale"))
        self.assertNotEqual(echo_signature(a, "bale"), echo_signature(video, "bale"))

    def test_document_uses_filename_and_size(self):
        a = {"document": {"file_name": "a.pdf", "file_size": 12, "file_id": "a"}}
        b = {"document": {"file_name": "a.pdf", "file_size": 12, "file_id": "b"}}
        self.assertEqual(echo_signature(a, "telegram"), echo_signature(b, "bale"))
        self.assertNotEqual(echo_signature(a, "telegram"), echo_signature({"document": {"file_name": "b.pdf", "file_size": 12}}, "bale"))

    def test_codec_removes_embedded_tokens_but_recovers_file_handles(self):
        with tempfile.TemporaryDirectory() as root:
            codec = PayloadCodec(config(Path(root)))
            original = {"photo": [{"file_id": f"prefix:{BALE_TOKEN}:suffix"}], "document": {"file_unique_id": TELEGRAM_TOKEN}}
            encoded = codec.encode(original)
            self.assertNotIn(TELEGRAM_TOKEN, json.dumps(encoded))
            self.assertNotIn(BALE_TOKEN, json.dumps(encoded))
            self.assertEqual(codec.decode(json.loads(json.dumps(encoded))), original)
