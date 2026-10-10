import json
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from channel_sync.clients import ApiError, BaleClient, TelegramClient, make_http_client, DOWNLOAD_LIMIT, PHOTO_LIMIT
from helpers import config, TELEGRAM_TOKEN, BALE_TOKEN


class ClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = config(Path(self.temp.name))
        self.calls = []
        self.delays = []

    async def sleeper(self, delay):
        self.delays.append(delay)

    def client(self, handler, *, platform="bale"):
        cls = BaleClient if platform == "bale" else TelegramClient
        client = cls(BALE_TOKEN if platform == "bale" else TELEGRAM_TOKEN, self.config,
                     transport=httpx.MockTransport(handler), sleeper=self.sleeper)
        self.addAsyncCleanup(client.aclose)
        return client

    async def test_proxy_routing_construction_is_explicit(self):
        conf = config(Path(self.temp.name), telegram_proxy_url="socks5://127.0.0.1:10808")
        with patch("channel_sync.clients.httpx.AsyncClient") as constructor:
            make_http_client(conf, "telegram")
            self.assertEqual(constructor.call_args.kwargs["proxy"], conf.telegram_proxy_url)
            self.assertFalse(constructor.call_args.kwargs["trust_env"])
            make_http_client(conf, "bale")
            self.assertNotIn("proxy", constructor.call_args.kwargs)
            self.assertFalse(constructor.call_args.kwargs["trust_env"])

    async def test_ambient_proxies_are_not_loaded(self):
        with patch.dict("os.environ", {"HTTPS_PROXY": "invalid-proxy", "ALL_PROXY": "invalid-proxy"}):
            client = make_http_client(self.config, "bale")
            try:
                self.assertFalse(client.trust_env)
                self.assertEqual(client._mounts, {})
            finally:
                await client.aclose()

    async def test_bale_uses_message_polling_without_allowed_updates(self):
        def handler(request):
            self.calls.append(json.loads(request.content))
            self.assertEqual(request.url.host, "tapi.bale.ai")
            return httpx.Response(200, json={"ok": True, "result": []})
        await self.client(handler).get_updates(42)
        self.assertEqual(self.calls, [{"offset": 42, "timeout": 10, "limit": 100}])

    async def test_telegram_requests_channel_posts(self):
        def handler(request):
            self.calls.append(json.loads(request.content))
            return httpx.Response(200, json={"ok": True, "result": []})
        await self.client(handler, platform="telegram").get_updates(None)
        self.assertEqual(self.calls[0]["allowed_updates"], ["channel_post"])
        self.assertNotIn("offset", self.calls[0])

    async def test_permanent_400_is_never_retried_or_leaked(self):
        def handler(request):
            self.calls.append(request)
            return httpx.Response(400, json={"ok": False, "error_code": 400,
                                           "description": f"UNSUPPORTED_FILE {BALE_TOKEN}"})
        client = self.client(handler)
        with self.assertRaises(ApiError) as caught:
            await client.send_photo_album(1, [(b"one", "one"), (b"two", "two")])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.delays, [])
        self.assertFalse(caught.exception.uncertain)
        self.assertNotIn(BALE_TOKEN, str(caught.exception))
        self.assertNotIn(BALE_TOKEN, json.dumps(caught.exception.as_dict()))

    async def test_reads_retry_503_with_backoff(self):
        def handler(request):
            self.calls.append(request)
            if len(self.calls) < 3:
                return httpx.Response(503, json={"ok": False, "error_code": 503})
            return httpx.Response(200, json={"ok": True, "result": {"id": 1}})
        response = await self.client(handler).get_me()
        self.assertEqual(response["result"]["id"], 1)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(len(self.delays), 2)

    async def test_read_transport_retries_are_bounded(self):
        def handler(request):
            self.calls.append(request)
            raise httpx.ConnectError(f"failed {request.url}", request=request)
        with self.assertRaises(ApiError) as caught:
            await self.client(handler).get_me()
        self.assertEqual(len(self.calls), 3)
        self.assertFalse(caught.exception.uncertain)
        self.assertNotIn(BALE_TOKEN, str(caught.exception))

    async def test_send_transport_failure_is_uncertain_and_never_retried(self):
        def handler(request):
            self.calls.append(request)
            raise httpx.ReadTimeout(f"lost response {request.url}", request=request)
        with self.assertRaises(ApiError) as caught:
            await self.client(handler).send_message(1, "probe")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.delays, [])
        self.assertTrue(caught.exception.uncertain)

    async def test_send_500_is_uncertain_and_never_retried(self):
        def handler(request):
            self.calls.append(request)
            return httpx.Response(500, json={"ok": False, "error_code": 500})
        with self.assertRaises(ApiError) as caught:
            await self.client(handler).send_message(1, "probe")
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(caught.exception.uncertain)

    async def test_explicit_rate_limit_rejection_can_retry_send(self):
        def handler(request):
            self.calls.append(request)
            if len(self.calls) == 1:
                return httpx.Response(429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 2}})
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})
        client = self.client(handler)
        attempts = []
        client.attempt_recorder = attempts.append
        await client.send_message(1, "probe")
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.delays, [2])
        self.assertEqual([attempt["response"]["ok"] for attempt in attempts], [False, True])
        self.assertEqual([attempt["attempt"] for attempt in attempts], [1, 2])

    async def test_malformed_rate_limit_does_not_retry_send(self):
        def handler(request):
            self.calls.append(request)
            return httpx.Response(429, text=f"broken response {BALE_TOKEN}")
        with self.assertRaises(ApiError) as caught:
            await self.client(handler).send_message(1, "probe")
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(caught.exception.uncertain)
        self.assertNotIn(BALE_TOKEN, json.dumps(caught.exception.as_dict()))

    async def test_photo_upload_limit_checked_before_request(self):
        client = self.client(lambda request: self.fail("Unexpected request"))
        with self.assertRaises(ValueError):
            await client.send_photo(1, b"x" * (PHOTO_LIMIT + 1), "caption")

    async def test_file_download_uses_platform_file_url(self):
        def handler(request):
            self.calls.append(request.url.path)
            if request.method == "POST":
                return httpx.Response(200, json={"ok": True, "result": {"file_path": "photos/a.png", "file_size": 3}})
            self.assertEqual(request.url.host, "tapi.bale.ai")
            return httpx.Response(200, content=b"png")
        self.assertEqual(await self.client(handler).download_file("platform-file-id"), b"png")
        self.assertEqual(self.calls[1], f"/file/bot{BALE_TOKEN}/photos/a.png")

    async def test_oversize_download_metadata_prevents_download(self):
        def handler(request):
            self.calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"file_path": "a.png", "file_size": DOWNLOAD_LIMIT + 1}})
        with self.assertRaises(ValueError):
            await self.client(handler).download_file("id")
        self.assertEqual(len(self.calls), 1)

    async def test_missing_file_size_still_has_stream_limit(self):
        def handler(request):
            if request.method == "POST":
                return httpx.Response(200, json={"ok": True, "result": {"file_path": "a.png"}})
            return httpx.Response(200, content=b"x" * (DOWNLOAD_LIMIT + 1))
        with self.assertRaises(ValueError):
            await self.client(handler).download_file("id")

    async def test_invalid_file_path_never_leaves_platform(self):
        def handler(request):
            self.calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"file_path": "../botOTHER/getMe"}})
        with self.assertRaises(ValueError):
            await self.client(handler).download_file("id")
        self.assertEqual(len(self.calls), 1)

    async def test_send_content_telegram_text_uses_entities_without_parse_mode(self):
        def handler(request):
            body = json.loads(request.content)
            self.assertEqual(body["entities"], [{"type": "bold", "offset": 0, "length": 4}])
            self.assertNotIn("parse_mode", body)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})
        await self.client(handler, platform="telegram").send_content(1, [{"kind": "text", "text": "bold",
            "entities": [{"type": "bold", "offset": 0, "length": 4}]}])

    async def test_send_content_mixed_album_multipart_and_caption_entities(self):
        def handler(request):
            mime = BytesParser(policy=default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\nMIME-Version: 1.0\r\n\r\n".encode() + request.content)
            parts = {part.get_param("name", header="content-disposition"): part for part in mime.iter_parts()}
            media = json.loads(parts["media"].get_payload(decode=True))
            self.assertEqual([item["type"] for item in media], ["photo", "video"])
            self.assertEqual([item["media"] for item in media], ["attach://item0", "attach://item1"])
            self.assertEqual(media[1]["caption_entities"], [{"type": "italic", "offset": 0, "length": 5}])
            self.assertEqual(parts["item1"].get_filename(), "clip.mp4")
            self.assertNotIn("parse_mode", parts)
            return httpx.Response(200, json={"ok": True, "result": [{"message_id": 1}, {"message_id": 2}]})
        items = [{"kind": "photo", "content": b"photo", "filename": "photo.png", "mime_type": "image/png", "text": "first", "entities": []},
                 {"kind": "video", "content": b"video", "filename": "clip.mp4", "mime_type": "video/mp4", "text": "video",
                  "entities": [{"type": "italic", "offset": 0, "length": 5}]}]
        await self.client(handler, platform="telegram").send_content(1, items)
