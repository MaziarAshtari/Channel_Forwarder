from contextlib import redirect_stdout
import io
import json
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from channel_sync.cli import main
from channel_sync.config import ConfigError, load_config
from channel_sync.redaction import Redactor, RedactingFormatter
from channel_sync.reports import save_report
from helpers import TELEGRAM_TOKEN, BALE_TOKEN


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / ".env"
        self.template = (Path(__file__).parents[1] / ".env.example").read_text()

    def write(self, **values):
        result = self.template
        values = {"TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN, "BALE_BOT_TOKEN": BALE_TOKEN, **values}
        for key, value in values.items():
            lines = result.splitlines()
            result = "\n".join(f"{key}={value}" if line.startswith(key + "=") else line for line in lines)
        self.path.write_text(result)

    def test_valid_defaults_and_private_repr(self):
        self.write()
        conf = load_config(self.path)
        self.assertEqual(conf.telegram_channel_id, -1004472119156)
        self.assertEqual(conf.bale_channel_id, 5716375015)
        self.assertTrue(conf.dry_run)
        self.assertFalse(conf.bale_to_telegram_enabled)
        self.assertEqual(conf.database_path, self.path.parent / "data/channel-sync.sqlite3")
        self.assertNotIn(TELEGRAM_TOKEN, repr(conf))
        self.assertNotIn("127.0.0.1", repr(conf))
        self.assertEqual(conf.signature_ttl, 600)
        self.assertEqual(conf.album_wait, 3)

    def test_short_direction_flags_and_legacy_compatibility(self):
        self.write(TG_TO_BALE="true", BALE_TO_TG="false")
        self.path.write_text(self.path.read_text() + "\nTELEGRAM_TO_BALE_ENABLED=false\nBALE_TO_TELEGRAM_ENABLED=true\n")
        conf = load_config(self.path)
        self.assertTrue(conf.telegram_to_bale_enabled)
        self.assertFalse(conf.bale_to_telegram_enabled)
        self.path.write_text(self.path.read_text().replace("TG_TO_BALE=true\n", "").replace("BALE_TO_TG=false\n", ""))
        legacy = load_config(self.path)
        self.assertFalse(legacy.telegram_to_bale_enabled)
        self.assertTrue(legacy.bale_to_telegram_enabled)

    def test_missing_tokens_cannot_come_from_environment(self):
        self.path.write_text(self.template)
        with patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN, "BALE_BOT_TOKEN": BALE_TOKEN}):
            with self.assertRaises(ConfigError):
                load_config(self.path)

    def test_no_environment_interpolation(self):
        self.write(TELEGRAM_BOT_TOKEN="${SECRET}")
        with patch.dict("os.environ", {"SECRET": TELEGRAM_TOKEN}):
            with self.assertRaises(ConfigError):
                load_config(self.path)

    def test_invalid_fields_do_not_leak_values(self):
        for key, value in {"TELEGRAM_PROXY_URL": "https://secret:password@proxy:80",
                           "DRY_RUN": "yes", "TELEGRAM_CHANNEL_ID": "42",
                           "BALE_CHANNEL_ID": "0", "REQUEST_TIMEOUT_SECONDS": "nan",
                           "RETRY_ATTEMPTS": "0", "POLL_TIMEOUT_SECONDS": "60",
                           "LOG_LEVEL": "invalid"}.items():
            with self.subTest(key=key):
                self.write(**{key: value})
                with self.assertRaises(ConfigError) as error:
                    load_config(self.path)
                self.assertNotIn("password", str(error.exception))

    def test_cli_rejects_incomplete_env_before_client_construction(self):
        self.path.write_text(self.template)
        with patch("channel_sync.cli.TelegramClient") as telegram, patch("channel_sync.cli.BaleClient") as bale:
            with redirect_stdout(io.StringIO()) as output:
                code = main(["--env", str(self.path), "health"])
            self.assertEqual(code, 2)
            self.assertIn("Fill TELEGRAM_BOT_TOKEN", output.getvalue())
            telegram.assert_not_called()
            bale.assert_not_called()


class RedactionTests(unittest.TestCase):
    def setUp(self):
        self.redactor = Redactor(TELEGRAM_TOKEN, BALE_TOKEN)

    def test_urls_tokens_encoded_urls_nested_keys(self):
        value = {TELEGRAM_TOKEN: [f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe",
                                f"https://tapi.bale.ai/file/bot{BALE_TOKEN}/photos/a.jpg",
                                TELEGRAM_TOKEN.replace(":", "%3A")]}
        result = json.dumps(self.redactor.data(value))
        self.assertNotIn(TELEGRAM_TOKEN, result)
        self.assertNotIn(BALE_TOKEN, result)
        self.assertNotIn("telegram_test_secret", result)
        self.assertIn("/file/bot[REDACTED]/photos/a.jpg", result)

    def test_logging_redacts_args_and_exception_traceback(self):
        try:
            raise RuntimeError(f"bad URL https://tapi.bale.ai/bot{BALE_TOKEN}/sendMessage")
        except RuntimeError:
            import sys
            record = logging.LogRecord("test", logging.ERROR, "test", 1, "%s", (TELEGRAM_TOKEN,), sys.exc_info())
        output = RedactingFormatter(self.redactor).format(record)
        self.assertNotIn(TELEGRAM_TOKEN, output)
        self.assertNotIn(BALE_TOKEN, output)
        self.assertIn("Traceback", output)

    def test_atomic_report_is_redacted_and_private(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "reports/report.json"
            save_report(path, {"result": BALE_TOKEN}, self.redactor)
            save_report(path, {"result": TELEGRAM_TOKEN}, self.redactor)
            self.assertEqual(json.loads(path.read_text()), {"result": "[REDACTED]"})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(len(list(path.parent.iterdir())), 1)

    def test_ordinary_link_with_bot_path_is_not_changed(self):
        url = "https://example.com/bot-article/link"
        self.assertEqual(self.redactor.text(url), url)
