from pathlib import Path

from channel_sync.config import Config

TELEGRAM_TOKEN = "123456789:telegram_test_secret_abcdefgh"
BALE_TOKEN = "987654321:bale_test_secret_abcdefgh"


def config(root: Path, **changes):
    options = {"telegram_token": TELEGRAM_TOKEN, "bale_token": BALE_TOKEN,
               "telegram_channel_id": -1004472119156, "bale_channel_id": 5716375015,
               "database_path": root / "state.sqlite3", "reports_dir": root / "reports",
               "retry_base": 0.001, "request_timeout": 1}
    options.update(changes)
    return Config(**options)
