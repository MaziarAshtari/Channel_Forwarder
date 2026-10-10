from dataclasses import dataclass, field
from pathlib import Path
import re
from urllib.parse import urlsplit

from dotenv import dotenv_values


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    telegram_token: str = field(repr=False)
    bale_token: str = field(repr=False)
    telegram_channel_id: int
    bale_channel_id: int
    telegram_proxy_url: str | None = field(default=None, repr=False)
    telegram_to_bale_enabled: bool = False
    bale_to_telegram_enabled: bool = False
    dry_run: bool = True
    database_path: Path = Path("data/channel-sync.sqlite3")
    reports_dir: Path = Path("reports")
    log_level: str = "INFO"
    request_timeout: float = 30
    poll_timeout: int = 10
    retry_attempts: int = 3
    retry_base: float = 1
    signature_ttl: int = 600
    circuit_window: int = 60
    circuit_max_sends: int = 20
    album_wait: float = 3


def load_config(env_path: Path) -> Config:
    if not env_path.is_file():
        raise ConfigError("Missing .env: copy .env.example and fill both bot tokens.")
    # No interpolation: shell variables must not supply secrets or change routing.
    values = dotenv_values(env_path, interpolate=False)

    def value(key: str, default: str = "") -> str:
        return (values.get(key) or default).strip()

    def integer(key: str, default: str = "", minimum: int | None = 1) -> int:
        try:
            result = int(value(key, default))
        except ValueError:
            raise ConfigError(f"{key} must be an integer.") from None
        if minimum is not None and result < minimum:
            raise ConfigError(f"{key} must be at least {minimum}.")
        return result

    def number(key: str, default: str) -> float:
        import math
        try:
            result = float(value(key, default))
        except ValueError:
            raise ConfigError(f"{key} must be a positive number.") from None
        if not math.isfinite(result) or result <= 0:
            raise ConfigError(f"{key} must be a positive finite number.")
        return result

    def boolean(key: str, default: str) -> bool:
        result = value(key, default).lower()
        if result not in {"true", "false"}:
            raise ConfigError(f"{key} must be true or false.")
        return result == "true"

    tokens = {}
    for key in ("TELEGRAM_BOT_TOKEN", "BALE_BOT_TOKEN"):
        token = value(key)
        if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]{8,}", token):
            raise ConfigError(f"Fill {key} in .env with a valid bot token.")
        tokens[key] = token
    telegram_id = integer("TELEGRAM_CHANNEL_ID", minimum=None)
    bale_id = integer("BALE_CHANNEL_ID", minimum=None)
    if telegram_id >= 0:
        raise ConfigError("TELEGRAM_CHANNEL_ID must be a negative numeric channel ID.")
    if bale_id == 0:
        raise ConfigError("BALE_CHANNEL_ID cannot be zero.")
    proxy = value("TELEGRAM_PROXY_URL") or None
    if proxy:
        try:
            parsed = urlsplit(proxy)
            valid = (parsed.scheme in {"socks5", "socks5h"} and parsed.hostname
                     and parsed.port and parsed.path in {"", "/"}
                     and not parsed.query and not parsed.fragment)
        except ValueError:
            valid = False
        if not valid:
            raise ConfigError("TELEGRAM_PROXY_URL must be a SOCKS5 URL with host and port.")
    level = value("LOG_LEVEL", "INFO").upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigError("LOG_LEVEL is invalid.")
    poll = integer("POLL_TIMEOUT_SECONDS", "10")
    if poll > 50:
        raise ConfigError("POLL_TIMEOUT_SECONDS must be at most 50.")
    base = env_path.resolve().parent
    return Config(
        telegram_token=tokens["TELEGRAM_BOT_TOKEN"], bale_token=tokens["BALE_BOT_TOKEN"],
        telegram_channel_id=telegram_id, bale_channel_id=bale_id,
        telegram_proxy_url=proxy,
        telegram_to_bale_enabled=boolean("TG_TO_BALE" if "TG_TO_BALE" in values else "TELEGRAM_TO_BALE_ENABLED", "false"),
        bale_to_telegram_enabled=boolean("BALE_TO_TG" if "BALE_TO_TG" in values else "BALE_TO_TELEGRAM_ENABLED", "false"),
        dry_run=boolean("DRY_RUN", "true"),
        database_path=base / value("DATABASE_PATH", "data/channel-sync.sqlite3"),
        reports_dir=base / value("REPORTS_DIR", "reports"), log_level=level,
        request_timeout=number("REQUEST_TIMEOUT_SECONDS", "30"), poll_timeout=poll,
        retry_attempts=integer("RETRY_ATTEMPTS", "3"),
        retry_base=number("RETRY_BASE_SECONDS", "1"),
        signature_ttl=integer("SIGNATURE_TTL_SECONDS", "600"),
        circuit_window=integer("CIRCUIT_WINDOW_SECONDS", "60"),
        circuit_max_sends=integer("CIRCUIT_MAX_SENDS", "20"),
        album_wait=number("ALBUM_WAIT_SECONDS", "3"),
    )
