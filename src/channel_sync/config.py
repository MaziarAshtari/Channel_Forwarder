from dataclasses import dataclass, field
from pathlib import Path
import math
import re
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    moderation_enabled: bool = True
    typesafe_api_key: str = field(default="", repr=False)
    jev_model: str = "jev-latest"
    moderation_threshold: float = 0.55
    moderation_review_low: float = 0.45
    moderation_channel_topic: str = "Technology news and tutorials"
    telegram_discussion_group_id: int | None = None
    bale_discussion_group_id: int | None = None
    moderation_skip_admins: bool = True
    moderation_alert_chat_id: int | None = None
    curator_enabled: bool = False
    env_path: Path = Path(".env")
    env_values: dict = field(default_factory=dict, repr=False, compare=False)
    x_browser_profile_dir: Path = Path("data/x-profile")
    x_proxy_url: str | None = field(default=None, repr=False)
    x_timezone: str = "Asia/Tehran"
    x_active_hours: str = "09:00-23:30"
    x_max_posts_per_day: int = 400
    x_session_minutes_min: int = 10
    x_session_minutes_max: int = 25
    x_break_minutes_min: int = 30
    x_break_minutes_max: int = 90
    x_browser_memory_mb: int = 1536
    x_browser_watchdog_seconds: int = 90
    curator_relevance_threshold: float = 0.7
    topic_dedup_days: int = 30
    topic_dedup_candidates: int = 8
    topic_same_threshold: float = 0.8
    llm_base_url: str = ""
    llm_api_key: str = field(default="", repr=False)
    llm_model: str = ""
    llm_max_tool_rounds: int = 8
    llm_use_proxy: bool = False
    mcp_use_proxy: bool = False
    admin_telegram_user_ids: tuple[int, ...] = ()
    admin_bale_user_ids: tuple[int, ...] = ()
    channel_signature: str = "🆔 @asrgooyeshpardaz"
    mcp_servers_path: Path = Path("mcp_servers.json")
    curator_examples_dir: Path = Path("curator/examples")


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
        try:
            result = float(value(key, default))
        except ValueError:
            raise ConfigError(f"{key} must be a positive number.") from None
        if not math.isfinite(result) or result <= 0:
            raise ConfigError(f"{key} must be a positive finite number.")
        return result

    def probability(key: str, default: str) -> float:
        try:
            result = float(value(key, default))
        except ValueError:
            raise ConfigError(f"{key} must be a probability between 0 and 1.") from None
        if not math.isfinite(result) or not 0 <= result <= 1:
            raise ConfigError(f"{key} must be a finite probability between 0 and 1.")
        return result

    def optional_id(key: str) -> int | None:
        if not value(key):
            return None
        result = integer(key, minimum=None)
        if result == 0:
            raise ConfigError(f"{key} cannot be zero.")
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
    threshold = probability("MODERATION_THRESHOLD", "0.55")
    review_low = probability("MODERATION_REVIEW_LOW", "0.45")
    if review_low > threshold:
        raise ConfigError("MODERATION_REVIEW_LOW cannot exceed MODERATION_THRESHOLD.")
    discussion_id = optional_id("TELEGRAM_DISCUSSION_GROUP_ID")
    if discussion_id is not None and (discussion_id >= 0 or discussion_id == telegram_id):
        raise ConfigError("TELEGRAM_DISCUSSION_GROUP_ID must be a negative group ID distinct from the channel.")
    def admin_ids(key):
        try:
            ids = tuple(dict.fromkeys(int(i.strip()) for i in value(key).split(',') if i.strip()))
            if any(i <= 0 for i in ids):
                raise ValueError()
            return ids
        except ValueError:
            raise ConfigError(f'{key} must contain comma-separated positive numeric user IDs.') from None
    timezone = value('X_TIMEZONE', 'Asia/Tehran')
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError:
        raise ConfigError('X_TIMEZONE must be an IANA timezone.') from None
    hours = value('X_ACTIVE_HOURS', '09:00-23:30')
    if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d-(?:[01]\d|2[0-3]):[0-5]\d', hours):
        raise ConfigError('X_ACTIVE_HOURS must use HH:MM-HH:MM.')
    bounds = {
        key: integer(key, default)
        for key, default in (
            ("X_SESSION_MINUTES_MIN", "10"),
            ("X_SESSION_MINUTES_MAX", "25"),
            ("X_BREAK_MINUTES_MIN", "30"),
            ("X_BREAK_MINUTES_MAX", "90"),
        )
    }
    for prefix in ("X_SESSION_MINUTES", "X_BREAK_MINUTES"):
        if bounds[prefix + "_MIN"] > bounds[prefix + "_MAX"]:
            raise ConfigError(f"{prefix}_MIN cannot exceed {prefix}_MAX.")
    x_proxy = value("X_PROXY_URL", proxy or "") or None
    if x_proxy:
        try:
            parsed = urlsplit(x_proxy)
            if (
                parsed.scheme not in {"http", "https", "socks5", "socks5h"}
                or not parsed.hostname
                or not parsed.port
            ):
                raise ValueError()
            if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
                raise ValueError()
            if (
                boolean("CURATOR_ENABLED", "false")
                and parsed.scheme.startswith("socks")
                and (parsed.username or parsed.password)
            ):
                raise ConfigError(
                    "Chrome does not support authenticated SOCKS proxies; use a local unauthenticated relay."
                )
        except ConfigError:
            raise
        except ValueError:
            raise ConfigError(
                "X_PROXY_URL must be a supported proxy URL with host and port."
            ) from None
    llm_url = value("LLM_BASE_URL")
    if llm_url:
        try:
            parsed = urlsplit(llm_url)
            _ = parsed.port
            valid = (
                parsed.scheme in {"http", "https"}
                and parsed.hostname
                and not parsed.username
                and not parsed.query
                and not parsed.fragment
            )
        except ValueError:
            valid = False
        if not valid:
            raise ConfigError(
                "LLM_BASE_URL must be an HTTP(S) API base URL without credentials or query parameters."
            )
    return Config(
        telegram_token=tokens["TELEGRAM_BOT_TOKEN"],
        bale_token=tokens["BALE_BOT_TOKEN"],
        telegram_channel_id=telegram_id,
        bale_channel_id=bale_id,
        telegram_proxy_url=proxy,
        telegram_to_bale_enabled=boolean(
            "TG_TO_BALE" if "TG_TO_BALE" in values else "TELEGRAM_TO_BALE_ENABLED",
            "false",
        ),
        bale_to_telegram_enabled=boolean(
            "BALE_TO_TG" if "BALE_TO_TG" in values else "BALE_TO_TELEGRAM_ENABLED",
            "false",
        ),
        dry_run=boolean("DRY_RUN", "true"),
        database_path=base / value("DATABASE_PATH", "data/channel-sync.sqlite3"),
        reports_dir=base / value("REPORTS_DIR", "reports"),
        log_level=level,
        request_timeout=number("REQUEST_TIMEOUT_SECONDS", "30"),
        poll_timeout=poll,
        retry_attempts=integer("RETRY_ATTEMPTS", "3"),
        retry_base=number("RETRY_BASE_SECONDS", "1"),
        signature_ttl=integer("SIGNATURE_TTL_SECONDS", "600"),
        circuit_window=integer("CIRCUIT_WINDOW_SECONDS", "60"),
        circuit_max_sends=integer("CIRCUIT_MAX_SENDS", "20"),
        album_wait=number("ALBUM_WAIT_SECONDS", "3"),
        moderation_enabled=boolean("MODERATION_ENABLED", "true"),
        typesafe_api_key=value("TYPESAFE_API_KEY"),
        jev_model=value("JEV_MODEL", "jev-latest"),
        moderation_threshold=threshold,
        moderation_review_low=review_low,
        moderation_channel_topic=value(
            "MODERATION_CHANNEL_TOPIC", "Technology news and tutorials"
        ),
        telegram_discussion_group_id=discussion_id,
        bale_discussion_group_id=optional_id("BALE_DISCUSSION_GROUP_ID"),
        moderation_skip_admins=boolean("MODERATION_SKIP_ADMINS", "true"),
        moderation_alert_chat_id=optional_id("MODERATION_ALERT_CHAT_ID"),
        curator_enabled=boolean("CURATOR_ENABLED", "false"),
        env_path=env_path.resolve(),
        env_values={k: v for k, v in values.items() if v is not None},
        x_browser_profile_dir=base / value("X_BROWSER_PROFILE_DIR", "data/x-profile"),
        x_proxy_url=x_proxy,
        x_timezone=timezone,
        x_active_hours=hours,
        x_max_posts_per_day=integer("X_MAX_POSTS_PER_DAY", "400"),
        x_session_minutes_min=bounds["X_SESSION_MINUTES_MIN"],
        x_session_minutes_max=bounds["X_SESSION_MINUTES_MAX"],
        x_break_minutes_min=bounds["X_BREAK_MINUTES_MIN"],
        x_break_minutes_max=bounds["X_BREAK_MINUTES_MAX"],
        x_browser_memory_mb=integer("X_BROWSER_MEMORY_MB", "1536", minimum=0),
        x_browser_watchdog_seconds=integer("X_BROWSER_WATCHDOG_SECONDS", "90"),
        curator_relevance_threshold=probability("CURATOR_RELEVANCE_THRESHOLD", "0.7"),
        topic_dedup_days=integer("TOPIC_DEDUP_DAYS", "30"),
        topic_dedup_candidates=integer("TOPIC_DEDUP_CANDIDATES", "8"),
        topic_same_threshold=probability("TOPIC_SAME_THRESHOLD", "0.8"),
        llm_base_url=llm_url.rstrip("/"),
        llm_api_key=value("LLM_API_KEY"),
        llm_model=value("LLM_MODEL"),
        llm_max_tool_rounds=integer("LLM_MAX_TOOL_ROUNDS", "8"),
        llm_use_proxy=boolean("LLM_USE_PROXY", "false"),
        mcp_use_proxy=boolean("MCP_USE_PROXY", "false"),
        admin_telegram_user_ids=admin_ids("ADMIN_TELEGRAM_USER_IDS"),
        admin_bale_user_ids=admin_ids("ADMIN_BALE_USER_IDS"),
        channel_signature=value("CHANNEL_SIGNATURE", "🆔 @asrgooyeshpardaz"),
        mcp_servers_path=base / value("MCP_SERVERS_PATH", "mcp_servers.json"),
        curator_examples_dir=base / value("CURATOR_EXAMPLES_DIR", "curator/examples"),
    )
