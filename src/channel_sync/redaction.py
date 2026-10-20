import logging
import re
from urllib.parse import quote


def config_secrets(config):
    values = [
        config.telegram_token,
        config.bale_token,
        config.telegram_proxy_url,
        config.typesafe_api_key,
        config.llm_api_key,
        config.x_proxy_url,
    ]
    values += [
        v
        for k, v in config.env_values.items()
        if any(
            word in k.upper()
            for word in ("KEY", "TOKEN", "PASSWORD", "SECRET", "AUTH", "PROXY_URL")
        )
    ]
    try:
        references = re.findall(
            r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}",
            config.mcp_servers_path.read_text(encoding="utf-8"),
        )
        values += [config.env_values.get(key) for key in references]
    except (OSError,UnicodeError):
        pass
    return values


class Redactor:
    def __init__(self, *secrets: str | None):
        self.secrets = sorted(
            {
                variant
                for secret in secrets
                if secret
                for variant in (secret, quote(secret, safe=""))
            },
            key=len,
            reverse=True,
        )

    def text(self, value: str) -> str:
        for secret in self.secrets:
            value = value.replace(secret, "[REDACTED]")
        value = re.sub(r"(?i)(https://(?:api\.telegram\.org|tapi\.bale\.ai)/(?:file/)?bot)([^/\s?\"'<>]+)", r"\1[REDACTED]", value)
        return re.sub(r"\b\d+:[A-Za-z0-9_-]{8,}\b", "[REDACTED]", value)

    def data(self, value):
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, dict):
            return {self.text(str(key)): self.data(item) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [self.data(item) for item in value]
        return value


class RedactingFormatter(logging.Formatter):
    def __init__(self, redactor: Redactor):
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s")
        self.redactor = redactor

    def format(self, record):
        # Redact after formatting, including args, stack traces, and exc_info.
        return self.redactor.text(super().format(record))


def configure_logging(level: str, redactor: Redactor):
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter(redactor))
    logging.basicConfig(level=level, handlers=[handler], force=True)
    # Transport debug output can include byte-encoded URLs and credentials.
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
