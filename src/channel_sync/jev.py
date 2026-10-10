import asyncio
from importlib.resources import files
import json
import logging
import math
import random

import httpx

from .clients import ApiError, make_http_client
from .config import Config, ConfigError

log = logging.getLogger(__name__)
QUESTION_IDS = {"is_profanity", "is_advertising", "is_sexual", "is_scam_or_phishing",
                "is_hate_or_threat", "should_delete"}
JEV_URL = "https://api.typesafe.ai/v1/systemone"


def load_questions() -> dict:
    try:
        questions = json.loads(files("channel_sync").joinpath("moderation_questions.json").read_text(encoding="utf-8"))
        valid = (isinstance(questions, dict) and set(questions) == QUESTION_IDS
                 and all(isinstance(q, dict) and q.get("type") == "noul"
                         and isinstance(q.get("instructions"), str) and q["instructions"].strip()
                         and isinstance(q.get("criteria"), dict)
                         and all(isinstance(q["criteria"].get(k), str) for k in ("true", "false"))
                         for q in questions.values()))
    except (OSError, ValueError):
        valid = False
    if not valid:
        raise ConfigError("moderation_questions.json must contain the six moderation noul questions and criteria.")
    return questions


def parse_scores(payload: dict) -> dict[str, float]:
    answers = payload.get("answers") if isinstance(payload, dict) else None
    if not isinstance(answers, dict):
        raise ValueError("Missing jev answers.")
    scores = {}
    for key in sorted(QUESTION_IDS):
        answer = answers.get(key)
        value = answer.get("noul") if isinstance(answer, dict) else None
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or not 0 <= value <= 1):
            raise ValueError(f"Invalid jev probability for {key}.")
        scores[key] = float(value)
    return scores


class JevClient:
    """Evaluation can be retried; requests share Telegram's explicit proxy policy."""

    def __init__(self, config: Config, *, transport=None, sleeper=asyncio.sleep):
        self.config = config
        self.questions = load_questions()
        self.http = make_http_client(config, "telegram", transport=transport)
        self._sleep = sleeper

    async def aclose(self):
        await self.http.aclose()

    async def classify(self, text: str) -> dict[str, float]:
        if not self.config.typesafe_api_key:
            raise ApiError("jev", "systemone", "Set TYPESAFE_API_KEY in .env.")
        body = {"model": self.config.jev_model,
                "state": {"comment": text, "channel_topic": self.config.moderation_channel_topic,
                          "comment_language": "Persian or English"},
                "questions": self.questions}
        for attempt in range(self.config.retry_attempts):
            retry_after = 0.0
            try:
                response = await self.http.post(
                    JEV_URL, headers={"Authorization": f"Bearer {self.config.typesafe_api_key}"}, json=body,
                )
            except httpx.TransportError as exc:
                error = ApiError("jev", "systemone", f"Transport failure ({type(exc).__name__})", temporary=True)
            else:
                if response.is_success:
                    try:
                        return parse_scores(response.json())
                    except ValueError:
                        error = ApiError("jev", "systemone", "Invalid evaluation response",
                                         code=response.status_code, temporary=True)
                else:
                    code = response.status_code
                    description = "API key rejected" if code in {401, 403} else "Evaluation request rejected"
                    # Remote bodies and exception text may echo credentials or user content.
                    error = ApiError("jev", "systemone", description, code=code,
                                     temporary=code == 429 or 500 <= code <= 599)
                    try:
                        value = float(response.headers.get("Retry-After", "0"))
                        if math.isfinite(value):
                            retry_after = max(0.0, value)
                    except ValueError:
                        pass
            if not error.temporary or attempt + 1 == self.config.retry_attempts:
                raise error from None
            delay = max(retry_after, self.config.retry_base * 2 ** attempt + random.uniform(0, 0.2))
            log.warning("jev temporary failure; retry %s/%s in %.1fs",
                        attempt + 2, self.config.retry_attempts, delay)
            await self._sleep(delay)
        raise AssertionError("Unreachable retry state.")
