import json
import asyncio
import re
import unicodedata
from difflib import SequenceMatcher
from importlib.resources import files

from ..config import ConfigError


def questions():
    data = json.loads(
        files("channel_sync")
        .joinpath("curator_questions.json")
        .read_text(encoding="utf-8")
    )
    required = {
        "is_ai_tech_news",
        "is_useful",
        "should_curate",
        "is_same_topic",
    }
    if not required <= data.keys() or any(
        q.get("type") != "noul"
        or not q.get("instructions")
        or not {"true", "false"} <= q.get("criteria", {}).keys()
        for q in data.values()
    ):
        raise ConfigError("curator_questions.json contains invalid noul questions.")
    return data


def normalize_topic(topic):
    for key in ("entity", "event", "topic_key", "summary"):
        if not isinstance(topic.get(key), str) or not topic[key].strip():
            raise ValueError("LLM topic is incomplete")
    key = unicodedata.normalize("NFKC", topic["topic_key"]).casefold()
    key = re.sub(r"[^\w.|-]+", "-", key).replace("_", "-")
    key = "|".join(re.sub("-+", "-", part).strip("-") for part in key.split("|"))
    if len(key) > 240 or len(key.split("|")) != 3:
        raise ValueError("topic_key must have entity|product-or-subject|event form")
    event = topic["event"].casefold().strip()
    if key.split("|")[2] != event or not isinstance(
        topic.get("specific_version"), bool
    ):
        raise ValueError(
            "Topic event and explicit version classification are inconsistent"
        )
    # A numeric product version must be explicit in the product segment. Generic
    # release/company keys must expire even if the model calls them permanent.
    version = re.search(r"\d+(?:\.\d+)*", key.split("|")[1])
    permanent = (
        event == "release" and topic.get("specific_version") is True and bool(version)
    )
    return {**topic, "topic_key": key, "event": event, "permanent": permanent}


async def find_duplicate(store, jev, config, topic, *, exclude=None):
    exact = store.exact(topic["topic_key"], config.topic_dedup_days, exclude)
    if exact:
        return exact
    recent = store.topics(config.topic_dedup_days, exclude=exclude)

    # Similarity can become CPU-heavy with a month's history; keep it off the
    # event loop shared by the bridge and moderation.
    def rank():
        return sorted(
            recent,
            key=lambda r: SequenceMatcher(
                None, topic["summary"].casefold(), r["summary"].casefold()
            ).ratio(),
            reverse=True,
        )

    ranked = await asyncio.to_thread(rank)
    for item in ranked[: config.topic_dedup_candidates]:
        scores = await jev.evaluate(
            {"candidate_summary": topic["summary"], "recent_summary": item["summary"]},
            {"is_same_topic": questions()["is_same_topic"]},
        )
        import logging

        logging.getLogger(__name__).info(
            "Curator dedup topic=%s scores=%s", item["id"], scores
        )
        if scores["is_same_topic"] >= config.topic_same_threshold:
            return item
    return None
