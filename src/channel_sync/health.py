import asyncio

from .clients import ApiError, BotClient
from .jev import JevClient

BALE_MODERATION_WARNING = (
    "Bale channel comment delivery and deletion are not documented by the official Bot API. "
    "Bale comment moderation is disabled; BALE_DISCUSSION_GROUP_ID is ignored."
)


async def discussion_health(config, client, channel_health=None) -> dict:
    result = {"platform": "telegram", "supported": True, "ok": False,
              "chat_id": config.telegram_discussion_group_id,
              "auto_detected": config.telegram_discussion_group_id is None}
    channel_health = channel_health or {}
    try:
        if result["chat_id"] is None:
            channel = channel_health.get("getChat") or await client.get_chat(config.telegram_channel_id)
            if not isinstance(channel.get("result"), dict):
                raise ApiError("telegram", "getChat", "Invalid channel metadata for discussion discovery")
            result["chat_id"] = channel["result"].get("linked_chat_id")
        chat_id = result["chat_id"]
        if (isinstance(chat_id, bool) or not isinstance(chat_id, int) or chat_id >= 0
                or chat_id == config.telegram_channel_id):
            result["chat_id"] = None
            result["error"] = "Configure TELEGRAM_DISCUSSION_GROUP_ID or link a Telegram discussion group to the channel."
            return result
        chat = await client.get_chat(chat_id)
        result["getChat"] = chat
        if (not isinstance(chat["result"], dict) or chat["result"].get("id") != chat_id
                or chat["result"].get("type") not in {"group", "supergroup"}):
            result["error"] = "The configured Telegram discussion chat is not the expected group."
            return result
        me = channel_health.get("getMe") or await client.get_me()
        if (not isinstance(me.get("result"), dict) or type(me["result"].get("id")) is not int
                or me["result"].get("is_bot") is not True):
            raise ApiError("telegram", "getMe", "Invalid bot identity for discussion rights check")
        bot_id = me["result"]["id"]
        result["bot_id"] = bot_id
        member = await client.get_chat_member(chat_id, bot_id)
        result["getChatMember"] = member
        if not isinstance(member.get("result"), dict):
            raise ApiError("telegram", "getChatMember", "Invalid discussion membership response")
        status = member["result"].get("status")
        result["membership_status"] = status
        result["can_delete_messages"] = member["result"].get("can_delete_messages")
        result["ok"] = (status in {"creator", "owner"}
                        or status == "administrator" and result["can_delete_messages"] is True)
        if not result["ok"]:
            result["error"] = "The Telegram bot must be an admin in the discussion group with Delete messages enabled."
    except ApiError as exc:
        result.update(exc.as_dict())
    return result


async def jev_health(config, client: JevClient) -> dict:
    result = {"ok": False, "api_key_configured": bool(config.typesafe_api_key),
              "api_key_valid": False, "reachable": False, "model": config.jev_model,
              "telegram_proxy_enabled": bool(config.telegram_proxy_url)}
    try:
        # One harmless evaluation validates authentication, model, and response shape.
        await client.classify("Thank you for the technology tutorial.")
        result.update(ok=True, api_key_valid=True, reachable=True)
    except ApiError as exc:
        result.update(exc.as_dict())
        result["reachable"] = exc.code is not None
    return result


async def health_one(client: BotClient, chat_id: int) -> dict:
    result = {"platform": client.platform, "configured_channel_id": chat_id, "ok": False,
              "telegram_proxy_enabled": bool(client.config.telegram_proxy_url) if client.platform == "telegram" else False}
    try:
        me = await client.get_me()
        result["getMe"] = me
        bot = me["result"]
        if not isinstance(bot, dict) or not isinstance(bot.get("id"), int) or bot.get("is_bot") is not True:
            result["error"] = "getMe did not return a valid bot identity."
            return result
        chat = await client.get_chat(chat_id)
        result["getChat"] = chat
        if not isinstance(chat["result"], dict) or chat["result"].get("id") != chat_id:
            result["error"] = "getChat returned a different channel ID."
            return result
        result["channel_access"] = True
        # Keep Bale's observed group type visible rather than rejecting it.
        result["chat_type"] = chat["result"].get("type")
        member = await client.get_chat_member(chat_id, bot["id"])
        result["getChatMember"] = member
        status = member["result"].get("status")
        result["membership_status"] = status
        result["posting_permission"] = member["result"].get("can_post_messages")
        if status in {"left", "kicked", "restricted"}:
            result["error"] = "The bot is not an active channel administrator."
            return result
        if status not in {"creator", "owner", "administrator"}:
            result["error"] = "Channel posting requires administrator membership."
            return result
        if result["posting_permission"] is False:
            result["error"] = "The bot cannot post messages to this channel."
            return result
        result["ok"] = True
    except ApiError as exc:
        result.update(exc.as_dict())
    return result


async def health(clients: dict[str, BotClient], channel_ids: dict[str, int], *,
                 check_moderation=True, jev_client: JevClient | None = None) -> dict:
    results = await asyncio.gather(*(health_one(client, channel_ids[platform])
                                     for platform, client in clients.items()))
    report = {"ok": all(item["ok"] for item in results), "bots": results}
    if not check_moderation:
        return report
    config = clients["telegram"].config
    if not config.moderation_enabled:
        report["moderation"] = {"enabled": False}
        return report
    owned = jev_client is None
    evaluator = jev_client or JevClient(config)
    try:
        telegram = next(item for item in results if item["platform"] == "telegram")
        group, jev = await asyncio.gather(discussion_health(config, clients["telegram"], telegram),
                                        jev_health(config, evaluator))
    finally:
        if owned:
            await evaluator.aclose()
    report["moderation"] = {"enabled": True, "ok": group["ok"] and jev["ok"],
                            "jev": jev, "telegram": group,
                            "bale": {"supported": False, "status": "unsupported", "warning": BALE_MODERATION_WARNING}}
    report["ok"] = report["ok"] and report["moderation"]["ok"]
    return report
