import asyncio

from .clients import ApiError, BotClient


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


async def health(clients: dict[str, BotClient], channel_ids: dict[str, int]) -> dict:
    results = await asyncio.gather(*(health_one(client, channel_ids[platform])
                                     for platform, client in clients.items()))
    return {"ok": all(item["ok"] for item in results), "bots": results}
