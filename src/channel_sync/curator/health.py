import asyncio
import time
from urllib.parse import urlsplit

from .mcp import ResearchMCP
from .research import LLM
from .reader import run_browser
from .store import CuratorStore
from .topics import questions
from ..config import ConfigError


async def curator_health(config, clients):
    store = CuratorStore(config.database_path, clients["telegram"].redactor)
    report = {
        "enabled": True,
        "ok": False,
        "admin_ids": {
            "telegram": list(config.admin_telegram_user_ids),
            "bale": list(config.admin_bale_user_ids),
        },
    }
    llm = LLM(config)

    async def check(label, operation):
        try:
            report[label] = await operation()
        except Exception as exc:
            report[label] = {
                "ok": False,
                "error": store.redactor.text(str(exc))[:700]
                if isinstance(exc, (ConfigError, ValueError, RuntimeError))
                else type(exc).__name__,
            }

    async def proxy():
        if not config.x_proxy_url:
            return {"ok": True, "enabled": False}
        parsed = urlsplit(config.x_proxy_url)
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(parsed.hostname, parsed.port), 5
        )
        writer.close()
        await writer.wait_closed()
        return {
            "ok": True,
            "enabled": True,
            "check": "Proxy TCP reachable; browser probe verifies routed X access",
        }

    async def llm_check():
        await llm.chat([{"role": "user", "content": "Reply with OK only."}])
        return {
            "ok": True,
            "model": config.llm_model,
            "proxy_enabled": config.llm_use_proxy,
        }

    async def admin_check():
        result = {
            "telegram": list(config.admin_telegram_user_ids),
            "bale": list(config.admin_bale_user_ids),
            "ok": False,
        }
        if not result["telegram"] or not result["bale"]:
            return {
                **result,
                "error": "Fill both admin user-ID lists and start both bots privately",
            }
        for platform in ("telegram", "bale"):
            for chat in result[platform]:
                metadata = (await clients[platform].get_chat(chat))["result"]
                if metadata.get("id") != chat or metadata.get("type") != "private":
                    return {
                        **result,
                        "error": f"{platform} admin target must be a private user chat",
                    }
        return {**result, "ok": True}

    async def mcp_check():
        async with ResearchMCP(config) as mcp:
            return {
                "ok": True,
                "tools": [t["function"]["name"] for t in mcp.tools],
                "proxy_enabled": config.mcp_use_proxy,
            }

    async def browser():
        state = store.reader()
        if state["challenge"]:
            return {"ok": False, "challenge_latched": True, "reason": state["reason"]}
        if (
            state["heartbeat"]
            and state["heartbeat"] > time.time() - 180
            and state["logged_in"]
        ):
            return {
                "ok": True,
                "logged_in": True,
                "checked_at": state["checked_at"],
                "check": "Recent reader observation",
            }
        observed = {"ok": False, "logged_in": False}

        async def callback(event):
            if event["kind"] == "heartbeat" and event["logged_in"]:
                observed.update(ok=True, logged_in=True)
                store.connection.execute(
                    "UPDATE curator_reader SET logged_in=1,checked_at=?", (time.time(),)
                )
            if event["kind"] == "challenge":
                store.pause_reader(event["reason"], challenge=True)
                observed.update(challenge_latched=True, reason=event["reason"])
                from .publisher import Publisher
                from .approvals import Approvals

                publisher = Publisher(config, clients, store)
                try:
                    await Approvals(config, clients, store, publisher).alert(
                        "X health check stopped the reader: " + event["reason"]
                    )
                finally:
                    await publisher.aclose()
            return True

        await run_browser(config, callback, mode="probe")
        return observed

    try:
        questions()
        # MCP SDK task groups must enter/exit in the same task; each check owns its connection.
        await asyncio.gather(
            check("proxy", proxy),
            check("llm", llm_check),
            check("mcp", mcp_check),
            check("browser", browser),
            check("admin_ids", admin_check),
        )
        report["ok"] = all(
            report[k].get("ok", False)
            for k in ("proxy", "llm", "mcp", "browser", "admin_ids")
        )
    finally:
        await llm.aclose()
        store.close()
    return report
