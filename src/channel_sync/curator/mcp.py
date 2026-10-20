"""Official MCP SDK transports, with credentials resolved only from private .env."""

from contextlib import AsyncExitStack
from datetime import timedelta
import json
import os
import re

import httpx
from jsonschema import validate

from ..config import ConfigError


def http_client(config, *, headers=None):
    return httpx.AsyncClient(
        headers=headers,
        trust_env=False,
        follow_redirects=False,
        proxy=config.telegram_proxy_url if config.llm_use_proxy else None,
        timeout=httpx.Timeout(
            config.request_timeout, read=max(60, config.request_timeout)
        ),
    )


def load_servers(config):
    try:
        data = json.loads(config.mcp_servers_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ConfigError("Provide mcp_servers.json with a servers object.") from None

    def resolve(value):
        if isinstance(value, str):

            def variable(match):
                key = match[1]
                if not config.env_values.get(key):
                    raise ConfigError(
                        f"Missing MCP credential/configuration reference: {key}"
                    )
                return config.env_values[key]

            return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", variable, value)
        if isinstance(value, list):
            return [resolve(v) for v in value]
        if isinstance(value, dict):
            return {k: resolve(v) for k, v in value.items()}
        return value

    servers = data.get("servers")
    if not isinstance(servers, dict):
        raise ConfigError("mcp_servers.json must contain a servers object.")
    if any(not isinstance(server, dict) for server in servers.values()):
        raise ConfigError("Each MCP server must be a configuration object.")
    return {
        name: resolve(server) if server.get("enabled", True) else server
        for name, server in servers.items()
    }


class ResearchMCP:
    def __init__(self, config):
        self.config = config
        self.stack = AsyncExitStack()
        self.tools = []
        self.routes = {}

    async def __aenter__(self):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
        from mcp.client.streamable_http import streamable_http_client

        try:
            for index, (name, server) in enumerate(load_servers(self.config).items()):
                if server.get("enabled", True) is False:
                    continue
                if server.get("type") == "stdio":
                    # Never expose bot or LLM credentials to an unrelated search process.
                    env = dict(server.get("env", {}))
                    if self.config.llm_use_proxy and self.config.telegram_proxy_url:
                        env.update(
                            HTTP_PROXY=self.config.telegram_proxy_url,
                            HTTPS_PROXY=self.config.telegram_proxy_url,
                            ALL_PROXY=self.config.telegram_proxy_url,
                        )
                    stderr = self.stack.enter_context(open(os.devnull, "w"))
                    streams = await self.stack.enter_async_context(
                        stdio_client(
                            StdioServerParameters(
                                command=server["command"],
                                args=server.get("args", []),
                                env=env,
                            ),
                            errlog=stderr,
                        )
                    )
                elif server.get("type") in {"http", "streamable_http"}:
                    client = await self.stack.enter_async_context(
                        http_client(self.config, headers=server.get("headers"))
                    )
                    streams = await self.stack.enter_async_context(
                        streamable_http_client(server["url"], http_client=client)
                    )
                else:
                    raise ConfigError(f"Unsupported MCP transport for server {name}.")
                session = await self.stack.enter_async_context(
                    ClientSession(
                        streams[0],
                        streams[1],
                        read_timeout_seconds=timedelta(
                            seconds=max(60, self.config.request_timeout)
                        ),
                    )
                )
                await session.initialize()
                cursor = None
                while True:
                    listing = await session.list_tools(cursor=cursor)
                    for tool in listing.tools:
                        allowed = server.get("tools")
                        read_only = (
                            tool.annotations and tool.annotations.readOnlyHint is True
                        )
                        if not (
                            tool.name in allowed
                            if isinstance(allowed, list)
                            else read_only
                        ):
                            continue
                        exposed = f"s{index}_{tool.name}"[:64]
                        if exposed in self.routes or not re.fullmatch(
                            r"[A-Za-z0-9_-]+", exposed
                        ):
                            raise ConfigError(
                                "MCP tool names must be unique function-compatible names."
                            )
                        self.routes[exposed] = (session, tool)
                        self.tools.append(
                            {
                                "type": "function",
                                "function": {
                                    "name": exposed,
                                    "description": (tool.description or tool.name)[
                                        :2500
                                    ],
                                    "parameters": tool.inputSchema,
                                },
                            }
                        )
                    cursor = listing.nextCursor
                    if not cursor:
                        break
            if not self.tools:
                raise ConfigError(
                    "No approved research MCP tools; configure servers and explicit read-only tools allowlists."
                )
            return self
        except BaseException:
            await self.stack.aclose()
            raise

    async def __aexit__(self, *args):
        await self.stack.aclose()

    async def call(self, name, arguments):
        if name not in self.routes:
            raise ValueError("Unknown research tool")
        session, tool = self.routes[name]
        validate(arguments, tool.inputSchema)
        result = await session.call_tool(tool.name, arguments)
        if result.isError:
            return json.dumps({"error": "Research tool failed"})
        return json.dumps(result.model_dump(mode="json"), ensure_ascii=False)[:80000]
