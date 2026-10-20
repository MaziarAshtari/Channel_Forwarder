"""Official MCP SDK transports, with credentials resolved only from private .env."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import timedelta
import json
import logging
import math
import os
import re
from typing import Any

import httpx
from jsonschema import validate

from ..config import ConfigError

log = logging.getLogger(__name__)
DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_POLL_INTERVAL_SECONDS = 5


def mcp_proxy(config):
    if not config.mcp_use_proxy:
        return None
    proxy = config.telegram_proxy_url
    # Explicit remote DNS, including when the shared proxy uses socks5://.
    if proxy and proxy.startswith("socks5://"):
        proxy = "socks5h://" + proxy[len("socks5://") :]
    return proxy


def http_client(config, *, headers=None, timeout_seconds=DEFAULT_TIMEOUT_SECONDS):
    return httpx.AsyncClient(
        headers=headers,
        trust_env=False,
        follow_redirects=False,
        proxy=mcp_proxy(config),
        timeout=httpx.Timeout(config.request_timeout, read=timeout_seconds),
    )


def positive_seconds(server, key, default):
    value = server.get(key, default)
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ConfigError(f"MCP {key} must be a positive finite number.") from None
    if isinstance(value, bool) or not math.isfinite(number) or number <= 0:
        raise ConfigError(f"MCP {key} must be a positive finite number.")
    return number


@dataclass(frozen=True)
class ToolRoute:
    session: Any
    tool: Any
    timeout_seconds: float
    poll_interval_seconds: float

    @property
    def is_agent_run(self):
        properties = self.tool.inputSchema.get("properties", {})
        return self.tool.name == "agent_run" and {"query", "runId"} <= properties.keys()


@dataclass
class AgentRequest:
    deadline: float
    arguments: dict
    run_id: str | None = None
    output: str | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def agent_payload(result):
    """Exa returns JSON text with id/status/outputReady/output, not runId.

    Contract: exa-labs/exa-mcp-server src/tools/agentRun.ts, outcomeToToolContent.
    Also accept the equivalent structured MCP result when supplied by a server.
    """
    structured = result.structuredContent
    if isinstance(structured, dict) and "status" in structured:
        return structured
    for block in result.content:
        if block.type != "text":
            continue
        try:
            data = json.loads(block.text)
        except (ValueError, TypeError):
            continue
        if isinstance(data, dict) and "status" in data:
            return data
    return None


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
        # One ResearchMCP instance belongs to one research operation. Retain errors
        # too: an ambiguous timeout must never cause a second billable create call.
        self.agent_requests = {}
        self.agent_runs = {}

    async def __aenter__(self):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
        from mcp.client.streamable_http import streamable_http_client

        try:
            for index, (name, server) in enumerate(load_servers(self.config).items()):
                if server.get("enabled", True) is False:
                    continue
                timeout = positive_seconds(
                    server, "timeout_seconds", DEFAULT_TIMEOUT_SECONDS
                )
                poll_interval = positive_seconds(
                    server, "poll_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS
                )
                if server.get("type") == "stdio":
                    # Never expose bot or LLM credentials to an unrelated search process.
                    env = dict(server.get("env", {}))
                    proxy = mcp_proxy(self.config)
                    if proxy:
                        env.update(
                            HTTP_PROXY=proxy,
                            HTTPS_PROXY=proxy,
                            ALL_PROXY=proxy,
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
                        http_client(
                            self.config,
                            headers=server.get("headers"),
                            timeout_seconds=timeout,
                        )
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
                        read_timeout_seconds=timedelta(seconds=timeout),
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
                        self.routes[exposed] = ToolRoute(
                            session, tool, timeout, poll_interval
                        )
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
        route = self.routes[name]
        validate(arguments, route.tool.inputSchema)
        if route.is_agent_run:
            return await self.call_agent(name, route, arguments)
        result = await route.session.call_tool(route.tool.name, arguments)
        if result.isError:
            return json.dumps({"error": "Research tool failed"})
        return json.dumps(result.model_dump(mode="json"), ensure_ascii=False)[:80000]

    async def call_agent(self, name, route, arguments):
        query, run_id = arguments.get("query"), arguments.get("runId")
        if (query is None) == (run_id is None):
            raise ValueError("Exa agent_run requires exactly one of query or runId")
        if run_id is not None and set(arguments) != {"runId"}:
            raise ValueError("Exa agent_run resume accepts only runId")
        loop = asyncio.get_running_loop()
        if run_id is not None:
            state = self.agent_runs.setdefault(
                (name, run_id),
                AgentRequest(loop.time() + route.timeout_seconds, arguments, run_id),
            )
        else:
            # Ignore effort/output-option changes for an otherwise identical
            # objective; those must not silently replace an existing run.
            key = (name, " ".join(query.split()).casefold())
            state = self.agent_requests.setdefault(
                key, AgentRequest(loop.time() + route.timeout_seconds, arguments)
            )
        async with state.lock:
            if state.output is not None:
                return state.output
            try:
                async with asyncio.timeout_at(state.deadline):
                    state.output = await self.poll_agent(name, route, state)
            except TimeoutError:
                self.log_agent(state.run_id, "deadline_exceeded")
                state.output = json.dumps(
                    {
                        "error": "Exa agent_run overall deadline exceeded; run was not restarted"
                    }
                )
            except asyncio.CancelledError:
                self.log_agent(state.run_id, "interrupted")
                state.output = json.dumps(
                    {"error": "Exa agent_run interrupted; run was not restarted"}
                )
                raise
            except Exception:
                # Do not include upstream error text, which may echo credentials.
                self.log_agent(state.run_id, "request_failed")
                state.output = json.dumps(
                    {"error": "Exa agent_run failed; run was not restarted"}
                )
            return state.output

    @staticmethod
    def log_agent(run_id, status):
        log.info("Exa agent_run id=%s status=%s", run_id or "unknown", status)

    async def poll_agent(self, name, route, state):
        arguments = {"runId": state.run_id} if state.run_id else state.arguments
        while True:
            result = await route.session.call_tool("agent_run", arguments)
            payload = agent_payload(result)
            if payload is None:
                raise ValueError("Exa agent_run did not return its result envelope")
            run_id = payload.get("id")
            if not isinstance(run_id, str) or not re.fullmatch(
                r"agent_run_[A-Za-z0-9_-]{1,512}", run_id
            ):
                raise ValueError("Exa agent_run returned no valid run ID")
            if state.run_id and state.run_id != run_id:
                raise ValueError("Exa agent_run returned a different run ID")
            state.run_id = run_id
            self.agent_runs[(name, run_id)] = state
            status = payload.get("status")
            if status not in {"running", "completed", "failed", "cancelled"}:
                raise ValueError("Exa agent_run returned an unknown status")
            self.log_agent(run_id, status)
            if status in {"failed", "cancelled"} or result.isError:
                return json.dumps(
                    {"error": f"Exa agent_run {status}; run was not restarted"}
                )
            if status == "completed":
                if payload.get("outputReady") is not True:
                    raise ValueError("Exa completed without ready output")
                return json.dumps(payload.get("output"), ensure_ascii=False)[:80000]
            await asyncio.sleep(route.poll_interval_seconds)
            arguments = {"runId": run_id}
