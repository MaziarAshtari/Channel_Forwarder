"""Exa's published MCP envelope, deadlines, and independent transport policies."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
from mcp.types import CallToolResult, ListToolsResult, TextContent, Tool

from channel_sync.config import ConfigError, load_config
from channel_sync.curator.mcp import ResearchMCP, ToolRoute, http_client
from channel_sync.curator.research import LLM
from helpers import BALE_TOKEN, TELEGRAM_TOKEN, config


def exa_tool():
    # Published agentRunInputShape: query and runId are optional individually;
    # Exa enforces their mutual exclusion and runId-only resumes at runtime.
    return Tool(
        name="agent_run",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "minLength": 1},
                "runId": {"type": "string", "pattern": "^agent_run_"},
                "effort": {
                    "type": "string",
                    "enum": ["minimal", "low", "medium", "high", "xhigh", "auto"],
                },
            },
        },
    )


def exa_result(status, *, structured=False, run_id="agent_run_test", **kwargs):
    payload = {
        "success": status not in {"failed", "cancelled"},
        "id": run_id,
        "status": status,
        "outputReady": status == "completed",
        **kwargs,
    }
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload))],
        structuredContent=payload if structured else None,
        isError=status == "failed",
    )


class AgentRunTests(unittest.IsolatedAsyncioTestCase):
    def make_mcp(self, results=(), *, timeout=1, interval=0.001):
        mcp = ResearchMCP(config(Path("/tmp")))
        session = MagicMock()
        session.call_tool = AsyncMock(side_effect=results)
        mcp.routes["s0_agent_run"] = ToolRoute(session, exa_tool(), timeout, interval)
        return mcp, session

    async def test_run_id_polling_returns_only_final_output(self):
        output = {
            "structured": {"facts": ["Verified primary evidence"]},
            "text": "https://official.example/release",
        }
        mcp, session = self.make_mcp(
            [
                exa_result("running", message="Do not expose interim output"),
                exa_result("running", structured=True),
                exa_result("completed", output=output),
            ]
        )
        arguments = {"query": "Research the release", "effort": "minimal"}
        with self.assertLogs("channel_sync.curator.mcp", level="INFO") as logs:
            result = await mcp.call("s0_agent_run", arguments)
        self.assertEqual(json.loads(result), output)
        self.assertEqual(
            session.call_tool.await_args_list,
            [
                call("agent_run", arguments),
                call("agent_run", {"runId": "agent_run_test"}),
                call("agent_run", {"runId": "agent_run_test"}),
            ],
        )
        self.assertNotIn("Do not expose interim output", result)
        self.assertIn("id=agent_run_test status=running", "\n".join(logs.output))
        self.assertIn("id=agent_run_test status=completed", "\n".join(logs.output))

    async def test_concurrent_duplicate_objectives_and_explicit_resume_reuse_result(
        self,
    ):
        mcp, session = self.make_mcp(
            [exa_result("running"), exa_result("completed", output="Evidence")]
        )
        results = await asyncio.gather(
            mcp.call("s0_agent_run", {"query": "Research release"}),
            mcp.call(
                "s0_agent_run", {"query": " RESEARCH  release ", "effort": "high"}
            ),
        )
        results.append(await mcp.call("s0_agent_run", {"runId": "agent_run_test"}))
        self.assertEqual(results, ['"Evidence"'] * 3)
        self.assertEqual(session.call_tool.await_count, 2)

    async def test_overall_deadline_includes_poll_pauses(self):
        mcp, session = self.make_mcp(timeout=0.025, interval=10)
        session.call_tool.side_effect = None
        session.call_tool.return_value = exa_result("running")
        result = await asyncio.wait_for(
            mcp.call("s0_agent_run", {"query": "Slow research"}), 1
        )
        self.assertIn("overall deadline exceeded", json.loads(result)["error"])
        session.call_tool.assert_awaited_once_with(
            "agent_run", {"query": "Slow research"}
        )
        self.assertEqual(
            result, await mcp.call("s0_agent_run", {"query": "Slow research"})
        )
        self.assertEqual(session.call_tool.await_count, 1)

    async def test_overall_deadline_is_not_reset_by_each_poll(self):
        mcp, session = self.make_mcp(timeout=0.08, interval=0.005)
        loop = asyncio.get_running_loop()
        started = loop.time()
        poll_started = None

        async def tool_call(name, arguments):
            nonlocal poll_started
            if "query" in arguments:
                await asyncio.sleep(0.05)
                return exa_result("running")
            poll_started = loop.time()
            await asyncio.Future()

        session.call_tool.side_effect = tool_call
        result = await asyncio.wait_for(
            mcp.call("s0_agent_run", {"query": "Slow research"}), 1
        )
        self.assertIn("overall deadline exceeded", result)
        self.assertIsNotNone(poll_started)
        # Inspect the retained deadline, rather than making timing assertions
        # sensitive to machine load. Every poll shares the original deadline.
        state = mcp.agent_runs[("s0_agent_run", "agent_run_test")]
        self.assertAlmostEqual(state.deadline - started, 0.08, delta=0.01)
        self.assertLess(state.deadline - poll_started, 0.04)
        self.assertEqual(session.call_tool.await_count, 2)

    async def test_initial_timeout_never_restarts_unknown_upstream_run(self):
        mcp, session = self.make_mcp(timeout=0.025)

        async def hung_call(*args):
            await asyncio.Future()

        session.call_tool.side_effect = hung_call
        result = await mcp.call("s0_agent_run", {"query": "Research"})
        self.assertIn("overall deadline exceeded", result)
        self.assertEqual(result, await mcp.call("s0_agent_run", {"query": "Research"}))
        self.assertEqual(session.call_tool.await_count, 1)

    async def test_failure_and_cancellation_are_terminal_and_cached(self):
        for status in ("failed", "cancelled"):
            with self.subTest(status=status):
                mcp, session = self.make_mcp(
                    [exa_result("running"), exa_result(status, error="private-key")]
                )
                with self.assertLogs("channel_sync.curator.mcp", level="INFO") as logs:
                    result = await mcp.call("s0_agent_run", {"query": "Research"})
                self.assertIn(status, json.loads(result)["error"])
                self.assertNotIn("private-key", result + "\n".join(logs.output))
                self.assertEqual(
                    result,
                    await mcp.call("s0_agent_run", {"query": "Research"}),
                )
                self.assertEqual(session.call_tool.await_count, 2)

    async def test_transport_failure_is_cached_without_leaking_error_text(self):
        mcp, session = self.make_mcp([httpx.ReadTimeout("private-api-key")])
        with self.assertLogs("channel_sync.curator.mcp", level="INFO") as logs:
            result = await mcp.call("s0_agent_run", {"query": "Research"})
        self.assertIn("failed", result)
        self.assertNotIn("private-api-key", result + "\n".join(logs.output))
        await mcp.call("s0_agent_run", {"query": "Research"})
        self.assertEqual(session.call_tool.await_count, 1)

    async def test_resume_only_accepts_run_id(self):
        mcp, session = self.make_mcp([exa_result("completed", output="Evidence")])
        for arguments in (
            {},
            {"query": "Research", "runId": "agent_run_test"},
            {"runId": "agent_run_test", "effort": "low"},
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                await mcp.call("s0_agent_run", arguments)
        self.assertEqual(
            await mcp.call("s0_agent_run", {"runId": "agent_run_test"}),
            '"Evidence"',
        )
        session.call_tool.assert_awaited_once_with(
            "agent_run", {"runId": "agent_run_test"}
        )

    async def test_invalid_envelope_is_withheld_and_does_not_restart(self):
        for result in (
            CallToolResult(content=[TextContent(type="text", text="unknown")]),
            exa_result("running", run_id=None),
            exa_result("unexpected-status"),
            exa_result("completed", outputReady=False),
        ):
            with self.subTest(result=result):
                mcp, session = self.make_mcp([result])
                response = await mcp.call("s0_agent_run", {"query": "Research"})
                self.assertIn("error", json.loads(response))
                self.assertEqual(
                    response,
                    await mcp.call("s0_agent_run", {"query": "Research"}),
                )
                self.assertEqual(session.call_tool.await_count, 1)


class MCPTransportTests(unittest.IsolatedAsyncioTestCase):
    @asynccontextmanager
    async def connect(self, server, **changes):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "mcp.json"
            path.write_text(json.dumps({"servers": {"exa": server}}))
            conf = config(Path(root), mcp_servers_path=path, **changes)
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=False)
            session.initialize = AsyncMock()
            session.list_tools = AsyncMock(
                return_value=ListToolsResult(tools=[exa_tool()])
            )
            session.call_tool = AsyncMock(
                return_value=exa_result("completed", output="Evidence")
            )
            captured = {}

            @asynccontextmanager
            async def http(url, *, http_client):
                captured.update(url=url, client=http_client)
                yield ("read", "write", None)

            @asynccontextmanager
            async def stdio(params, **kwargs):
                captured["stdio"] = params
                yield ("read", "write")

            with (
                patch("mcp.ClientSession", return_value=session) as constructor,
                patch(
                    "mcp.client.streamable_http.streamable_http_client",
                    side_effect=http,
                ),
                patch("mcp.client.stdio.stdio_client", side_effect=stdio),
            ):
                async with ResearchMCP(conf) as mcp:
                    yield mcp, captured, constructor

    async def test_server_timeout_applies_to_http_and_sdk_and_discovered_agent(self):
        for value in (None, 1234, 0.125):
            server = {
                "type": "streamable_http",
                "url": "https://mcp.exa.ai/mcp?tools=agent_run",
                "headers": {"x-api-key": "${EXA_API_KEY}"},
                "tools": ["agent_run"],
            }
            if value is not None:
                server["timeout_seconds"] = value
            with self.subTest(value=value):
                async with self.connect(
                    server, env_values={"EXA_API_KEY": "private-key"}
                ) as (mcp, captured, constructor):
                    expected = 600 if value is None else value
                    self.assertEqual(captured["client"].timeout.read, expected)
                    self.assertEqual(
                        constructor.call_args.kwargs["read_timeout_seconds"],
                        timedelta(seconds=expected),
                    )
                    self.assertTrue(mcp.routes["s0_agent_run"].is_agent_run)
                    self.assertEqual(
                        await mcp.call("s0_agent_run", {"query": "Research"}),
                        '"Evidence"',
                    )
                    self.assertEqual(
                        captured["client"].headers["x-api-key"], "private-key"
                    )

    async def test_invalid_server_deadlines_and_poll_intervals_fail_without_values(
        self,
    ):
        for key in ("timeout_seconds", "poll_interval_seconds"):
            for value in (0, -1, float("nan"), float("inf"), True, None, "secret"):
                server = {
                    "type": "http",
                    "url": "https://example.test",
                    key: value,
                }
                with self.subTest(key=key, value=value):
                    with self.assertRaises(ConfigError) as caught:
                        async with self.connect(server):
                            self.fail("Invalid server setting was accepted")
                    self.assertNotIn("secret", str(caught.exception))

    async def test_stdio_proxy_is_independent_of_llm_and_uses_remote_dns(self):
        server = {
            "type": "stdio",
            "command": "exa-mcp-server",
            "tools": ["agent_run"],
        }
        for llm, mcp in ((True, False), (False, True)):
            with self.subTest(llm=llm, mcp=mcp):
                async with self.connect(
                    server,
                    llm_use_proxy=llm,
                    mcp_use_proxy=mcp,
                    telegram_proxy_url="socks5://127.0.0.1:10808",
                ) as (_, captured, constructor):
                    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
                        self.assertEqual(
                            captured["stdio"].env.get(key),
                            "socks5h://127.0.0.1:10808" if mcp else None,
                        )
                    self.assertEqual(
                        constructor.call_args.kwargs["read_timeout_seconds"],
                        timedelta(seconds=600),
                    )


class ProxyConfigTests(unittest.TestCase):
    def test_http_proxy_split_for_all_flag_combinations(self):
        for llm, mcp in ((False, False), (False, True), (True, False), (True, True)):
            with self.subTest(llm=llm, mcp=mcp):
                conf = config(
                    Path("/tmp"),
                    llm_use_proxy=llm,
                    mcp_use_proxy=mcp,
                    telegram_proxy_url="socks5://127.0.0.1:10808",
                )
                with patch("channel_sync.curator.mcp.httpx.AsyncClient") as factory:
                    http_client(conf, timeout_seconds=900)
                    self.assertEqual(
                        factory.call_args.kwargs["proxy"],
                        "socks5h://127.0.0.1:10808" if mcp else None,
                    )
                    self.assertFalse(factory.call_args.kwargs["trust_env"])
                    self.assertEqual(factory.call_args.kwargs["timeout"].read, 900)
                    LLM(conf)
                    self.assertEqual(
                        factory.call_args.kwargs["proxy"],
                        "socks5://127.0.0.1:10808" if llm else None,
                    )
                    self.assertFalse(factory.call_args.kwargs["trust_env"])

    def test_config_proxy_flags_default_false_and_load_independently(self):
        with tempfile.TemporaryDirectory() as root:
            env = Path(root) / ".env"
            base = (
                f"TELEGRAM_BOT_TOKEN={TELEGRAM_TOKEN}\n"
                f"BALE_BOT_TOKEN={BALE_TOKEN}\n"
                "TELEGRAM_CHANNEL_ID=-1004472119156\n"
                "BALE_CHANNEL_ID=5716375015\n"
                "MODERATION_ENABLED=false\n"
            )
            for flags in (
                "",
                "LLM_USE_PROXY=true\nMCP_USE_PROXY=false\n",
                "LLM_USE_PROXY=false\nMCP_USE_PROXY=true\n",
            ):
                env.write_text(base + flags)
                conf = load_config(env)
                self.assertEqual(conf.llm_use_proxy, "LLM_USE_PROXY=true" in flags)
                self.assertEqual(conf.mcp_use_proxy, "MCP_USE_PROXY=true" in flags)
            env.write_text(base + "MCP_USE_PROXY=secret\n")
            with self.assertRaises(ConfigError) as caught:
                load_config(env)
            self.assertNotIn("secret", str(caught.exception))
