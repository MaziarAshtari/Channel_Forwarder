"""Headed Chrome child process and asynchronous parent watchdog.

Only navigation, response observation, DOM reads and scrolling are automated.
No cookies, request headers, passwords or full browser errors leave the child.
"""

import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import os
import random
import re
import signal
import sys
import time
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from ..config import load_config


def active_now(config, now=None):
    now = now or datetime.now(ZoneInfo(config.x_timezone))
    current = now.hour * 60 + now.minute
    start, end = [
        int(v[:2]) * 60 + int(v[3:]) for v in config.x_active_hours.split("-")
    ]
    return start <= current < end if start < end else current >= start or current < end


def tweet_url(value):
    match = re.fullmatch(
        r"https://(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)(?:[/?#].*)?",
        value.strip(),
    )
    if not match:
        raise ValueError("Expected an X post URL.")
    return f"https://x.com/{match[1]}/status/{match[2]}", match[2]


def challenge_reason(url, text):
    path = urlsplit(url).path.lower()
    text = text.lower()
    if any(
        s in path
        for s in ("/account/access", "/i/flow/challenge", "/i/flow/login", "/login")
    ):
        return "login or account challenge"
    patterns = (
        "unusual activity",
        "unusual login",
        "verify you are human",
        "authenticate your account",
        "account has been locked",
        "account is locked",
        "account is suspended",
        "rate limit exceeded",
        "you are rate limited",
        "too many requests",
        "captcha",
        "prove you are human",
    )
    return next((p for p in patterns if p in text), None)


def parse_timeline(payload):
    posts = {}

    def extract(node):
        if not isinstance(node, dict):
            return None
        if node.get("__typename") == "TweetWithVisibilityResults":
            node = node.get("tweet", {})
        legacy = node.get("legacy", {})
        if not (
            node.get("rest_id") and isinstance(legacy, dict) and "full_text" in legacy
        ):
            return None
        retweet = legacy.get("retweeted_status_result", {}).get("result")
        if retweet:
            return extract(retweet)
        user = node.get("core", {}).get("user_results", {}).get("result", {})
        author = user.get("core", {}).get("screen_name") or user.get("legacy", {}).get(
            "screen_name", ""
        )
        note = (
            node.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {})
        )
        text = (
            node.get("note_tweet", {})
            .get("note_tweet_results", {})
            .get("result", {})
            .get("text")
            or legacy["full_text"]
        )
        links = [
            u["expanded_url"]
            for u in legacy.get("entities", {}).get("urls", [])
            + note.get("entity_set", {}).get("urls", [])
            if u.get("expanded_url")
        ]
        media = []
        for item in legacy.get("extended_entities", {}).get("media", []):
            kind = "photo" if item.get("type") == "photo" else "video"
            url = item.get("media_url_https")
            if kind == "video":
                variants = [
                    v
                    for v in item.get("video_info", {}).get("variants", [])
                    if v.get("content_type") == "video/mp4"
                ]
                url = (
                    max(variants, key=lambda v: v.get("bitrate") or 0).get("url")
                    if variants
                    else None
                )
            if url:
                media.append(
                    {
                        "kind": kind,
                        "url": url,
                        "width": item.get("original_info", {}).get("width"),
                        "height": item.get("original_info", {}).get("height"),
                    }
                )
        quote = extract(node.get("quoted_status_result", {}).get("result"))
        post = {
            "id": str(node["rest_id"]),
            "url": f"https://x.com/{author or 'i/web'}/status/{node['rest_id']}",
            "author": author,
            "text": text,
            "created_at": legacy.get("created_at"),
            "media": media,
            "external_links": links,
            "reply_to": legacy.get("in_reply_to_status_id_str"),
        }
        if quote:
            post["quoted_post"] = quote
            post["text"] += "\n\nQuoted @" + quote["author"] + ":\n" + quote["text"]
            post["external_links"] += quote["external_links"]
        return post

    def walk(node):
        if isinstance(node, dict):
            post = extract(node)
            if post:
                posts[post["id"]] = post
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    for post in posts.values():
        # Include only thread context delivered to this page by X itself.
        related = [
            p
            for p in posts.values()
            if p.get("reply_to") == post["id"] and p["author"] == post["author"]
        ]
        if related:
            post["visible_thread"] = [
                {"id": p["id"], "text": p["text"], "url": p["url"]} for p in related
            ]
    return list(posts.values())


def emit(kind, **values):
    print(json.dumps({"kind": kind, **values}, ensure_ascii=False), flush=True)


@contextmanager
def profile_lock(path):
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    fd = os.open(path.parent / (path.name + ".lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


async def browser_child(config, mode="session", url=None):
    from playwright.async_api import async_playwright

    proxy = None
    if config.x_proxy_url:
        parsed = urlsplit(config.x_proxy_url)
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        proxy = {
            "server": urlunsplit(
                (
                    parsed.scheme.replace("socks5h", "socks5"),
                    f"{host}:{parsed.port}",
                    "",
                    "",
                    "",
                )
            )
        }
        if parsed.username:
            from urllib.parse import unquote

            proxy.update(
                username=unquote(parsed.username),
                password=unquote(parsed.password or ""),
            )
    with profile_lock(config.x_browser_profile_dir):
        async with async_playwright() as playwright:
            context = await playwright.chromium.launch_persistent_context(
                str(config.x_browser_profile_dir),
                channel="chrome",
                headless=False,
                chromium_sandbox=True,
                no_viewport=True,
                proxy=proxy,
                timeout=45000,
            )
            page = context.pages[0] if context.pages else await context.new_page()
            page.set_default_timeout(15000)
            stopped = asyncio.Event()
            tasks = set()
            seen = set()

            async def response_received(response):
                try:
                    parsed = urlsplit(response.url)
                    if parsed.hostname not in {
                        "x.com",
                        "www.x.com",
                        "api.x.com",
                        "twitter.com",
                        "api.twitter.com",
                    }:
                        return
                    if "/graphql/" not in parsed.path:
                        return
                    if response.status in {401, 403, 429}:
                        emit(
                            "challenge",
                            reason=f"X timeline access rejected ({response.status})",
                        )
                        stopped.set()
                        return
                    if not any(
                        op in parsed.path
                        for op in (
                            "HomeTimeline",
                            "HomeLatestTimeline",
                            "TweetDetail",
                            "HomeMixer",
                        )
                    ):
                        return
                    if stopped.is_set():
                        return
                    payload = await response.json()
                    if isinstance(payload, dict) and any(
                        e.get("code") in {32, 64, 88, 89, 326}
                        for e in payload.get("errors", [])
                        if isinstance(e, dict)
                    ):
                        emit(
                            "challenge",
                            reason="X timeline reported authentication, account or rate-limit challenge",
                        )
                        stopped.set()
                        return
                    for post in parse_timeline(payload):
                        if stopped.is_set():
                            return
                        if post["id"] not in seen:
                            seen.add(post["id"])
                            emit("post", post=post)
                except Exception:
                    # DOM fallback remains available for response schema changes.
                    emit("parse_fallback")

            def response_handler(response):
                task = asyncio.create_task(response_received(response))
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            if mode in {"session", "tweet"}:
                page.on("response", response_handler)
            try:
                await page.goto(
                    url or "https://x.com/home",
                    wait_until="domcontentloaded",
                    timeout=45000,
                )
                if mode == "login":
                    emit(
                        "login",
                        message="Log in manually in Chrome, then close its window. No password is recorded by channel-sync.",
                    )
                    while context.pages:
                        await asyncio.sleep(1)
                    return
                deadline = time.monotonic() + (
                    35
                    if mode in {"probe", "tweet"}
                    else random.uniform(
                        config.x_session_minutes_min, config.x_session_minutes_max
                    )
                    * 60
                )
                login_deadline = time.monotonic() + 20
                while time.monotonic() < deadline and not stopped.is_set():
                    # Exclude posts from challenge text to avoid stopping for a tweet discussing CAPTCHA.
                    notice = await page.evaluate("""() => {let text=document.body.innerText;
                        document.querySelectorAll('article').forEach(e=>{text=text.replace(e.innerText,'');});
                        return text;}""")
                    reason = challenge_reason(page.url, notice)
                    if reason:
                        emit("challenge", reason=reason)
                        stopped.set()
                        break
                    logged_in = (
                        await page.locator(
                            '[data-testid="SideNav_AccountSwitcher_Button"]'
                        ).count()
                        > 0
                    )
                    if not logged_in and time.monotonic() > login_deadline:
                        emit(
                            "challenge",
                            reason="Login wall or signed-in account could not be confirmed",
                        )
                        stopped.set()
                        break
                    emit("heartbeat", logged_in=logged_in)
                    if mode == "probe" and logged_in:
                        return
                    articles = await page.locator(
                        'article[data-testid="tweet"]'
                    ).evaluate_all("""els => els.map(e=>({
                        text:[...e.querySelectorAll('[data-testid="tweetText"]')].map(n=>n.innerText).join('\n\n'),
                        urls:[...e.querySelectorAll('a[href]')].map(a=>a.href),
                        created_at:e.querySelector('time')?.dateTime,
                        photos:[...e.querySelectorAll('[data-testid="tweetPhoto"] img')].map(i=>({url:i.src,width:i.naturalWidth,height:i.naturalHeight}))
                    }))""")
                    for item in articles:
                        urls = [
                            u for u in item["urls"] if re.search(r"/status/\d+$", u)
                        ]
                        if not urls:
                            continue
                        post_url, post_id = tweet_url(urls[0])
                        if post_id in seen:
                            continue
                        seen.add(post_id)
                        emit(
                            "post",
                            post={
                                "id": post_id,
                                "url": post_url,
                                "author": post_url.split("/")[3],
                                "text": item["text"],
                                "created_at": item.get("created_at"),
                                "media": [
                                    {"kind": "photo", **p} for p in item["photos"]
                                ],
                                "external_links": [
                                    u
                                    for u in item["urls"]
                                    if urlsplit(u).hostname
                                    not in {"x.com", "twitter.com"}
                                ],
                            },
                        )
                    if mode == "tweet" and url and tweet_url(url)[1] in seen:
                        return
                    await asyncio.sleep(
                        random.uniform(12, 25)
                        if random.random() < 0.12
                        else random.uniform(3, 9)
                    )
                    if mode == "session":
                        await page.mouse.wheel(0, random.randint(320, 950))
                if mode in {"probe", "tweet"} and not stopped.is_set():
                    emit(
                        "challenge",
                        reason="Login could not be confirmed"
                        if mode == "probe"
                        else "Post inaccessible; manual review required",
                    )
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await context.close()


async def kill_browser(process, known=()):
    import psutil

    try:
        descendants = psutil.Process(process.pid).children(recursive=True)
    except psutil.Error:
        descendants = []
    descendants = list({p.pid: p for p in [*known, *descendants]}.values())
    for child in reversed(descendants):
        try:
            child.kill()
        except psutil.Error:
            pass
    try:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await process.wait()


async def run_browser(config, callback, *, mode="session", url=None):
    import psutil

    argv = [
        sys.executable,
        "-m",
        "channel_sync.curator.reader",
        "--env",
        str(config.env_path),
        "--mode",
        mode,
    ]
    if url:
        argv += ["--url", url]
    if not os.environ.get("DISPLAY"):
        argv = ["xvfb-run", "-a", "--server-args=-screen 0 1280x900x24", *argv]
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
        limit=2 * 1024 * 1024,
    )
    last_event = time.monotonic()
    known = {}
    line_task = asyncio.create_task(process.stdout.readline())
    try:
        while True:
            done, _ = await asyncio.wait({line_task}, timeout=2)
            try:
                root = psutil.Process(process.pid)
                processes = [root, *root.children(recursive=True)]
                known.update({p.pid: p for p in processes if p.pid != process.pid})
                rss = sum(p.memory_info().rss for p in processes if p.is_running())
                if (
                    config.x_browser_memory_mb
                    and rss > config.x_browser_memory_mb * 1024 * 1024
                ):
                    raise RuntimeError("Browser memory budget exceeded")
            except psutil.NoSuchProcess:
                pass
            if (
                mode != "login"
                and time.monotonic() - last_event > config.x_browser_watchdog_seconds
            ):
                raise TimeoutError("Browser watchdog expired")
            if not done:
                continue
            line = line_task.result()
            if not line:
                if await process.wait():
                    raise RuntimeError("Browser process exited unsuccessfully")
                return
            event = json.loads(line)
            last_event = time.monotonic()
            if event["kind"] == "challenge":
                await kill_browser(process, known.values())
            if await callback(event) is False or event["kind"] == "challenge":
                return
            line_task = asyncio.create_task(process.stdout.readline())
    finally:
        line_task.cancel()
        await asyncio.gather(line_task, return_exceptions=True)
        await kill_browser(process, known.values())


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument(
        "--mode", choices=["session", "login", "probe", "tweet"], default="session"
    )
    parser.add_argument("--url")
    args = parser.parse_args()
    from pathlib import Path

    try:
        asyncio.run(browser_child(load_config(Path(args.env)), args.mode, args.url))
    except KeyboardInterrupt:
        pass
    except Exception:
        emit(
            "error",
            reason="Browser launch or operation failed; check Chrome, display, proxy and profile lock",
        )
        raise SystemExit(1)
