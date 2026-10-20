import argparse
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import fcntl
import json
import logging
import os
from pathlib import Path

from .clients import BaleClient, TelegramClient
from .config import ConfigError, load_config
from .health import health
from .inspection import inspect_echo, utc_now
from .jev import JevClient
from .redaction import Redactor, configure_logging, config_secrets
from .reports import save_report
from .store import Store
from .bridge import Bridge
from .bridge_store import BridgeStore


@contextmanager
def process_lock(database_path: Path):
    database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = database_path.with_suffix(database_path.suffix + ".lock")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another channel-sync process is using this database.") from None
        yield
    finally:
        os.close(fd)


def parser():
    result = argparse.ArgumentParser(prog="channel-sync", description="Durable Telegram/Bale channel bridge and diagnostics.")
    result.add_argument("--env", type=Path, default=Path(".env"), help="Path to the private .env file")
    sub = result.add_subparsers(dest="command", required=True)
    sub.add_parser("check-config", help="Validate .env locally; no network calls")
    init = sub.add_parser("init-db", help="Create SQLite schema locally; no tokens required")
    init.add_argument("--database", type=Path, default=Path("data/channel-sync.sqlite3"))
    health_parser = sub.add_parser("health", help="Check both bots, discussion delete rights, and a harmless jev evaluation")
    health_parser.add_argument("--output", type=Path, help="Also save the redacted health report")
    inspect_parser = sub.add_parser("inspect-echo", help="Poll first, send test probes, observe for 60 seconds")
    inspect_parser.add_argument("--output", type=Path, help="Report filename (default: reports/inspect-echo-TIMESTAMP.json)")
    runner = sub.add_parser("run", help="Poll both platforms and forward enabled directions")
    runner.add_argument("--phase", choices=["a", "b", "c"], help="Live-test direction override: a=TG→Bale, b=Bale→TG, c=both")
    runner.add_argument("--new-posts-only", action="store_true", help="Save and ignore the initial pending update backlog")
    runner.add_argument("--stop-on-error", action="store_true", help="Stop the live experiment on the first forwarding problem")
    runner.add_argument("--output", type=Path, help="Redacted run report filename")
    runner.add_argument("--dry-run", action="store_true", help="Suppress channel sends/deletions; curator can send admin previews")
    runner.add_argument("--watch-duplicates", action="store_true", help="Supervised test: stop on a new message ID reusing exhausted bridge signatures")
    runner.add_argument("--session-control", type=Path, help="Private JSON control file that arms the final observation timer")
    batch = sub.add_parser("live-batch", help="Publish authorized bot fixtures for phases B and C; no manual confirmations")
    batch.add_argument("--output", type=Path, help="Redacted batch report filename")
    batch.add_argument("--video", type=Path, default=Path("data/batch-fixtures/clip.mp4"), help="64×64 MP4 test fixture")
    sub.add_parser("status", help="Offline job, signature, and circuit status")
    moderate_test = sub.add_parser(
        "moderate-test",
        help="Print all jev scores for text; never delete or send messages",
    )
    moderate_test.add_argument("text", help="Comment text to classify")
    sub.add_parser(
        "x-login",
        help="Open headed Google Chrome for manual X login; close Chrome when finished",
    )
    curate_test = sub.add_parser(
        "curate-test",
        help="Research a post URL or text and send a draft only to configured admins",
    )
    curate_test.add_argument("input", help="X post URL or quoted post text")
    sub.add_parser(
        "curator-stats",
        help="Offline curator reader, draft and daily counts; works while run is active",
    )
    sub.add_parser(
        "curator-resume",
        help="Explicitly clear the X challenge/pause latch and schedule reading",
    )
    return result


async def run_live(args, config, redactor) -> int:
    if args.command == "x-login":
        from .curator.reader import run_browser

        async def login_event(event):
            if event["kind"] == "login":
                print(event["message"], flush=True)
            if event["kind"] == "error":
                raise RuntimeError(event["reason"])
            return True

        await run_browser(config, login_event, mode="login")
        print("Chrome closed. Use curator-resume after resolving any reader challenge.")
        return 0
    if args.command == "moderate-test":
        evaluator = JevClient(config)
        try:
            print(
                json.dumps(
                    await evaluator.classify(args.text), indent=2, ensure_ascii=False
                )
            )
            return 0
        finally:
            await evaluator.aclose()
    clients = {}
    try:
        clients["telegram"] = TelegramClient(config.telegram_token, config)
        clients["bale"] = BaleClient(config.bale_token, config)
        if args.command == "curate-test":
            from .curator.service import Curator
            from .curator.store import CuratorStore

            store = CuratorStore(config.database_path, redactor)
            curator = Curator(config, clients, store)
            try:
                if curator.setup_errors():
                    raise ConfigError("; ".join(curator.setup_errors()))
                previous_errors = store.connection.execute(
                    "SELECT COUNT(*) FROM curator_posts WHERE state='error'"
                ).fetchone()[0]
                job = store.enqueue("input", {"value": args.input})
                try:
                    with process_lock(config.database_path):
                        store.recover()
                        while await curator.pipeline_once():
                            pass
                        # Only preview drafts here; publication is handled by run after admin approval.
                        for row in store.connection.execute(
                            "SELECT id FROM curator_topics WHERE state IN ('pending','error')"
                        ):
                            await curator.approvals.sync_topic(row[0])
                except RuntimeError as exc:
                    if "Another channel-sync process" not in str(exc):
                        raise
                    if not config.curator_enabled:
                        raise ConfigError(
                            "The running worker needs CURATOR_ENABLED=true and a restart to handle queued curator tests."
                        ) from None
                    print(
                        f"Curator test queued as task {job}; the running worker will send the admin preview."
                    )
                    return 0
                task = store.connection.execute(
                    "SELECT state,error FROM curator_tasks WHERE id=?", (job,)
                ).fetchone()
                print(
                    json.dumps(
                        redactor.data(
                            {
                                "task": job,
                                "state": task["state"],
                                "error": task["error"],
                                "stats": store.stats(config),
                            }
                        ),
                        indent=2,
                    )
                )
                errors = store.connection.execute(
                    "SELECT COUNT(*) FROM curator_posts WHERE state='error'"
                ).fetchone()[0]
                return 1 if task["state"] == "error" or errors > previous_errors else 0
            finally:
                await curator.aclose()
                store.close()
        if args.command == "live-batch":
            from .live_batch import live_batch

            output = (
                args.output
                or config.reports_dir / f"live-batch-{utc_now().replace(':', '-')}.json"
            )
            with process_lock(config.database_path):
                store = BridgeStore(config.database_path, redactor)
                try:
                    if store.recover_uncertain() or any(
                        store.paused(d) for d in ("tg_to_bale", "bale_to_tg")
                    ):
                        raise RuntimeError(
                            "Unresolved sends or paused circuits; batch will not start"
                        )
                    if store.connection.execute(
                        "SELECT 1 FROM sent_messages WHERE status='uncertain' LIMIT 1"
                    ).fetchone():
                        raise RuntimeError(
                            "Unresolved send requires reconciliation before batch"
                        )
                    report = await live_batch(
                        config, clients, store, output, args.video
                    )
                finally:
                    store.close()
            print(
                json.dumps(
                    redactor.data(
                        {
                            "status": report["status"],
                            "report": str(output.resolve()),
                            "cases": len(report["cases"]),
                        }
                    ),
                    indent=2,
                )
            )
            return 0
        if args.command == "health":
            report = await health(
                clients,
                {
                    "telegram": config.telegram_channel_id,
                    "bale": config.bale_channel_id,
                },
            )
            if args.output:
                save_report(args.output, report, redactor)
            print(json.dumps(redactor.data(report), indent=2, ensure_ascii=False))
            return 0 if report["ok"] else 1
        label = "run" if args.command == "run" else "inspect-echo"
        output = args.output or config.reports_dir / f"{label}-{utc_now().replace(':', '-')}.json"
        with process_lock(config.database_path):
            store = BridgeStore(config.database_path, redactor) if args.command == "run" else Store(config.database_path, redactor)
            try:
                recovered = store.recover_uncertain()
                if recovered:
                    logging.warning("Recorded %s interrupted sends as uncertain; none will be replayed", recovered)
                if args.command == "run":
                    bridge = Bridge(config, clients, store, output=output, stop_on_error=args.stop_on_error,
                                    new_posts_only=args.new_posts_only, phase=args.phase,
                                    watch_duplicates=args.watch_duplicates, session_control=args.session_control)
                    logging.info("Run report: %s", output.resolve())
                    await bridge.run()
                    return 0
                report = await inspect_echo(config, clients, store, output)
            finally:
                store.close()
        summary = {"status": report["status"], "report": str(output.resolve()),
                   "findings": report.get("findings"), "note": report.get("note"), "error": report.get("error")}
        print(json.dumps(redactor.data(summary), indent=2, ensure_ascii=False))
        return 0 if report["status"] in {"complete", "dry_run"} else 1
    finally:
        await asyncio.gather(*(client.aclose() for client in clients.values()))


def main(argv=None) -> int:
    # Private defaults also protect SQLite WAL/SHM files and interrupted reports.
    os.umask(0o077)
    args = parser().parse_args(argv)
    redactor = Redactor()
    configure_logging("INFO", redactor)
    try:
        if args.command == "init-db":
            with process_lock(args.database):
                store = Store(args.database)
                store.close()
            print(f"SQLite schema ready: {args.database.resolve()}")
            return 0
        config = load_config(args.env)
        redactor = Redactor(*config_secrets(config))
        configure_logging(config.log_level, redactor)
        if args.command in {"curator-stats", "curator-resume"}:
            from .curator.store import CuratorStore

            store = CuratorStore(config.database_path, redactor)
            try:
                if args.command == "curator-resume":
                    store.resume_reader()
                print(
                    json.dumps(
                        redactor.data(store.stats(config)), indent=2, ensure_ascii=False
                    )
                )
            finally:
                store.close()
            return 0
        if args.command == "status":
            with process_lock(config.database_path):
                store = BridgeStore(config.database_path, redactor)
                try:
                    print(json.dumps(redactor.data(store.snapshot()), indent=2))
                finally:
                    store.close()
            return 0
        if args.command == "run":
            if args.phase:
                config = replace(
                    config,
                    telegram_to_bale_enabled=args.phase in {"a", "c"},
                    bale_to_telegram_enabled=args.phase in {"b", "c"},
                )
            if args.dry_run:
                config = replace(config, dry_run=True)
        if args.command == "check-config":
            print(
                json.dumps(
                    {
                        "valid": True,
                        "telegram_channel_id": config.telegram_channel_id,
                        "bale_channel_id": config.bale_channel_id,
                        "telegram_proxy_enabled": bool(config.telegram_proxy_url),
                        "bale_connects_directly": True,
                        "dry_run": config.dry_run,
                        "telegram_to_bale_enabled": config.telegram_to_bale_enabled,
                        "bale_to_telegram_enabled": config.bale_to_telegram_enabled,
                        "moderation_enabled": config.moderation_enabled,
                        "typesafe_api_key_configured": bool(config.typesafe_api_key),
                        "curator_enabled": config.curator_enabled,
                        "curator_admin_ids_configured": bool(
                            config.admin_telegram_user_ids
                            and config.admin_bale_user_ids
                        ),
                        "curator_llm_configured": bool(
                            config.llm_base_url
                            and config.llm_api_key
                            and config.llm_model
                        ),
                        "topic_dedup_days": config.topic_dedup_days,
                        "moderation_threshold": config.moderation_threshold,
                    },
                    indent=2,
                )
            )
            return 0
        return asyncio.run(run_live(args, config, redactor))
    except ConfigError as exc:
        print(f"Configuration error: {redactor.text(str(exc))}")
        return 2
    except KeyboardInterrupt:
        print("Stopped. Any in-flight send is uncertain and will not be replayed.")
        return 130
    except Exception as exc:
        logging.error("%s", redactor.text(str(exc)))
        return 1
