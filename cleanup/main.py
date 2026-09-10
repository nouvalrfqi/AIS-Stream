import argparse
import json
import logging
import time

from cleanup import retention
from cleanup.config import load_config
from cleanup.s3_lifecycle import S3Lifecycle

logger = logging.getLogger("cleanup")


def _log(level: str, msg: str, **fields):
    record = {"event": msg}
    record.update(fields)
    getattr(logger, level)(json.dumps(record))


def run_once(config: dict, dry_run: bool = False) -> dict:
    results: dict = {"dry_run": dry_run}
    conn = retention.connect(config)
    try:
        with conn:
            if dry_run:
                results["track_aging_count"] = retention.count_aging_tracks(
                    conn, config["track_retention_hours"]
                )
                results["stale_candidates"] = retention.count_stale_states(
                    conn, config["state_stale_minutes"]
                )
            else:
                results["track_deleted"] = retention.track_cleanup(
                    conn, config["track_retention_hours"]
                )
                results["stale_marked"] = retention.mark_stale(
                    conn, config["state_stale_minutes"]
                )
            if config["state_purge_after_hours"] > 0:
                if dry_run:
                    results["stale_purge_candidates"] = retention.count_purge_candidates(
                        conn, config["state_purge_after_hours"]
                    )
                else:
                    results["stale_purged"] = retention.purge_stale(
                        conn, config["state_purge_after_hours"]
                    )
    finally:
        conn.close()

    if config["s3_lifecycle_on"]:
        lifecycle = S3Lifecycle(config)
        if dry_run:
            results["s3_lifecycle_rule"] = "present" if lifecycle.get_rule() else "would-apply"
        else:
            results["s3_lifecycle_rule"] = "applied" if lifecycle.apply_rule() else "unchanged"
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Maritime cleanup jobs")
    parser.add_argument("--once", action="store_true", help="run a single cycle then exit")
    parser.add_argument("--dry-run", action="store_true", help="report what would be removed without executing")
    args = parser.parse_args()

    config = load_config()
    logging.basicConfig(
        level=config["log_level"],
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _log(
        "info",
        "cleanup starting",
        once=args.once,
        dry_run=args.dry_run,
        track_retention_hours=config["track_retention_hours"],
        state_stale_minutes=config["state_stale_minutes"],
        state_purge_after_hours=config["state_purge_after_hours"],
        s3_lifecycle_on=config["s3_lifecycle_on"],
        interval_seconds=config["cleanup_interval_seconds"],
    )

    if args.once:
        try:
            results = run_once(config, dry_run=args.dry_run)
            _log("info", "cleanup cycle done", **results)
        except Exception as e:
            _log("error", "cleanup cycle failed", error=str(e))
            raise SystemExit(1)
        return

    while True:
        started = time.time()
        try:
            results = run_once(config, dry_run=args.dry_run)
            results["elapsed_ms"] = int((time.time() - started) * 1000)
            _log("info", "cleanup cycle done", **results)
        except Exception as e:
            _log("error", "cleanup cycle failed", error=str(e))
        time.sleep(config["cleanup_interval_seconds"])


if __name__ == "__main__":
    main()