import json
import sys

from app import app, run_ingestion_pipeline, reconcile_stale_ingestion_runs


def main():
    payload = {}
    if len(sys.argv) > 1:
        try:
            payload = json.loads(sys.argv[1])
        except json.JSONDecodeError:
            payload = {}

    channel_limits = payload.get("channel_limits") or {}
    clear_existing = bool(payload.get("clear_existing", True))
    triggered_by = payload.get("triggered_by") or "worker"

    with app.app_context():
        reconcile_stale_ingestion_runs()
        run_ingestion_pipeline(
            channel_limits=channel_limits,
            clear_existing=clear_existing,
            triggered_by=triggered_by,
        )


if __name__ == "__main__":
    main()
