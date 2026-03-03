from app import DEFAULT_CHANNEL_LIMITS, run_ingestion_pipeline

# Keep loader defaults aligned with previous high-volume script behavior.
LOADER_CHANNEL_LIMITS = {
    "amazon": 5000,
    "twitter": 5000,
    "reddit_posts": 2000,
    "reddit_comments": 3000,
    "reddit_simulated": 5000,
    "instagram": 5000,
    "nykaa": 5000,
    "google": 5000,
    "flipkart": 5000,
}


def load_all_channels(channel_limits=None, clear_existing=True):
    limits = dict(DEFAULT_CHANNEL_LIMITS)
    limits.update(channel_limits or LOADER_CHANNEL_LIMITS)

    ok, payload = run_ingestion_pipeline(
        channel_limits=limits,
        clear_existing=clear_existing,
        triggered_by="data_loader_script",
    )

    if not ok:
        raise RuntimeError(payload.get("error", "Ingestion pipeline failed."))

    return payload


if __name__ == "__main__":
    result = load_all_channels()
    print("Ingestion + aggregation completed.")
    print(result)
