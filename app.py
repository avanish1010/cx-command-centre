from flask import Flask, render_template, jsonify, request, session, redirect, url_for, g
from datetime import datetime, timedelta
import random
import re
import json
import os
import sys
import secrets
import threading
import math
import statistics
import uuid
from sqlalchemy import inspect, text, func, cast, Date
from werkzeug.security import generate_password_hash, check_password_hash

from config import DATABASE_URL, SQLALCHEMY_ENGINE_OPTIONS
from extensions import db
try:
    from utils.sentiment_engine import analyze_sentiment
except ModuleNotFoundError:
    # Fallback for environments where the app directory is not on sys.path.
    app_dir = os.path.dirname(os.path.abspath(__file__))
    if app_dir not in sys.path:
        sys.path.append(app_dir)
    from utils.sentiment_engine import analyze_sentiment

app = Flask(__name__)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = SQLALCHEMY_ENGINE_OPTIONS
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'change-me-in-prod')
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = os.getenv('SESSION_COOKIE_SECURE', 'false').lower() == 'true'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=8)

# Initialize DB
db.init_app(app)

# Import models AFTER db is initialized
from models import Review, DailyMetric, RiskAlert, User, AuditLog, IngestionRun

ISSUE_TAXONOMY = [
    "Packaging",
    "Quality",
    "Delivery",
    "Pricing",
    "Support",
    "Side Effects",
    "Trust"
]

ISSUE_COLUMN_MAP = {
    "Packaging": "packaging_count",
    "Quality": "quality_count",
    "Delivery": "delivery_count",
    "Pricing": "pricing_count",
    "Support": "support_count",
    "Side Effects": "side_effects_count",
    "Trust": "trust_count"
}

ISSUE_ALIASES = {
    "packaging": "Packaging",
    "package": "Packaging",
    "delivery": "Delivery",
    "shipping": "Delivery",
    "quality": "Quality",
    "defect": "Quality",
    "price": "Pricing",
    "pricing": "Pricing",
    "refund": "Support",
    "support": "Support",
    "service": "Support",
    "side effects": "Side Effects",
    "reaction": "Side Effects",
    "rash": "Side Effects",
    "allergy": "Side Effects",
    "fake": "Trust",
    "counterfeit": "Trust",
    "scam": "Trust",
    "authenticity": "Trust",
    "trust": "Trust"
}

DELIVERY_PARTNERS = {"delhivery", "xpressbees", "ekart", "blue dart", "dtdc", "fedex", "ups"}


def normalize_issue_label(raw_issue):
    if not raw_issue:
        return None
    cleaned = raw_issue.strip().lower()
    if cleaned in ISSUE_ALIASES:
        return ISSUE_ALIASES[cleaned]
    for phrase, mapped in ISSUE_ALIASES.items():
        if phrase in cleaned:
            return mapped
    return None


def extract_identifiers(review_text):
    text_value = review_text or ""

    order_ids = re.findall(r"\b(?:order|ord|awb|tracking)[-:\s#]*([A-Z0-9]{6,20})\b", text_value, flags=re.IGNORECASE)
    locations = re.findall(r"\b(?:in|at|from)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b", text_value)

    lower_text = text_value.lower()
    partners = [partner for partner in DELIVERY_PARTNERS if partner in lower_text]

    issue_hits = [issue for issue, alias in ISSUE_COLUMN_MAP.items() if issue.lower() in lower_text]
    normalized = normalize_issue_label(text_value)
    if normalized and normalized not in issue_hits:
        issue_hits.append(normalized)

    return {
        "order_ids": sorted(set(order_ids)),
        "locations": sorted(set(locations)),
        "delivery_partners": sorted(set(partners)),
        "issue_hints": sorted(set(issue_hits))
    }


def ensure_daily_metric_columns():
    inspector = inspect(db.engine)
    columns = {c["name"] for c in inspector.get_columns("daily_metrics")}
    required = {
        "side_effects_count": "INTEGER DEFAULT 0",
        "trust_count": "INTEGER DEFAULT 0"
    }

    for column_name, column_type in required.items():
        if column_name not in columns:
            db.session.execute(text(f"ALTER TABLE daily_metrics ADD COLUMN {column_name} {column_type}"))
    db.session.commit()


def ensure_review_columns():
    inspector = inspect(db.engine)
    columns = {c["name"] for c in inspector.get_columns("reviews")}
    required = {
        "extracted_order_ids": "TEXT",
        "extracted_delivery_partners": "TEXT",
        "extracted_locations": "TEXT",
        "source_followers": "BIGINT",
        "post_views": "BIGINT",
        "engagement_count": "INTEGER",
        "influence_factor": "FLOAT DEFAULT 1.0"
    }

    for column_name, column_type in required.items():
        if column_name not in columns:
            db.session.execute(text(f"ALTER TABLE reviews ADD COLUMN {column_name} {column_type}"))
    db.session.commit()


ROLE_SUPERADMIN = "superadmin"
ROLE_CX_HEAD = "cxhead"
ALLOWED_ROLES = {ROLE_SUPERADMIN, ROLE_CX_HEAD}

# Fallback bootstrap credentials for environments where Render env vars are missing.
# Replace these before production use.
BOOTSTRAP_SUPERADMIN_EMAIL = (os.getenv("SUPERADMIN_EMAIL") or "superadmin@cx-command-centre.local").strip().lower()
BOOTSTRAP_SUPERADMIN_PASSWORD = os.getenv("SUPERADMIN_PASSWORD") or "ChangeMe@12345"

SUPERADMIN_ONLY_ENDPOINTS = {
    "aggregate_daily_metrics",
    "ingest_run",
    "ingest_status",
    "ingest_history",
    "ingest_auto_start",
    "ingest_auto_stop",
    "reset_db",
    "admin_users",
    "admin_toggle_user",
    "admin_reset_user_password",
    "admin_operations",
    "admin_operations_run",
    "admin_operations_auto_start",
    "admin_operations_auto_stop"
}

AUTH_EXEMPT_ENDPOINTS = {"static", "login"}


def _is_api_like_path(path_value):
    protected_api_prefixes = (
        "/ingest",
        "/evaluate",
        "/identifiers",
        "/aggregate",
        "/reset",
        "/brand_health",
        "/daily_brief"
    )
    return (path_value or "").startswith(protected_api_prefixes)


def _get_next_url(default_path="/dashboard/all"):
    next_url = request.args.get("next") or request.form.get("next") or default_path
    if isinstance(next_url, str) and next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return default_path


def _csrf_token():
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(24)
        session["csrf_token"] = token
    return token


def _verify_csrf():
    submitted = request.form.get("csrf_token", "")
    return bool(submitted) and secrets.compare_digest(submitted, session.get("csrf_token", ""))


def _current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return User.query.filter_by(id=user_id, is_active=True).first()


def _audit(action, target_email=None, metadata=None, actor_email=None):
    actor = actor_email or session.get("user_email") or "system"
    payload = json.dumps(metadata or {})
    db.session.add(AuditLog(actor_email=actor, action=action, target_email=target_email, metadata_json=payload))
    db.session.commit()


def _bootstrap_superadmin_if_needed():
    email = BOOTSTRAP_SUPERADMIN_EMAIL
    password = BOOTSTRAP_SUPERADMIN_PASSWORD
    if not email or not password:
        app.logger.warning("No superadmin exists and bootstrap credentials are not configured.")
        return

    user = User.query.filter_by(email=email).first()
    if user:
        user.password_hash = generate_password_hash(password)
        user.role = ROLE_SUPERADMIN
        user.is_active = True
        user.force_password_reset = False
        db.session.commit()
        _audit("bootstrap_superadmin_updated", target_email=email, metadata={"role": ROLE_SUPERADMIN}, actor_email="system")
        return

    user = User(
        email=email,
        password_hash=generate_password_hash(password),
        role=ROLE_SUPERADMIN,
        is_active=True,
        force_password_reset=False
    )
    db.session.add(user)
    db.session.commit()
    _audit("bootstrap_superadmin_created", target_email=email, metadata={"role": ROLE_SUPERADMIN}, actor_email="system")


@app.before_request
def enforce_authentication_and_roles():
    endpoint = request.endpoint or ""
    if endpoint in AUTH_EXEMPT_ENDPOINTS:
        return None

    user = _current_user()
    if not user:
        if _is_api_like_path(request.path):
            return jsonify({"ok": False, "error": "authentication_required"}), 401
        return redirect(url_for("login", next=request.path))

    g.current_user = user
    session["user_email"] = user.email
    session["user_role"] = user.role
    session.permanent = True

    if endpoint in SUPERADMIN_ONLY_ENDPOINTS and user.role != ROLE_SUPERADMIN:
        if _is_api_like_path(request.path):
            return jsonify({"ok": False, "error": "forbidden_superadmin_only"}), 403
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403

    return None


@app.context_processor
def inject_auth_context():
    return {
        "current_user": getattr(g, "current_user", None),
        "csrf_token": _csrf_token()
    }

# Create tables
with app.app_context():
    db.create_all()
    ensure_daily_metric_columns()
    ensure_review_columns()
    _bootstrap_superadmin_if_needed()


DEFAULT_CHANNEL_LIMITS = {
    "amazon": 1500,
    "twitter": 1500,
    "reddit_posts": 700,
    "reddit_comments": 900,
    "reddit_simulated": 1200,
    "instagram": 1200,
    "nykaa": 1200,
    "google": 1200,
    "flipkart": 1200
}

INGESTION_PRESETS = {
    "demo_fast": {
        "label": "Incremental Refresh",
        "clear_existing": False,
        "limits": dict(DEFAULT_CHANNEL_LIMITS)
    },
    "high_volume": {
        "label": "Full Refresh",
        "clear_existing": True,
        "limits": {
            "amazon": 5000,
            "twitter": 5000,
            "reddit_posts": 2000,
            "reddit_comments": 3000,
            "reddit_simulated": 5000,
            "instagram": 5000,
            "nykaa": 5000,
            "google": 5000,
            "flipkart": 5000
        }
    }
}

ingestion_state = {
    "last_run_started_at": None,
    "last_run_finished_at": None,
    "last_success_at": None,
    "last_status": "never_run",
    "last_message": "No ingestion run yet.",
    "last_counts": {},
    "last_run_id": None,
    "last_triggered_by": None,
    "auto_mode_enabled": False,
    "auto_interval_minutes": None,
    "auto_preset": "demo_fast"
}

ingestion_lock = threading.Lock()
stop_auto_ingest_event = threading.Event()
auto_ingest_thread = None


def parse_int(name, default_value):
    raw = request.args.get(name, default=default_value, type=int)
    if raw is None:
        return default_value
    return max(1, raw)


def get_health_threshold_hours():
    healthy_hours = max(1, int(os.getenv("INGEST_HEALTHY_HOURS", "6")))
    degraded_hours = max(healthy_hours + 1, int(os.getenv("INGEST_DEGRADED_HOURS", "12")))
    return healthy_hours, degraded_hours


def normalize_channel_limits(channel_limits=None):
    limits = dict(DEFAULT_CHANNEL_LIMITS)
    if channel_limits:
        for key, value in channel_limits.items():
            if key in limits:
                try:
                    limits[key] = max(1, int(value))
                except (TypeError, ValueError):
                    continue
    return limits


def resolve_ingestion_limits_from_preset(preset_name, overrides=None):
    preset_key = (preset_name or "demo_fast").strip().lower()
    if preset_key not in INGESTION_PRESETS:
        preset_key = "demo_fast"

    preset_config = INGESTION_PRESETS[preset_key]
    limits = dict(preset_config["limits"])
    if overrides:
        for key, value in overrides.items():
            if key in limits:
                try:
                    limits[key] = max(1, int(value))
                except (TypeError, ValueError):
                    continue
    clear_existing = bool(preset_config.get("clear_existing", True))
    return preset_key, limits, clear_existing


def get_ingestion_history(limit=25):
    rows = (
        IngestionRun.query
        .order_by(IngestionRun.started_at.desc())
        .limit(max(1, int(limit)))
        .all()
    )
    history = []
    for row in rows:
        channels = {}
        if row.channel_counts_json:
            try:
                channels = json.loads(row.channel_counts_json)
            except json.JSONDecodeError:
                channels = {}
        history.append({
            "run_id": row.run_id,
            "triggered_by": row.triggered_by,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "ended_at": row.ended_at.isoformat() if row.ended_at else None,
            "duration_ms": row.duration_ms,
            "status": row.status,
            "records_processed": row.records_processed,
            "metric_rows": row.metric_rows,
            "channel_counts": channels,
            "error_message": row.error_message
        })
    return history


def get_ingestion_health_snapshot():
    healthy_hours, degraded_hours = get_health_threshold_hours()
    last_success = (
        IngestionRun.query
        .filter_by(status="success")
        .order_by(IngestionRun.ended_at.desc(), IngestionRun.started_at.desc())
        .first()
    )

    if not last_success or not last_success.ended_at:
        return {
            "last_successful_sync_at": None,
            "hours_since_last_success": None,
            "data_freshness": "No successful sync yet",
            "system_health_status": "Critical",
            "healthy_threshold_hours": healthy_hours,
            "degraded_threshold_hours": degraded_hours
        }

    now_utc = datetime.utcnow()
    age_hours = round((now_utc - last_success.ended_at).total_seconds() / 3600, 2)

    if age_hours <= healthy_hours:
        health_status = "Healthy"
        freshness = "Fresh"
    elif age_hours <= degraded_hours:
        health_status = "Degraded"
        freshness = "Stale"
    else:
        health_status = "Critical"
        freshness = "Very Stale"

    return {
        "last_successful_sync_at": last_success.ended_at.isoformat(),
        "hours_since_last_success": age_hours,
        "data_freshness": freshness,
        "system_health_status": health_status,
        "healthy_threshold_hours": healthy_hours,
        "degraded_threshold_hours": degraded_hours
    }


def format_hms(total_seconds):
    if total_seconds is None:
        return "N/A"
    seconds_value = max(0, int(total_seconds))
    hours, remainder = divmod(seconds_value, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}h {minutes:02d}m {seconds:02d}s"


def get_scheduler_snapshot():
    interval_minutes = ingestion_state.get("auto_interval_minutes")
    interval_seconds = int(interval_minutes * 60) if interval_minutes else None

    latest_scheduler_run = (
        IngestionRun.query
        .filter_by(triggered_by="scheduler")
        .order_by(IngestionRun.started_at.desc())
        .first()
    )

    next_expected_run_at = None
    seconds_until_next_run = None

    if ingestion_state.get("auto_mode_enabled") and interval_seconds:
        reference_time = datetime.utcnow()
        if latest_scheduler_run:
            reference_time = latest_scheduler_run.ended_at or latest_scheduler_run.started_at or reference_time
        next_expected = reference_time + timedelta(seconds=interval_seconds)
        next_expected_run_at = next_expected.isoformat()
        seconds_until_next_run = max(0, int((next_expected - datetime.utcnow()).total_seconds()))

    return {
        "auto_mode_enabled": bool(ingestion_state.get("auto_mode_enabled")),
        "preset": ingestion_state.get("auto_preset"),
        "interval_minutes": interval_minutes,
        "interval_seconds": interval_seconds,
        "interval_hms": format_hms(interval_seconds),
        "next_expected_run_at": next_expected_run_at,
        "seconds_until_next_run": seconds_until_next_run,
        "countdown_hms": format_hms(seconds_until_next_run),
        "last_scheduler_started_at": latest_scheduler_run.started_at.isoformat() if latest_scheduler_run and latest_scheduler_run.started_at else None,
        "last_scheduler_finished_at": latest_scheduler_run.ended_at.isoformat() if latest_scheduler_run and latest_scheduler_run.ended_at else None,
        "last_scheduler_status": latest_scheduler_run.status if latest_scheduler_run else None
    }


def _start_auto_ingestion(interval_minutes, channel_limits, clear_existing, preset_name="demo_fast"):
    global auto_ingest_thread

    if auto_ingest_thread and auto_ingest_thread.is_alive():
        return False, "Auto-ingestion already running."

    stop_auto_ingest_event.clear()
    auto_ingest_thread = threading.Thread(
        target=auto_ingest_worker,
        args=(interval_minutes, channel_limits, clear_existing),
        daemon=True
    )
    auto_ingest_thread.start()

    ingestion_state["auto_mode_enabled"] = True
    ingestion_state["auto_interval_minutes"] = interval_minutes
    ingestion_state["auto_preset"] = preset_name
    ingestion_state["last_message"] = f"Auto-ingestion started every {interval_minutes} minutes."
    return True, ingestion_state["last_message"]


def _stop_auto_ingestion():
    stop_auto_ingest_event.set()
    ingestion_state["auto_mode_enabled"] = False
    ingestion_state["auto_interval_minutes"] = None
    ingestion_state["last_message"] = "Auto-ingestion stop signal issued."


def compute_influence_factor(channel, followers=None, views=None, engagement_count=None):
    social_channels = {"twitter", "instagram", "reddit"}
    if (channel or "").lower() not in social_channels:
        return 1.0

    followers = followers or 0
    views = views or 0
    engagement_count = engagement_count or 0

    views_signal = min(1.0, math.log10(max(views, 1)) / 6.0) if views > 0 else 0.0
    followers_signal = min(1.0, math.log10(max(followers, 1)) / 6.0) if followers > 0 else 0.0

    if views > 0:
        engagement_rate = engagement_count / views
    elif followers > 0:
        engagement_rate = engagement_count / followers
    else:
        engagement_rate = 0.0
    engagement_signal = min(1.0, max(0.0, engagement_rate * 20))

    weighted_signal = (0.45 * views_signal) + (0.40 * followers_signal) + (0.15 * engagement_signal)
    factor = 1.0 + (0.5 * weighted_signal)
    return round(max(1.0, min(1.5, factor)), 3)


def build_influence_lookup():
    rows = (
        db.session.query(
            Review.brand_name,
            Review.product_name,
            Review.channel,
            cast(Review.timestamp, Date).label("d"),
            func.avg(Review.influence_factor)
        )
        .filter(Review.timestamp.isnot(None))
        .group_by(Review.brand_name, Review.product_name, Review.channel, cast(Review.timestamp, Date))
        .all()
    )

    lookup = {}
    for brand_name, product_name, channel, day_value, avg_factor in rows:
        if day_value is None:
            continue
        factor = round(max(1.0, min(1.5, avg_factor or 1.0)), 3)
        lookup[(brand_name, product_name, channel, day_value)] = factor
    return lookup


def get_metric_influence(lookup, brand_name, product_name, channel, metric_date):
    return lookup.get((brand_name, product_name, channel, metric_date), 1.0)


def _build_issue_time_signals(records, column):
    signals = []
    for idx, rec in enumerate(records):
        value = getattr(rec, column, 0) or 0
        baseline_slice = records[max(0, idx - 7):idx]
        baseline_values = [(getattr(r, column, 0) or 0) for r in baseline_slice]

        if len(baseline_values) >= 3:
            mean = sum(baseline_values) / len(baseline_values)
            if len(baseline_values) > 1:
                stdev = statistics.stdev(baseline_values)
            else:
                stdev = 0
            z = ((value - mean) / stdev) if stdev > 0 else 0
        else:
            mean = 0
            z = 0

        if idx >= 2:
            prev_two = records[idx - 2:idx]
            prev_avg = sum((getattr(r, column, 0) or 0) for r in prev_two) / 2
            growth = (value / prev_avg) if prev_avg > 0 else None
        else:
            growth = None

        is_anomaly = (z > 2) or (growth is not None and growth >= 2.5)
        signals.append({
            "z": z,
            "growth": growth,
            "value": value,
            "mean": mean,
            "is_anomaly": is_anomaly
        })
    return signals


def _count_consecutive_anomaly_days(time_signals):
    count = 0
    for s in reversed(time_signals):
        if s["is_anomaly"]:
            count += 1
        else:
            break
    return count


def _persistence_multiplier(persistence_days):
    if persistence_days < 2:
        return 1.0
    return round(min(1.35, 1.0 + (0.08 * (persistence_days - 1))), 3)


def _is_recovery_initiated(time_signals):
    if len(time_signals) < 4:
        return False, 0

    today = time_signals[-1]
    prev = time_signals[-2]
    prior_run = _count_consecutive_anomaly_days(time_signals[:-1])

    # Sustained anomaly first, then a visible drop toward baseline with negative velocity.
    if prior_run >= 2 and today["growth"] is not None:
        moving_down = today["growth"] < 1.0 and today["value"] < prev["value"]
        moving_toward_baseline = abs(today["value"] - today["mean"]) < abs(prev["value"] - prev["mean"])
        if moving_down and moving_toward_baseline:
            return True, prior_run

    return False, 0


def enrich_review_record(review):
    enriched = dict(review)
    identifiers = extract_identifiers(enriched.get("review_text", ""))
    sentiment_meta = analyze_sentiment(enriched.get("review_text", ""))

    enriched["extracted_order_ids"] = json.dumps(identifiers["order_ids"])
    enriched["extracted_delivery_partners"] = json.dumps(identifiers["delivery_partners"])
    enriched["extracted_locations"] = json.dumps(identifiers["locations"])

    if not enriched.get("location") and identifiers["locations"]:
        enriched["location"] = identifiers["locations"][0]

    if (not enriched.get("issue_category") or enriched.get("issue_category") == "Other") and identifiers["issue_hints"]:
        enriched["issue_category"] = identifiers["issue_hints"][0]

    if not enriched.get("issue_category"):
        enriched["issue_category"] = normalize_issue_label(enriched.get("review_text", "")) or "Other"

    # Standardized free/offline sentiment across all channels for consistency.
    enriched["sentiment"] = sentiment_meta["sentiment"]
    enriched["sentiment_score"] = sentiment_meta["score"]
    enriched["influence_factor"] = compute_influence_factor(
        enriched.get("channel"),
        enriched.get("source_followers"),
        enriched.get("post_views"),
        enriched.get("engagement_count")
    )

    return enriched


def collect_channel_reviews(channel_limits):
    from connectors.amazon_connector import fetch_amazon_reviews
    from connectors.twitter_connector import fetch_twitter_mentions
    from connectors.reddit_connector import fetch_reddit_reviews
    from connectors.instagram_connector import fetch_instagram_mentions
    from connectors.nykaa_connector import fetch_nykaa_reviews
    from connectors.google_connector import fetch_google_reviews
    from connectors.flipkart_connector import fetch_flipkart_reviews

    all_reviews = []
    channel_counts = {}

    amazon_path = os.path.join("data", "Health_and_Personal_Care.jsonl")
    reddit_posts_path = os.path.join("data", "reddit_posts.jsonl")
    reddit_comments_path = os.path.join("data", "reddit_comments.jsonl")

    if os.path.exists(amazon_path):
        amazon_reviews = fetch_amazon_reviews(amazon_path, limit=channel_limits["amazon"])
    else:
        amazon_reviews = []
    channel_counts["Amazon"] = len(amazon_reviews)
    all_reviews.extend(amazon_reviews)

    twitter_reviews = fetch_twitter_mentions(limit=channel_limits["twitter"])
    channel_counts["Twitter"] = len(twitter_reviews)
    all_reviews.extend(twitter_reviews)

    reddit_reviews = fetch_reddit_reviews(
        posts_path=reddit_posts_path if os.path.exists(reddit_posts_path) else None,
        comments_path=reddit_comments_path if os.path.exists(reddit_comments_path) else None,
        post_limit=channel_limits["reddit_posts"],
        comment_limit=channel_limits["reddit_comments"],
        simulated_limit=channel_limits["reddit_simulated"]
    )
    channel_counts["Reddit"] = len(reddit_reviews)
    all_reviews.extend(reddit_reviews)

    instagram_reviews = fetch_instagram_mentions(limit=channel_limits["instagram"])
    channel_counts["Instagram"] = len(instagram_reviews)
    all_reviews.extend(instagram_reviews)

    nykaa_reviews = fetch_nykaa_reviews(limit=channel_limits["nykaa"])
    channel_counts["Nykaa"] = len(nykaa_reviews)
    all_reviews.extend(nykaa_reviews)

    google_reviews = fetch_google_reviews(limit=channel_limits["google"])
    channel_counts["Google"] = len(google_reviews)
    all_reviews.extend(google_reviews)

    flipkart_reviews = fetch_flipkart_reviews(limit=channel_limits["flipkart"])
    channel_counts["Flipkart"] = len(flipkart_reviews)
    all_reviews.extend(flipkart_reviews)

    return all_reviews, channel_counts


def rebuild_daily_metrics():
    from collections import defaultdict
    from models import Review, DailyMetric

    DailyMetric.query.delete()
    db.session.commit()

    reviews = Review.query.all()
    daily_data = defaultdict(lambda: {
        "total": 0,
        "positive": 0,
        "neutral": 0,
        "negative": 0,
        "Packaging": 0,
        "Delivery": 0,
        "Quality": 0,
        "Pricing": 0,
        "Support": 0,
        "Side Effects": 0,
        "Trust": 0
    })

    for review in reviews:
        if not review.timestamp:
            continue

        key = (
            review.timestamp.date(),
            review.brand_name,
            review.product_name,
            review.channel
        )

        daily_data[key]["total"] += 1
        sentiment = (review.sentiment or "").lower()
        if sentiment == "positive":
            daily_data[key]["positive"] += 1
        elif sentiment == "neutral":
            daily_data[key]["neutral"] += 1
        elif sentiment == "negative":
            daily_data[key]["negative"] += 1

        if review.issue_category in daily_data[key]:
            daily_data[key][review.issue_category] += 1

    inserted = 0
    for (date, brand, product, channel), data in daily_data.items():
        negative_percentage = (data["negative"] / data["total"]) * 100 if data["total"] > 0 else 0

        metric = DailyMetric(
            date=date,
            channel=channel,
            product_name=f"{brand} | {product}",
            total_mentions=data["total"],
            positive_count=data["positive"],
            neutral_count=data["neutral"],
            negative_count=data["negative"],
            negative_percentage=negative_percentage,
            packaging_count=data["Packaging"],
            delivery_count=data["Delivery"],
            quality_count=data["Quality"],
            pricing_count=data["Pricing"],
            support_count=data["Support"],
            side_effects_count=data["Side Effects"],
            trust_count=data["Trust"]
        )
        db.session.add(metric)
        inserted += 1

    db.session.commit()
    return inserted


def run_ingestion_pipeline(channel_limits, clear_existing=True, triggered_by="manual"):
    if not ingestion_lock.acquire(blocking=False):
        return False, {"error": "Ingestion already running. Try again after the current run finishes."}

    started_at = datetime.utcnow()
    run_id = f"ing-{uuid.uuid4().hex}"
    normalized_limits = normalize_channel_limits(channel_limits)
    run_row_id = None

    ingestion_state["last_run_started_at"] = started_at.isoformat()
    ingestion_state["last_status"] = "running"
    ingestion_state["last_message"] = "Ingestion in progress."
    ingestion_state["last_counts"] = {}
    ingestion_state["last_run_id"] = run_id
    ingestion_state["last_triggered_by"] = triggered_by

    try:
        with app.app_context():
            run_row = IngestionRun(
                run_id=run_id,
                triggered_by=triggered_by,
                started_at=started_at,
                status="running"
            )
            db.session.add(run_row)
            db.session.commit()
            run_row_id = run_row.id

        reviews, channel_counts = collect_channel_reviews(normalized_limits)
        enriched_reviews = [enrich_review_record(r) for r in reviews]

        with app.app_context():
            if clear_existing:
                Review.query.delete()
                db.session.commit()

            batch_size = 1000
            for i in range(0, len(enriched_reviews), batch_size):
                db.session.bulk_insert_mappings(Review, enriched_reviews[i:i + batch_size])
                db.session.commit()

            metric_rows = rebuild_daily_metrics()

            finished_at = datetime.utcnow()
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)
            persisted_row = IngestionRun.query.get(run_row_id)
            if persisted_row:
                persisted_row.ended_at = finished_at
                persisted_row.duration_ms = duration_ms
                persisted_row.status = "success"
                persisted_row.records_processed = len(enriched_reviews)
                persisted_row.metric_rows = metric_rows
                persisted_row.channel_counts_json = json.dumps(channel_counts or {})
                persisted_row.error_message = None
                db.session.commit()

        ingestion_state["last_run_finished_at"] = finished_at.isoformat()
        ingestion_state["last_success_at"] = finished_at.isoformat()
        ingestion_state["last_status"] = "success"
        ingestion_state["last_message"] = "Ingestion and aggregation completed."
        ingestion_state["last_counts"] = {
            "run_id": run_id,
            "reviews_loaded": len(enriched_reviews),
            "daily_metric_rows": metric_rows,
            "channels": channel_counts
        }
        return True, ingestion_state["last_counts"]
    except Exception as exc:
        finished_at = datetime.utcnow()
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)
        with app.app_context():
            if run_row_id:
                persisted_row = IngestionRun.query.get(run_row_id)
                if persisted_row:
                    persisted_row.ended_at = finished_at
                    persisted_row.duration_ms = duration_ms
                    persisted_row.status = "failed"
                    persisted_row.records_processed = 0
                    persisted_row.metric_rows = 0
                    persisted_row.channel_counts_json = json.dumps({})
                    persisted_row.error_message = str(exc)
                    db.session.commit()
        ingestion_state["last_run_finished_at"] = finished_at.isoformat()
        ingestion_state["last_status"] = "failed"
        ingestion_state["last_message"] = str(exc)
        ingestion_state["last_counts"] = {"run_id": run_id}
        return False, {"error": str(exc), "run_id": run_id}
    finally:
        ingestion_lock.release()


def auto_ingest_worker(interval_minutes, channel_limits, clear_existing):
    while not stop_auto_ingest_event.is_set():
        run_ingestion_pipeline(
            channel_limits=channel_limits,
            clear_existing=clear_existing,
            triggered_by="scheduler"
        )
        if stop_auto_ingest_event.wait(interval_minutes * 60):
            break


def build_live_risk_alerts(metrics, influence_lookup=None):
    from collections import defaultdict
    import statistics

    issues = {
        "Packaging": "packaging_count",
        "Delivery": "delivery_count",
        "Quality": "quality_count",
        "Pricing": "pricing_count",
        "Support": "support_count",
        "Side Effects": "side_effects_count",
        "Trust": "trust_count"
    }

    channel_weights = {
        "Reddit": 1.30,
        "Twitter": 1.20,
        "Instagram": 1.10,
        "Google": 1.05,
        "Amazon": 1.00,
        "Flipkart": 0.95,
        "Nykaa": 0.90
    }

    if influence_lookup is None:
        influence_lookup = build_influence_lookup()

    grouped = defaultdict(list)
    for m in metrics:
        brand = (m.product_name or "").split(" | ")[0]
        grouped[(brand, m.product_name, m.channel)].append(m)

    alerts = []
    for (brand, product_name, channel), records in grouped.items():
        records.sort(key=lambda x: x.date)
        if len(records) < 5:
            continue

        today = records[-1]
        weight = channel_weights.get(channel, 1.0)
        influence_factor = get_metric_influence(
            influence_lookup,
            brand,
            product_name,
            channel,
            today.date
        )

        for issue_name, column in issues.items():
            today_value = getattr(today, column, 0) or 0
            baseline = records[-8:-1] if len(records) >= 8 else records[:-1]
            baseline_values = [(getattr(r, column, 0) or 0) for r in baseline]
            if len(baseline_values) < 3:
                continue

            mean = statistics.mean(baseline_values)
            std = statistics.stdev(baseline_values) if len(baseline_values) > 1 else 0
            time_signals = _build_issue_time_signals(records, column)
            persistence_days = _count_consecutive_anomaly_days(time_signals)
            persistence_mult = _persistence_multiplier(persistence_days)
            recovery_initiated, prior_run_days = _is_recovery_initiated(time_signals)

            z_score = None
            risk_score = None
            spike_type = None

            if std > 0:
                z = (today_value - mean) / std
                if z > 2:
                    z_score = round(z, 2)
                    risk_score = round(min(100, z * 20 * weight * influence_factor * persistence_mult), 1)
                    spike_type = "Statistical"

            last_two = records[-3:-1]
            if len(last_two) == 2 and not risk_score:
                avg_last_two = sum((getattr(r, column, 0) or 0) for r in last_two) / 2
                if avg_last_two > 0:
                    growth = today_value / avg_last_two
                    if growth >= 2.5:
                        z_score = round(growth, 2)
                        risk_score = round(min(100, growth * 18 * weight * influence_factor * persistence_mult), 1)
                        spike_type = "Velocity"

            if not risk_score and recovery_initiated:
                growth_value = time_signals[-1]["growth"] or 1.0
                recovery_progress = max(0.0, (1.0 - growth_value))
                recovery_base = (35 + (prior_run_days * 4)) * weight * influence_factor
                risk_score = round(max(20, min(60, recovery_base * (0.7 - min(0.3, recovery_progress)))), 1)
                z_score = round(growth_value, 2)
                spike_type = "Recovery Initiated"

            if not risk_score:
                continue

            if spike_type == "Recovery Initiated":
                level = "Recovery"
            elif risk_score >= 75:
                level = "Critical"
            elif risk_score >= 60:
                level = "High"
            elif risk_score >= 45:
                level = "Moderate"
            elif risk_score >= 30:
                level = "Watch"
            else:
                continue

            alerts.append({
                "brand_name": brand,
                "product_name": product_name,
                "issue_type": issue_name,
                "channel": channel,
                "spike_type": spike_type,
                "z_score": z_score,
                "risk_score": risk_score,
                "alert_level": level,
                "influence_factor": influence_factor,
                "persistence_days": persistence_days
            })

    alerts.sort(key=lambda a: a["risk_score"], reverse=True)
    return alerts


def build_daily_brief_text(metrics, live_alerts):
    from collections import defaultdict

    if not metrics:
        now_label = datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
        return (
            f"Daily AI Brief ({now_label})\n"
            "No aggregated metrics are available yet.\n"
            "Next Step: Run ingestion and aggregation to generate the executive brief."
        )

    brand_scores = defaultdict(list)
    for m in metrics:
        brand = (m.product_name or "Unknown").split(" | ")[0]
        brand_scores[brand].append(m.negative_percentage)

    health_scores = {}
    for brand, values in brand_scores.items():
        clean_values = [v for v in values if v is not None]
        if not clean_values:
            continue
        avg_negative = sum(clean_values) / len(clean_values)
        health_scores[brand] = round(max(0, 100 - avg_negative), 2)

    issue_totals = {issue: 0 for issue in ISSUE_TAXONOMY}
    for m in metrics:
        for issue, column in ISSUE_COLUMN_MAP.items():
            issue_totals[issue] += getattr(m, column, 0) or 0

    channel_negative = defaultdict(int)
    for m in metrics:
        channel_negative[m.channel or "Unknown"] += (m.negative_count or 0)

    top_issues = [
        issue for issue, total in sorted(issue_totals.items(), key=lambda x: x[1], reverse=True)
        if total > 0
    ][:3]
    issue_leader = top_issues[0] if top_issues else "General"
    issues_line = ", ".join(top_issues) if top_issues else "no material issue spikes"

    sorted_health = sorted(health_scores.items(), key=lambda x: x[1], reverse=True)
    top_health = sorted_health[:3]
    if top_health:
        leader = top_health[0][0]
        health_line = ", ".join(f"{name} {score}/100" for name, score in top_health)
    else:
        leader = "No clear leader"
        health_line = "insufficient benchmark data"

    weakest = sorted_health[-1] if sorted_health else None
    weakest_line = f"{weakest[0]} ({weakest[1]}/100)" if weakest else "N/A"

    critical_count = sum(1 for a in live_alerts if a["alert_level"] == "Critical")
    high_count = sum(1 for a in live_alerts if a["alert_level"] == "High")
    watch_count = sum(1 for a in live_alerts if a["alert_level"] in {"Watch", "Moderate", "Recovery"})

    top_channel = max(channel_negative.items(), key=lambda item: item[1])[0] if channel_negative else "N/A"
    total_mentions = sum(m.total_mentions or 0 for m in metrics)
    total_negative = sum(m.negative_count or 0 for m in metrics)
    avg_negative_rate = round((total_negative / total_mentions) * 100, 2) if total_mentions else 0.0

    deduped_alerts = []
    seen_alert_keys = set()
    for alert in sorted(live_alerts, key=lambda a: a.get("risk_score", 0), reverse=True):
        key = (
            alert.get("brand_name"),
            alert.get("issue_type"),
            alert.get("channel"),
            alert.get("alert_level")
        )
        if key in seen_alert_keys:
            continue
        seen_alert_keys.add(key)
        deduped_alerts.append(alert)
        if len(deduped_alerts) >= 2:
            break

    top_alerts = deduped_alerts
    top_alert_lines = []
    for alert in top_alerts:
        top_alert_lines.append(
            f"- {alert.get('brand_name', 'Unknown')} | {alert.get('issue_type', 'Issue')} | "
            f"{alert.get('channel', 'Unknown')} | Risk {round(alert.get('risk_score', 0), 1)} ({alert.get('alert_level', 'Watch')})"
        )
    if not top_alert_lines:
        top_alert_lines.append("- No immediate high-risk spikes detected.")

    now_label = datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
    return "\n".join([
        f"Daily AI Brief ({now_label})",
        f"Overall: {leader} is currently strongest on sentiment stability; watchlist priority is {weakest_line}.",
        f"Health Snapshot: {health_line}.",
        f"Risk Posture: Critical {critical_count} | High {high_count} | Watch/Moderate/Recovery {watch_count}.",
        f"Issue Concentration: {issues_line} (primary driver: {issue_leader}).",
        f"Channel Pressure: Highest negative load is on {top_channel}; blended negative rate is {avg_negative_rate}%.",
        "Top Escalations:",
        *top_alert_lines,
        f"Action Plan (24h): Assign owner for {issue_leader}, execute channel-level containment on {top_channel}, and review top 2 escalation threads with RCA closure."
    ])


@app.route("/login", methods=["GET", "POST"])
def login():
    next_url = _get_next_url(default_path="/")
    if request.method == "POST":
        if not _verify_csrf():
            return render_template("login.html", error="Session token invalid. Refresh and try again.", next_url=next_url), 400

        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        user = User.query.filter_by(email=email).first()

        if not user or not user.is_active or not check_password_hash(user.password_hash, password):
            if user and not user.is_active:
                error = "Account is disabled. Contact your SuperAdmin."
            else:
                error = "Invalid email or password."
            return render_template("login.html", error=error, next_url=next_url), 401

        session["user_id"] = user.id
        session["user_email"] = user.email
        session["user_role"] = user.role
        session.permanent = True

        user.last_login_at = datetime.utcnow()
        db.session.commit()
        _audit("login_success", target_email=user.email, metadata={"role": user.role}, actor_email=user.email)

        if user.force_password_reset:
            return redirect(url_for("password_reset"))
        role_default = "/" if user.role == ROLE_CX_HEAD else "/dashboard/all"
        next_url = _get_next_url(default_path=role_default)
        return redirect(next_url)

    return render_template("login.html", error=None, next_url=next_url)


@app.route("/logout")
def logout():
    actor = session.get("user_email")
    session.clear()
    if actor:
        with app.app_context():
            _audit("logout", target_email=actor, metadata={}, actor_email=actor)
    return redirect(url_for("login"))


@app.route("/password/reset", methods=["GET", "POST"])
def password_reset():
    user = _current_user()
    if not user:
        return redirect(url_for("login", next="/password/reset"))

    if request.method == "POST":
        if not _verify_csrf():
            return render_template("password_reset.html", error="Session token invalid. Refresh and try again."), 400

        current_password = request.form.get("current_password") or ""
        new_password = request.form.get("new_password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        if not check_password_hash(user.password_hash, current_password):
            return render_template("password_reset.html", error="Current password is incorrect."), 400
        if len(new_password) < 10:
            return render_template("password_reset.html", error="New password must be at least 10 characters."), 400
        if new_password != confirm_password:
            return render_template("password_reset.html", error="New password and confirmation do not match."), 400

        user.password_hash = generate_password_hash(new_password)
        user.force_password_reset = False
        db.session.commit()
        _audit("password_reset", target_email=user.email, metadata={}, actor_email=user.email)
        if user.role == ROLE_CX_HEAD:
            return redirect(url_for("home_page"))
        return redirect(url_for("dashboard"))

    return render_template("password_reset.html", error=None)


@app.route("/admin/users", methods=["GET", "POST"])
def admin_users():
    current_user = _current_user()
    if not current_user or current_user.role != ROLE_SUPERADMIN:
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403

    if request.method == "POST":
        if not _verify_csrf():
            return render_template("error.html", code=400, message="Invalid session token."), 400

        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        role = (request.form.get("role") or ROLE_CX_HEAD).strip().lower()

        if role != ROLE_CX_HEAD:
            return render_template("error.html", code=400, message="Only CX Head accounts can be created from this page."), 400
        if not email:
            return render_template("error.html", code=400, message="Email is required."), 400
        if len(password) < 10:
            return render_template("error.html", code=400, message="Temporary password must be at least 10 characters."), 400
        if User.query.filter_by(email=email).first():
            return render_template("error.html", code=409, message="A user with this email already exists."), 409

        user = User(
            email=email,
            password_hash=generate_password_hash(password),
            role=ROLE_CX_HEAD,
            is_active=True,
            force_password_reset=True
        )
        db.session.add(user)
        db.session.commit()
        _audit("user_created", target_email=email, metadata={"role": ROLE_CX_HEAD}, actor_email=current_user.email)
        return redirect(url_for("admin_users"))

    users = User.query.order_by(User.created_at.desc()).all()
    return render_template("admin_users.html", users=users, roles=sorted(ALLOWED_ROLES))


@app.route("/admin/users/<int:user_id>/toggle", methods=["POST"])
def admin_toggle_user(user_id):
    current_user = _current_user()
    if not current_user or current_user.role != ROLE_SUPERADMIN:
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403
    if not _verify_csrf():
        return render_template("error.html", code=400, message="Invalid session token."), 400

    user = User.query.get_or_404(user_id)
    if user.role == ROLE_SUPERADMIN and user.id == current_user.id and user.is_active:
        return render_template("error.html", code=400, message="You cannot disable your own superadmin account."), 400

    user.is_active = not user.is_active
    db.session.commit()
    _audit(
        "user_status_toggled",
        target_email=user.email,
        metadata={"is_active": user.is_active, "role": user.role},
        actor_email=current_user.email
    )
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/reset-password", methods=["POST"])
def admin_reset_user_password(user_id):
    current_user = _current_user()
    if not current_user or current_user.role != ROLE_SUPERADMIN:
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403
    if not _verify_csrf():
        return render_template("error.html", code=400, message="Invalid session token."), 400

    new_password = request.form.get("new_password") or ""
    if len(new_password) < 10:
        return render_template("error.html", code=400, message="Temporary password must be at least 10 characters."), 400

    user = User.query.get_or_404(user_id)
    user.password_hash = generate_password_hash(new_password)
    user.force_password_reset = True
    user.is_active = True
    db.session.commit()
    _audit(
        "user_password_reset_by_admin",
        target_email=user.email,
        metadata={"force_password_reset": True},
        actor_email=current_user.email
    )
    return redirect(url_for("admin_users"))


@app.route("/admin/operations")
def admin_operations():
    current_user = _current_user()
    if not current_user or current_user.role != ROLE_SUPERADMIN:
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403

    history_rows = get_ingestion_history(limit=40)
    health_snapshot = get_ingestion_health_snapshot()
    return render_template(
        "admin_operations.html",
        ingestion_state=ingestion_state,
        history_rows=history_rows,
        health_snapshot=health_snapshot,
        scheduler_snapshot=get_scheduler_snapshot()
    )


@app.route("/admin/operations/run", methods=["POST"])
def admin_operations_run():
    current_user = _current_user()
    if not current_user or current_user.role != ROLE_SUPERADMIN:
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403
    if not _verify_csrf():
        return render_template("error.html", code=400, message="Invalid session token."), 400

    source = request.form
    preset_name, channel_limits, clear_existing = resolve_ingestion_limits_from_preset(source.get("preset"))

    threading.Thread(
        target=run_ingestion_pipeline,
        kwargs={
            "channel_limits": channel_limits,
            "clear_existing": clear_existing,
            "triggered_by": f"admin:{current_user.email}:{preset_name}"
        },
        daemon=True
    ).start()
    ingestion_state["last_message"] = "Ingestion queued from Admin Operations."
    _audit(
        "admin_ingestion_triggered_async",
        target_email=current_user.email,
        metadata={"queued": True, "clear_existing": clear_existing, "preset": preset_name},
        actor_email=current_user.email
    )
    return redirect(url_for("admin_operations"))


@app.route("/admin/operations/auto/start", methods=["POST"])
def admin_operations_auto_start():
    current_user = _current_user()
    if not current_user or current_user.role != ROLE_SUPERADMIN:
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403
    if not _verify_csrf():
        return render_template("error.html", code=400, message="Invalid session token."), 400

    interval_hours_raw = request.form.get("interval_hours", "6")
    try:
        interval_hours = max(1, int(interval_hours_raw))
    except ValueError:
        interval_hours = 6
    interval_minutes = max(5, interval_hours * 60)
    preset_name, channel_limits, clear_existing = resolve_ingestion_limits_from_preset(request.form.get("preset"))

    started, message = _start_auto_ingestion(
        interval_minutes=interval_minutes,
        channel_limits=channel_limits,
        clear_existing=clear_existing,
        preset_name=preset_name
    )
    _audit(
        "admin_auto_ingestion_start",
        target_email=current_user.email,
        metadata={
            "started": started,
            "interval_minutes": interval_minutes,
            "preset": preset_name,
            "message": message
        },
        actor_email=current_user.email
    )
    return redirect(url_for("admin_operations"))


@app.route("/admin/operations/auto/stop", methods=["POST"])
def admin_operations_auto_stop():
    current_user = _current_user()
    if not current_user or current_user.role != ROLE_SUPERADMIN:
        return render_template("error.html", code=403, message="This section is restricted to SuperAdmin users."), 403
    if not _verify_csrf():
        return render_template("error.html", code=400, message="Invalid session token."), 400

    _stop_auto_ingestion()
    _audit(
        "admin_auto_ingestion_stop",
        target_email=current_user.email,
        metadata={"message": ingestion_state["last_message"]},
        actor_email=current_user.email
    )
    return redirect(url_for("admin_operations"))


@app.route("/")
def home_page():
    return render_template("home.html")


def _render_dashboard(view_mode="all"):
    from models import DailyMetric, Review
    from collections import defaultdict

    requested_limit = request.args.get("top_negative", default=5, type=int)
    top_negative_limit = min(10, max(1, requested_limit))

    metrics = DailyMetric.query.order_by(DailyMetric.date).all()
    influence_lookup = build_influence_lookup()

    brand_daily_neg = defaultdict(lambda: defaultdict(list))
    for m in metrics:
        brand = m.product_name.split(" | ")[0]
        brand_daily_neg[brand][m.date].append(m.negative_percentage or 0)

    health_scores = {}
    brand_cards = []
    for brand, date_map in brand_daily_neg.items():
        sorted_days = sorted(date_map.keys())
        daily_neg_series = [
            (sum(date_map[d]) / len(date_map[d])) if date_map[d] else 0
            for d in sorted_days
        ]

        avg_negative = (sum(daily_neg_series) / len(daily_neg_series)) if daily_neg_series else 0
        score = round(max(0, 100 - avg_negative), 2)
        health_scores[brand] = score

        recent_slice = daily_neg_series[-3:]
        previous_slice = daily_neg_series[-6:-3]

        if previous_slice and recent_slice:
            recent_avg = sum(recent_slice) / len(recent_slice)
            previous_avg = sum(previous_slice) / len(previous_slice)
            trend_delta = round(recent_avg - previous_avg, 2)
        elif len(daily_neg_series) >= 2:
            trend_delta = round(daily_neg_series[-1] - daily_neg_series[-2], 2)
        else:
            trend_delta = 0.0

        if score >= 85:
            health_band = "Strong"
            health_class = "strong"
        elif score >= 70:
            health_band = "Watch"
            health_class = "watch"
        else:
            health_band = "At Risk"
            health_class = "risk"

        brand_cards.append({
            "name": brand,
            "score": score,
            "trend_delta": trend_delta,
            "health_band": health_band,
            "health_class": health_class
        })

    brand_cards.sort(key=lambda card: card["score"], reverse=True)
    leader = brand_cards[0]["name"] if brand_cards else "N/A"

    primary_brand_candidates = []
    env_primary_brand = os.getenv("PRIMARY_BRAND", "").strip()
    if env_primary_brand:
        primary_brand_candidates.append(env_primary_brand)
    primary_brand_candidates.extend(["AuraWell Labs", "OurBrand"])

    brand_names_present = {card["name"] for card in brand_cards}
    primary_brand_name = next((b for b in primary_brand_candidates if b in brand_names_present), None)
    if not primary_brand_name and brand_cards:
        primary_brand_name = brand_cards[0]["name"]

    primary_brand_card = next((c for c in brand_cards if c["name"] == primary_brand_name), None)
    competitor_brand_cards = [c for c in brand_cards if c["name"] != primary_brand_name]

    def _dashboard_level_from_risk(risk_score, spike_type):
        if spike_type == "Recovery":
            return "Recovery"
        if risk_score >= 80:
            return "Critical"
        if risk_score >= 60:
            return "High"
        if risk_score >= 40:
            return "Moderate"
        if risk_score >= 30:
            return "Watch"
        return "OK"

    def _compute_main_detect_alerts(target_brand):
        if not target_brand:
            return []

        from collections import defaultdict
        import statistics

        channel_weights = {
            "Reddit": 1.30,
            "Twitter": 1.20,
            "Instagram": 1.10,
            "Google": 1.05,
            "Amazon": 1.00,
            "Flipkart": 0.95,
            "Nykaa": 0.90
        }

        grouped_data = defaultdict(list)
        for m in metrics:
            brand = (m.product_name or "").split(" | ")[0]
            if brand == target_brand:
                grouped_data[(m.product_name, m.channel)].append(m)

        structured_alerts = []
        issue_channel_map = defaultdict(set)

        for (product, channel), records in grouped_data.items():
            records.sort(key=lambda x: x.date)
            if len(records) < 5:
                continue

            today = records[-1]
            weight = channel_weights.get(channel, 1.0)
            brand_name = (product or "").split(" | ")[0]
            influence_factor = get_metric_influence(influence_lookup, brand_name, product, channel, today.date)

            for issue_name, column in ISSUE_COLUMN_MAP.items():
                today_value = getattr(today, column, 0) or 0
                baseline = records[-8:-1] if len(records) >= 8 else records[:-1]
                baseline_values = [(getattr(r, column, 0) or 0) for r in baseline]
                if len(baseline_values) < 3:
                    continue

                mean = statistics.mean(baseline_values)
                std = statistics.stdev(baseline_values) if len(baseline_values) > 1 else 0
                time_signals = _build_issue_time_signals(records, column)
                persistence_days = _count_consecutive_anomaly_days(time_signals)
                persistence_mult = _persistence_multiplier(persistence_days)
                recovery_initiated, prior_run_days = _is_recovery_initiated(time_signals)

                if std > 0:
                    z = (today_value - mean) / std
                    if z > 2:
                        multiplier = 22 if z >= 3 else 19 if z >= 2.5 else 17
                        risk = round(min(100, z * multiplier * weight * influence_factor * persistence_mult), 1)
                        structured_alerts.append({
                            "brand_name": brand_name,
                            "product_name": product,
                            "issue_type": issue_name,
                            "channel": channel,
                            "spike_type": "Statistical",
                            "z_score": round(z, 2),
                            "risk_score": risk,
                            "influence_factor": influence_factor
                        })
                        issue_channel_map[issue_name].add(channel)

                last_two = records[-3:-1]
                if len(last_two) == 2:
                    avg_last_two = sum((getattr(r, column, 0) or 0) for r in last_two) / 2
                    if avg_last_two > 0:
                        growth = today_value / avg_last_two
                        if growth >= 2.5:
                            risk = round(min(100, growth * 18 * weight * influence_factor * persistence_mult), 1)
                            structured_alerts.append({
                                "brand_name": brand_name,
                                "product_name": product,
                                "issue_type": issue_name,
                                "channel": channel,
                                "spike_type": "Velocity",
                                "z_score": round(growth, 2),
                                "risk_score": risk,
                                "influence_factor": influence_factor
                            })
                            issue_channel_map[issue_name].add(channel)

                has_issue_alert = any(
                    a["product_name"] == product and a["channel"] == channel and a["issue_type"] == issue_name
                    for a in structured_alerts
                )
                if not has_issue_alert and recovery_initiated:
                    growth_value = time_signals[-1]["growth"] or 1.0
                    recovery_progress = max(0.0, (1.0 - growth_value))
                    recovery_base = (35 + (prior_run_days * 4)) * weight * influence_factor
                    recovery_risk = round(max(20, min(60, recovery_base * (0.7 - min(0.3, recovery_progress)))), 1)
                    structured_alerts.append({
                        "brand_name": brand_name,
                        "product_name": product,
                        "issue_type": issue_name,
                        "channel": channel,
                        "spike_type": "Recovery",
                        "z_score": round(growth_value, 2),
                        "risk_score": recovery_risk,
                        "influence_factor": influence_factor
                    })
                    issue_channel_map[issue_name].add(channel)

        for alert in structured_alerts:
            channels = issue_channel_map.get(alert["issue_type"], set())
            if len(channels) >= 2:
                alert["channel"] = ", ".join(sorted(channels))

            level = _dashboard_level_from_risk(alert["risk_score"], alert["spike_type"])
            alert["alert_level"] = level

        final_alerts = [a for a in structured_alerts if a["alert_level"] != "OK"]
        final_alerts.sort(key=lambda a: a["risk_score"], reverse=True)
        return final_alerts

    def _compute_competitor_detect_alerts(target_brands):
        if not target_brands:
            return []

        from collections import defaultdict
        import statistics

        channel_weights = {
            "Twitter": 1.2,
            "Instagram": 1.1,
            "Reddit": 1.3,
            "Amazon": 1.0,
            "Nykaa": 0.9,
            "Google": 1.1,
            "Flipkart": 1.0
        }

        grouped_data = defaultdict(list)
        for m in metrics:
            brand = (m.product_name or "").split(" | ")[0]
            if brand in target_brands:
                grouped_data[(m.product_name, m.channel)].append(m)

        product_alerts = []
        cross_channel_map = defaultdict(set)

        for (product, channel), records in grouped_data.items():
            records.sort(key=lambda x: x.date)
            if len(records) < 3:
                continue

            today = records[-1]
            brand = (product or "").split(" | ")[0]
            channel_weight = channel_weights.get(channel, 1.0)
            influence_factor = get_metric_influence(influence_lookup, brand, product, channel, today.date)

            for issue_name, column in ISSUE_COLUMN_MAP.items():
                today_value = getattr(today, column, 0) or 0
                time_signals = _build_issue_time_signals(records, column)
                persistence_days = _count_consecutive_anomaly_days(time_signals)
                persistence_mult = _persistence_multiplier(persistence_days)
                recovery_initiated, prior_run_days = _is_recovery_initiated(time_signals)

                if len(records) >= 7:
                    baseline = records[-8:-1] if len(records) >= 8 else records[:-1]
                    baseline_values = [getattr(r, column, 0) or 0 for r in baseline]
                    if len(baseline_values) > 1:
                        mean = statistics.mean(baseline_values)
                        std = statistics.stdev(baseline_values)
                        if std > 0:
                            z_score = (today_value - mean) / std
                            if z_score > 2:
                                risk_score = min(100, z_score * 20 * channel_weight * influence_factor * persistence_mult)
                                product_alerts.append({
                                    "brand_name": brand,
                                    "product_name": product,
                                    "issue_type": issue_name,
                                    "channel": channel,
                                    "spike_type": "Statistical",
                                    "z_score": round(z_score, 2),
                                    "risk_score": round(risk_score, 1),
                                    "influence_factor": influence_factor
                                })
                                cross_channel_map[(brand, issue_name)].add(channel)

                last_two = records[-3:-1]
                if len(last_two) == 2:
                    avg_last_two = sum((getattr(r, column, 0) or 0) for r in last_two) / 2
                    if avg_last_two > 0:
                        growth_ratio = today_value / avg_last_two
                        if growth_ratio >= 3:
                            risk_score = min(100, growth_ratio * 25 * channel_weight * influence_factor * persistence_mult)
                            product_alerts.append({
                                "brand_name": brand,
                                "product_name": product,
                                "issue_type": issue_name,
                                "channel": channel,
                                "spike_type": "Velocity",
                                "z_score": round(growth_ratio, 2),
                                "risk_score": round(risk_score, 1),
                                "influence_factor": influence_factor
                            })
                            cross_channel_map[(brand, issue_name)].add(channel)

                has_issue_alert = any(
                    a["brand_name"] == brand and a["product_name"] == product and a["channel"] == channel and a["issue_type"] == issue_name
                    for a in product_alerts
                )
                if not has_issue_alert and recovery_initiated:
                    growth_value = time_signals[-1]["growth"] or 1.0
                    recovery_progress = max(0.0, (1.0 - growth_value))
                    recovery_base = (35 + (prior_run_days * 4)) * channel_weight * influence_factor
                    recovery_risk = round(max(20, min(60, recovery_base * (0.7 - min(0.3, recovery_progress)))), 1)
                    product_alerts.append({
                        "brand_name": brand,
                        "product_name": product,
                        "issue_type": issue_name,
                        "channel": channel,
                        "spike_type": "Recovery",
                        "z_score": round(growth_value, 2),
                        "risk_score": recovery_risk,
                        "influence_factor": influence_factor
                    })
                    cross_channel_map[(brand, issue_name)].add(channel)

        for alert in product_alerts:
            channels = cross_channel_map.get((alert["brand_name"], alert["issue_type"]), set())
            if len(channels) >= 2:
                alert["channel"] = ", ".join(sorted(channels))

            level = _dashboard_level_from_risk(alert["risk_score"], alert["spike_type"])
            alert["alert_level"] = level

        final_alerts = [a for a in product_alerts if a["alert_level"] != "OK"]
        final_alerts.sort(key=lambda a: a["risk_score"], reverse=True)
        return final_alerts

    primary_alerts = _compute_main_detect_alerts(primary_brand_name)
    competitor_alerts = _compute_competitor_detect_alerts([c["name"] for c in competitor_brand_cards])
    all_alerts = sorted(primary_alerts + competitor_alerts, key=lambda a: a["risk_score"], reverse=True)
    alerts = primary_alerts[:6] if primary_alerts else all_alerts[:6]

    issue_summary = defaultdict(lambda: {issue: 0 for issue in ISSUE_TAXONOMY})

    for m in metrics:
        brand = m.product_name.split(" | ")[0]
        for issue, column in ISSUE_COLUMN_MAP.items():
            issue_summary[brand][issue] += getattr(m, column, 0) or 0

    issue_rows = []
    for brand, issues in issue_summary.items():
        row = dict(issues)
        row["brand"] = brand
        row["total"] = sum(issues.values())
        issue_rows.append(row)

    issue_rows.sort(key=lambda row: row["total"], reverse=True)

    issue_totals = {
        issue: sum(row[issue] for row in issue_rows)
        for issue in ISSUE_TAXONOMY
    }

    date_values = sorted({m.date for m in metrics if m.date})
    date_labels = [d.strftime("%Y-%m-%d") for d in date_values]

    channel_summary = defaultdict(int)
    reviews = Review.query.all()
    for r in reviews:
        channel_summary[r.channel] += 1

    total_channel_mentions = sum(channel_summary.values())
    channel_cards = []
    for channel, count in channel_summary.items():
        share = round((count / total_channel_mentions) * 100, 1) if total_channel_mentions else 0
        channel_cards.append({
            "name": channel,
            "count": count,
            "share": share
        })
    channel_cards.sort(key=lambda c: c["count"], reverse=True)

    negative_reviews = Review.query.filter(
        (Review.sentiment == "negative") |
        (Review.sentiment_score.isnot(None))
    ).all()

    channel_icons = {
        "Amazon": "\U0001F6D2",
        "Twitter": "\U0001F426",
        "Instagram": "\U0001F4F8",
        "Reddit": "\U0001F4AC",
        "Google": "\U0001F50D",
        "Flipkart": "\U0001F4E6",
        "Nykaa": "\U0001F48B"
    }

    primary_negative_comments_by_channel = defaultdict(list)
    competitor_negative_comments_by_channel = defaultdict(list)
    all_negative_comments_by_channel = defaultdict(list)
    for review in negative_reviews:
        is_negative_label = (review.sentiment or "").lower() == "negative"
        has_negative_score = review.sentiment_score is not None and review.sentiment_score < 0
        if not (is_negative_label or has_negative_score):
            continue

        score = review.sentiment_score if review.sentiment_score is not None else 0
        comment_payload = {
            "text": review.review_text or "",
            "brand": review.brand_name or "Unknown",
            "product": review.product_name or "Unknown",
            "timestamp": review.timestamp,
            "when": review.timestamp.strftime("%d %b %Y") if review.timestamp else "Unknown date",
            "sort_score": score
        }
        all_negative_comments_by_channel[review.channel].append(dict(comment_payload))

        if review.brand_name == primary_brand_name:
            primary_negative_comments_by_channel[review.channel].append(comment_payload)
        else:
            competitor_negative_comments_by_channel[review.channel].append(comment_payload)

    primary_issue_by_date = defaultdict(lambda: {issue: 0 for issue in ISSUE_TAXONOMY})
    primary_issue_by_channel = defaultdict(lambda: {issue: 0 for issue in ISSUE_TAXONOMY})
    primary_channel_daily_neg = defaultdict(lambda: defaultdict(list))
    brand_channel_mentions = defaultdict(lambda: defaultdict(int))
    overall_issue_by_brand_channel = defaultdict(lambda: defaultdict(lambda: {issue: 0 for issue in ISSUE_TAXONOMY}))
    overall_channel_daily_neg = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    competitor_channel_mentions = defaultdict(lambda: defaultdict(int))
    competitor_issue_by_brand_channel = defaultdict(lambda: defaultdict(lambda: {issue: 0 for issue in ISSUE_TAXONOMY}))
    competitor_channel_daily_neg = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for m in metrics:
        metric_brand = (m.product_name or "").split(" | ")[0]
        if not m.date:
            continue

        if metric_brand == primary_brand_name:
            primary_channel_daily_neg[m.channel][m.date].append(m.negative_percentage or 0)
            for issue, column in ISSUE_COLUMN_MAP.items():
                value = getattr(m, column, 0) or 0
                primary_issue_by_date[m.date][issue] += value
                primary_issue_by_channel[m.channel][issue] += value
        else:
            competitor_channel_mentions[metric_brand][m.channel] += (m.total_mentions or 0)
            competitor_channel_daily_neg[metric_brand][m.channel][m.date].append(m.negative_percentage or 0)
            for issue, column in ISSUE_COLUMN_MAP.items():
                competitor_issue_by_brand_channel[metric_brand][m.channel][issue] += (getattr(m, column, 0) or 0)
        overall_channel_daily_neg[metric_brand][m.channel][m.date].append(m.negative_percentage or 0)
        for issue, column in ISSUE_COLUMN_MAP.items():
            overall_issue_by_brand_channel[metric_brand][m.channel][issue] += (getattr(m, column, 0) or 0)
        brand_channel_mentions[metric_brand][m.channel] += (m.total_mentions or 0)

    def _series_for_brand(brand_name):
        date_map = brand_daily_neg.get(brand_name, {})
        series = []
        for d in date_values:
            values = date_map.get(d, [])
            if values:
                series.append(round(sum(values) / len(values), 2))
            else:
                series.append(None)
        return series

    def _rolling_7d_delta(series):
        deltas = []
        for idx, current in enumerate(series):
            if current is None or idx < 7:
                deltas.append(None)
                continue
            window = [v for v in series[idx - 7:idx] if v is not None]
            if not window:
                deltas.append(None)
                continue
            baseline = sum(window) / len(window)
            deltas.append(round(current - baseline, 2))
        return deltas

    color_palette = [
        "#8AAAE5", "#E8A0BF", "#F2C6A0", "#9BC8B4", "#B8A1D9",
        "#9CCDDC", "#BFD8A8", "#E7B3B3", "#D7BFAF", "#AEB8E6"
    ]

    main_negative_datasets = []
    if primary_brand_name:
        primary_series = _series_for_brand(primary_brand_name)
        if any(v is not None for v in primary_series):
            main_negative_datasets.append({
                "label": f"{primary_brand_name} (Overall)",
                "data": primary_series,
                "borderColor": "#8AAAE5",
                "backgroundColor": "rgba(138,170,229,0.24)",
                "tension": 0.3,
                "fill": True
            })

        for idx, channel_name in enumerate(sorted(primary_channel_daily_neg.keys())):
            channel_series = []
            for d in date_values:
                points = primary_channel_daily_neg[channel_name].get(d, [])
                channel_series.append(round(sum(points) / len(points), 2) if points else None)
            if any(v is not None for v in channel_series):
                main_negative_datasets.append({
                    "label": channel_name,
                    "data": channel_series,
                    "borderColor": color_palette[(idx + 1) % len(color_palette)],
                    "backgroundColor": "rgba(138,170,229,0.08)",
                    "tension": 0.28,
                    "fill": False
                })
    else:
        primary_series = []

    channel_labels_main = sorted(primary_issue_by_channel.keys())
    main_issue_channel_datasets = []
    for idx, issue in enumerate(ISSUE_TAXONOMY):
        main_issue_channel_datasets.append({
            "label": issue,
            "data": [primary_issue_by_channel[ch][issue] for ch in channel_labels_main],
            "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc"
        })

    main_channel_negative_labels = sorted(primary_negative_comments_by_channel.keys())
    main_channel_negative_values = [
        len(primary_negative_comments_by_channel[ch]) for ch in main_channel_negative_labels
    ]
    primary_metrics = [
        m for m in metrics
        if ((m.product_name or "").split(" | ")[0] == primary_brand_name)
    ] if primary_brand_name else []
    main_sentiment_distribution = {
        "labels": ["Positive", "Neutral", "Negative"],
        "data": [
            sum((m.positive_count or 0) for m in primary_metrics),
            sum((m.neutral_count or 0) for m in primary_metrics),
            sum((m.negative_count or 0) for m in primary_metrics)
        ]
    }
    main_sentiment_distribution_by_channel = {}
    for channel in sorted(primary_channel_daily_neg.keys()):
        channel_metrics = [m for m in primary_metrics if (m.channel or "Unknown") == channel]
        main_sentiment_distribution_by_channel[channel] = {
            "labels": ["Positive", "Neutral", "Negative"],
            "data": [
                sum((m.positive_count or 0) for m in channel_metrics),
                sum((m.neutral_count or 0) for m in channel_metrics),
                sum((m.negative_count or 0) for m in channel_metrics)
            ]
        }

    main_momentum_by_channel = {}
    for channel_name, date_map in primary_channel_daily_neg.items():
        channel_series = []
        for d in date_values:
            points = date_map.get(d, [])
            channel_series.append(round(sum(points) / len(points), 2) if points else None)
        main_momentum_by_channel[channel_name] = _rolling_7d_delta(channel_series)

    competitor_names = [card["name"] for card in competitor_brand_cards]
    competitor_trend_datasets = []
    for idx, brand in enumerate(competitor_names):
        competitor_series = _series_for_brand(brand)
        if any(v is not None for v in competitor_series):
            competitor_trend_datasets.append({
                "label": brand,
                "data": competitor_series,
                "borderColor": color_palette[idx % len(color_palette)],
                "backgroundColor": f"{color_palette[idx % len(color_palette)]}33",
                "tension": 0.3,
                "fill": False
            })

    competitor_issue_datasets = []
    for idx, brand in enumerate(competitor_names):
        brand_issue_map = issue_summary.get(brand, {issue: 0 for issue in ISSUE_TAXONOMY})
        competitor_issue_datasets.append({
            "label": brand,
            "data": [brand_issue_map[issue] for issue in ISSUE_TAXONOMY],
            "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc",
            "borderColor": color_palette[idx % len(color_palette)]
        })

    competitor_name_set = set(competitor_names)
    competitor_sentiment_totals = {
        brand: [0, 0, 0] for brand in competitor_names
    }
    competitor_sentiment_by_channel = defaultdict(
        lambda: defaultdict(lambda: [0, 0, 0])
    )
    for m in metrics:
        metric_brand = (m.product_name or "").split(" | ")[0]
        if metric_brand not in competitor_name_set:
            continue
        metric_channel = m.channel or "Unknown"
        competitor_sentiment_totals[metric_brand][0] += (m.positive_count or 0)
        competitor_sentiment_totals[metric_brand][1] += (m.neutral_count or 0)
        competitor_sentiment_totals[metric_brand][2] += (m.negative_count or 0)
        competitor_sentiment_by_channel[metric_channel][metric_brand][0] += (m.positive_count or 0)
        competitor_sentiment_by_channel[metric_channel][metric_brand][1] += (m.neutral_count or 0)
        competitor_sentiment_by_channel[metric_channel][metric_brand][2] += (m.negative_count or 0)

    competitor_alerts_by_brand = defaultdict(list)
    competitor_alerts_by_brand_channel = defaultdict(lambda: defaultdict(list))
    for alert in competitor_alerts:
        brand_key = alert.get("brand_name") or "Unknown"
        channel_key = alert.get("channel") or "Unknown"
        competitor_alerts_by_brand[brand_key].append(alert)
        competitor_alerts_by_brand_channel[brand_key][channel_key].append(alert)

    risk_brand_labels = sorted(competitor_alerts_by_brand.keys())
    competitor_avg_risk = []
    competitor_alert_count = []
    for brand in risk_brand_labels:
        risks = [a.get("risk_score") or 0 for a in competitor_alerts_by_brand[brand]]
        competitor_avg_risk.append(round(sum(risks) / len(risks), 2) if risks else 0)
        competitor_alert_count.append(len(risks))

    all_channels = sorted({c["name"] for c in channel_cards})
    competitor_sentiment_datasets = []
    for idx, brand in enumerate(competitor_names):
        competitor_sentiment_datasets.append({
            "label": brand,
            "data": competitor_sentiment_totals.get(brand, [0, 0, 0]),
            "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc",
            "borderColor": color_palette[idx % len(color_palette)]
        })
    competitor_sentiment_by_channel_payload = {}
    for channel in all_channels:
        competitor_sentiment_by_channel_payload[channel] = {
            "labels": ["Positive", "Neutral", "Negative"],
            "datasets": [
                {
                    "label": brand,
                    "data": competitor_sentiment_by_channel[channel][brand],
                    "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc",
                    "borderColor": color_palette[idx % len(color_palette)]
                }
                for idx, brand in enumerate(competitor_names)
            ]
        }

    overall_sentiment_distribution = {
        "labels": ["Positive", "Neutral", "Negative"],
        "data": [
            sum((m.positive_count or 0) for m in metrics),
            sum((m.neutral_count or 0) for m in metrics),
            sum((m.negative_count or 0) for m in metrics)
        ]
    }
    overall_sentiment_distribution_by_channel = {}
    for channel in all_channels:
        channel_metrics = [m for m in metrics if (m.channel or "Unknown") == channel]
        overall_sentiment_distribution_by_channel[channel] = {
            "labels": ["Positive", "Neutral", "Negative"],
            "data": [
                sum((m.positive_count or 0) for m in channel_metrics),
                sum((m.neutral_count or 0) for m in channel_metrics),
                sum((m.negative_count or 0) for m in channel_metrics)
            ]
        }
    competitor_channel_share_datasets = []
    for idx, brand in enumerate(competitor_names):
        totals = competitor_channel_mentions.get(brand, {})
        total_mentions_for_brand = sum(totals.values())
        competitor_channel_share_datasets.append({
            "label": brand,
            "data": [
                round((totals.get(channel, 0) / total_mentions_for_brand) * 100, 2) if total_mentions_for_brand else 0
                for channel in all_channels
            ],
            "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc"
        })

    brand_order = [card["name"] for card in brand_cards]
    overall_negative_trend_datasets = []
    for idx, brand in enumerate(brand_order):
        series = _series_for_brand(brand)
        if any(v is not None for v in series):
            overall_negative_trend_datasets.append({
                "label": brand,
                "data": series,
                "borderColor": color_palette[idx % len(color_palette)],
                "backgroundColor": f"{color_palette[idx % len(color_palette)]}33",
                "tension": 0.3,
                "fill": False
            })

    overall_issue_by_brand_datasets = []
    for idx, issue in enumerate(ISSUE_TAXONOMY):
        overall_issue_by_brand_datasets.append({
            "label": issue,
            "data": [issue_summary.get(brand, {}).get(issue, 0) for brand in brand_order],
            "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc"
        })

    overall_channel_share_datasets = []
    for idx, brand in enumerate(brand_order):
        totals = brand_channel_mentions.get(brand, {})
        total_mentions_for_brand = sum(totals.values())
        overall_channel_share_datasets.append({
            "label": brand,
            "data": [
                round((totals.get(channel, 0) / total_mentions_for_brand) * 100, 2) if total_mentions_for_brand else 0
                for channel in all_channels
            ],
            "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc"
        })

    all_alerts_by_brand = defaultdict(list)
    all_alerts_by_brand_channel = defaultdict(lambda: defaultdict(list))
    for alert in all_alerts:
        brand_key = alert.get("brand_name") or "Unknown"
        channel_key = alert.get("channel") or "Unknown"
        all_alerts_by_brand[brand_key].append(alert)
        all_alerts_by_brand_channel[brand_key][channel_key].append(alert)
    overall_risk_labels = sorted(all_alerts_by_brand.keys())
    overall_avg_risk = []
    overall_alert_count = []
    for brand in overall_risk_labels:
        risks = [a.get("risk_score") or 0 for a in all_alerts_by_brand[brand]]
        overall_avg_risk.append(round(sum(risks) / len(risks), 2) if risks else 0)
        overall_alert_count.append(len(risks))

    overall_trend_by_channel = {}
    for channel in all_channels:
        channel_datasets = []
        for idx, brand in enumerate(brand_order):
            series = []
            for d in date_values:
                points = overall_channel_daily_neg[brand][channel].get(d, [])
                series.append(round(sum(points) / len(points), 2) if points else None)
            if any(v is not None for v in series):
                channel_datasets.append({
                    "label": brand,
                    "data": series,
                    "borderColor": color_palette[idx % len(color_palette)],
                    "backgroundColor": f"{color_palette[idx % len(color_palette)]}33",
                    "tension": 0.3,
                    "fill": False
                })
        overall_trend_by_channel[channel] = channel_datasets

    overall_issue_by_brand_by_channel = {}
    for channel in all_channels:
        channel_datasets = []
        for idx, issue in enumerate(ISSUE_TAXONOMY):
            channel_datasets.append({
                "label": issue,
                "data": [overall_issue_by_brand_channel[brand][channel][issue] for brand in brand_order],
                "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc"
            })
        overall_issue_by_brand_by_channel[channel] = channel_datasets

    overall_risk_overview_by_channel = {}
    for channel in all_channels:
        labels = []
        avg_risk = []
        alert_count = []
        for brand in brand_order:
            risks = [a.get("risk_score") or 0 for a in all_alerts_by_brand_channel[brand].get(channel, [])]
            if risks:
                labels.append(brand)
                avg_risk.append(round(sum(risks) / len(risks), 2))
                alert_count.append(len(risks))
        overall_risk_overview_by_channel[channel] = {
            "labels": labels,
            "avg_risk": avg_risk,
            "alert_count": alert_count
        }

    competitor_trend_by_channel = {}
    for channel in all_channels:
        channel_datasets = []
        for idx, brand in enumerate(competitor_names):
            series = []
            for d in date_values:
                points = competitor_channel_daily_neg[brand][channel].get(d, [])
                series.append(round(sum(points) / len(points), 2) if points else None)
            if any(v is not None for v in series):
                channel_datasets.append({
                    "label": brand,
                    "data": series,
                    "borderColor": color_palette[idx % len(color_palette)],
                    "backgroundColor": f"{color_palette[idx % len(color_palette)]}33",
                    "tension": 0.3,
                    "fill": False
                })
        competitor_trend_by_channel[channel] = channel_datasets

    competitor_issue_grouped_by_channel = {}
    for channel in all_channels:
        channel_datasets = []
        for idx, brand in enumerate(competitor_names):
            issue_map = competitor_issue_by_brand_channel[brand][channel]
            channel_datasets.append({
                "label": brand,
                "data": [issue_map[issue] for issue in ISSUE_TAXONOMY],
                "backgroundColor": f"{color_palette[idx % len(color_palette)]}cc",
                "borderColor": color_palette[idx % len(color_palette)]
            })
        competitor_issue_grouped_by_channel[channel] = channel_datasets

    competitor_risk_overview_by_channel = {}
    for channel in all_channels:
        labels = []
        avg_risk = []
        alert_count = []
        for brand in competitor_names:
            risks = [a.get("risk_score") or 0 for a in competitor_alerts_by_brand_channel[brand].get(channel, [])]
            if risks:
                labels.append(brand)
                avg_risk.append(round(sum(risks) / len(risks), 2))
                alert_count.append(len(risks))
        competitor_risk_overview_by_channel[channel] = {
            "labels": labels,
            "avg_risk": avg_risk,
            "alert_count": alert_count
        }

    chart_data = {
        "main": {
            "sentiment_distribution": main_sentiment_distribution,
            "sentiment_distribution_by_channel": main_sentiment_distribution_by_channel,
            "negative_trend": {
                "labels": date_labels,
                "datasets": main_negative_datasets
            },
            "issue_channel_mix": {
                "labels": channel_labels_main,
                "datasets": main_issue_channel_datasets
            },
            "channel_negative_counts": {
                "labels": main_channel_negative_labels,
                "data": main_channel_negative_values
            },
            "negative_momentum_delta": {
                "labels": date_labels,
                "data": _rolling_7d_delta(primary_series if primary_brand_name else [])
            },
            "negative_momentum_delta_by_channel": {
                "labels": date_labels,
                "series": main_momentum_by_channel
            },
            "channel_filter_options": sorted({
                *channel_labels_main,
                *main_channel_negative_labels,
                *main_momentum_by_channel.keys()
            })
        },
        "competitors": {
            "trend_comparison": {
                "labels": date_labels,
                "datasets": competitor_trend_datasets
            },
            "issue_grouped": {
                "labels": ISSUE_TAXONOMY,
                "datasets": competitor_issue_datasets
            },
            "sentiment_distribution_by_brand": {
                "labels": ["Positive", "Neutral", "Negative"],
                "datasets": competitor_sentiment_datasets
            },
            "sentiment_distribution_by_brand_by_channel": competitor_sentiment_by_channel_payload,
            "issue_radar": {
                "labels": ISSUE_TAXONOMY,
                "datasets": competitor_issue_datasets
            },
            "risk_overview": {
                "labels": risk_brand_labels,
                "avg_risk": competitor_avg_risk,
                "alert_count": competitor_alert_count
            },
            "channel_share": {
                "labels": all_channels,
                "datasets": competitor_channel_share_datasets
            },
            "trend_by_channel": {
                "labels": date_labels,
                "datasets_by_channel": competitor_trend_by_channel
            },
            "issue_grouped_by_channel": {
                "labels": ISSUE_TAXONOMY,
                "datasets_by_channel": competitor_issue_grouped_by_channel
            },
            "risk_overview_by_channel": competitor_risk_overview_by_channel,
            "channel_filter_options": all_channels
        },
        "overall": {
            "sentiment_distribution": overall_sentiment_distribution,
            "sentiment_distribution_by_channel": overall_sentiment_distribution_by_channel,
            "negative_trend": {
                "labels": date_labels,
                "datasets": overall_negative_trend_datasets
            },
            "negative_trend_by_channel": {
                "labels": date_labels,
                "datasets_by_channel": overall_trend_by_channel
            },
            "issue_by_brand": {
                "labels": brand_order,
                "datasets": overall_issue_by_brand_datasets
            },
            "issue_by_brand_by_channel": {
                "labels": brand_order,
                "datasets_by_channel": overall_issue_by_brand_by_channel
            },
            "channel_share_by_brand": {
                "labels": all_channels,
                "datasets": overall_channel_share_datasets
            },
            "risk_overview": {
                "labels": overall_risk_labels,
                "avg_risk": overall_avg_risk,
                "alert_count": overall_alert_count
            },
            "risk_overview_by_channel": overall_risk_overview_by_channel,
            "channel_filter_options": all_channels
        }
    }

    def build_comment_sections(comment_map):
        top_negative_comments = {}
        for channel, items in comment_map.items():
            items.sort(key=lambda item: (item["sort_score"], item["timestamp"] or datetime.min))
            for item in items:
                item.pop("sort_score", None)
            top_negative_comments[channel] = items[:top_negative_limit]

        sections = []
        for card in channel_cards:
            channel_name = card["name"]
            comments = top_negative_comments.get(channel_name, [])
            if comments:
                sections.append({
                    "name": channel_name,
                    "icon": channel_icons.get(channel_name, "\U0001F4DD"),
                    "comments": comments
                })
        return sections

    primary_top_negative_sections = build_comment_sections(primary_negative_comments_by_channel)
    competitor_top_negative_sections = build_comment_sections(competitor_negative_comments_by_channel)
    overall_top_negative_sections = build_comment_sections(all_negative_comments_by_channel)

    total_mentions = sum(m.total_mentions for m in metrics)
    avg_negative_rate = round(
        (sum(m.negative_count for m in metrics) / total_mentions) * 100, 2
    ) if total_mentions else 0

    critical_alerts = sum(1 for a in all_alerts if a["alert_level"] == "Critical")
    high_alerts = sum(1 for a in all_alerts if a["alert_level"] == "High")
    watch_alerts = sum(1 for a in all_alerts if a["alert_level"] in {"Watch", "Moderate", "Recovery"})
    active_brands = len(brand_cards)

    if view_mode == "main":
        scoped_alerts = primary_alerts
        alert_scope_label = f"{primary_brand_name or 'Main Brand'} Alerts"
        scope_brands = {primary_brand_name} if primary_brand_name else set()
    elif view_mode == "competitors":
        scoped_alerts = competitor_alerts
        alert_scope_label = "Competitor Alerts"
        scope_brands = {c["name"] for c in competitor_brand_cards}
    else:
        scoped_alerts = all_alerts
        alert_scope_label = "All Alerts"
        scope_brands = {c["name"] for c in brand_cards}

    scoped_metrics = [
        m for m in metrics
        if ((m.product_name or "").split(" | ")[0] in scope_brands)
    ] if scope_brands else []

    if view_mode == "all":
        scoped_total_mentions = total_mentions
        scoped_avg_negative_rate = avg_negative_rate
        scoped_active_brands = active_brands
    else:
        scoped_total_mentions = sum((m.total_mentions or 0) for m in scoped_metrics)
        scoped_negative_mentions = sum((m.negative_count or 0) for m in scoped_metrics)
        scoped_avg_negative_rate = round(
            (scoped_negative_mentions / scoped_total_mentions) * 100, 2
        ) if scoped_total_mentions else 0
        scoped_active_brands = len(scope_brands)

    scoped_critical_alerts = sum(1 for a in scoped_alerts if a["alert_level"] == "Critical")
    scoped_high_alerts = sum(1 for a in scoped_alerts if a["alert_level"] == "High")
    scoped_watch_alerts = sum(1 for a in scoped_alerts if a["alert_level"] in {"Watch", "Moderate", "Recovery"})

    brief = build_daily_brief_text(metrics, all_alerts)

    return render_template(
        "dashboard.html",
        view_mode=view_mode,
        health_scores=health_scores,
        brand_cards=brand_cards,
        primary_brand_name=primary_brand_name,
        primary_brand_card=primary_brand_card,
        competitor_brand_cards=competitor_brand_cards,
        alerts=alerts,
        primary_alerts=primary_alerts[:6],
        competitor_alerts=competitor_alerts[:6],
        overall_alerts=all_alerts[:6],
        brief=brief,
        issue_rows=issue_rows,
        issue_totals=issue_totals,
        issue_taxonomy=ISSUE_TAXONOMY,
        channel_cards=channel_cards,
        primary_top_negative_comments=primary_top_negative_sections,
        competitor_top_negative_comments=competitor_top_negative_sections,
        overall_top_negative_comments=overall_top_negative_sections,
        top_negative_limit=top_negative_limit,
        total_mentions=scoped_total_mentions,
        avg_negative_rate=scoped_avg_negative_rate,
        critical_alerts=critical_alerts,
        high_alerts=high_alerts,
        watch_alerts=watch_alerts,
        scoped_critical_alerts=scoped_critical_alerts,
        scoped_high_alerts=scoped_high_alerts,
        scoped_watch_alerts=scoped_watch_alerts,
        alert_scope_label=alert_scope_label,
        active_brands=scoped_active_brands,
        chart_data=chart_data
    )


@app.route("/dashboard")
@app.route("/dashboard/all")
def dashboard():
    return _render_dashboard("all")


@app.route("/dashboard/main")
def dashboard_main():
    return _render_dashboard("main")


@app.route("/dashboard/competitors")
def dashboard_competitors():
    return _render_dashboard("competitors")


@app.route("/brief")
def brief_page():
    metrics = DailyMetric.query.order_by(DailyMetric.date).all()
    influence_lookup = build_influence_lookup()
    live_alerts = build_live_risk_alerts(metrics, influence_lookup=influence_lookup)
    brief = build_daily_brief_text(metrics, live_alerts)
    return render_template("daily_brief.html", brief=brief)


@app.route("/aggregate")
def aggregate_daily_metrics():
    inserted = rebuild_daily_metrics()
    return f"Channel-level aggregation complete! Inserted {inserted} metric rows."


@app.route("/ingest/run", methods=["GET", "POST"])
def ingest_run():
    if request.method == "POST":
        if not _verify_csrf():
            return jsonify({"ok": False, "error": "invalid_csrf"}), 400
        source = request.form
        clear_existing = (source.get("clear_existing") or "true").lower() != "false"
        triggered_by = "manual_admin_ui"
    else:
        source = request.args
        clear_existing = source.get("clear_existing", default="true").lower() != "false"
        triggered_by = "manual_api"

    channel_limits = normalize_channel_limits({
        "amazon": source.get("amazon_limit", DEFAULT_CHANNEL_LIMITS["amazon"]),
        "twitter": source.get("twitter_limit", DEFAULT_CHANNEL_LIMITS["twitter"]),
        "reddit_posts": source.get("reddit_post_limit", DEFAULT_CHANNEL_LIMITS["reddit_posts"]),
        "reddit_comments": source.get("reddit_comment_limit", DEFAULT_CHANNEL_LIMITS["reddit_comments"]),
        "reddit_simulated": source.get("reddit_simulated_limit", DEFAULT_CHANNEL_LIMITS["reddit_simulated"]),
        "instagram": source.get("instagram_limit", DEFAULT_CHANNEL_LIMITS["instagram"]),
        "nykaa": source.get("nykaa_limit", DEFAULT_CHANNEL_LIMITS["nykaa"]),
        "google": source.get("google_limit", DEFAULT_CHANNEL_LIMITS["google"]),
        "flipkart": source.get("flipkart_limit", DEFAULT_CHANNEL_LIMITS["flipkart"])
    })

    ok, payload = run_ingestion_pipeline(
        channel_limits=channel_limits,
        clear_existing=clear_existing,
        triggered_by=triggered_by
    )
    status_code = 200 if ok else 500
    return jsonify({
        "ok": ok,
        "clear_existing": clear_existing,
        "health": get_ingestion_health_snapshot(),
        "state": ingestion_state,
        "result": payload
    }), status_code


@app.route("/ingest/status")
def ingest_status():
    return jsonify({
        "state": ingestion_state,
        "health": get_ingestion_health_snapshot(),
        "scheduler": get_scheduler_snapshot(),
        "recent_runs": get_ingestion_history(limit=10)
    })


@app.route("/ingest/history")
def ingest_history():
    limit = request.args.get("limit", default=25, type=int) or 25
    return jsonify({
        "ok": True,
        "history": get_ingestion_history(limit=limit)
    })


@app.route("/ingest/auto/start")
def ingest_auto_start():
    interval_minutes = parse_int("minutes", 360)
    if interval_minutes < 5:
        interval_minutes = 5

    preset_name, channel_limits, clear_existing = resolve_ingestion_limits_from_preset(request.args.get("preset"))
    limit_query_map = {
        "amazon": "amazon_limit",
        "twitter": "twitter_limit",
        "reddit_posts": "reddit_post_limit",
        "reddit_comments": "reddit_comment_limit",
        "reddit_simulated": "reddit_simulated_limit",
        "instagram": "instagram_limit",
        "nykaa": "nykaa_limit",
        "google": "google_limit",
        "flipkart": "flipkart_limit"
    }
    explicit_overrides = {}
    for channel_key, query_key in limit_query_map.items():
        raw_value = request.args.get(query_key)
        if raw_value is None:
            continue
        try:
            explicit_overrides[channel_key] = max(1, int(raw_value))
        except ValueError:
            continue
    if explicit_overrides:
        _, channel_limits, clear_existing = resolve_ingestion_limits_from_preset(preset_name, explicit_overrides)

    started, message = _start_auto_ingestion(
        interval_minutes=interval_minutes,
        channel_limits=channel_limits,
        clear_existing=clear_existing,
        preset_name=preset_name
    )
    return jsonify({
        "ok": True,
        "already_running": not started,
        "preset": preset_name,
        "message": message,
        "health": get_ingestion_health_snapshot(),
        "state": ingestion_state
    })


@app.route("/ingest/auto/stop")
def ingest_auto_stop():
    _stop_auto_ingestion()
    return jsonify({
        "ok": True,
        "message": ingestion_state["last_message"],
        "health": get_ingestion_health_snapshot(),
        "state": ingestion_state
    })


@app.route("/evaluate/classifier")
def evaluate_classifier():
    from utils.issue_classifier import classify_issue

    benchmark = [
        {"text": "Order #ORD12345 delayed by courier in Mumbai", "issue": "Delivery"},
        {"text": "Product came with damaged box and broken seal", "issue": "Packaging"},
        {"text": "Customer support is not responding for refund request", "issue": "Support"},
        {"text": "Too expensive and not value for money", "issue": "Pricing"},
        {"text": "Developed rash and irritation after using this", "issue": "Quality"},
        {"text": "Tracking shows shipped but still not arrived", "issue": "Delivery"},
        {"text": "Bottle cap leaked and wrapper was torn", "issue": "Packaging"},
        {"text": "Formula changed and quality is poor now", "issue": "Quality"},
        {"text": "Return pickup failed twice, support is useless", "issue": "Support"},
        {"text": "Price has increased too much lately", "issue": "Pricing"}
    ]

    correct = 0
    results = []
    for case in benchmark:
        pred = classify_issue(case["text"])
        ok = pred == case["issue"]
        if ok:
            correct += 1
        results.append({
            "text": case["text"],
            "expected": case["issue"],
            "predicted": pred,
            "match": ok
        })

    accuracy = round((correct / len(benchmark)) * 100, 2)
    return jsonify({
        "benchmark_size": len(benchmark),
        "accuracy_percent": accuracy,
        "target_percent": 85,
        "meets_target": accuracy >= 85,
        "results": results
    })


@app.route("/evaluate/sentiment")
def evaluate_sentiment():
    benchmark = [
        # English - direct tone
        {"text": "Product quality is amazing and delivery was fast", "label": "positive", "language_style": "english", "tone": "direct"},
        {"text": "Worst packaging ever, totally disappointed", "label": "negative", "language_style": "english", "tone": "direct"},
        {"text": "The product is okay, nothing special", "label": "neutral", "language_style": "english", "tone": "direct"},
        {"text": "Customer service was great and helpful", "label": "positive", "language_style": "english", "tone": "direct"},
        {"text": "Overpriced and not worth the money", "label": "negative", "language_style": "english", "tone": "direct"},
        {"text": "Not bad overall", "label": "neutral", "language_style": "english", "tone": "nuanced"},

        # Hinglish / Roman Hindi
        {"text": "Bahut accha product, mast results", "label": "positive", "language_style": "hinglish", "tone": "direct"},
        {"text": "Ye bekaar hai, bilkul pasand nahi aaya", "label": "negative", "language_style": "hinglish", "tone": "direct"},
        {"text": "Service sahi thi but delivery late tha", "label": "neutral", "language_style": "hinglish", "tone": "mixed"},
        {"text": "Quality ghatiya nikli, pura dhokha", "label": "negative", "language_style": "hinglish", "tone": "direct"},
        {"text": "Packaging badhiya hai aur product sahi chal raha", "label": "positive", "language_style": "hinglish", "tone": "direct"},
        {"text": "Itna mehenga hai but result theek tha", "label": "neutral", "language_style": "hinglish", "tone": "mixed"},

        # Code-mixed social tone (sarcasm/complaint/emphasis)
        {"text": "Great job, another delayed order. Awesome.", "label": "negative", "language_style": "code_mixed", "tone": "sarcastic"},
        {"text": "Love the formula, but refund support is pathetic", "label": "negative", "language_style": "code_mixed", "tone": "mixed"},
        {"text": "Super happy with results, totally recommended", "label": "positive", "language_style": "code_mixed", "tone": "emphatic"},
        {"text": "The item is authentic and effective, very satisfied", "label": "positive", "language_style": "code_mixed", "tone": "direct"},
        {"text": "Delivery was late but product quality is good", "label": "neutral", "language_style": "code_mixed", "tone": "mixed"},
        {"text": "Not terrible, not great, just average", "label": "neutral", "language_style": "code_mixed", "tone": "nuanced"},

        # Additional robustness samples
        {"text": "I got rash and irritation after use", "label": "negative", "language_style": "english", "tone": "direct"},
        {"text": "Support has no response for refund", "label": "negative", "language_style": "english", "tone": "direct"},
        {"text": "Works perfectly and feels premium", "label": "positive", "language_style": "english", "tone": "direct"},
        {"text": "Never again, absolute waste of money", "label": "negative", "language_style": "english", "tone": "emphatic"},
        {"text": "Fast delivery and authentic item", "label": "positive", "language_style": "english", "tone": "direct"},
        {"text": "Quality changed this time, somewhat disappointed", "label": "negative", "language_style": "english", "tone": "nuanced"},

        # Real-world ambiguity samples (harder/mixed intent)
        {"text": "I wanted to hate it, but it is okay", "label": "neutral", "language_style": "code_mixed", "tone": "mixed"},
        {"text": "Delivery was on time, quality is average", "label": "neutral", "language_style": "english", "tone": "nuanced"},
        {"text": "Not good, not bad, manageable", "label": "neutral", "language_style": "english", "tone": "nuanced"},
        {"text": "Sahi hai overall but expensive", "label": "neutral", "language_style": "hinglish", "tone": "mixed"},
        {"text": "Accha hai but not worth the price maybe", "label": "neutral", "language_style": "hinglish", "tone": "mixed"},
        {"text": "Awesome packaging, product useless", "label": "negative", "language_style": "code_mixed", "tone": "sarcastic"},

        # Expanded sarcasm set (hard-tone robustness)
        {"text": "Great service, delayed again and totally disappointed.", "label": "negative", "language_style": "english", "tone": "sarcastic"},
        {"text": "Awesome support, no response and pathetic handling.", "label": "negative", "language_style": "english", "tone": "sarcastic"},
        {"text": "Fantastic product, fake and counterfeit item delivered.", "label": "negative", "language_style": "english", "tone": "sarcastic"},
        {"text": "Perfect experience, refund never came and worst support.", "label": "negative", "language_style": "english", "tone": "sarcastic"},
        {"text": "Bahut badhiya service, bekaar support and late delivery.", "label": "negative", "language_style": "hinglish", "tone": "sarcastic"},
        {"text": "Kya mast delivery, delayed and damaged package.", "label": "negative", "language_style": "hinglish", "tone": "sarcastic"},
        {"text": "Super authentic product, fake, useless and worst quality.", "label": "negative", "language_style": "code_mixed", "tone": "sarcastic"},
        {"text": "Love this support, no response and pathetic handling.", "label": "negative", "language_style": "code_mixed", "tone": "sarcastic"},
        {"text": "Perfect skin care, rash and irritation free bonus.", "label": "negative", "language_style": "hinglish", "tone": "sarcastic"},
        {"text": "Brilliant pricing, overpriced and not worth it.", "label": "negative", "language_style": "english", "tone": "sarcastic"},

        # Hindi (Devanagari) coverage
        {"text": "यह उत्पाद बहुत अच्छा है और डिलीवरी तेज थी", "label": "positive", "language_style": "hindi", "tone": "direct"},
        {"text": "पैकेजिंग खराब थी और मैं निराश हूं", "label": "negative", "language_style": "hindi", "tone": "direct"},
        {"text": "सेवा ठीक है, कुछ खास नहीं", "label": "neutral", "language_style": "hindi", "tone": "nuanced"},
        {"text": "यह आइटम नकली निकला", "label": "negative", "language_style": "hindi", "tone": "direct"},
        {"text": "सपोर्ट से कोई जवाब नहीं मिला", "label": "negative", "language_style": "hindi", "tone": "direct"},
        {"text": "कीमत बहुत महंगा है लेकिन गुणवत्ता ठीक है", "label": "neutral", "language_style": "hindi", "tone": "mixed"},
        {"text": "परिणाम शानदार हैं, मैं संतुष्ट हूं", "label": "positive", "language_style": "hindi", "tone": "emphatic"},
        {"text": "बहुत बढ़िया क्वालिटी और असली उत्पाद", "label": "positive", "language_style": "hindi", "tone": "direct"},
        {"text": "डिलीवरी में देरी हुई लेकिन प्रोडक्ट अच्छा है", "label": "neutral", "language_style": "hindi", "tone": "mixed"},
        {"text": "यह पैसे की बर्बादी है", "label": "negative", "language_style": "hindi", "tone": "emphatic"},
        {"text": "अनुभव सामान्य था", "label": "neutral", "language_style": "hindi", "tone": "nuanced"},
        {"text": "बढ़िया पैकेजिंग, शानदार अनुभव", "label": "positive", "language_style": "hindi", "tone": "direct"},
        {"text": "अच्छा है पर थोड़ा महंगा", "label": "neutral", "language_style": "hindi", "tone": "mixed"},
        {"text": "सबसे खराब सेवा", "label": "negative", "language_style": "hindi", "tone": "direct"},
        {"text": "मुझे rash और irritation हुआ", "label": "negative", "language_style": "hindi", "tone": "direct"},
        {"text": "Authentic लगा और results अच्छे थे", "label": "positive", "language_style": "hindi", "tone": "direct"},
        {"text": "support late tha but issue resolve ho gaya", "label": "neutral", "language_style": "hindi", "tone": "mixed"},
        {"text": "क्या मस्त प्रोडक्ट, highly recommended", "label": "positive", "language_style": "hindi", "tone": "emphatic"},
        {"text": "बहुत बेकार packaging and delayed delivery", "label": "negative", "language_style": "hindi", "tone": "direct"},
        {"text": "Bilkul perfect service, issue wahi ka wahi.", "label": "negative", "language_style": "hindi", "tone": "sarcastic"},

        # Spanish coverage
        {"text": "El producto es muy bueno y la entrega fue rapida", "label": "positive", "language_style": "spanish", "tone": "direct"},
        {"text": "Servicio horrible y producto falso", "label": "negative", "language_style": "spanish", "tone": "direct"},
        {"text": "Esta bien, nada especial", "label": "neutral", "language_style": "spanish", "tone": "nuanced"},
        {"text": "Muy caro pero calidad buena", "label": "neutral", "language_style": "spanish", "tone": "mixed"},
        {"text": "Sin respuesta del soporte, pesimo servicio", "label": "negative", "language_style": "spanish", "tone": "direct"},
        {"text": "Excelente producto, muy satisfecho", "label": "positive", "language_style": "spanish", "tone": "emphatic"},
        {"text": "No vale la pena, muy mala calidad", "label": "negative", "language_style": "spanish", "tone": "direct"},
        {"text": "Autentico y efectivo, recomendado", "label": "positive", "language_style": "spanish", "tone": "direct"},
        {"text": "Entrega tarde pero producto bueno", "label": "neutral", "language_style": "spanish", "tone": "mixed"},
        {"text": "Regular, mas o menos", "label": "neutral", "language_style": "spanish", "tone": "nuanced"},
        {"text": "Genial empaque y envio rapido", "label": "positive", "language_style": "spanish", "tone": "direct"},
        {"text": "Estafa total, nunca mas", "label": "negative", "language_style": "spanish", "tone": "emphatic"},
        {"text": "Producto promedio, cumple lo basico", "label": "neutral", "language_style": "spanish", "tone": "nuanced"},
        {"text": "Excelente atencion, muy feliz", "label": "positive", "language_style": "spanish", "tone": "direct"},
        {"text": "Demasiado caro para ese resultado", "label": "negative", "language_style": "spanish", "tone": "direct"},
        {"text": "Bueno pero el reembolso tarda", "label": "neutral", "language_style": "spanish", "tone": "mixed"},
        {"text": "Excelente, pedido tarde otra vez.", "label": "negative", "language_style": "spanish", "tone": "sarcastic"},
        {"text": "Muy bueno, funciona perfecto", "label": "positive", "language_style": "spanish", "tone": "emphatic"},
        {"text": "Malo y decepcionante", "label": "negative", "language_style": "spanish", "tone": "direct"},
        {"text": "Autentico item con entrega rapida", "label": "positive", "language_style": "spanish", "tone": "direct"},

        # Additional mixed real-world cases
        {"text": "Product accha hai but support no response", "label": "neutral", "language_style": "code_mixed", "tone": "mixed"},
        {"text": "Great quality, but refund delayed", "label": "negative", "language_style": "english", "tone": "mixed"},
        {"text": "Average product, okay for now", "label": "neutral", "language_style": "english", "tone": "nuanced"},
        {"text": "Bekaar quality and fake item", "label": "negative", "language_style": "hinglish", "tone": "direct"},
        {"text": "Badhiya result, very satisfied", "label": "positive", "language_style": "hinglish", "tone": "emphatic"},
        {"text": "Not worth it, too expensive", "label": "negative", "language_style": "english", "tone": "direct"},
        {"text": "Sahi packaging but late delivery", "label": "neutral", "language_style": "hinglish", "tone": "mixed"},
        {"text": "Love the product, works perfectly", "label": "positive", "language_style": "english", "tone": "direct"},
        {"text": "Hate this, worst experience ever", "label": "negative", "language_style": "english", "tone": "emphatic"},
        {"text": "Thik tha, normal experience", "label": "neutral", "language_style": "hinglish", "tone": "nuanced"},
        {"text": "Yeah, perfect support again.", "label": "negative", "language_style": "english", "tone": "sarcastic"},
        {"text": "Awesome quality and authentic product", "label": "positive", "language_style": "english", "tone": "direct"},
        {"text": "Overpriced but quality good", "label": "neutral", "language_style": "english", "tone": "mixed"},
        {"text": "Pathetic support and no refund", "label": "negative", "language_style": "english", "tone": "direct"},
        {"text": "Fast delivery and premium feel", "label": "positive", "language_style": "english", "tone": "direct"},
        {"text": "Nothing special, just average quality", "label": "neutral", "language_style": "english", "tone": "nuanced"},
        {"text": "Bahut badhiya but thoda mehenga", "label": "neutral", "language_style": "hinglish", "tone": "mixed"},
        {"text": "Counterfeit product, never again", "label": "negative", "language_style": "english", "tone": "emphatic"},
        {"text": "Recommended purchase, excellent quality", "label": "positive", "language_style": "english", "tone": "emphatic"},
        {"text": "Good service, delayed delivery again", "label": "negative", "language_style": "english", "tone": "sarcastic"},

        # French
        {"text": "Produit excellent et livraison rapide", "label": "positive", "language_style": "french", "tone": "direct"},
        {"text": "Service mauvais et remboursement en retard", "label": "negative", "language_style": "french", "tone": "direct"},
        {"text": "C'est correct, rien de special", "label": "neutral", "language_style": "french", "tone": "nuanced"},
        {"text": "Qualite bonne mais prix cher", "label": "neutral", "language_style": "french", "tone": "mixed"},
        {"text": "Genial, encore une livraison en retard", "label": "negative", "language_style": "french", "tone": "sarcastic"},
        {"text": "Je suis tres satisfait du resultat", "label": "positive", "language_style": "french", "tone": "emphatic"},

        # German
        {"text": "Sehr gutes Produkt und schnelle Lieferung", "label": "positive", "language_style": "german", "tone": "direct"},
        {"text": "Schlechter Support und keine Antwort", "label": "negative", "language_style": "german", "tone": "direct"},
        {"text": "Es ist okay, nichts besonderes", "label": "neutral", "language_style": "german", "tone": "nuanced"},
        {"text": "Qualitat gut aber zu teuer", "label": "neutral", "language_style": "german", "tone": "mixed"},
        {"text": "Perfekt, wieder verspätet geliefert", "label": "negative", "language_style": "german", "tone": "sarcastic"},
        {"text": "Ich bin sehr zufrieden", "label": "positive", "language_style": "german", "tone": "emphatic"},

        # Portuguese
        {"text": "Produto excelente e entrega rapida", "label": "positive", "language_style": "portuguese", "tone": "direct"},
        {"text": "Suporte ruim e sem resposta", "label": "negative", "language_style": "portuguese", "tone": "direct"},
        {"text": "Esta ok, nada demais", "label": "neutral", "language_style": "portuguese", "tone": "nuanced"},
        {"text": "Qualidade boa mas muito caro", "label": "neutral", "language_style": "portuguese", "tone": "mixed"},
        {"text": "Perfeito, pedido atrasado de novo", "label": "negative", "language_style": "portuguese", "tone": "sarcastic"},
        {"text": "Estou muito satisfeito com o resultado", "label": "positive", "language_style": "portuguese", "tone": "emphatic"},

        # Italian
        {"text": "Prodotto ottimo e consegna veloce", "label": "positive", "language_style": "italian", "tone": "direct"},
        {"text": "Supporto pessimo e nessuna risposta", "label": "negative", "language_style": "italian", "tone": "direct"},
        {"text": "Va bene, niente di speciale", "label": "neutral", "language_style": "italian", "tone": "nuanced"},
        {"text": "Qualita buona ma prezzo caro", "label": "neutral", "language_style": "italian", "tone": "mixed"},
        {"text": "Perfetto, ancora consegna in ritardo", "label": "negative", "language_style": "italian", "tone": "sarcastic"},
        {"text": "Sono molto soddisfatto", "label": "positive", "language_style": "italian", "tone": "emphatic"},

        # Arabic (transliterated for stable cross-platform evaluation)
        {"text": "almontaj momtaz wal tawsil saree", "label": "positive", "language_style": "arabic", "tone": "direct"},
        {"text": "alkhidma sayyi wa la rad", "label": "negative", "language_style": "arabic", "tone": "direct"},
        {"text": "aadi la shay mumaayaz", "label": "neutral", "language_style": "arabic", "tone": "nuanced"},
        {"text": "aljawda jayyida laken alseir ghali", "label": "neutral", "language_style": "arabic", "tone": "mixed"},
        {"text": "momtaz altalab motaakher marra okhra", "label": "negative", "language_style": "arabic", "tone": "sarcastic"},
        {"text": "ana saeid jiddan belnatija", "label": "positive", "language_style": "arabic", "tone": "emphatic"},

        # Hard real-world ambiguity set to prevent over-optimistic benchmark inflation
        {"text": "Perfect support, absolutely no progress.", "label": "negative", "language_style": "english", "tone": "sarcastic"},
        {"text": "Bueno pero nada cambia al final", "label": "neutral", "language_style": "spanish", "tone": "mixed"},
        {"text": "momtaz, la rad marra okhra", "label": "negative", "language_style": "arabic", "tone": "sarcastic"},
        {"text": "Wah kya support hai, reply hi nahi aaya", "label": "negative", "language_style": "hinglish", "tone": "sarcastic"},
        {"text": "Perfect experience, still unresolved.", "label": "negative", "language_style": "code_mixed", "tone": "sarcastic"},
        {"text": "Produto bom mas suporte fraco", "label": "neutral", "language_style": "portuguese", "tone": "mixed"}
    ]

    correct = 0
    details = []
    language_stats = {}
    tone_stats = {}

    def _bump(stats, key, matched):
        if key not in stats:
            stats[key] = {"total": 0, "correct": 0}
        stats[key]["total"] += 1
        if matched:
            stats[key]["correct"] += 1

    for case in benchmark:
        pred = analyze_sentiment(case["text"])
        matched = pred["sentiment"] == case["label"]
        if matched:
            correct += 1
        _bump(language_stats, case["language_style"], matched)
        _bump(tone_stats, case["tone"], matched)
        details.append({
            "text": case["text"],
            "expected": case["label"],
            "predicted": pred["sentiment"],
            "score": pred["score"],
            "match": matched,
            "language_style": case["language_style"],
            "tone": case["tone"]
        })

    language_accuracy = {
        key: round((value["correct"] / value["total"]) * 100, 2) if value["total"] else 0.0
        for key, value in language_stats.items()
    }
    tone_accuracy = {
        key: round((value["correct"] / value["total"]) * 100, 2) if value["total"] else 0.0
        for key, value in tone_stats.items()
    }
    accuracy = round((correct / len(benchmark)) * 100, 2)
    return jsonify({
        "benchmark_size": len(benchmark),
        "accuracy_percent": accuracy,
        "target_percent": 85,
        "meets_target": accuracy >= 85,
        "language_style_accuracy_percent": language_accuracy,
        "tone_accuracy_percent": tone_accuracy,
        "meets_language_tone_target": all(v >= 85 for v in language_accuracy.values()) and all(v >= 85 for v in tone_accuracy.values()),
        "results": details
    })


@app.route("/evaluate/sentiment/view")
def evaluate_sentiment_view():
    payload = evaluate_sentiment().get_json()
    language_order = [
        ("english", "English"),
        ("hinglish", "Hinglish"),
        ("hindi", "Hindi"),
        ("spanish", "Spanish"),
        ("french", "French"),
        ("german", "German"),
        ("portuguese", "Portuguese"),
        ("italian", "Italian"),
        ("arabic", "Arabic"),
        ("code_mixed", "Code-mixed"),
    ]
    tone_order = [
        ("direct", "Direct"),
        ("emphatic", "Emphatic"),
        ("mixed", "Mixed"),
        ("nuanced", "Nuanced"),
        ("sarcastic", "Sarcastic"),
    ]

    lang_map = payload.get("language_style_accuracy_percent", {})
    tone_map = payload.get("tone_accuracy_percent", {})
    language_rows = [(label, lang_map.get(key)) for key, label in language_order if key in lang_map]
    tone_rows = [(label, tone_map.get(key)) for key, label in tone_order if key in tone_map]
    return render_template(
        "sentiment_evaluation.html",
        payload=payload,
        language_rows=language_rows,
        tone_rows=tone_rows,
        sample_rows=payload.get("results", [])[:30]
    )


@app.route("/evaluate/leadtime")
def evaluate_leadtime():
    import statistics
    from collections import defaultdict

    metrics = DailyMetric.query.order_by(DailyMetric.date).all()
    if not metrics:
        return jsonify({"message": "No daily metrics found for lead-time evaluation."}), 404

    issues = {
        "Packaging": "packaging_count",
        "Delivery": "delivery_count",
        "Quality": "quality_count",
        "Pricing": "pricing_count",
        "Support": "support_count",
        "Side Effects": "side_effects_count",
        "Trust": "trust_count"
    }

    grouped = defaultdict(list)
    for m in metrics:
        grouped[(m.product_name, m.channel)].append(m)

    lead_hours = []
    for _, records in grouped.items():
        records.sort(key=lambda x: x.date)
        if len(records) < 7:
            continue

        for issue, column in issues.items():
            warning_date = None
            escalation_date = None

            for idx in range(3, len(records)):
                current = records[idx]
                baseline = [getattr(r, column, 0) or 0 for r in records[max(0, idx - 4):idx]]
                if len(baseline) < 3:
                    continue

                mean = statistics.mean(baseline)
                std = statistics.stdev(baseline) if len(baseline) > 1 else 0
                curr_value = getattr(current, column, 0) or 0
                z = (curr_value - mean) / std if std > 0 else 0

                if warning_date is None and z >= 1.8:
                    warning_date = current.date
                if z >= 2.5:
                    escalation_date = current.date
                    break

            if warning_date and escalation_date and warning_date < escalation_date:
                delta_hours = int((escalation_date - warning_date).days * 24)
                lead_hours.append(delta_hours)

    if not lead_hours:
        return jsonify({
            "evaluated_series": len(grouped),
            "message": "Not enough warning/escalation pairs yet to evaluate lead-time."
        })

    between_24_48 = sum(1 for h in lead_hours if 24 <= h <= 48)
    pct_24_48 = round((between_24_48 / len(lead_hours)) * 100, 2)

    return jsonify({
        "evaluated_series": len(grouped),
        "pair_count": len(lead_hours),
        "avg_lead_hours": round(sum(lead_hours) / len(lead_hours), 2),
        "min_lead_hours": min(lead_hours),
        "max_lead_hours": max(lead_hours),
        "pct_between_24_48_hours": pct_24_48
    })


@app.route("/identifiers/coverage")
def identifier_coverage():
    total_reviews = Review.query.count()
    with_order = Review.query.filter(Review.extracted_order_ids.isnot(None), Review.extracted_order_ids != "[]").count()
    with_partner = Review.query.filter(
        Review.extracted_delivery_partners.isnot(None),
        Review.extracted_delivery_partners != "[]"
    ).count()
    with_location = Review.query.filter(Review.extracted_locations.isnot(None), Review.extracted_locations != "[]").count()

    def pct(v):
        return round((v / total_reviews) * 100, 2) if total_reviews else 0

    return jsonify({
        "total_reviews": total_reviews,
        "with_order_ids": {"count": with_order, "percent": pct(with_order)},
        "with_delivery_partner_mentions": {"count": with_partner, "percent": pct(with_partner)},
        "with_location_mentions": {"count": with_location, "percent": pct(with_location)}
    })


@app.route("/evaluate/success_criteria")
def evaluate_success_criteria():
    channels = [row[0] for row in db.session.query(Review.channel).distinct().all() if row[0]]
    channel_count = len(channels)

    classifier_eval = evaluate_classifier().get_json()
    sentiment_eval = evaluate_sentiment().get_json()
    lead_eval_response = evaluate_leadtime()
    lead_eval = lead_eval_response[0].get_json() if isinstance(lead_eval_response, tuple) else lead_eval_response.get_json()

    brief_text = daily_brief()
    sentence_count = len([s for s in re.split(r"[.!?]+", brief_text) if s.strip()])

    actionable_terms = ["assign", "investigate", "containment", "owners", "monitoring", "action"]
    actionable_score = sum(1 for term in actionable_terms if term in brief_text.lower())

    has_issue_panel = bool(DailyMetric.query.count())
    has_alert_panel = bool(RiskAlert.query.count())
    has_negative_comment_panel = bool(Review.query.filter(Review.sentiment == "negative").first())

    return jsonify({
        "criteria": {
            "covers_5_plus_channels": {
                "status": "pass" if channel_count >= 5 else "partial",
                "channel_count": channel_count,
                "channels": channels
            },
            "sentiment_or_taxonomy_accuracy_above_85": {
                "status": "pass" if classifier_eval.get("meets_target") and sentiment_eval.get("meets_target") else "partial",
                "taxonomy_accuracy_percent": classifier_eval.get("accuracy_percent"),
                "sentiment_accuracy_percent": sentiment_eval.get("accuracy_percent")
            },
            "crisis_leadtime_24_48_hours": {
                "status": "pass" if lead_eval.get("pct_between_24_48_hours", 0) >= 60 else "partial",
                "details": lead_eval
            },
            "daily_brief_actionable": {
                "status": "pass" if sentence_count == 5 and actionable_score >= 2 else "partial",
                "sentence_count": sentence_count,
                "actionable_term_hits": actionable_score
            },
            "dashboard_actionability": {
                "status": "pass" if has_issue_panel and has_alert_panel and has_negative_comment_panel else "partial",
                "signals": {
                    "issue_metrics_available": has_issue_panel,
                    "risk_alerts_available": has_alert_panel,
                    "negative_comments_available": has_negative_comment_panel
                }
            }
        }
    })


@app.route("/detect")
def detect_spike():
    from models import DailyMetric
    from collections import defaultdict
    import statistics

    TARGET_BRAND = "AuraWell Labs"

    CHANNEL_WEIGHTS = {
        "Reddit": 1.30,
        "Twitter": 1.20,
        "Instagram": 1.10,
        "Google": 1.05,
        "Amazon": 1.00,
        "Flipkart": 0.95,
        "Nykaa": 0.90
    }

    metrics = DailyMetric.query.order_by(DailyMetric.date).all()
    influence_lookup = build_influence_lookup()
    if not metrics:
        return "<h2>No data available.</h2>"

    grouped_data = defaultdict(list)
    for m in metrics:
        brand = m.product_name.split(" | ")[0]
        if brand == TARGET_BRAND:
            grouped_data[(m.product_name, m.channel)].append(m)

    if not grouped_data:
        return f"<h2>No data available for {TARGET_BRAND}.</h2>"

    issues = ISSUE_COLUMN_MAP

    structured_alerts = []
    product_risks = []
    issue_channel_map = defaultdict(set)

    for (product, channel), records in grouped_data.items():
        records.sort(key=lambda x: x.date)
        if len(records) < 5:
            continue

        today = records[-1]
        weight = CHANNEL_WEIGHTS.get(channel, 1.0)
        brand_name = product.split(" | ")[0]
        influence_factor = get_metric_influence(
            influence_lookup,
            brand_name,
            product,
            channel,
            today.date
        )

        for issue_name, column in issues.items():
            today_value = getattr(today, column, 0) or 0
            baseline = records[-8:-1] if len(records) >= 8 else records[:-1]
            baseline_values = [(getattr(r, column, 0) or 0) for r in baseline]
            if len(baseline_values) < 3:
                continue

            mean = statistics.mean(baseline_values)
            std = statistics.stdev(baseline_values) if len(baseline_values) > 1 else 0
            time_signals = _build_issue_time_signals(records, column)
            persistence_days = _count_consecutive_anomaly_days(time_signals)
            persistence_mult = _persistence_multiplier(persistence_days)
            recovery_initiated, prior_run_days = _is_recovery_initiated(time_signals)

            if std > 0:
                z = (today_value - mean) / std
                if z > 2:
                    if z >= 3:
                        band = "Severe"
                        multiplier = 22
                    elif z >= 2.5:
                        band = "Elevated"
                        multiplier = 19
                    else:
                        band = "Moderate"
                        multiplier = 17

                    risk = round(min(100, z * multiplier * weight * influence_factor * persistence_mult), 1)
                    structured_alerts.append({
                        "type": "Statistical",
                        "product": product,
                        "channel": channel,
                        "issue": issue_name,
                        "metric": f"Z-Score ({band})",
                        "signal": round(z, 2),
                        "risk": risk,
                        "influence_factor": influence_factor,
                        "persistence_days": persistence_days
                    })
                    product_risks.append({"channel": channel, "risk": risk})
                    issue_channel_map[issue_name].add(channel)

            last_two = records[-3:-1]
            if len(last_two) == 2:
                avg_last_two = sum((getattr(r, column, 0) or 0) for r in last_two) / 2
                if avg_last_two > 0:
                    growth = today_value / avg_last_two
                    if growth >= 2.5:
                        risk = round(min(100, growth * 18 * weight * influence_factor * persistence_mult), 1)
                        structured_alerts.append({
                            "type": "Velocity",
                            "product": product,
                            "channel": channel,
                            "issue": issue_name,
                            "metric": "Acceleration",
                            "signal": round(growth, 2),
                            "risk": risk,
                            "influence_factor": influence_factor,
                            "persistence_days": persistence_days
                        })
                        product_risks.append({"channel": channel, "risk": risk})
                        issue_channel_map[issue_name].add(channel)

            has_issue_alert = any(
                a["product"] == product and a["channel"] == channel and a["issue"] == issue_name
                for a in structured_alerts
            )
            if not has_issue_alert and recovery_initiated:
                growth_value = time_signals[-1]["growth"] or 1.0
                recovery_progress = max(0.0, (1.0 - growth_value))
                recovery_base = (35 + (prior_run_days * 4)) * weight * influence_factor
                recovery_risk = round(max(20, min(60, recovery_base * (0.7 - min(0.3, recovery_progress)))), 1)
                structured_alerts.append({
                    "type": "Recovery",
                    "product": product,
                    "channel": channel,
                    "issue": issue_name,
                    "metric": "Recovery Initiated",
                    "signal": round(growth_value, 2),
                    "risk": recovery_risk,
                    "influence_factor": influence_factor,
                    "persistence_days": persistence_days
                })
                product_risks.append({"channel": channel, "risk": max(5, recovery_risk * 0.4)})
                issue_channel_map[issue_name].add(channel)

    for alert in structured_alerts:
        channels = issue_channel_map.get(alert["issue"], set())
        if len(channels) >= 2:
            alert["channel"] = ", ".join(sorted(channels))

    momentum_score = 0
    status = "Stable"
    if product_risks:
        avg_risk = sum(a["risk"] for a in product_risks) / len(product_risks)
        channel_spread = len(set(a["channel"] for a in product_risks))
        momentum_score = round(min(100, (avg_risk * 0.7) + (channel_spread * 6)), 1)

        if momentum_score >= 75:
            status = "Critical"
        elif momentum_score >= 60:
            status = "High"
        elif momentum_score >= 45:
            status = "Moderate"
        elif momentum_score >= 30:
            status = "Watch"

    def risk_band(score):
        if score >= 75:
            return "Critical", "critical"
        if score >= 60:
            return "High", "high"
        if score >= 45:
            return "Moderate", "moderate"
        if score >= 30:
            return "Watch", "watch"
        return "OK", "ok"

    structured_alerts.sort(key=lambda x: x["risk"], reverse=True)
    cross_channel_issues = [
        f"{issue}: {', '.join(sorted(channels))}"
        for issue, channels in issue_channel_map.items()
        if len(channels) >= 2
    ]

    if not structured_alerts:
        return f"""
        <html>
        <body style="background:#0f172a;color:#f1f5f9;padding:40px;font-family:Segoe UI, Arial, sans-serif;">
            <h2 style="color:#22c55e;">OK Report: No significant crisis signals detected for {TARGET_BRAND}.</h2>
        </body>
        </html>
        """

    rows_html = ""
    for alert in structured_alerts:
        level_label, level_class = risk_band(alert["risk"])
        display_product = alert["product"].split(" | ")[-1]
        signal_label = "Z" if alert["type"] == "Statistical" else "Growth"

        rows_html += f"""
        <tr class="risk-{level_class}">
            <td>{display_product}</td>
            <td>{alert['channel']}</td>
            <td>{alert['issue']}</td>
            <td>{alert['type']}</td>
            <td>{alert['signal']} ({signal_label})</td>
            <td>{alert['metric']}</td>
            <td><span class="pill {level_class}">{alert['risk']} ({level_label})</span></td>
        </tr>
        """

    escalation_html = ""
    if cross_channel_issues:
        escalation_items = "".join(f"<li>{line}</li>" for line in cross_channel_issues)
        escalation_html = f"""
        <div class="escalation-box">
            <h3>Cross-Channel Escalations</h3>
            <ul>{escalation_items}</ul>
        </div>
        """

    momentum_class = status.lower() if status != "Stable" else "ok"
    momentum_label = "OK" if status == "Stable" else status

    return f"""
    <html>
    <head>
    <style>
    body {{
        font-family: "Segoe UI", Arial, sans-serif;
        background:#0f172a;
        color:#f1f5f9;
        padding:32px;
    }}
    .container {{ max-width:1200px; margin:0 auto; }}
    h1 {{ margin:0 0 8px; color:#e2e8f0; }}
    .subtext {{ margin:0 0 20px; color:#94a3b8; }}
    .kpis {{
        display:grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap:12px;
        margin-bottom:18px;
    }}
    .kpi {{
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        padding:14px;
    }}
    .kpi-label {{ font-size:12px; color:#94a3b8; text-transform:uppercase; }}
    .kpi-value {{ font-size:24px; font-weight:700; margin-top:4px; }}
    table {{
        width:100%;
        border-collapse:collapse;
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        overflow:hidden;
    }}
    th, td {{ padding:12px; text-align:left; }}
    th {{
        background:#1e293b;
        font-size:12px;
        text-transform:uppercase;
        color:#94a3b8;
        letter-spacing:0.02em;
    }}
    tr {{ border-bottom:1px solid #1f2937; }}
    tr.risk-critical {{ border-left:4px solid #ef4444; background:rgba(239,68,68,0.10); }}
    tr.risk-high {{ border-left:4px solid #f97316; background:rgba(249,115,22,0.10); }}
    tr.risk-moderate {{ border-left:4px solid #eab308; background:rgba(234,179,8,0.10); }}
    tr.risk-watch {{ border-left:4px solid #3b82f6; background:rgba(59,130,246,0.10); }}
    tr.risk-ok {{ border-left:4px solid #22c55e; background:rgba(34,197,94,0.10); }}
    .pill {{
        display:inline-block;
        padding:4px 10px;
        border-radius:999px;
        font-size:12px;
        font-weight:700;
    }}
    .critical {{ color:#fecaca; background:rgba(239,68,68,0.20); }}
    .high {{ color:#fed7aa; background:rgba(249,115,22,0.20); }}
    .moderate {{ color:#fde68a; background:rgba(234,179,8,0.20); }}
    .watch {{ color:#bfdbfe; background:rgba(59,130,246,0.20); }}
    .ok {{ color:#bbf7d0; background:rgba(34,197,94,0.20); }}
    .escalation-box {{
        margin:18px 0;
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        padding:14px;
    }}
    .escalation-box h3 {{ margin:0 0 8px; }}
    .escalation-box ul {{ margin:0; padding-left:18px; color:#d1d5db; }}
    </style>
    </head>
    <body>
        <div class="container">
            <h1>{TARGET_BRAND} Crisis Intelligence</h1>
            <p class="subtext">Severity colors indicate operational priority across channels.</p>

            <div class="kpis">
                <div class="kpi">
                    <div class="kpi-label">Total Alerts</div>
                    <div class="kpi-value">{len(structured_alerts)}</div>
                </div>
                <div class="kpi">
                    <div class="kpi-label">Affected Channels</div>
                    <div class="kpi-value">{len(set(a['channel'] for a in product_risks)) if product_risks else 0}</div>
                </div>
                <div class="kpi">
                    <div class="kpi-label">Crisis Momentum</div>
                    <div class="kpi-value"><span class="pill {momentum_class}">{momentum_score} ({momentum_label})</span></div>
                </div>
            </div>

            {escalation_html}

            <table>
                <tr>
                    <th>Product</th>
                    <th>Channel(s)</th>
                    <th>Issue</th>
                    <th>Signal Type</th>
                    <th>Signal</th>
                    <th>Metric</th>
                    <th>Risk</th>
                </tr>
                {rows_html}
            </table>
        </div>
    </body>
    </html>
    """

@app.route("/competitors")
def competitor_spike():
    from models import DailyMetric
    from collections import defaultdict
    import statistics

    TARGET_BRANDS = ["GlowNest", "NutraZen"]

    CHANNEL_WEIGHTS = {
        "Twitter": 1.2,
        "Instagram": 1.1,
        "Reddit": 1.3,
        "Amazon": 1.0,
        "Nykaa": 0.9,
        "Google": 1.1,
        "Flipkart": 1.0
    }

    metrics = DailyMetric.query.order_by(DailyMetric.date).all()
    influence_lookup = build_influence_lookup()
    if not metrics:
        return "<h2>No data available.</h2>"

    grouped_data = defaultdict(list)
    for m in metrics:
        brand = m.product_name.split(" | ")[0]
        if brand in TARGET_BRANDS:
            grouped_data[(m.product_name, m.channel)].append(m)

    if not grouped_data:
        return "<h2>No competitor data available.</h2>"

    issues = ISSUE_COLUMN_MAP

    product_alerts = []
    cross_channel_map = defaultdict(set)

    for (product, channel), records in grouped_data.items():
        records.sort(key=lambda x: x.date)
        if len(records) < 3:
            continue

        today = records[-1]
        brand = product.split(" | ")[0]
        channel_weight = CHANNEL_WEIGHTS.get(channel, 1.0)
        influence_factor = get_metric_influence(
            influence_lookup,
            brand,
            product,
            channel,
            today.date
        )

        for issue_name, column in issues.items():
            today_value = getattr(today, column, 0) or 0
            time_signals = _build_issue_time_signals(records, column)
            persistence_days = _count_consecutive_anomaly_days(time_signals)
            persistence_mult = _persistence_multiplier(persistence_days)
            recovery_initiated, prior_run_days = _is_recovery_initiated(time_signals)

            if len(records) >= 7:
                baseline = records[-8:-1] if len(records) >= 8 else records[:-1]
                baseline_values = [getattr(r, column, 0) or 0 for r in baseline]
                if len(baseline_values) > 1:
                    mean = statistics.mean(baseline_values)
                    std = statistics.stdev(baseline_values)
                    if std > 0:
                        z_score = (today_value - mean) / std
                        if z_score > 2:
                            risk_score = min(100, z_score * 20 * channel_weight * influence_factor * persistence_mult)
                            product_alerts.append({
                                "brand": brand,
                                "issue": issue_name,
                                "product": product,
                                "channel": channel,
                                "signal": round(z_score, 2),
                                "risk": round(risk_score, 1),
                                "type": "Statistical",
                                "influence_factor": influence_factor,
                                "persistence_days": persistence_days
                            })
                            cross_channel_map[(brand, issue_name)].add(channel)

            last_two = records[-3:-1]
            if len(last_two) == 2:
                avg_last_two = sum((getattr(r, column, 0) or 0) for r in last_two) / 2
                if avg_last_two > 0:
                    growth_ratio = today_value / avg_last_two
                    if growth_ratio >= 3:
                        risk_score = min(100, growth_ratio * 25 * channel_weight * influence_factor * persistence_mult)
                        product_alerts.append({
                            "brand": brand,
                            "issue": issue_name,
                            "product": product,
                            "channel": channel,
                            "signal": round(growth_ratio, 2),
                            "risk": round(risk_score, 1),
                            "type": "Velocity",
                            "influence_factor": influence_factor,
                            "persistence_days": persistence_days
                        })
                        cross_channel_map[(brand, issue_name)].add(channel)

            has_issue_alert = any(
                a["brand"] == brand and a["product"] == product and a["channel"] == channel and a["issue"] == issue_name
                for a in product_alerts
            )
            if not has_issue_alert and recovery_initiated:
                growth_value = time_signals[-1]["growth"] or 1.0
                recovery_progress = max(0.0, (1.0 - growth_value))
                recovery_base = (35 + (prior_run_days * 4)) * channel_weight * influence_factor
                recovery_risk = round(max(20, min(60, recovery_base * (0.7 - min(0.3, recovery_progress)))), 1)
                product_alerts.append({
                    "brand": brand,
                    "issue": issue_name,
                    "product": product,
                    "channel": channel,
                    "signal": round(growth_value, 2),
                    "risk": recovery_risk,
                    "type": "Recovery",
                    "influence_factor": influence_factor,
                    "persistence_days": persistence_days
                })
                cross_channel_map[(brand, issue_name)].add(channel)

    brand_alert_map = defaultdict(list)
    for alert in product_alerts:
        brand_alert_map[alert["brand"]].append(alert)

    brand_momentum = []
    for brand, alerts in brand_alert_map.items():
        avg_signal = sum(a["signal"] for a in alerts) / len(alerts)
        avg_risk = sum(a["risk"] for a in alerts) / len(alerts)
        channel_spread = len(set(a["channel"] for a in alerts))

        momentum_score = (avg_signal * 20 * 0.5) + (avg_risk * 0.3) + ((channel_spread * 15) * 0.2)
        momentum_score = round(momentum_score, 1)

        if momentum_score > 130:
            status = "Critical"
        elif momentum_score > 100:
            status = "Escalating"
        elif momentum_score > 70:
            status = "Accelerating"
        elif momentum_score > 40:
            status = "Watch"
        else:
            status = "Stable"

        brand_momentum.append({
            "brand": brand,
            "score": momentum_score,
            "status": status
        })

    def risk_level(score):
        if score >= 80:
            return "Critical", "critical"
        if score >= 60:
            return "High", "high"
        if score >= 40:
            return "Moderate", "moderate"
        return "Watch", "watch"

    product_alerts.sort(key=lambda a: a["risk"], reverse=True)
    brand_momentum.sort(key=lambda a: a["score"], reverse=True)

    cross_channel_escalations = []
    for (brand, issue), channels in cross_channel_map.items():
        if len(channels) >= 2:
            cross_channel_escalations.append(f"{brand}: {issue} across {', '.join(sorted(channels))}")

    if not product_alerts:
        return """
        <html>
        <body style="background:#0f172a;color:#f1f5f9;padding:40px;font-family:Segoe UI, Arial, sans-serif;">
            <h2 style="color:#22c55e;">No product-channel level spikes detected for competitors.</h2>
        </body>
        </html>
        """

    momentum_cards = ""
    for item in brand_momentum:
        status_class = item["status"].lower()
        momentum_cards += f"""
        <div class="momentum-card">
            <div class="momentum-brand">{item['brand']}</div>
            <div class="momentum-score">{item['score']}</div>
            <div class="pill {status_class}">{item['status']}</div>
        </div>
        """

    rows_html = ""
    for alert in product_alerts:
        level_label, level_class = risk_level(alert["risk"])
        display_product = alert["product"].split(" | ")[-1]
        signal_label = "Z" if alert["type"] == "Statistical" else "Growth"

        rows_html += f"""
        <tr class="risk-{level_class}">
            <td>{alert['brand']}</td>
            <td>{display_product}</td>
            <td>{alert['channel']}</td>
            <td>{alert['issue']}</td>
            <td>{alert['type']}</td>
            <td>{alert['signal']} ({signal_label})</td>
            <td><span class="pill {level_class}">{alert['risk']} ({level_label})</span></td>
        </tr>
        """

    escalation_html = ""
    if cross_channel_escalations:
        items = "".join(f"<li>{line}</li>" for line in cross_channel_escalations)
        escalation_html = f"""
        <div class="escalation-box">
            <h3>Cross-Channel Escalations</h3>
            <ul>{items}</ul>
        </div>
        """

    return f"""
    <html>
    <head>
    <style>
    body {{
        font-family: "Segoe UI", Arial, sans-serif;
        background:#0f172a;
        color:#f1f5f9;
        padding:32px;
    }}
    .container {{ max-width:1200px; margin:0 auto; }}
    h1 {{ margin:0 0 8px; color:#e2e8f0; }}
    .subtext {{ margin:0 0 20px; color:#94a3b8; }}
    .kpis {{
        display:grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap:12px;
        margin-bottom:18px;
    }}
    .kpi {{
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        padding:14px;
    }}
    .kpi-label {{ font-size:12px; color:#94a3b8; text-transform:uppercase; }}
    .kpi-value {{ font-size:24px; font-weight:700; margin-top:4px; }}
    .momentum-grid {{
        display:grid;
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        gap:12px;
        margin-bottom:18px;
    }}
    .momentum-card {{
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        padding:14px;
    }}
    .momentum-brand {{ font-weight:600; margin-bottom:4px; }}
    .momentum-score {{ font-size:28px; font-weight:700; margin-bottom:8px; }}
    table {{
        width:100%;
        border-collapse:collapse;
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        overflow:hidden;
    }}
    th, td {{ padding:12px; text-align:left; }}
    th {{
        background:#1e293b;
        font-size:12px;
        text-transform:uppercase;
        color:#94a3b8;
        letter-spacing:0.02em;
    }}
    tr {{ border-bottom:1px solid #1f2937; }}
    tr.risk-critical {{ border-left:4px solid #ef4444; background:rgba(239,68,68,0.10); }}
    tr.risk-high {{ border-left:4px solid #f97316; background:rgba(249,115,22,0.10); }}
    tr.risk-moderate {{ border-left:4px solid #eab308; background:rgba(234,179,8,0.10); }}
    tr.risk-watch {{ border-left:4px solid #3b82f6; background:rgba(59,130,246,0.10); }}
    .pill {{
        display:inline-block;
        padding:4px 10px;
        border-radius:999px;
        font-size:12px;
        font-weight:700;
    }}
    .critical {{ color:#fecaca; background:rgba(239,68,68,0.20); }}
    .high {{ color:#fed7aa; background:rgba(249,115,22,0.20); }}
    .moderate {{ color:#fde68a; background:rgba(234,179,8,0.20); }}
    .watch {{ color:#bfdbfe; background:rgba(59,130,246,0.20); }}
    .stable {{ color:#bbf7d0; background:rgba(34,197,94,0.20); }}
    .escalation-box {{
        margin:18px 0;
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        padding:14px;
    }}
    .escalation-box h3 {{ margin:0 0 8px; }}
    .escalation-box ul {{ margin:0; padding-left:18px; color:#d1d5db; }}
    </style>
    </head>
    <body>
        <div class="container">
            <h1>Competitor Risk Intelligence</h1>
            <p class="subtext">Monitoring brands: {", ".join(TARGET_BRANDS)}. Severity colors indicate risk priority.</p>

            <div class="kpis">
                <div class="kpi">
                    <div class="kpi-label">Total Alerts</div>
                    <div class="kpi-value">{len(product_alerts)}</div>
                </div>
                <div class="kpi">
                    <div class="kpi-label">Brands Impacted</div>
                    <div class="kpi-value">{len(brand_momentum)}</div>
                </div>
                <div class="kpi">
                    <div class="kpi-label">Cross-Channel Escalations</div>
                    <div class="kpi-value">{len(cross_channel_escalations)}</div>
                </div>
            </div>

            <div class="momentum-grid">
                {momentum_cards}
            </div>

            {escalation_html}

            <table>
                <tr>
                    <th>Brand</th>
                    <th>Product</th>
                    <th>Channel</th>
                    <th>Issue</th>
                    <th>Signal Type</th>
                    <th>Signal</th>
                    <th>Risk</th>
                </tr>
                {rows_html}
            </table>
        </div>
    </body>
    </html>
    """


@app.route("/reset")
def reset_db():
    from models import Review, DailyMetric, RiskAlert

    Review.query.delete()
    DailyMetric.query.delete()
    RiskAlert.query.delete()

    db.session.commit()
    return "Database reset successfully!"

@app.route("/brand_health")
def brand_health():
    from models import DailyMetric
    from collections import defaultdict

    metrics = DailyMetric.query.all()

    brand_scores = defaultdict(list)

    for m in metrics:
        brand = m.product_name.split(" | ")[0]
        brand_scores[brand].append(m.negative_percentage)

    results = {}

    for brand, values in brand_scores.items():
        avg_negative = sum(values) / len(values)
        health_score = max(0, 100 - avg_negative)

        results[brand] = round(health_score, 2)

    return str(results)

@app.route("/daily_brief")
def daily_brief():
    metrics = DailyMetric.query.order_by(DailyMetric.date).all()
    influence_lookup = build_influence_lookup()
    live_alerts = build_live_risk_alerts(metrics, influence_lookup=influence_lookup)
    return build_daily_brief_text(metrics, live_alerts)

if __name__ == "__main__":
    auto_boot = os.getenv("AUTO_INGEST_ON_BOOT", "false").lower() == "true"
    if auto_boot:
        interval_minutes = max(5, int(os.getenv("AUTO_INGEST_INTERVAL_MINUTES", "360")))
        _, boot_limits, boot_clear_existing = resolve_ingestion_limits_from_preset("demo_fast")
        _start_auto_ingestion(
            interval_minutes=interval_minutes,
            channel_limits=boot_limits,
            clear_existing=boot_clear_existing,
            preset_name="demo_fast"
        )

    app.run(debug=True)

