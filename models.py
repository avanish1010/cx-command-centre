from extensions import db
from datetime import datetime
from zoneinfo import ZoneInfo


IST_ZONE = ZoneInfo("Asia/Kolkata")


def ist_now():
    return datetime.now(IST_ZONE).replace(tzinfo=None)

# Raw reviews table
class Review(db.Model):
    __tablename__ = "reviews"

    id = db.Column(db.Integer, primary_key=True)
    brand_name = db.Column(db.String(100), nullable=False)
    channel = db.Column(db.String(50), nullable=False)
    review_text = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime)
    sentiment = db.Column(db.String(20))
    sentiment_score = db.Column(db.Float)
    issue_category = db.Column(db.String(50))
    product_name = db.Column(db.String(100))
    location = db.Column(db.String(100))
    extracted_order_ids = db.Column(db.Text)
    extracted_delivery_partners = db.Column(db.Text)
    extracted_locations = db.Column(db.Text)
    source_followers = db.Column(db.BigInteger)
    post_views = db.Column(db.BigInteger)
    engagement_count = db.Column(db.Integer)
    influence_factor = db.Column(db.Float, default=1.0)

# Daily aggregated metrics table
class DailyMetric(db.Model):
    __tablename__ = "daily_metrics"

    id = db.Column(db.Integer, primary_key=True)

    date = db.Column(db.Date, nullable=False)
    product_name = db.Column(db.String(100), nullable=False)
    channel = db.Column(db.String(50))
    total_mentions = db.Column(db.Integer, default=0)
    positive_count = db.Column(db.Integer, default=0)
    neutral_count = db.Column(db.Integer, default=0)
    negative_count = db.Column(db.Integer, default=0)

    negative_percentage = db.Column(db.Float)

    packaging_count = db.Column(db.Integer, default=0)
    delivery_count = db.Column(db.Integer, default=0)
    quality_count = db.Column(db.Integer, default=0)
    pricing_count = db.Column(db.Integer, default=0)
    support_count = db.Column(db.Integer, default=0)
    side_effects_count = db.Column(db.Integer, default=0)
    trust_count = db.Column(db.Integer, default=0)

# Risk alerts table

class RiskAlert(db.Model):
    __tablename__ = "risk_alert"

    id = db.Column(db.Integer, primary_key=True)

    # Core identification
    brand_name = db.Column(db.String(100))        # NEW
    product_name = db.Column(db.String(150))

    issue_type = db.Column(db.String(100))
    channel = db.Column(db.String(50))
    spike_type = db.Column(db.String(30))

    # Risk metrics
    z_score = db.Column(db.Float)
    risk_score = db.Column(db.Float)
    alert_level = db.Column(db.String(50))

    # Aggregation control
    aggregation_level = db.Column(
        db.String(20),
        default="product"
    )

    # Metadata
    created_at = db.Column(
        db.DateTime,
        default=ist_now
    )


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(32), nullable=False, index=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    force_password_reset = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=ist_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=ist_now, onupdate=ist_now, nullable=False)
    last_login_at = db.Column(db.DateTime)


class AuditLog(db.Model):
    __tablename__ = "audit_logs"

    id = db.Column(db.Integer, primary_key=True)
    actor_email = db.Column(db.String(255), nullable=False, index=True)
    action = db.Column(db.String(120), nullable=False, index=True)
    target_email = db.Column(db.String(255))
    metadata_json = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=ist_now, nullable=False, index=True)


class IngestionRun(db.Model):
    __tablename__ = "ingestion_runs"

    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.String(64), unique=True, nullable=False, index=True)
    triggered_by = db.Column(db.String(64), nullable=False, default="system", index=True)
    started_at = db.Column(db.DateTime, nullable=False, default=ist_now, index=True)
    ended_at = db.Column(db.DateTime)
    duration_ms = db.Column(db.Integer)
    status = db.Column(db.String(32), nullable=False, default="running", index=True)
    records_processed = db.Column(db.Integer, nullable=False, default=0)
    metric_rows = db.Column(db.Integer, nullable=False, default=0)
    channel_counts_json = db.Column(db.Text)
    error_message = db.Column(db.Text)

