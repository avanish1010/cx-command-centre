import random
from datetime import datetime, timedelta
from utils.issue_classifier import classify_issue

from .base_connector import (
    BRANDS,
    PRODUCTS,
    generate_timestamp,
    generate_sentiment_score
)

LOCATIONS = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]

# Nykaa issue weighting (beauty focused)
NYKAA_ISSUE_DISTRIBUTION = {
    "Quality": 0.35,
    "Packaging": 0.25,
    "Pricing": 0.20,
    "Delivery": 0.15,
    "Support": 0.05
}


def choose_issue():
    issues = list(NYKAA_ISSUE_DISTRIBUTION.keys())
    weights = list(NYKAA_ISSUE_DISTRIBUTION.values())
    return random.choices(issues, weights=weights)[0]


def sentiment_from_issue(issue, crisis_mode=False):

    if crisis_mode:
        return random.choices(
            ["negative", "neutral"],
            weights=[0.80, 0.20]
        )[0]

    # Nykaa slightly more positive baseline
    return random.choices(
        ["positive", "neutral", "negative"],
        weights=[0.45, 0.25, 0.30]
    )[0]


def generate_review_text(brand, product, issue):

    templates = {
        "Quality": f"The quality of {brand} {product} feels different this time.",
        "Packaging": f"The packaging of {brand} {product} was slightly damaged.",
        "Pricing": f"{brand} {product} feels a bit overpriced for what it offers.",
        "Delivery": f"Delivery for {brand} {product} was slower than expected.",
        "Support": f"Customer support response from {brand} was not helpful."
    }

    return templates[issue]


def fetch_nykaa_reviews(limit=5000):

    reviews = []
    spike_day = datetime.utcnow() - timedelta(days=2)

    for _ in range(limit):

        brand = random.choice(BRANDS)
        product = random.choice(PRODUCTS[brand])
        product_key = f"{brand} | {product}"

        issue = choose_issue()

        crisis_mode = random.random() < 0.20
        spike_intensity = 0.6 if crisis_mode else 0.2

        review_text = generate_review_text(brand, product, issue)

        sentiment = sentiment_from_issue(issue, crisis_mode)
        sentiment_score = generate_sentiment_score(sentiment)

        timestamp = generate_timestamp(
            spike_day=spike_day,
            spike_intensity=spike_intensity
        )

        reviews.append({
            "brand_name": brand,
            "channel": "Nykaa",
            "review_text": review_text,
            "timestamp": timestamp,
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "issue_category": issue,
            "product_name": product_key,
            "location": random.choice(LOCATIONS)
        })

    return reviews