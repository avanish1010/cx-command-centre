import random
from datetime import datetime, timedelta

from .base_connector import (
    BRANDS,
    PRODUCTS,
    generate_timestamp,
    generate_sentiment_score
)

LOCATIONS = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]

GOOGLE_ISSUE_DISTRIBUTION = {
    "Quality": 0.25,
    "Delivery": 0.25,
    "Support": 0.25,
    "Packaging": 0.15,
    "Pricing": 0.10
}


def choose_issue():
    issues = list(GOOGLE_ISSUE_DISTRIBUTION.keys())
    weights = list(GOOGLE_ISSUE_DISTRIBUTION.values())
    return random.choices(issues, weights=weights)[0]


def sentiment_from_issue(issue, crisis_mode=False):

    if crisis_mode:
        return random.choices(
            ["negative", "neutral"],
            weights=[0.75, 0.25]
        )[0]

    return random.choices(
        ["positive", "neutral", "negative"],
        weights=[0.50, 0.25, 0.25]
    )[0]


def generate_review_text(brand, issue):

    templates = {
        "Quality": f"The overall quality of {brand} products has been inconsistent lately.",
        "Delivery": f"Delivery experience with {brand} was slower than expected.",
        "Support": f"Customer service from {brand} needs improvement.",
        "Packaging": f"Packaging could be better for {brand} products.",
        "Pricing": f"{brand} pricing feels slightly high compared to alternatives."
    }

    return templates[issue]


def fetch_google_reviews(limit=5000):

    reviews = []
    spike_day = datetime.utcnow() - timedelta(days=2)

    for _ in range(limit):

        brand = random.choice(BRANDS)
        product = random.choice(PRODUCTS[brand])
        product_key = f"{brand} | {product}"

        issue = choose_issue()

        crisis_mode = random.random() < 0.15
        spike_intensity = 0.5 if crisis_mode else 0.2

        review_text = generate_review_text(brand, issue)

        sentiment = sentiment_from_issue(issue, crisis_mode)
        sentiment_score = generate_sentiment_score(sentiment)

        timestamp = generate_timestamp(
            spike_day=spike_day,
            spike_intensity=spike_intensity
        )

        reviews.append({
            "brand_name": brand,
            "channel": "Google",
            "review_text": review_text,
            "timestamp": timestamp,
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "issue_category": issue,
            "product_name": product_key,
            "location": random.choice(LOCATIONS)
        })

    return reviews