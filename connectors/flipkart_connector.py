import random
from datetime import datetime, timedelta

from .base_connector import (
    BRANDS,
    PRODUCTS,
    generate_timestamp,
    generate_sentiment_score
)

LOCATIONS = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]

FLIPKART_ISSUE_DISTRIBUTION = {
    "Delivery": 0.30,
    "Quality": 0.25,
    "Packaging": 0.20,
    "Pricing": 0.15,
    "Support": 0.10
}


def choose_issue():
    issues = list(FLIPKART_ISSUE_DISTRIBUTION.keys())
    weights = list(FLIPKART_ISSUE_DISTRIBUTION.values())
    return random.choices(issues, weights=weights)[0]


def sentiment_from_issue(issue, crisis_mode=False):

    if crisis_mode:
        return random.choices(
            ["negative", "neutral"],
            weights=[0.80, 0.20]
        )[0]

    return random.choices(
        ["positive", "neutral", "negative"],
        weights=[0.40, 0.25, 0.35]
    )[0]


def generate_review_text(brand, product, issue):

    templates = {
        "Delivery": f"{brand} {product} delivery was delayed and frustrating.",
        "Quality": f"The quality of {brand} {product} was not as expected.",
        "Packaging": f"The packaging of {brand} {product} was slightly damaged.",
        "Pricing": f"{brand} {product} seems overpriced on Flipkart.",
        "Support": f"Refund process for {brand} {product} took too long."
    }

    return templates[issue]


def fetch_flipkart_reviews(limit=5000):

    reviews = []
    spike_day = datetime.utcnow() - timedelta(days=2)

    for _ in range(limit):

        brand = random.choice(BRANDS)
        product = random.choice(PRODUCTS[brand])
        product_key = f"{brand} | {product}"

        issue = choose_issue()

        crisis_mode = random.random() < 0.22
        spike_intensity = 0.7 if crisis_mode else 0.3

        review_text = generate_review_text(brand, product, issue)

        sentiment = sentiment_from_issue(issue, crisis_mode)
        sentiment_score = generate_sentiment_score(sentiment)

        timestamp = generate_timestamp(
            spike_day=spike_day,
            spike_intensity=spike_intensity
        )

        reviews.append({
            "brand_name": brand,
            "channel": "Flipkart",
            "review_text": review_text,
            "timestamp": timestamp,
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "issue_category": issue,
            "product_name": product_key,
            "location": random.choice(LOCATIONS)
        })

    return reviews