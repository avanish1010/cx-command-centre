import json
import random
from datetime import datetime, timedelta

from utils.issue_classifier import classify_issue

from .base_connector import (
    BRANDS,
    generate_timestamp,
    sentiment_with_bias,
    generate_sentiment_score,
)

# Crisis distribution for Amazon (quality-heavy platform)
CRISIS_DISTRIBUTION = {
    "Quality": 0.35,
    "Packaging": 0.20,
    "Delivery": 0.20,
    "Pricing": 0.15,
    "Support": 0.10,
}


def choose_crisis_issue():
    issues = list(CRISIS_DISTRIBUTION.keys())
    weights = list(CRISIS_DISTRIBUTION.values())
    return random.choices(issues, weights=weights)[0]


def _synthetic_amazon_text(brand, asin):
    templates = [
        f"{brand} product {asin} arrived with damaged packaging.",
        f"Delivery for {brand} {asin} was delayed and support did not help.",
        f"Quality of {brand} {asin} feels inconsistent this month.",
        f"{brand} {asin} is overpriced for the value.",
        f"Good experience overall with {brand} {asin}, authentic and effective.",
    ]
    return random.choice(templates)


def fetch_amazon_reviews(path=None, limit=5000):
    reviews = []
    spike_day = datetime.utcnow() - timedelta(days=2)

    asin_to_brand = {}
    asin_crisis_map = {}

    data_rows = []
    if path:
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= limit:
                    break
                data_rows.append(json.loads(line))
    else:
        for _ in range(limit):
            asin_value = f"B0{random.randint(100000000, 999999999)}"
            rating_value = random.randint(1, 5)
            data_rows.append(
                {
                    "asin": asin_value,
                    "rating": rating_value,
                    "text": _synthetic_amazon_text(random.choice(BRANDS), asin_value),
                    "timestamp": int(datetime.utcnow().timestamp() * 1000),
                }
            )

    for data in data_rows:
        asin = data.get("asin")
        rating = data.get("rating")
        text = data.get("text")
        timestamp_ms = data.get("timestamp")

        if not asin or rating is None or not text or not timestamp_ms:
            continue

        if asin not in asin_to_brand:
            asin_to_brand[asin] = random.choice(BRANDS)

        brand = asin_to_brand[asin]
        full_product_name = f"{brand} | {asin}"

        if rating >= 4:
            sentiment = "positive"
        elif rating == 3:
            sentiment = "neutral"
        else:
            sentiment = "negative"

        crisis_mode = False
        spike_intensity = 0.0

        if asin not in asin_crisis_map:
            asin_crisis_map[asin] = choose_crisis_issue() if random.random() < 0.15 else None

        crisis_issue = asin_crisis_map[asin]

        if crisis_issue and random.random() < 0.30:
            crisis_mode = True
            spike_intensity = 0.6
            sentiment = sentiment_with_bias(brand, crisis_mode=True)

        issue = classify_issue(text)
        if crisis_mode and random.random() < 0.50:
            issue = crisis_issue

        sentiment_score = generate_sentiment_score(sentiment)

        final_timestamp = generate_timestamp(
            spike_day=spike_day,
            spike_intensity=spike_intensity,
        )

        review = {
            "brand_name": brand,
            "channel": "Amazon",
            "review_text": text,
            "timestamp": final_timestamp,
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "issue_category": issue,
            "product_name": full_product_name,
            "location": "India",
        }

        reviews.append(review)

    return reviews
