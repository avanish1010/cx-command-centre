import json
import random
from datetime import datetime, timedelta
from utils.issue_classifier import classify_issue

from .base_connector import (
    BRANDS,
    generate_timestamp,
    sentiment_with_bias,
    generate_sentiment_score
)

# Crisis distribution for Amazon (quality-heavy platform)
CRISIS_DISTRIBUTION = {
    "Quality": 0.35,
    "Packaging": 0.20,
    "Delivery": 0.20,
    "Pricing": 0.15,
    "Support": 0.10
}

def choose_crisis_issue():
    issues = list(CRISIS_DISTRIBUTION.keys())
    weights = list(CRISIS_DISTRIBUTION.values())
    return random.choices(issues, weights=weights)[0]


def fetch_amazon_reviews(path, limit=5000):

    reviews = []
    spike_day = datetime.utcnow() - timedelta(days=2)

    asin_to_brand = {}
    asin_crisis_map = {}

    with open(path, "r", encoding="utf-8") as f:

        for i, line in enumerate(f):

            if i >= limit:
                break

            data = json.loads(line)

            asin = data.get("asin")
            rating = data.get("rating")
            text = data.get("text")
            timestamp_ms = data.get("timestamp")

            if not asin or not rating or not text or not timestamp_ms:
                continue

            # Convert timestamp
            timestamp_real = datetime.fromtimestamp(timestamp_ms / 1000)

            # Assign brand to ASIN
            if asin not in asin_to_brand:
                asin_to_brand[asin] = random.choice(BRANDS)

            brand = asin_to_brand[asin]
            full_product_name = f"{brand} | {asin}"

            # Rating → Sentiment
            if rating >= 4:
                sentiment = "positive"
            elif rating == 3:
                sentiment = "neutral"
            else:
                sentiment = "negative"

            # ----------------------------------------
            # LOGICAL CRISIS WINDOW (ASIN LEVEL)
            # ----------------------------------------
            crisis_mode = False
            spike_intensity = 0.0

            # Assign some ASINs to have temporary crisis
            if asin not in asin_crisis_map:
                # Only 15% of products enter crisis window
                if random.random() < 0.15:
                    asin_crisis_map[asin] = choose_crisis_issue()
                else:
                    asin_crisis_map[asin] = None

            crisis_issue = asin_crisis_map[asin]

            if crisis_issue:
                # Amazon crisis builds slowly
                if random.random() < 0.30:
                    crisis_mode = True
                    spike_intensity = 0.6
                    sentiment = sentiment_with_bias(brand, crisis_mode=True)

            # ----------------------------------------
            # Issue Classification
            # ----------------------------------------
            issue = classify_issue(text)

            # If crisis mode active, override occasionally
            if crisis_mode and random.random() < 0.50:
                issue = crisis_issue

            sentiment_score = generate_sentiment_score(sentiment)

            final_timestamp = generate_timestamp(
                spike_day=spike_day,
                spike_intensity=spike_intensity
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
                "location": "India"
            }

            reviews.append(review)

    return reviews