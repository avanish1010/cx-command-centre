import random
from datetime import datetime, timedelta

from .base_connector import (
    BRANDS,
    PRODUCTS,
    generate_timestamp,
    generate_sentiment_score
)

from utils.issue_classifier import classify_issue


EMOJIS_NEG = ["😡", "😤", "😭", "🤦‍♀️", "💔"]
EMOJIS_POS = ["😍", "✨", "❤️", "🔥", "💯"]

HASHTAGS = [
    "#qualityissue",
    "#packagingfail",
    "#deliverydelay",
    "#skincarereview",
    "#notworthit",
    "#lovethis",
    "#disappointed",
    "#honestreviews"
]

LOCATIONS = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]


def generate_instagram_caption(brand, product, crisis_mode=False):

    base_templates = [
        f"Just tried {product} from {brand} and I'm not sure about this...",
        f"{brand} what is happening with your {product}?",
        f"This {product} experience was unexpected.",
        f"Trying out {brand}'s {product} today!",
        f"Anyone else facing issues with {product}?"
    ]

    caption = random.choice(base_templates)

    if crisis_mode:
        caption += f" This is getting worse {random.choice(EMOJIS_NEG)}"

    else:
        if random.random() < 0.5:
            caption += f" {random.choice(EMOJIS_POS)}"

    caption += " " + random.choice(HASHTAGS)

    return caption


def sentiment_from_caption(text, crisis_mode=False):

    if crisis_mode:
        return random.choices(
            ["negative", "neutral"],
            weights=[0.85, 0.15]
        )[0]

    if any(tag in text for tag in ["#lovethis", "#skincarereview"]):
        return "positive"

    return random.choices(
        ["positive", "neutral", "negative"],
        weights=[0.45, 0.15, 0.40]
    )[0]


def fetch_instagram_mentions(limit=1000):

    reviews = []

    spike_day = datetime.utcnow() - timedelta(days=1)

    for _ in range(limit):

        brand = random.choice(BRANDS)
        product = random.choice(PRODUCTS[brand])

        crisis_mode = False
        spike_intensity = 0.0

        # Instagram crisis spreads fast visually
        if brand == "AuraWell Labs" and random.random() < 0.35:
            crisis_mode = True
            spike_intensity = 0.85

        caption = generate_instagram_caption(
            brand,
            product,
            crisis_mode
        )

        sentiment = sentiment_from_caption(
            caption,
            crisis_mode
        )

        sentiment_score = generate_sentiment_score(sentiment)
        source_followers = random.randint(800, 4_000_000)
        post_views = int(source_followers * random.uniform(0.3, 10.0))
        engagement_count = int(post_views * random.uniform(0.008, 0.12))

        timestamp = generate_timestamp(
            spike_day=spike_day,
            spike_intensity=spike_intensity
        )

        issue = classify_issue(caption)

        review = {
            "brand_name": brand,
            "channel": "Instagram",
            "review_text": caption,
            "timestamp": timestamp,
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "issue_category": issue,
            "product_name": f"{brand} | {product}",
            "location": random.choice(LOCATIONS),
            "source_followers": source_followers,
            "post_views": post_views,
            "engagement_count": engagement_count
        }

        reviews.append(review)

    return reviews
