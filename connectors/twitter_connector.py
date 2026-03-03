import random
from datetime import datetime, timedelta
from utils.issue_classifier import classify_issue

from .base_connector import (
    BRANDS,
    PRODUCTS,
    generate_timestamp,
    generate_sentiment_score
)

DELIVERY_PARTNERS = ["Delhivery", "BlueDart", "EcomExpress", "Amazon Logistics"]
LOCATIONS = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]
HASHTAGS = ["#fail", "#disappointed", "#qualityissue", "#deliverydelay", "#unhappy"]

# Twitter crisis issue weighting (more support/delivery heavy)
CRISIS_DISTRIBUTION = {
    "Support": 0.30,
    "Delivery": 0.30,
    "Quality": 0.20,
    "Packaging": 0.15,
    "Pricing": 0.05
}


def choose_crisis_issue():
    issues = list(CRISIS_DISTRIBUTION.keys())
    weights = list(CRISIS_DISTRIBUTION.values())
    return random.choices(issues, weights=weights)[0]


def generate_order_id():
    return f"ORD{random.randint(10000, 99999)}"


def generate_tweet_text(brand, product, forced_issue=None, crisis_mode=False):

    order_id = generate_order_id()
    location = random.choice(LOCATIONS)
    partner = random.choice(DELIVERY_PARTNERS)
    hashtag = random.choice(HASHTAGS)

    templates = {
        "Packaging": f"My {product} from {brand} arrived with damaged packaging. {hashtag}",
        "Quality": f"{brand} quality has dropped recently. Very disappointed.",
        "Delivery": f"Delivery from {partner} for {brand} was delayed again in {location}.",
        "Support": f"Customer support from {brand} is not responding to Order #{order_id}.",
        "Pricing": f"{brand} {product} feels overpriced for the quality offered."
    }

    if forced_issue and forced_issue in templates:
        base = templates[forced_issue]
    else:
        base = random.choice(list(templates.values()))

    if crisis_mode:
        base += f" This is getting worse every day!"

    return base


def sentiment_from_text(text, crisis_mode=False):

    text = text.lower()

    if crisis_mode:
        return random.choices(
            ["negative", "neutral"],
            weights=[0.90, 0.10]
        )[0]

    if any(word in text for word in ["damaged", "defective", "delayed", "broken", "disappointed", "refund"]):
        return "negative"

    if any(word in text for word in ["good", "great", "love", "happy"]):
        return "positive"

    return random.choice(["neutral", "negative"])


def fetch_twitter_mentions(limit=5000):

    reviews = []
    spike_day = datetime.utcnow() - timedelta(days=1)

    product_crisis_map = {}

    for _ in range(limit):

        brand = random.choice(BRANDS)
        product = random.choice(PRODUCTS[brand])
        product_key = f"{brand} | {product}"

        crisis_mode = False
        spike_intensity = 0.0

        # Assign some products to temporary crisis
        if product_key not in product_crisis_map:
            if random.random() < 0.20:
                product_crisis_map[product_key] = choose_crisis_issue()
            else:
                product_crisis_map[product_key] = None

        crisis_issue = product_crisis_map[product_key]

        if crisis_issue and random.random() < 0.50:
            crisis_mode = True
            spike_intensity = 0.9

        tweet_text = generate_tweet_text(
            brand,
            product,
            forced_issue=crisis_issue,
            crisis_mode=crisis_mode
        )

        issue = classify_issue(tweet_text)
        sentiment = sentiment_from_text(tweet_text, crisis_mode)
        sentiment_score = generate_sentiment_score(sentiment)
        source_followers = random.randint(500, 3_000_000)
        post_views = int(source_followers * random.uniform(0.2, 8.0))
        engagement_count = int(post_views * random.uniform(0.005, 0.08))

        timestamp = generate_timestamp(
            spike_day=spike_day,
            spike_intensity=spike_intensity
        )

        review = {
            "brand_name": brand,
            "channel": "Twitter",
            "review_text": tweet_text,
            "timestamp": timestamp,
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "issue_category": issue,
            "product_name": product_key,
            "location": random.choice(LOCATIONS),
            "source_followers": source_followers,
            "post_views": post_views,
            "engagement_count": engagement_count
        }

        reviews.append(review)

    return reviews
