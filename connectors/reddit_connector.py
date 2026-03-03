import random
from datetime import datetime, timedelta

from utils.issue_classifier import classify_issue
from ingestion.reddit_ingestion import (
    load_reddit_posts,
    load_reddit_comments
)

from .base_connector import (
    BRANDS,
    PRODUCTS,
    generate_timestamp,
    generate_sentiment_score
)

LOCATIONS = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]

# Reddit tends to emphasize quality & side effects
REDDIT_ISSUE_DISTRIBUTION = {
    "Quality": 0.40,
    "Delivery": 0.15,
    "Packaging": 0.20,
    "Support": 0.15,
    "Pricing": 0.10
}


def choose_reddit_issue():
    issues = list(REDDIT_ISSUE_DISTRIBUTION.keys())
    weights = list(REDDIT_ISSUE_DISTRIBUTION.values())
    return random.choices(issues, weights=weights)[0]


def sentiment_from_issue(issue, crisis_mode=False):

    if crisis_mode:
        return random.choices(
            ["negative", "neutral"],
            weights=[0.85, 0.15]
        )[0]

    return random.choices(
        ["positive", "neutral", "negative"],
        weights=[0.25, 0.25, 0.50]
    )[0]


# --------------------------
# NORMALIZE REAL DATA
# --------------------------
def normalize_post(text, timestamp):

    brand = random.choice(BRANDS)
    product = random.choice(PRODUCTS[brand])
    product_key = f"{brand} | {product}"

    issue = classify_issue(text)
    sentiment = sentiment_from_issue(issue)
    sentiment_score = generate_sentiment_score(sentiment)
    source_followers = random.randint(100, 1_500_000)
    post_views = int(source_followers * random.uniform(0.2, 6.0))
    engagement_count = int(post_views * random.uniform(0.01, 0.15))

    return {
        "brand_name": brand,
        "channel": "Reddit",
        "review_text": text,
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


# --------------------------
# SIMULATED THREADS
# --------------------------
def simulate_reddit_threads(limit=2000):

    reviews = []

    spike_day = datetime.utcnow() - timedelta(days=2)

    for _ in range(limit // 10):  # each loop = 1 thread

        brand = random.choice(BRANDS)
        product = random.choice(PRODUCTS[brand])
        product_key = f"{brand} | {product}"

        issue = choose_reddit_issue()

        crisis_mode = random.random() < 0.25
        spike_intensity = 0.8 if crisis_mode else 0.3

        # Original Post
        base_text = (
            f"Has anyone else faced {issue.lower()} issues with "
            f"{brand} {product}? I'm noticing something strange."
        )

        timestamp = generate_timestamp(
            spike_day=spike_day,
            spike_intensity=spike_intensity
        )

        sentiment = sentiment_from_issue(issue, crisis_mode)
        sentiment_score = generate_sentiment_score(sentiment)
        source_followers = random.randint(100, 1_500_000)
        post_views = int(source_followers * random.uniform(0.2, 6.0))
        engagement_count = int(post_views * random.uniform(0.01, 0.15))

        reviews.append({
            "brand_name": brand,
            "channel": "Reddit",
            "review_text": base_text,
            "timestamp": timestamp,
            "sentiment": sentiment,
            "sentiment_score": sentiment_score,
            "issue_category": issue,
            "product_name": product_key,
            "location": random.choice(LOCATIONS),
            "source_followers": source_followers,
            "post_views": post_views,
            "engagement_count": engagement_count
        })

        # Simulate comments inside thread
        for i in range(random.randint(5, 15)):

            comment_text = (
                f"I experienced similar {issue.lower()} problems with "
                f"{brand}. Definitely concerning."
            )

            comment_timestamp = timestamp + timedelta(
                hours=random.randint(1, 48)
            )

            sentiment = sentiment_from_issue(issue, crisis_mode)
            sentiment_score = generate_sentiment_score(sentiment)
            source_followers = random.randint(100, 1_500_000)
            post_views = int(source_followers * random.uniform(0.2, 6.0))
            engagement_count = int(post_views * random.uniform(0.01, 0.15))

            reviews.append({
                "brand_name": brand,
                "channel": "Reddit",
                "review_text": comment_text,
                "timestamp": comment_timestamp,
                "sentiment": sentiment,
                "sentiment_score": sentiment_score,
                "issue_category": issue,
                "product_name": product_key,
                "location": random.choice(LOCATIONS),
                "source_followers": source_followers,
                "post_views": post_views,
                "engagement_count": engagement_count
            })

    return reviews


# --------------------------
# MASTER FETCH
# --------------------------
def fetch_reddit_reviews(posts_path=None, comments_path=None,
                         post_limit=2000, comment_limit=3000,
                         simulated_limit=2000):

    reviews = []

    # REAL POSTS
    if posts_path:
        posts = load_reddit_posts(posts_path, post_limit)
        for p in posts:
            reviews.append(normalize_post(p["text"], p["timestamp"]))

    # REAL COMMENTS
    if comments_path:
        comments = load_reddit_comments(comments_path, comment_limit)
        for c in comments:
            reviews.append(normalize_post(c["text"], c["timestamp"]))

    # SIMULATED THREADS
    simulated = simulate_reddit_threads(simulated_limit)
    reviews.extend(simulated)

    return reviews
