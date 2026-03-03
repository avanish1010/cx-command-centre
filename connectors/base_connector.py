from datetime import datetime, timedelta
import random

# -----------------------------
# BRAND ECOSYSTEM
# -----------------------------

BRANDS = ["AuraWell Labs", "GlowNest", "NutraZen"]

PRODUCTS = {
    "AuraWell Labs": ["Anti-Acne Cream", "Vitamin C Serum"],
    "GlowNest": ["Vitamin C Serum", "Hair Growth Oil"],
    "NutraZen": ["Gut Balance Probiotic", "Ashwagandha Capsules"]
}

ISSUES = ["Packaging", "Quality", "Delivery", "Pricing", "Support"]


# -----------------------------
# TIMESTAMP GENERATOR
# -----------------------------

def generate_timestamp(days_back=30, spike_day=None, spike_intensity=0.0):
    """
    Generate baseline timestamps across a rolling window.

    Parameters:
    - days_back: historical window size
    - spike_day: datetime object where spike should cluster
    - spike_intensity: probability (0–1) of clustering near spike_day

    Returns:
    - datetime object
    """

    # If spike_day is defined and probability condition passes,
    # cluster timestamps around spike_day
    if spike_day and random.random() < spike_intensity:
        return spike_day + timedelta(hours=random.randint(0, 24))

    # Otherwise distribute across historical window
    return datetime.utcnow() - timedelta(days=random.randint(0, days_back))


# -----------------------------
# SENTIMENT DISTRIBUTION
# -----------------------------

def sentiment_with_bias(brand, crisis_mode=False):
    """
    Returns sentiment label based on brand-level bias.
    crisis_mode increases negative probability.
    """

    # Baseline brand distributions
    if brand == "GlowNest":
        weights = [0.65, 0.15, 0.20]  # Best performing brand
    elif brand == "NutraZen":
        weights = [0.50, 0.15, 0.35]  # Slightly weaker brand
    else:  # AuraWell Labs (Our primary brand)
        weights = [0.55, 0.15, 0.30]

    # Crisis mode amplifies negative weight
    if crisis_mode:
        weights = [
            weights[0] * 0.7,  # reduce positives
            weights[1] * 0.8,  # slightly reduce neutral
            weights[2] * 1.6   # increase negative
        ]

    sentiment = random.choices(
        ["positive", "neutral", "negative"],
        weights=weights
    )[0]

    return sentiment


# -----------------------------
# ISSUE CATEGORY GENERATOR
# -----------------------------

def generate_issue(crisis_mode=False, forced_issue=None):
    """
    Generates issue category.
    If crisis_mode is True, bias toward forced_issue.
    """

    if crisis_mode and forced_issue:
        # 60% chance of being forced issue during crisis
        if random.random() < 0.6:
            return forced_issue

    return random.choice(ISSUES)


# -----------------------------
# SENTIMENT SCORE GENERATOR
# -----------------------------

def generate_sentiment_score(sentiment):
    """
    Convert sentiment label into realistic score range.
    """

    if sentiment == "positive":
        return round(random.uniform(0.7, 1.0), 2)
    elif sentiment == "neutral":
        return round(random.uniform(0.4, 0.6), 2)
    else:
        return round(random.uniform(0.0, 0.3), 2)