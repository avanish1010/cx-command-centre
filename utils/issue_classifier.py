import re
from collections import defaultdict

# Expanded and normalized keyword sets
ISSUE_KEYWORDS = {
    "Packaging": [
        "packag", "box", "seal", "sealed",
        "damag", "broken", "leak", "spill",
        "container", "bottle", "cap", "lid",
        "torn", "crush", "wrapper"
    ],
    "Quality": [
        "quality", "defect", "fake", "expire",
        "bad", "poor", "not work", "ineffective",
        "allerg", "rash", "reaction", "side effect",
        "irritat", "itch", "burn", "redness",
        "smell", "taste", "texture",
        "greasy", "sticky", "watery",
        "formula", "changed", "different",
        "result", "effect", "useless"
    ],
    "Side Effects": [
        "side effect", "reaction", "allergy", "allergic", "rash", "irritation",
        "itch", "itching", "burn", "burning", "redness", "swelling", "pimples",
        "breakout", "acne", "inflam"
    ],
    "Delivery": [
        "delivery", "late", "delay",
        "shipment", "arriv", "courier",
        "dispatch", "tracking"
    ],
    "Trust": [
        "fake", "counterfeit", "scam", "fraud", "not authentic", "duplicate",
        "tampered", "suspicious", "misleading", "dhokha", "nakli"
    ],
    "Pricing": [
        "price", "expens", "overprice",
        "costly", "discount", "cheap",
        "value for money", "worth"
    ],
    "Support": [
        "support", "customer service",
        "no response", "helpdesk",
        "refund", "replacement",
        "return", "complaint",
        "contact", "call center"
    ]
}


NEGATIVE_WORDS = [
    "bad", "poor", "worst", "terrible",
    "awful", "horrible", "disappointed",
    "waste", "useless", "never again", "bakwas", "bekaar", "ghatiya", "dhokha"
]


def classify_issue(text, sentiment=None):

    if not text:
        return "Other"

    text = text.lower()

    issue_scores = defaultdict(int)

    # Count keyword hits per issue
    for issue, keywords in ISSUE_KEYWORDS.items():
        for keyword in keywords:
            if re.search(rf"{keyword}", text):
                issue_scores[issue] += 1

    # If we found at least one keyword
    if issue_scores:
        # Return issue with highest score
        return max(issue_scores, key=issue_scores.get)

    # Secondary logic: detect strong negative tone
    negative_hits = sum(1 for word in NEGATIVE_WORDS if word in text)

    if negative_hits > 0:
        return "Quality"

    # If sentiment explicitly negative but no keyword
    if sentiment == "negative":
        return "Quality"

    return "Other"
