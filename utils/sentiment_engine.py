import re


NEGATIVE_TERMS = {
    "bad", "poor", "worst", "terrible", "awful", "horrible", "disappointed",
    "waste", "useless", "fake", "counterfeit", "scam", "refund", "return",
    "delay", "delayed", "damaged", "broken", "leak", "late", "rash", "allergy",
    "irritation", "burning", "itching", "fraud", "expensive", "overpriced",
    "not worth", "worse", "never again", "pathetic", "hate",
    # Hinglish / Indic-English social terms
    "bekaar", "bakwas", "ghatiya", "dhokha", "nakli", "mehenga", "kharab"
}

POSITIVE_TERMS = {
    "good", "great", "excellent", "amazing", "love", "happy", "satisfied",
    "worth", "premium", "authentic", "fast", "recommended", "works", "effective",
    "awesome", "best", "fantastic", "perfect",
    # Hinglish / Indic-English social terms
    "accha", "mast", "badhiya", "sahi"
}

INTENSIFIERS = {"very", "extremely", "highly", "super", "too", "so"}
NEGATORS = {"not", "never", "no", "nahi", "nahin", "mat"}


def _tokenize(text):
    return re.findall(r"[a-zA-Z']+", (text or "").lower())


def analyze_sentiment(text):
    tokens = _tokenize(text)
    if not tokens:
        return {"sentiment": "neutral", "score": 0.0, "confidence": 0.0}

    score = 0.0
    for idx, tok in enumerate(tokens):
        prev = tokens[idx - 1] if idx > 0 else ""
        prev2 = tokens[idx - 2] if idx > 1 else ""

        is_negated = prev in NEGATORS or prev2 in NEGATORS
        boost = 1.5 if prev in INTENSIFIERS else 1.0

        if tok in POSITIVE_TERMS:
            score += (-1.0 if is_negated else 1.0) * boost
        elif tok in NEGATIVE_TERMS:
            score += (1.0 if is_negated else -1.0) * boost

    # Normalize to [-1, 1] with lightweight scaling.
    normalized = max(-1.0, min(1.0, score / 4.0))

    if normalized > 0.2:
        label = "positive"
    elif normalized < -0.2:
        label = "negative"
    else:
        label = "neutral"

    confidence = round(min(1.0, abs(normalized) + (0.1 if len(tokens) > 8 else 0.0)), 3)
    return {"sentiment": label, "score": round(normalized, 3), "confidence": confidence}

