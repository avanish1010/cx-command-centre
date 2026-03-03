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
    "accha", "mast", "badhiya", "sahi", "theek", "thik"
}

INTENSIFIERS = {"very", "extremely", "highly", "super", "too", "so"}
NEGATORS = {"not", "never", "no", "nahi", "nahin", "mat"}
CONTRAST_MARKERS = {"but", "however", "though", "although", "lekin", "par"}

NEUTRAL_PHRASES = (
    r"\bnot bad\b",
    r"\bnot terrible\b",
    r"\bnot great\b",
    r"\bnothing special\b",
    r"\bjust average\b",
    r"\bokay\b",
)

SARCASM_POSITIVE_CUES = {"great", "awesome", "amazing", "fantastic", "perfect", "love"}
SARCASM_NEGATIVE_CUES = {
    "delay", "delayed", "late", "refund", "broken", "damaged",
    "worst", "pathetic", "disappointed", "again", "another"
}


def _tokenize(text):
    return re.findall(r"[a-zA-Z']+", (text or "").lower())


def _has_neutral_phrase(text_value):
    lower_text = (text_value or "").lower()
    return any(re.search(pattern, lower_text) for pattern in NEUTRAL_PHRASES)


def _is_sarcastic_negative(tokens):
    token_set = set(tokens)
    has_positive_cue = bool(token_set.intersection(SARCASM_POSITIVE_CUES))
    has_negative_cue = bool(token_set.intersection(SARCASM_NEGATIVE_CUES))
    return has_positive_cue and has_negative_cue


def analyze_sentiment(text):
    raw_text = text or ""
    tokens = _tokenize(text)
    if not tokens:
        return {"sentiment": "neutral", "score": 0.0, "confidence": 0.0}

    if _has_neutral_phrase(raw_text):
        return {"sentiment": "neutral", "score": 0.0, "confidence": 0.72}

    score = 0.0
    positive_hits = 0
    negative_hits = 0
    has_contrast = False
    for idx, tok in enumerate(tokens):
        prev = tokens[idx - 1] if idx > 0 else ""
        prev2 = tokens[idx - 2] if idx > 1 else ""

        if tok in CONTRAST_MARKERS:
            has_contrast = True
            continue

        is_negated = prev in NEGATORS or prev2 in NEGATORS
        boost = 1.5 if prev in INTENSIFIERS else 1.0

        if tok in POSITIVE_TERMS:
            positive_hits += 1
            score += (-1.0 if is_negated else 1.0) * boost
        elif tok in NEGATIVE_TERMS:
            negative_hits += 1
            score += (1.0 if is_negated else -1.0) * boost

    # Tone handling: mixed clauses around "but/however/lekin" are often neutral.
    if has_contrast and positive_hits > 0 and negative_hits > 0 and abs(score) <= 1.5:
        score *= 0.35

    # Sarcasm handling: positive words used with explicit negative event context.
    if _is_sarcastic_negative(tokens) and negative_hits > 0:
        score = min(score, -1.2)

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
