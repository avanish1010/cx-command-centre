import re
import unicodedata


NEGATIVE_TERMS = {
    "bad", "poor", "worst", "terrible", "awful", "horrible", "disappointed",
    "waste", "useless", "fake", "counterfeit", "scam", "refund", "return",
    "delay", "delayed", "damaged", "broken", "leak", "late", "rash", "allergy",
    "irritation", "burning", "itching", "fraud", "expensive", "overpriced",
    "not worth", "worse", "never again", "pathetic", "hate",
    # Hinglish / Indic-English social terms
    "bekaar", "bakwas", "ghatiya", "dhokha", "nakli", "mehenga", "kharab",
    # Hindi (Devanagari)
    "बेकार", "घटिया", "खराब", "नकली", "महंगा", "धोखा", "देरी", "निराश",
    # Spanish
    "malo", "peor", "horrible", "falso", "caro", "tarde", "tarda", "estafa", "reembolso"
}

POSITIVE_TERMS = {
    "good", "great", "excellent", "amazing", "love", "happy", "satisfied",
    "worth", "premium", "authentic", "fast", "recommended", "works", "effective",
    "awesome", "best", "fantastic", "perfect",
    # Hinglish / Indic-English social terms
    "accha", "mast", "badhiya", "sahi", "theek", "thik", "resolve",
    # Hindi (Devanagari)
    "अच्छा", "बहुतिया", "बढ़िया", "शानदार", "संतुष्ट", "तेज", "असली",
    # Spanish
    "bueno", "buena", "excelente", "genial", "feliz", "satisfecho", "autentico", "auténtico"
}

INTENSIFIERS = {"very", "extremely", "highly", "super", "too", "so"}
NEGATORS = {"not", "never", "no", "nahi", "nahin", "mat", "nunca", "ni", "नहीं", "मत"}
CONTRAST_MARKERS = {"but", "however", "though", "although", "lekin", "par", "pero", "लेकिन"}

NEUTRAL_PHRASES = (
    r"\bnot bad\b",
    r"\bnot terrible\b",
    r"\bnot great\b",
    r"\bnothing special\b",
    r"\bjust average\b",
    r"\bokay\b",
    r"\baverage\b",
    r"ठीक है",
    r"कुछ खास नहीं",
    r"regular",
    r"normal",
    r"mas o menos",
    r"más o menos",
)

NEGATIVE_PHRASES = (
    "no response",
    "not worth",
    "never again",
    "waste of money",
    "late delivery",
    "delayed delivery",
    "customer support not responding",
    "sin respuesta",
    "no vale la pena",
    "muy caro",
    "muy mala",
    "बहुत खराब",
    "कोई जवाब नहीं",
    "पैसे की बर्बादी",
    "बहुत महंगा",
)

POSITIVE_PHRASES = (
    "highly recommended",
    "very good",
    "works perfectly",
    "authentic item",
    "fast delivery",
    "muy bueno",
    "excelente producto",
    "muy satisfecho",
    "बहुत अच्छा",
    "बहुत बढ़िया",
    "बहुतिया",
    "संतुष्ट हूं",
)

SARCASM_POSITIVE_CUES = {"great", "awesome", "amazing", "fantastic", "perfect", "love"}
SARCASM_NEGATIVE_CUES = {
    "delay", "delayed", "late", "refund", "broken", "damaged",
    "worst", "pathetic", "disappointed", "again", "another", "useless", "fake", "counterfeit"
}

NON_FLIPPABLE_NEGATIVE_TOKENS = {"refund", "return", "reembolso"}


def _tokenize(text):
    # Script-agnostic tokenizer: normalize, split by whitespace, strip punctuation.
    normalized = unicodedata.normalize("NFKC", text or "").lower()
    raw_tokens = normalized.split()
    tokens = []
    for tok in raw_tokens:
        clean_tok = tok.strip(".,!?;:\"'()[]{}<>/\\|-_`~@#$%^&*+=…।॥")
        if clean_tok and not clean_tok.isdigit():
            tokens.append(clean_tok)
    return tokens


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

    lower_text = raw_text.lower()
    score = 0.0
    positive_hits = 0
    negative_hits = 0
    has_contrast = False

    # Multi-word phrase cues improve tone and multilingual reliability.
    phrase_positive_hits = sum(1 for phrase in POSITIVE_PHRASES if phrase in lower_text)
    phrase_negative_hits = sum(1 for phrase in NEGATIVE_PHRASES if phrase in lower_text)
    if phrase_positive_hits:
        positive_hits += phrase_positive_hits
        score += phrase_positive_hits * 1.2
    if phrase_negative_hits:
        negative_hits += phrase_negative_hits
        score -= phrase_negative_hits * 1.2

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
            if tok in NON_FLIPPABLE_NEGATIVE_TOKENS:
                score += -1.0 * boost
            else:
                score += (1.0 if is_negated else -1.0) * boost

    # Tone handling: mixed clauses around "but/however/lekin" are often neutral.
    if has_contrast and positive_hits > 0 and negative_hits > 0 and abs(score) <= 1.5:
        score *= 0.35

    # Sarcasm handling: positive words used with explicit negative event context.
    if _is_sarcastic_negative(tokens) and negative_hits > 0:
        score = min(score, -1.2)
    elif positive_hits > 0 and negative_hits > 0 and {"useless", "fake", "counterfeit", "pathetic", "worst"}.intersection(set(tokens)):
        score = min(score, -0.8)

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
