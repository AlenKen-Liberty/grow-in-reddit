from __future__ import annotations

import re

TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_+.-]{1,}")
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "been",
    "but",
    "by",
    "for",
    "from",
    "had",
    "has",
    "have",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "my",
    "of",
    "on",
    "or",
    "our",
    "out",
    "so",
    "that",
    "the",
    "their",
    "them",
    "there",
    "they",
    "this",
    "to",
    "up",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "with",
    "you",
    "your",
}


def clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def normalize_subreddit_name(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    text = text.strip("/")
    if text.lower().startswith("r/"):
        text = text[2:]
    return text


def tokenize(text: str | None) -> set[str]:
    if not text:
        return set()
    return {
        match.group(0).lower()
        for match in TOKEN_RE.finditer(text)
        if match.group(0).lower() not in STOPWORDS
    }


def extract_preview(text: str | None, max_length: int = 200) -> str:
    if not text:
        return ""
    normalized = " ".join(text.split())
    if len(normalized) <= max_length:
        return normalized
    return normalized[: max_length - 3] + "..."
