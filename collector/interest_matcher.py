from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from storage import Post
from utils import clamp, normalize_subreddit_name, tokenize


@dataclass(frozen=True, slots=True)
class TopicRule:
    topic: str
    keywords: frozenset[str]
    subreddits: frozenset[str]
    default_weight: float


class InterestMatcher:
    """
    Phase-1 lexical matcher.

    The design doc calls for dense embeddings and Qdrant. This version provides a
    deterministic fallback so collection can start before vector infra is connected.
    """

    def __init__(
        self,
        *,
        interest_vector: dict[str, float] | None = None,
        seed_config: dict[str, Any] | None = None,
    ):
        self.interest_vector = interest_vector or {}
        self.seed_config = seed_config or {}
        self.default_threshold = float(
            self.seed_config.get("similarity_threshold", 0.65)
        )
        self.topic_rules = self._compile_topic_rules(self.seed_config)

    def refresh_interest_vector(self, interest_vector: dict[str, float]) -> None:
        self.interest_vector = interest_vector

    def embed_text(self, text: str) -> list[float]:
        raise NotImplementedError(
            "Embedding-based matching is not wired yet. Use match_interest() for the lexical fallback."
        )

    def match_interest(self, post: Post) -> float:
        normalized_subreddit = normalize_subreddit_name(post.subreddit).lower()
        text_tokens = tokenize(" ".join([post.subreddit, post.title, post.body]))
        best_score = 0.0

        for rule in self.topic_rules:
            keyword_overlap = (
                len(text_tokens & set(rule.keywords)) / len(rule.keywords)
                if rule.keywords
                else 0.0
            )
            subreddit_match = 1.0 if normalized_subreddit in rule.subreddits else 0.0
            learned_weight = self.interest_vector.get(rule.topic, rule.default_weight)
            subreddit_signal = self.interest_vector.get(
                f"subreddit/{normalized_subreddit}", 0.0
            )
            raw_score = (
                keyword_overlap * 0.65
                + subreddit_match * 0.25
                + clamp(subreddit_signal) * 0.10
            )
            best_score = max(best_score, clamp(raw_score * max(0.15, learned_weight)))

        return round(best_score, 4)

    def find_relevant_posts(
        self, posts: list[Post], threshold: float | None = None
    ) -> list[Post]:
        cutoff = self.default_threshold if threshold is None else threshold
        relevant: list[Post] = []
        for post in posts:
            post.interest_score = self.match_interest(post)
            if post.interest_score >= cutoff:
                relevant.append(post)
        return relevant

    @staticmethod
    def _compile_topic_rules(seed_config: dict[str, Any]) -> list[TopicRule]:
        topic_rules: list[TopicRule] = []
        for bucket, default_weight in (("primary", 0.7), ("secondary", 0.45)):
            for entry in seed_config.get(bucket, []):
                topic = str(entry.get("topic") or "").strip()
                if not topic:
                    continue
                keywords: set[str] = set()
                for keyword in entry.get("keywords", []):
                    keywords.update(tokenize(str(keyword)))
                subreddits = {
                    normalize_subreddit_name(subreddit).lower()
                    for subreddit in entry.get("subreddits", [])
                    if normalize_subreddit_name(subreddit)
                }
                topic_rules.append(
                    TopicRule(
                        topic=topic,
                        keywords=frozenset(keywords),
                        subreddits=frozenset(subreddits),
                        default_weight=default_weight,
                    )
                )
        return topic_rules
