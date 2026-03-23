from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from storage import Post
from utils import clamp, normalize_subreddit_name, tokenize


@dataclass(frozen=True, slots=True)
class TopicRule:
    topic: str
    keywords: frozenset[str]
    subreddits: frozenset[str]
    default_weight: float
    bucket: str


class InterestMatcher:
    """
    Phase-2 deterministic hybrid matcher.

    The browser collector still runs without embeddings/Qdrant, so the matcher leans
    on subreddit affinity first and uses keyword overlap plus lightweight quality
    signals as additive evidence.
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
            self.seed_config.get("similarity_threshold", 0.40)
        )
        self.topic_rules = self._compile_topic_rules(self.seed_config)
        self.blacklist_authors = {
            str(author).strip().lower()
            for author in self.seed_config.get("blacklist_authors", [])
            if str(author).strip()
        }
        self.configured_subreddits = {
            subreddit for rule in self.topic_rules for subreddit in rule.subreddits
        }

    def refresh_interest_vector(self, interest_vector: dict[str, float]) -> None:
        self.interest_vector = interest_vector

    def embed_text(self, text: str) -> list[float]:
        raise NotImplementedError(
            "Embedding-based matching is not wired yet. Use match_interest() for the lexical fallback."
        )

    def match_interest(self, post: Post) -> float:
        if self._negative_check(post):
            return 0.0

        normalized_subreddit = normalize_subreddit_name(post.subreddit).lower()
        subreddit_score = self._subreddit_score(normalized_subreddit)
        keyword_score = self._keyword_score(post, normalized_subreddit)
        quality_score = self._quality_score(post)
        learned_weight = self._learned_weight(normalized_subreddit)

        raw_score = subreddit_score + keyword_score + quality_score
        boosted_score = clamp(raw_score * learned_weight)
        if subreddit_score > 0:
            boosted_score = max(
                boosted_score, clamp(subreddit_score + keyword_score * 0.85 + quality_score)
            )
        return round(clamp(boosted_score), 4)

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

    def _negative_check(self, post: Post) -> bool:
        title = (post.title or "").strip().lower()
        body = (post.body or "").strip().lower()
        author = (post.author or "").strip().lower()
        if "[deleted]" in title or "[removed]" in title:
            return True
        if body in {"[deleted]", "[removed]"}:
            return True
        if author and author in self.blacklist_authors:
            return True
        if post.score < 0:
            return True
        return post.created_utc < datetime.now(timezone.utc) - timedelta(days=7)

    def _subreddit_score(self, normalized_subreddit: str) -> float:
        if not normalized_subreddit:
            return 0.0
        bucket_scores = {"primary": 0.50, "secondary": 0.35}
        score = 0.0
        for rule in self.topic_rules:
            if normalized_subreddit in rule.subreddits:
                score = max(score, bucket_scores.get(rule.bucket, 0.0))
        if score > 0.0:
            return score
        subreddit_signal = clamp(
            self.interest_vector.get(f"subreddit/{normalized_subreddit}", 0.0)
        )
        if subreddit_signal >= 0.05:
            return 0.15
        return 0.0

    def _keyword_score(self, post: Post, normalized_subreddit: str) -> float:
        text_tokens = tokenize(" ".join([post.subreddit, post.title, post.body]))
        best_score = 0.0
        for rule in self.topic_rules:
            if not rule.keywords:
                continue
            overlap = len(text_tokens & set(rule.keywords)) / len(rule.keywords)
            if overlap <= 0:
                continue
            learned_weight = self.interest_vector.get(rule.topic, rule.default_weight)
            subreddit_bonus = 0.04 if normalized_subreddit in rule.subreddits else 0.0
            keyword_score = overlap * 0.26 * (0.7 + clamp(learned_weight))
            best_score = max(best_score, clamp(keyword_score + subreddit_bonus, 0.0, 0.30))
        return round(best_score, 4)

    def _quality_score(self, post: Post) -> float:
        score = 0.0
        if post.score > 10:
            score += 0.05
        if post.num_comments > 5:
            score += 0.05
        if post.created_utc >= datetime.now(timezone.utc) - timedelta(hours=6):
            score += 0.05
        if (post.author_karma or 0) > 1000:
            score += 0.05
        return round(clamp(score, 0.0, 0.20), 4)

    def _learned_weight(self, normalized_subreddit: str) -> float:
        subreddit_signal = clamp(
            self.interest_vector.get(f"subreddit/{normalized_subreddit}", 0.0)
        )
        topic_weights = [
            self.interest_vector.get(rule.topic, rule.default_weight)
            for rule in self.topic_rules
            if normalized_subreddit in rule.subreddits
        ]
        if topic_weights:
            return clamp(max(max(topic_weights), 0.65))
        if subreddit_signal > 0:
            return clamp(0.75 + subreddit_signal * 0.25)
        return 1.0

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
                        bucket=bucket,
                    )
                )
        return topic_rules
