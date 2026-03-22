from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from storage import ActionLog, InterestTopic, SQLiteStore
from utils import clamp, normalize_subreddit_name, tokenize


class InterestProfiler:
    ACTION_WEIGHTS = {
        "post": 0.16,
        "comment": 0.12,
        "search": 0.18,
        "browse": 0.03,
        "vote": 0.02,
    }

    def __init__(
        self, sqlite_store: SQLiteStore, seed_config: dict[str, Any] | None = None
    ):
        self.sqlite_store = sqlite_store
        self.seed_config = seed_config or {}
        self._topic_seed_map = self._compile_seed_map(self.seed_config)

    def bootstrap_from_seed(self) -> list[InterestTopic]:
        topics: list[InterestTopic] = []
        for bucket, default_weight in (("primary", 0.65), ("secondary", 0.45)):
            for entry in self.seed_config.get(bucket, []):
                topic = str(entry.get("topic") or "").strip()
                if not topic:
                    continue
                topics.append(
                    self.sqlite_store.set_interest_topic(
                        topic,
                        default_weight,
                        source="seed",
                        evidence_count=max(
                            len(entry.get("subreddits", []))
                            + len(entry.get("keywords", [])),
                            1,
                        ),
                        reason="seed bootstrap",
                    )
                )
        return topics

    def build_from_history(
        self, *, recent_days: int = 180, reset: bool = True
    ) -> list[InterestTopic]:
        if reset:
            self.sqlite_store.clear_interest_profile()
        self.bootstrap_from_seed()
        actions = self.sqlite_store.list_actions(limit=5000, days=recent_days)
        self.update_from_actions(sorted(actions, key=lambda item: item.timestamp))
        return self.sqlite_store.list_interest_topics(limit=200)

    def update_from_actions(self, recent_actions: list[ActionLog]) -> int:
        updates = 0
        for action in recent_actions:
            for topic, delta, reason in self._signals_from_action(action):
                self.sqlite_store.increment_interest(
                    topic,
                    delta,
                    source=action.action_type,
                    reason=reason,
                    observed_at=action.timestamp,
                )
                updates += 1
        return updates

    def get_interest_vector(self) -> dict[str, float]:
        now = datetime.now(timezone.utc)
        vector: dict[str, float] = {}
        for topic in self.sqlite_store.list_interest_topics(limit=500, min_weight=0.01):
            weeks = max(0.0, (now - topic.last_updated).days / 7.0)
            decayed = topic.weight * (topic.decay_rate**weeks)
            if decayed >= 0.05:
                vector[topic.topic] = round(clamp(decayed), 4)
        return vector

    def suggest_new_interests(self, limit: int = 5) -> list[str]:
        known_subreddits = {
            subreddit
            for topic in self._topic_seed_map.values()
            for subreddit in topic["subreddits"]
        }
        subreddit_counter: Counter[str] = Counter()
        for action in self.sqlite_store.list_actions(limit=2000, days=365):
            subreddit = normalize_subreddit_name(action.subreddit)
            if subreddit and subreddit.lower() not in known_subreddits:
                subreddit_counter[subreddit] += 1
        return [f"r/{name}" for name, _ in subreddit_counter.most_common(limit)]

    @staticmethod
    def _compile_seed_map(
        seed_config: dict[str, Any],
    ) -> dict[str, dict[str, set[str]]]:
        topic_map: dict[str, dict[str, set[str]]] = {}
        for bucket in ("primary", "secondary"):
            for entry in seed_config.get(bucket, []):
                topic = str(entry.get("topic") or "").strip()
                if not topic:
                    continue
                topic_map[topic] = {
                    "keywords": {
                        token
                        for keyword in entry.get("keywords", [])
                        for token in tokenize(str(keyword))
                    },
                    "subreddits": {
                        normalize_subreddit_name(subreddit).lower()
                        for subreddit in entry.get("subreddits", [])
                        if normalize_subreddit_name(subreddit)
                    },
                }
        return topic_map

    def _signals_from_action(self, action: ActionLog) -> list[tuple[str, float, str]]:
        delta = self.ACTION_WEIGHTS.get(action.action_type, 0.02)
        normalized_subreddit = normalize_subreddit_name(action.subreddit).lower()
        content_tokens = tokenize(
            " ".join([action.subreddit or "", action.content_preview or ""])
        )
        signals: list[tuple[str, float, str]] = []

        if normalized_subreddit:
            signals.append(
                (
                    f"subreddit/{normalized_subreddit}",
                    delta * 0.5,
                    f"{action.action_type} in r/{normalized_subreddit}",
                )
            )

        for topic, seed in self._topic_seed_map.items():
            keyword_overlap = (
                len(content_tokens & seed["keywords"]) / len(seed["keywords"])
                if seed["keywords"]
                else 0.0
            )
            subreddit_hit = 1.0 if normalized_subreddit in seed["subreddits"] else 0.0
            if keyword_overlap == 0.0 and subreddit_hit == 0.0:
                continue
            topic_delta = delta * (0.55 + keyword_overlap * 0.35 + subreddit_hit * 0.10)
            signals.append(
                (
                    topic,
                    round(topic_delta, 4),
                    f"{action.action_type} matched topic {topic}",
                )
            )

        return signals
