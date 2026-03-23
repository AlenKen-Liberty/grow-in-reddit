from __future__ import annotations

import copy
import random
from typing import Any


class KarmaStrategy:
    """Phase-specific activity envelopes for the scheduler."""

    PHASES = {
        "newborn": {
            "daily_comments": (2, 4),
            "daily_posts": (0, 1),
            "daily_votes": (5, 15),
            "daily_browses": (2, 4),
            "engage_per_session": (0, 1),
            "subreddit_mix": {"farming": 0.7, "interest": 0.3},
            "reply_mode": "template",
            "comment_style": "agreeable",
            "max_comments_per_subreddit": 2,
            "avoid_types": ["controversial", "political", "nsfw"],
        },
        "infant": {
            "daily_comments": (3, 8),
            "daily_posts": (0, 2),
            "daily_votes": (10, 25),
            "daily_browses": (2, 5),
            "engage_per_session": (1, 2),
            "subreddit_mix": {"farming": 0.5, "interest": 0.5},
            "reply_mode": "template",
            "comment_style": "helpful",
            "max_comments_per_subreddit": 3,
            "avoid_types": ["controversial", "nsfw"],
        },
        "growing": {
            "daily_comments": (5, 12),
            "daily_posts": (1, 3),
            "daily_votes": (10, 30),
            "daily_browses": (2, 4),
            "engage_per_session": (1, 3),
            "subreddit_mix": {"farming": 0.3, "interest": 0.7},
            "reply_mode": "llm",
            "comment_style": "expert",
            "max_comments_per_subreddit": 4,
            "avoid_types": ["nsfw"],
        },
        "established": {
            "daily_comments": (3, 10),
            "daily_posts": (1, 4),
            "daily_votes": (5, 20),
            "daily_browses": (1, 3),
            "engage_per_session": (1, 3),
            "subreddit_mix": {"farming": 0.1, "interest": 0.9},
            "reply_mode": "llm",
            "comment_style": "authentic",
            "max_comments_per_subreddit": 5,
            "avoid_types": [],
        },
    }

    DEFAULT_FARMING_SUBREDDITS = [
        "AskReddit",
        "todayilearned",
        "LifeProTips",
        "NoStupidQuestions",
        "mildlyinteresting",
        "TooAfraidToAsk",
        "DoesAnybodyElse",
        "Showerthoughts",
    ]

    @classmethod
    def get_phase_config(
        cls,
        phase: str,
        *,
        farming_subreddits: list[str] | None = None,
    ) -> dict[str, Any]:
        config = copy.deepcopy(cls.PHASES.get(phase, cls.PHASES["established"]))
        config["farming_subreddits"] = list(
            farming_subreddits or cls.DEFAULT_FARMING_SUBREDDITS
        )
        return config

    @classmethod
    def pick_daily_count(cls, phase: str, key: str) -> int:
        lo, hi = cls.PHASES.get(phase, cls.PHASES["established"])[key]
        return random.randint(lo, hi)

