from __future__ import annotations

import unittest
from datetime import timedelta

from collector.interest_matcher import InterestMatcher
from storage import Post
from storage.models import utc_now


SEED_CONFIG = {
    "primary": [
        {
            "topic": "swimming",
            "subreddits": ["r/Swimming"],
            "keywords": ["freestyle", "training plan", "breaststroke"],
        }
    ],
    "secondary": [
        {
            "topic": "llm",
            "subreddits": ["r/LocalLLaMA"],
            "keywords": ["inference", "quantization"],
        }
    ],
    "similarity_threshold": 0.40,
    "blacklist_authors": ["spam_bot_01"],
}


class InterestMatcherTest(unittest.TestCase):
    def test_primary_subreddit_receives_baseline_plus_quality(self) -> None:
        matcher = InterestMatcher(seed_config=SEED_CONFIG)
        post = Post(
            url="https://reddit.test/r/Swimming/comments/1",
            subreddit="r/Swimming",
            title="New to swimming, is this poor etiquette in the lane?",
            author="helpful_user",
            score=24,
            num_comments=8,
            created_utc=utc_now() - timedelta(hours=2),
        )

        self.assertGreaterEqual(matcher.match_interest(post), 0.60)

    def test_learned_subreddit_signal_gives_unconfigured_baseline(self) -> None:
        matcher = InterestMatcher(
            seed_config=SEED_CONFIG,
            interest_vector={"subreddit/triathlon": 0.35},
        )
        post = Post(
            url="https://reddit.test/r/triathlon/comments/2",
            subreddit="r/triathlon",
            title="Brick workout advice for a first sprint race",
            author="tri_user",
            score=12,
            num_comments=6,
            created_utc=utc_now() - timedelta(hours=1),
        )

        self.assertGreaterEqual(matcher.match_interest(post), 0.25)

    def test_negative_checks_filter_removed_or_blacklisted_content(self) -> None:
        matcher = InterestMatcher(seed_config=SEED_CONFIG)
        removed = Post(
            url="https://reddit.test/r/Swimming/comments/3",
            subreddit="r/Swimming",
            title="[removed]",
            author="helpful_user",
        )
        blacklisted = Post(
            url="https://reddit.test/r/Swimming/comments/4",
            subreddit="r/Swimming",
            title="Useful drills",
            author="spam_bot_01",
        )
        old_post = Post(
            url="https://reddit.test/r/Swimming/comments/5",
            subreddit="r/Swimming",
            title="Old lane etiquette post",
            author="helpful_user",
            created_utc=utc_now() - timedelta(days=10),
        )

        self.assertEqual(matcher.match_interest(removed), 0.0)
        self.assertEqual(matcher.match_interest(blacklisted), 0.0)
        self.assertEqual(matcher.match_interest(old_post), 0.0)


if __name__ == "__main__":
    unittest.main()
