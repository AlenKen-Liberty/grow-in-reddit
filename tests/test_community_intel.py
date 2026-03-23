from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from reddit_memory.community_intel import CommunityIntelligence
from storage import Comment, CommunitySnapshot, Post, PostDetail, SQLiteStore
from storage.models import UserProfile, utc_now


class FakeBrowser:
    def __init__(self) -> None:
        now = utc_now()
        self.hot_posts = [
            Post(
                url="https://reddit.test/r/swimming/comments/1",
                subreddit="r/Swimming",
                title="Form check help",
                author="alpha",
                score=15,
                num_comments=4,
                created_utc=now - timedelta(hours=4),
            )
        ]
        self.new_posts = [
            self.hot_posts[0],
            Post(
                url="https://reddit.test/r/swimming/comments/2",
                subreddit="r/Swimming",
                title="Kickboard drills",
                author="beta",
                score=8,
                num_comments=1,
                created_utc=now - timedelta(hours=2),
            ),
        ]

    def get_subreddit_feed(self, subreddit: str, sort: str = "hot", limit: int = 25) -> list[Post]:
        return list(self.hot_posts if sort == "hot" else self.new_posts)

    def get_post_detail(self, post_url: str) -> PostDetail:
        if post_url.endswith("/1"):
            post = Post(
                url=post_url,
                subreddit="r/Swimming",
                title="Form check help",
                author="alpha",
                score=22,
                num_comments=7,
            )
        else:
            post = Post(
                url=post_url,
                subreddit="r/Swimming",
                title="Kickboard drills",
                author="beta",
                score=11,
                num_comments=3,
            )
        return PostDetail(
            post=post,
            comments=[
                Comment(
                    id="t1_mod",
                    post_url=post_url,
                    author="AutoModerator",
                    body="Please remember the sidebar rules.",
                )
            ],
        )

    def get_user_profile(self, username: str) -> UserProfile:
        totals = {"alpha": 4000, "beta": 1200}
        return UserProfile(
            username=username,
            karma_post=totals.get(username, 0),
            karma_comment=0,
        )


class CommunityIntelligenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.tmpdir.name) / "test.db")
        self.browser = FakeBrowser()
        self.intel = CommunityIntelligence(self.browser, self.store)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_collect_and_revisit_snapshots(self) -> None:
        collected = self.intel.collect_snapshot("r/Swimming")
        revisited = self.intel.revisit_snapshots(hours_ago=0)

        self.assertEqual(collected, 2)
        self.assertEqual(revisited, 2)
        snapshots = self.store.list_community_snapshots(subreddit="r/Swimming", limit=10)
        self.assertEqual(len(snapshots), 2)
        self.assertTrue(any(snapshot.score_after_24h == 22 for snapshot in snapshots))

    def test_identify_power_users_persists_profiles(self) -> None:
        now = utc_now()
        for idx in range(3):
            self.store.upsert_community_snapshot(
                CommunitySnapshot(
                    subreddit="r/Swimming",
                    post_url=f"https://reddit.test/r/swimming/comments/a{idx}",
                    title=f"Alpha {idx}",
                    author="alpha",
                    score_at_capture=20 + idx,
                    captured_at=now - timedelta(days=1),
                )
            )
            self.store.upsert_community_snapshot(
                CommunitySnapshot(
                    subreddit="r/Swimming",
                    post_url=f"https://reddit.test/r/swimming/comments/b{idx}",
                    title=f"Beta {idx}",
                    author="beta",
                    score_at_capture=5 + idx,
                    captured_at=now - timedelta(days=1),
                )
            )

        power_users = self.intel.identify_power_users("r/Swimming")

        self.assertEqual(len(power_users), 1)
        self.assertEqual(power_users[0]["username"], "alpha")
        stored = self.store.list_community_power_users(subreddit="r/Swimming", limit=10)
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].estimated_karma, 4000)


if __name__ == "__main__":
    unittest.main()
