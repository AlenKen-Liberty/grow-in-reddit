from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from replier.thread_tracker import ThreadTracker
from storage import Comment, Post, PostDetail, SQLiteStore, TrackedPost
from storage.models import utc_now


class FakeBrowser:
    def __init__(self, detail: PostDetail) -> None:
        self.detail = detail

    def get_post_detail(self, post_url: str) -> PostDetail:
        return self.detail


class ThreadTrackerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.tmpdir.name) / "test.db")

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_check_new_replies_marks_seen_and_deduplicates(self) -> None:
        tracked = TrackedPost(
            url="https://reddit.test/r/swimming/comments/post1",
            subreddit="r/Swimming",
            title="Lane etiquette question",
            posted_at=utc_now() - timedelta(hours=2),
        )
        self.store.track_post(tracked)
        detail = PostDetail(
            post=Post(
                url=tracked.url,
                subreddit="r/Swimming",
                title=tracked.title,
                id="t3_post1",
                author="me",
            ),
            comments=[
                Comment(
                    id="t1_reply1",
                    post_url=tracked.url,
                    author="helpful_user",
                    body="I would stay to one side of the lane.",
                    parent_id="t3_post1",
                    depth=0,
                )
            ],
        )
        tracker = ThreadTracker(FakeBrowser(detail), self.store, own_username="me")

        first = tracker.check_new_replies()
        second = tracker.check_new_replies()

        self.assertEqual(len(first), 1)
        self.assertTrue(first[0].is_direct_reply)
        self.assertEqual(first[0].comment.id, "t1_reply1")
        self.assertEqual(second, [])
        seen = self.store.get_seen_comment("t1_reply1")
        self.assertIsNotNone(seen)
        assert seen is not None
        self.assertEqual(seen.reply_status, "pending")

    def test_list_pending_replies_returns_unreplied_items(self) -> None:
        tracked = TrackedPost(
            url="https://reddit.test/r/swimming/comments/post2",
            subreddit="r/Swimming",
            title="Kick timing question",
            posted_at=utc_now() - timedelta(hours=2),
        )
        self.store.track_post(tracked)
        detail = PostDetail(
            post=Post(
                url=tracked.url,
                subreddit="r/Swimming",
                title=tracked.title,
                id="t3_post2",
                author="me",
            ),
            comments=[
                Comment(
                    id="t1_reply2",
                    post_url=tracked.url,
                    author="helpful_user",
                    body="Try a softer two-beat kick first.",
                    parent_id="t3_post2",
                    depth=0,
                )
            ],
        )
        tracker = ThreadTracker(FakeBrowser(detail), self.store, own_username="me")

        tracker.check_new_replies()
        pending = tracker.list_pending_replies()

        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].comment.id, "t1_reply2")
        tracker.mark_replied("t1_reply2")
        self.assertEqual(tracker.list_pending_replies(), [])


if __name__ == "__main__":
    unittest.main()
