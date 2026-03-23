from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from storage import ActionLog, SQLiteStore, SeenComment


class SQLiteStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.db"
        self.store = SQLiteStore(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_log_action_round_trip(self) -> None:
        self.store.log_action(
            ActionLog(
                action_type="comment",
                subreddit="r/Swimming",
                content_preview="Freestyle drills helped a lot.",
            )
        )
        actions = self.store.list_actions(limit=5)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action_type, "comment")
        self.assertEqual(actions[0].subreddit, "r/Swimming")

    def test_interest_increment_records_topic(self) -> None:
        self.store.increment_interest(
            "swimming",
            0.2,
            source="comment",
            reason="commented in r/Swimming",
        )
        topics = self.store.list_interest_topics(limit=10)
        self.assertEqual(len(topics), 1)
        self.assertEqual(topics[0].topic, "swimming")
        self.assertGreaterEqual(topics[0].weight, 0.2)

    def test_seen_comment_round_trip(self) -> None:
        self.store.upsert_seen_comment(
            SeenComment(
                comment_id="t1_abc",
                post_url="https://reddit.test/r/swimming/comments/1",
                author="helper",
                body_preview="Useful answer",
                is_direct_reply=True,
            )
        )

        seen = self.store.get_seen_comment("t1_abc")

        self.assertIsNotNone(seen)
        assert seen is not None
        self.assertTrue(seen.is_direct_reply)
        self.assertEqual(seen.reply_status, "pending")


if __name__ == "__main__":
    unittest.main()
