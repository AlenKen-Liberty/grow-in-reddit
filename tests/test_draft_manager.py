from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from poster import DraftManager


class DraftManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.drafts_dir = Path(self.tmpdir.name) / "drafts"
        self.posted_dir = self.drafts_dir / "posted"
        self.drafts_dir.mkdir(parents=True, exist_ok=True)
        self.manager = DraftManager(
            drafts_dir=self.drafts_dir,
            posted_dir=self.posted_dir,
        )

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_pick_next_prefers_due_draft_and_mark_posted_moves_file(self) -> None:
        immediate = self.drafts_dir / "001-now.yaml"
        future = self.drafts_dir / "002-future.yaml"
        immediate.write_text(
            "subreddit: r/Swimming\n"
            "title: Immediate post\n"
            "body: Ready now\n",
            encoding="utf-8",
        )
        future.write_text(
            "subreddit: r/OpenAI\n"
            "title: Future post\n"
            "body: Wait for later\n"
            f"scheduled_after: {(datetime.now() + timedelta(days=1)).isoformat()}\n",
            encoding="utf-8",
        )

        draft = self.manager.pick_next()

        assert draft is not None
        self.assertEqual(draft.title, "Immediate post")
        posted_path = self.manager.mark_posted(
            draft,
            post_url="https://reddit.test/r/swimming/comments/1",
        )
        self.assertTrue(posted_path.exists())
        self.assertFalse(immediate.exists())
        self.assertIn("posted_url", posted_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
