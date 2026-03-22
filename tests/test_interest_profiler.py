from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from reddit_memory.interest_profiler import InterestProfiler
from storage import ActionLog, SQLiteStore

SEED_CONFIG = {
    "primary": [
        {
            "topic": "swimming",
            "subreddits": ["r/Swimming"],
            "keywords": ["freestyle", "training plan"],
        }
    ],
    "secondary": [],
    "similarity_threshold": 0.65,
}


class InterestProfilerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.tmpdir.name) / "test.db")

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_build_from_history_learns_seed_topic(self) -> None:
        self.store.log_action(
            ActionLog(
                action_type="comment",
                subreddit="r/Swimming",
                content_preview="Freestyle drills made my training plan much better.",
            )
        )
        profiler = InterestProfiler(self.store, seed_config=SEED_CONFIG)
        profiler.build_from_history(reset=True)
        vector = profiler.get_interest_vector()
        self.assertIn("swimming", vector)
        self.assertGreater(vector["swimming"], 0.65)
        self.assertIn("subreddit/swimming", vector)


if __name__ == "__main__":
    unittest.main()
