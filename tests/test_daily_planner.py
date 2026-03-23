from __future__ import annotations

import random
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from scheduler import DailyPlanner
from storage import AccountSnapshot, ActionLog, SQLiteStore
from storage.models import utc_now


SEED_CONFIG = {
    "primary": [
        {
            "topic": "swimming",
            "subreddits": ["r/Swimming", "r/ApplyingToCollege"],
            "keywords": ["swimming", "recruiting"],
        }
    ],
    "secondary": [
        {
            "topic": "ai agents",
            "subreddits": ["r/OpenAI", "r/LocalLLaMA"],
            "keywords": ["llm", "agent"],
        }
    ],
    "similarity_threshold": 0.40,
}


class DailyPlannerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.tmpdir.name) / "test.db")

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_generate_plan_for_established_account(self) -> None:
        today = date(2026, 3, 23)
        self.store.record_account_snapshot(
            AccountSnapshot(
                day=today,
                karma_post=4000,
                karma_comment=9000,
                karma_total=13000,
            )
        )
        self.store.log_action(
            ActionLog(
                action_type="comment",
                subreddit="r/Swimming",
                content_preview="helpful comment",
                timestamp=utc_now() - timedelta(days=120),
            )
        )
        planner = DailyPlanner(
            self.store,
            seed_config=SEED_CONFIG,
            timezone="America/New_York",
            farming_subreddits=["AskReddit", "NoStupidQuestions"],
        )
        random.seed(1)

        with patch("scheduler.planner.random.random", return_value=0.99):
            plan = planner.generate_plan(today, force=True)

        self.assertEqual(plan.phase, "established")
        self.assertFalse(plan.skip_today)
        self.assertEqual(len(plan.sessions), 3)
        self.assertTrue(any(task.task_type == "collect" for task in plan.sessions[-1].tasks))
        self.assertIsNotNone(self.store.get_schedule_log(today))

    def test_existing_plan_is_reused_without_force(self) -> None:
        today = date(2026, 3, 23)
        self.store.record_account_snapshot(
            AccountSnapshot(
                day=today,
                karma_post=10,
                karma_comment=10,
                karma_total=20,
            )
        )
        planner = DailyPlanner(self.store, seed_config=SEED_CONFIG, timezone="America/New_York")

        with patch("scheduler.planner.random.random", return_value=0.99):
            first = planner.generate_plan(today, force=True)
            second = planner.generate_plan(today, force=False)

        self.assertEqual(first.to_dict(), second.to_dict())


if __name__ == "__main__":
    unittest.main()
