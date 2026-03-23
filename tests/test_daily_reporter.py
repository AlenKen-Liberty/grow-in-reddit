from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from account_care import DailyReporter
from storage import AccountSnapshot, ActionLog, SQLiteStore
from storage.models import ScheduleLogEntry, utc_now


class DailyReporterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.store = SQLiteStore(self.root / "test.db")

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_write_report_includes_karma_delta_and_actions(self) -> None:
        today = date(2026, 3, 23)
        yesterday = today - timedelta(days=1)
        self.store.record_account_snapshot(
            AccountSnapshot(
                day=yesterday,
                karma_post=50,
                karma_comment=50,
                karma_total=100,
            )
        )
        self.store.record_account_snapshot(
            AccountSnapshot(
                day=today,
                karma_post=60,
                karma_comment=55,
                karma_total=115,
            )
        )
        self.store.log_action(
            ActionLog(
                action_type="comment",
                subreddit="r/Swimming",
                content_preview="Useful answer",
                timestamp=utc_now().replace(year=2026, month=3, day=23),
            )
        )
        self.store.upsert_schedule_log(
            ScheduleLogEntry(
                day=today,
                planned_actions={
                    "date": today.isoformat(),
                    "phase": "established",
                    "skip_today": False,
                    "skip_reason": "",
                    "sessions": [{"executed": True, "tasks": []}],
                },
                executed_actions={"sessions": [{"executed": True, "tasks": []}]},
            )
        )
        reporter = DailyReporter(
            self.store,
            output_dir=self.root / "reports",
            email_to="liuyl.david@gmail.com",
        )

        summary = reporter.build_summary(today)
        report_path = reporter.write_report(today)

        self.assertEqual(summary.karma_delta, 15)
        self.assertEqual(summary.action_counts["comment"], 1)
        self.assertTrue(report_path.exists())
        self.assertIn("liuyl.david@gmail.com", report_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
