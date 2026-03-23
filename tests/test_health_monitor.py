from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from account_care import HealthMonitor
from storage import AccountSnapshot, ActionOutcome, PlaybookEntry, SQLiteStore
from storage.models import utc_now


class FakeBrowser:
    def __init__(self, visible: bool) -> None:
        self.visible = visible

    def is_profile_publicly_visible(self, username: str) -> bool:
        return self.visible


class HealthMonitorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.tmpdir.name) / "test.db")

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_shadowban_warning_stops_scheduler(self) -> None:
        today = date(2026, 3, 23)
        self.store.upsert_community_playbook(PlaybookEntry(subreddit="r/Swimming"))
        for offset, karma_total in enumerate([120, 110, 100, 90]):
            self.store.record_account_snapshot(
                AccountSnapshot(
                    day=today - timedelta(days=3 - offset),
                    karma_post=karma_total // 2,
                    karma_comment=karma_total // 2,
                    karma_total=karma_total,
                )
            )
        for removed in (True, True, False):
            self.store.record_action_outcome(
                ActionOutcome(
                    subreddit="r/Swimming",
                    action_type="comment",
                    content_summary="summary",
                    was_removed=removed,
                    timestamp=utc_now(),
                )
            )
        monitor = HealthMonitor(
            self.store,
            FakeBrowser(visible=False),
            username="tester",
            karma_decline_days=3,
            removal_rate_threshold=0.2,
        )

        report = monitor.run_health_check()

        self.assertFalse(report.is_healthy)
        self.assertEqual(report.recommended_action, "stop")
        self.assertTrue(any("shadowban" in warning.lower() for warning in report.warnings))


if __name__ == "__main__":
    unittest.main()
