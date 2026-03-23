from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from scheduler import BehaviorProfile


class BehaviorProfileTest(unittest.TestCase):
    def test_should_be_active_now_respects_hour_weight(self) -> None:
        profile = BehaviorProfile("America/New_York")
        evening = datetime(2026, 3, 23, 20, 0, tzinfo=ZoneInfo("America/New_York"))
        late_night = datetime(2026, 3, 23, 3, 0, tzinfo=ZoneInfo("America/New_York"))

        with patch("scheduler.behavior.random.random", return_value=0.5):
            self.assertTrue(profile.should_be_active_now(evening))
        with patch("scheduler.behavior.random.random", return_value=0.5):
            self.assertFalse(profile.should_be_active_now(late_night))

    def test_delay_helpers_stay_in_expected_ranges(self) -> None:
        profile = BehaviorProfile()
        for _ in range(20):
            self.assertGreaterEqual(
                profile.jitter_minutes(0, 15, min_value=-10, max_value=10),
                -10,
            )
            self.assertLessEqual(
                profile.jitter_minutes(0, 15, min_value=-10, max_value=10),
                10,
            )
            self.assertGreaterEqual(profile.reading_delay(50), 5)
            self.assertLessEqual(profile.reading_delay(50), 15)
            self.assertGreaterEqual(profile.typing_delay(30), 10)
            self.assertLessEqual(profile.typing_delay(30), 30)


if __name__ == "__main__":
    unittest.main()
