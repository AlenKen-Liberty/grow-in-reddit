from __future__ import annotations

import random
from datetime import datetime
from zoneinfo import ZoneInfo


class BehaviorProfile:
    """Human-like timing heuristics for scheduler sessions."""

    ACTIVITY_WEIGHT = {
        0: 0.10,
        1: 0.05,
        2: 0.02,
        3: 0.01,
        4: 0.01,
        5: 0.02,
        6: 0.10,
        7: 0.30,
        8: 0.50,
        9: 0.70,
        10: 0.80,
        11: 0.80,
        12: 0.90,
        13: 0.70,
        14: 0.60,
        15: 0.60,
        16: 0.50,
        17: 0.60,
        18: 0.70,
        19: 0.90,
        20: 1.00,
        21: 0.90,
        22: 0.70,
        23: 0.40,
    }

    def __init__(self, timezone: str = "America/New_York") -> None:
        self.tz = ZoneInfo(timezone)

    def should_be_active_now(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(self.tz)
        return random.random() < self.ACTIVITY_WEIGHT.get(current.hour, 0.5)

    def jitter_minutes(
        self,
        base: int = 0,
        sigma: int = 15,
        *,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> int:
        value = base + int(round(random.gauss(0, sigma)))
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    def reading_delay(self, text_length: int) -> float:
        if text_length < 100:
            return random.uniform(5, 15)
        if text_length <= 500:
            return random.uniform(15, 60)
        return random.uniform(60, 180)

    def typing_delay(self, text_length: int) -> float:
        if text_length < 50:
            return random.uniform(10, 30)
        if text_length <= 200:
            return random.uniform(30, 90)
        return random.uniform(60, 180)

    def inter_task_delay(self) -> float:
        return max(5.0, random.gauss(30.0, 15.0))

