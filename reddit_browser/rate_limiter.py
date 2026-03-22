from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True, slots=True)
class RateLimitPolicy:
    minimum_interval: float
    mean_interval: float
    stddev_interval: float


class RateLimiter:
    LIMITS = {
        "post": RateLimitPolicy(600, 900, 120),
        "comment": RateLimitPolicy(120, 300, 60),
        "vote": RateLimitPolicy(3, 8, 2),
        "browse": RateLimitPolicy(2, 5, 1),
    }

    def __init__(
        self,
        *,
        policies: dict[str, RateLimitPolicy] | None = None,
        enabled: bool = True,
        clock: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        rng: Callable[[float, float], float] | None = None,
    ):
        self.policies = {**self.LIMITS, **(policies or {})}
        self.enabled = enabled
        self._clock = clock or time.monotonic
        self._sleep = sleep_fn or time.sleep
        self._rng = rng or random.gauss
        self._last_action_at: dict[str, float] = {}
        self._lock = threading.RLock()

    def sample_delay(self, action_type: str) -> float:
        policy = self._get_policy(action_type)
        sampled = self._rng(policy.mean_interval, policy.stddev_interval)
        return max(policy.minimum_interval, round(sampled, 2))

    def can_act(self, action_type: str) -> bool:
        policy = self._get_policy(action_type)
        with self._lock:
            last = self._last_action_at.get(action_type)
        if last is None:
            return True
        return (self._clock() - last) >= policy.minimum_interval

    def peek_remaining(self, action_type: str) -> float:
        policy = self._get_policy(action_type)
        with self._lock:
            last = self._last_action_at.get(action_type)
        if last is None:
            return 0.0
        elapsed = self._clock() - last
        return max(0.0, round(policy.minimum_interval - elapsed, 2))

    def mark_action(self, action_type: str) -> None:
        with self._lock:
            self._last_action_at[action_type] = self._clock()

    def wait(self, action_type: str) -> float:
        if not self.enabled:
            self.mark_action(action_type)
            return 0.0

        with self._lock:
            last = self._last_action_at.get(action_type)

        if last is None:
            self.mark_action(action_type)
            return 0.0

        target_gap = self.sample_delay(action_type)
        elapsed = max(0.0, self._clock() - last)
        wait_for = max(0.0, target_gap - elapsed)
        if wait_for > 0:
            self._sleep(wait_for)
        self.mark_action(action_type)
        return wait_for

    def _get_policy(self, action_type: str) -> RateLimitPolicy:
        try:
            return self.policies[action_type]
        except KeyError as exc:
            raise ValueError(f"Unsupported action type: {action_type}") from exc
