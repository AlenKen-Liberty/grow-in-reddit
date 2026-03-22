from __future__ import annotations

import unittest

from reddit_browser.rate_limiter import RateLimiter, RateLimitPolicy


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def now(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


class RateLimiterTest(unittest.TestCase):
    def test_wait_respects_sampled_gap(self) -> None:
        clock = FakeClock()
        limiter = RateLimiter(
            policies={"browse": RateLimitPolicy(2, 5, 0)},
            clock=clock.now,
            sleep_fn=clock.sleep,
            rng=lambda mean, stddev: mean,
        )

        self.assertTrue(limiter.can_act("browse"))
        self.assertEqual(limiter.wait("browse"), 0.0)
        self.assertFalse(limiter.can_act("browse"))
        self.assertEqual(limiter.wait("browse"), 5.0)
        self.assertEqual(clock.value, 5.0)


if __name__ == "__main__":
    unittest.main()
