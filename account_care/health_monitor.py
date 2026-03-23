from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta

from reddit_browser import RedditBrowser
from storage import ActionOutcome, SQLiteStore
from storage.models import utc_now


@dataclass(slots=True)
class HealthReport:
    is_healthy: bool
    warnings: list[str] = field(default_factory=list)
    recommended_action: str = "continue"


class HealthMonitor:
    """Read recent local telemetry and downgrade activity when the account looks risky."""

    def __init__(
        self,
        store: SQLiteStore,
        browser: RedditBrowser,
        *,
        username: str | None = None,
        karma_decline_days: int = 3,
        removal_rate_threshold: float = 0.2,
    ) -> None:
        self.store = store
        self.browser = browser
        self.username = username
        self.karma_decline_days = max(2, karma_decline_days)
        self.removal_rate_threshold = max(0.0, removal_rate_threshold)

    def run_health_check(self) -> HealthReport:
        warnings: list[str] = []
        karma_warning = self._karma_trend_warning()
        if karma_warning:
            warnings.append(karma_warning)

        removal_warning = self._removal_rate_warning()
        if removal_warning:
            warnings.append(removal_warning)

        anomaly_warning = self._behavior_anomaly_warning()
        if anomaly_warning:
            warnings.append(anomaly_warning)

        if self.detect_shadowban():
            warnings.append("Profile visibility check failed; account may be shadowbanned.")

        report = HealthReport(is_healthy=not warnings, warnings=warnings)
        report.recommended_action = self.get_recommended_action(report)
        report.is_healthy = report.recommended_action == "continue"
        return report

    def detect_shadowban(self) -> bool:
        if not self.username:
            return False
        try:
            return not self.browser.is_profile_publicly_visible(self.username)
        except Exception:
            return False

    def get_recommended_action(self, report: HealthReport) -> str:
        joined = " ".join(report.warnings).lower()
        if "shadowban" in joined:
            return "stop"
        if "removal rate" in joined:
            return "pause"
        if "karma" in joined or "action count spike" in joined:
            return "reduce"
        return "continue"

    def _karma_trend_warning(self) -> str | None:
        snapshots = list(
            reversed(
                self.store.list_account_snapshots(
                    days=max(14, self.karma_decline_days + 2),
                    limit=30,
                )
            )
        )
        if len(snapshots) < self.karma_decline_days + 1:
            return None
        recent = snapshots[-(self.karma_decline_days + 1) :]
        declines = [
            current.karma_total < previous.karma_total
            for previous, current in zip(recent, recent[1:])
        ]
        if all(declines):
            delta = recent[-1].karma_total - recent[0].karma_total
            return (
                f"Karma declined for {self.karma_decline_days} straight days "
                f"({delta:+d} total)."
            )
        return None

    def _removal_rate_warning(self) -> str | None:
        cutoff = utc_now() - timedelta(days=7)
        outcomes = [
            outcome
            for outcome in self.store.list_action_outcomes(limit=1000)
            if outcome.timestamp >= cutoff and outcome.action_type in {"post", "comment"}
        ]
        if not outcomes:
            return None
        removed = sum(1 for outcome in outcomes if outcome.was_removed)
        rate = removed / len(outcomes)
        if rate > self.removal_rate_threshold:
            return (
                f"Removal rate is {rate:.0%} over the last 7 days "
                f"({removed}/{len(outcomes)})."
            )
        return None

    def _behavior_anomaly_warning(self) -> str | None:
        cutoff = utc_now() - timedelta(days=7)
        actions = self.store.list_actions(limit=5000, days=8)
        daily_counts: defaultdict[date, int] = defaultdict(int)
        for action in actions:
            if action.timestamp < cutoff:
                continue
            daily_counts[action.timestamp.date()] += 1
        if len(daily_counts) < 3:
            return None
        today = max(daily_counts)
        today_count = daily_counts[today]
        prior_counts = [count for day, count in daily_counts.items() if day != today]
        if not prior_counts:
            return None
        average = sum(prior_counts) / len(prior_counts)
        if average >= 1 and today_count >= max(20, average * 2.5):
            return (
                f"Action count spike detected today: {today_count} vs {average:.1f} average."
            )
        return None

