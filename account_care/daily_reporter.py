from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from storage import ScheduleLogEntry, SQLiteStore


@dataclass(slots=True)
class DailySummary:
    day: date
    action_counts: dict[str, int]
    karma_total: int | None
    karma_delta: int | None
    phase: str | None
    executed_sessions: int
    planned_sessions: int
    recommended_action: str | None
    warnings: list[str]


class DailyReporter:
    """Write a local end-of-day summary when outbound email is unavailable."""

    def __init__(
        self,
        store: SQLiteStore,
        *,
        output_dir: Path,
        email_to: str | None = None,
    ) -> None:
        self.store = store
        self.output_dir = output_dir
        self.email_to = email_to

    def build_summary(
        self,
        day: date,
        *,
        plan_payload: dict[str, Any] | None = None,
        health_report: Any | None = None,
    ) -> DailySummary:
        actions = [
            action
            for action in self.store.list_actions(limit=5000, days=3)
            if action.timestamp.date() == day
        ]
        action_counts: dict[str, int] = {}
        for action in actions:
            action_counts[action.action_type] = action_counts.get(action.action_type, 0) + 1

        snapshots = list(reversed(self.store.list_account_snapshots(days=30, limit=30)))
        karma_total: int | None = None
        karma_delta: int | None = None
        current_index: int | None = None
        for index, snapshot in enumerate(snapshots):
            if snapshot.day == day:
                current_index = index
                karma_total = snapshot.karma_total
                break
        if current_index is not None and current_index > 0:
            karma_delta = karma_total - snapshots[current_index - 1].karma_total  # type: ignore[operator]

        schedule = self.store.get_schedule_log(day) or ScheduleLogEntry(day=day)
        payload = plan_payload or schedule.planned_actions or {}
        sessions = payload.get("sessions") if isinstance(payload, dict) else []
        executed_sessions = sum(
            1
            for session in sessions
            if isinstance(session, dict) and session.get("executed")
        )
        planned_sessions = len(sessions) if isinstance(sessions, list) else 0

        warnings = list(getattr(health_report, "warnings", []) or [])
        return DailySummary(
            day=day,
            action_counts=action_counts,
            karma_total=karma_total,
            karma_delta=karma_delta,
            phase=payload.get("phase") if isinstance(payload, dict) else None,
            executed_sessions=executed_sessions,
            planned_sessions=planned_sessions,
            recommended_action=getattr(health_report, "recommended_action", None),
            warnings=warnings,
        )

    def write_report(
        self,
        day: date,
        *,
        plan_payload: dict[str, Any] | None = None,
        health_report: Any | None = None,
    ) -> Path:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        summary = self.build_summary(
            day,
            plan_payload=plan_payload,
            health_report=health_report,
        )
        report_path = self.output_dir / f"{day.isoformat()}.md"
        report_path.write_text(
            self._render_markdown(summary),
            encoding="utf-8",
        )
        json_path = self.output_dir / f"{day.isoformat()}.json"
        json_path.write_text(
            json.dumps(
                {
                    "day": day.isoformat(),
                    "action_counts": summary.action_counts,
                    "karma_total": summary.karma_total,
                    "karma_delta": summary.karma_delta,
                    "phase": summary.phase,
                    "executed_sessions": summary.executed_sessions,
                    "planned_sessions": summary.planned_sessions,
                    "recommended_action": summary.recommended_action,
                    "warnings": summary.warnings,
                    "delivery_mode": "local_log_fallback",
                    "email_to": self.email_to,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return report_path

    def _render_markdown(self, summary: DailySummary) -> str:
        action_text = ", ".join(
            f"{action}={count}"
            for action, count in sorted(summary.action_counts.items())
        ) or "none"
        karma_text = (
            f"{summary.karma_total} ({summary.karma_delta:+d})"
            if summary.karma_total is not None and summary.karma_delta is not None
            else str(summary.karma_total)
            if summary.karma_total is not None
            else "unknown"
        )
        warnings = "\n".join(f"- {warning}" for warning in summary.warnings) or "- none"
        return (
            f"# Reddit Daily Summary {summary.day.isoformat()}\n\n"
            f"- Delivery: local log fallback\n"
            f"- Target email: {self.email_to or 'not configured'}\n"
            f"- Phase: {summary.phase or 'unknown'}\n"
            f"- Actions: {action_text}\n"
            f"- Karma total / delta: {karma_text}\n"
            f"- Sessions: {summary.executed_sessions}/{summary.planned_sessions}\n"
            f"- Recommended action: {summary.recommended_action or 'continue'}\n\n"
            f"## Warnings\n{warnings}\n"
        )
