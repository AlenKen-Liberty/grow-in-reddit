from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from account_care import KarmaStrategy
from storage import SQLiteStore
from utils import normalize_subreddit_name


@dataclass(slots=True)
class PlannedTask:
    task_type: str
    subreddit: str | None
    priority: int = 0
    executed: bool = False
    result: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_type": self.task_type,
            "subreddit": self.subreddit,
            "priority": self.priority,
            "executed": self.executed,
            "result": self.result,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PlannedTask":
        return cls(
            task_type=str(payload.get("task_type") or ""),
            subreddit=payload.get("subreddit"),
            priority=int(payload.get("priority") or 0),
            executed=bool(payload.get("executed")),
            result=str(payload.get("result") or ""),
        )


@dataclass(slots=True)
class PlannedSession:
    window_start: time
    window_end: time
    session_type: str
    tasks: list[PlannedTask] = field(default_factory=list)
    executed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat(timespec="minutes"),
            "window_end": self.window_end.isoformat(timespec="minutes"),
            "session_type": self.session_type,
            "tasks": [task.to_dict() for task in self.tasks],
            "executed": self.executed,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PlannedSession":
        return cls(
            window_start=time.fromisoformat(str(payload.get("window_start") or "07:00")),
            window_end=time.fromisoformat(str(payload.get("window_end") or "08:00")),
            session_type=str(payload.get("session_type") or "session"),
            tasks=[
                PlannedTask.from_dict(item)
                for item in payload.get("tasks", [])
                if isinstance(item, dict)
            ],
            executed=bool(payload.get("executed")),
        )


@dataclass(slots=True)
class DailyPlan:
    date: date
    phase: str
    skip_today: bool
    skip_reason: str
    sessions: list[PlannedSession] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "phase": self.phase,
            "skip_today": self.skip_today,
            "skip_reason": self.skip_reason,
            "sessions": [session.to_dict() for session in self.sessions],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DailyPlan":
        return cls(
            date=date.fromisoformat(str(payload.get("date") or date.today().isoformat())),
            phase=str(payload.get("phase") or "established"),
            skip_today=bool(payload.get("skip_today")),
            skip_reason=str(payload.get("skip_reason") or ""),
            sessions=[
                PlannedSession.from_dict(item)
                for item in payload.get("sessions", [])
                if isinstance(item, dict)
            ],
        )


class DailyPlanner:
    """Generate and persist one local-time plan per day."""

    def __init__(
        self,
        store: SQLiteStore,
        seed_config: dict[str, Any],
        timezone: str,
        *,
        farming_subreddits: list[str] | None = None,
    ) -> None:
        self.store = store
        self.seed_config = seed_config
        self.tz = ZoneInfo(timezone)
        self.farming_subreddits = farming_subreddits or KarmaStrategy.DEFAULT_FARMING_SUBREDDITS
        self._recent_session_subreddits: list[str] = []

    def generate_plan(self, target_day: date | None = None, *, force: bool = False) -> DailyPlan:
        day = target_day or datetime.now(self.tz).date()
        if not force:
            existing = self.store.get_schedule_log(day)
            if existing and existing.planned_actions:
                return self._merge_execution_state(
                    DailyPlan.from_dict(existing.planned_actions),
                    existing.executed_actions or {},
                )

        phase = self.get_current_phase(reference_day=day)
        skip_today, skip_reason = self._should_skip_today(day)
        sessions = [] if skip_today else self._generate_sessions(phase, day)
        plan = DailyPlan(
            date=day,
            phase=phase,
            skip_today=skip_today,
            skip_reason=skip_reason,
            sessions=sessions,
        )
        self.store.upsert_schedule_log(
            self._plan_entry(plan, executed_actions=self._serialize_execution(plan))
        )
        return plan

    def get_current_phase(self, *, reference_day: date | None = None) -> str:
        snapshot = self.store.get_latest_account_snapshot()
        if snapshot is None:
            return "newborn"
        today = reference_day or datetime.now(self.tz).date()
        oldest_action = self.store.get_oldest_action()
        days_active = 0
        if oldest_action is not None:
            days_active = max(0, (today - oldest_action.timestamp.date()).days)

        if snapshot.karma_total < 50 and days_active < 7:
            return "newborn"
        if snapshot.karma_total < 500 and days_active < 30:
            return "infant"
        if snapshot.karma_total < 2000 and days_active < 90:
            return "growing"
        return "established"

    def build_immediate_session(self, phase: str | None = None) -> PlannedSession:
        today = datetime.now(self.tz).date()
        chosen_phase = phase or self.get_current_phase(reference_day=today)
        sessions = self._generate_sessions(chosen_phase, today)
        if not sessions:
            now = datetime.now(self.tz)
            return PlannedSession(
                window_start=now.time().replace(second=0, microsecond=0),
                window_end=(now + timedelta(hours=1)).time().replace(second=0, microsecond=0),
                session_type="manual_nurture",
                tasks=[PlannedTask(task_type="browse", subreddit=None, priority=0)],
            )
        first = sessions[0]
        now = datetime.now(self.tz)
        first.window_start = now.time().replace(second=0, microsecond=0)
        first.window_end = (now + timedelta(hours=1)).time().replace(second=0, microsecond=0)
        first.session_type = f"manual_{first.session_type}"
        return first

    def persist_plan(self, plan: DailyPlan) -> int:
        return self.store.upsert_schedule_log(
            self._plan_entry(plan, executed_actions=self._serialize_execution(plan))
        )

    def _should_skip_today(self, today: date) -> tuple[bool, str]:
        skip_probability = 0.15 if today.weekday() >= 5 else 0.10
        if random.random() >= skip_probability:
            return False, ""
        return True, "weekend_lazy" if today.weekday() >= 5 else "random_rest"

    def _generate_sessions(self, phase: str, today: date) -> list[PlannedSession]:
        self._recent_session_subreddits = []
        morning = PlannedSession(
            window_start=self._jittered_time(today, 7, 0, sigma=20, min_offset=-20, max_offset=45),
            window_end=self._jittered_time(today, 8, 30, sigma=15, min_offset=-10, max_offset=30),
            session_type="morning_browse",
            tasks=self._build_morning_tasks(phase),
        )
        midday = PlannedSession(
            window_start=self._jittered_time(today, 12, 0, sigma=15, min_offset=-10, max_offset=25),
            window_end=self._jittered_time(today, 13, 20, sigma=10, min_offset=-5, max_offset=20),
            session_type="midday_engage",
            tasks=self._build_midday_tasks(phase),
        )
        evening = PlannedSession(
            window_start=self._jittered_time(today, 19, 0, sigma=25, min_offset=-20, max_offset=55),
            window_end=self._jittered_time(today, 21, 30, sigma=20, min_offset=-10, max_offset=40),
            session_type="evening_active",
            tasks=self._build_evening_tasks(phase),
        )
        return [morning, midday, evening]

    def _build_morning_tasks(self, phase: str) -> list[PlannedTask]:
        tasks = [PlannedTask(task_type="browse", subreddit=self._pick_one_subreddit("browse", phase), priority=0)]
        if phase in {"infant", "growing", "established"}:
            tasks.append(PlannedTask(task_type="vote", subreddit=None, priority=1))
        if phase in {"growing", "established"}:
            tasks.append(PlannedTask(task_type="reply_check", subreddit=None, priority=2))
        return tasks

    def _build_midday_tasks(self, phase: str) -> list[PlannedTask]:
        engage_count = 1 if phase in {"newborn", "infant"} else random.randint(1, 2)
        tasks = [
            PlannedTask(task_type="engage", subreddit=subreddit, priority=index)
            for index, subreddit in enumerate(
                self._select_subreddits_for_session("engage", phase, engage_count)
            )
        ]
        tasks.append(PlannedTask(task_type="reply_check", subreddit=None, priority=len(tasks)))
        if phase != "newborn":
            tasks.append(PlannedTask(task_type="reply_auto", subreddit=None, priority=len(tasks)))
        return tasks

    def _build_evening_tasks(self, phase: str) -> list[PlannedTask]:
        collect_count = {"newborn": 1, "infant": 2, "growing": 3, "established": 3}[phase]
        engage_count = {"newborn": 1, "infant": 2, "growing": 2, "established": 2}[phase]
        tasks: list[PlannedTask] = []
        for subreddit in self._select_subreddits_for_session("collect", phase, collect_count):
            tasks.append(PlannedTask(task_type="collect", subreddit=subreddit, priority=len(tasks)))
        for subreddit in self._select_subreddits_for_session("engage", phase, engage_count):
            tasks.append(PlannedTask(task_type="engage", subreddit=subreddit, priority=len(tasks)))
        post_probability = {
            "newborn": 0.10,
            "infant": 0.20,
            "growing": 0.50,
            "established": 0.70,
        }[phase]
        if random.random() < post_probability:
            tasks.append(
                PlannedTask(
                    task_type="post",
                    subreddit=self._pick_one_subreddit("post", phase),
                    priority=len(tasks),
                )
            )
        tasks.append(
            PlannedTask(
                task_type="intel",
                subreddit=self._pick_one_subreddit("intel", phase),
                priority=len(tasks),
            )
        )
        tasks.append(PlannedTask(task_type="snapshot", subreddit=None, priority=len(tasks)))
        return tasks

    def _pick_one_subreddit(self, session_type: str, phase: str) -> str | None:
        picks = self._select_subreddits_for_session(session_type, phase, 1)
        return picks[0] if picks else None

    def _select_subreddits_for_session(
        self,
        session_type: str,
        phase: str,
        count: int,
    ) -> list[str]:
        if count <= 0:
            return []
        config = KarmaStrategy.get_phase_config(
            phase,
            farming_subreddits=self.farming_subreddits,
        )
        interest_pool = self._interest_subreddits(prefer_primary=session_type in {"collect", "intel", "post"})
        recent_pool = self._recent_action_subreddits()
        farming_pool = [
            self._normalize(subreddit)
            for subreddit in config["farming_subreddits"]
            if self._normalize(subreddit)
        ]
        chosen: list[str] = []
        mix = dict(config["subreddit_mix"])
        for _ in range(count):
            source = "interest"
            roll = random.random()
            if roll < mix.get("farming", 0.0):
                source = "farming"
            elif phase == "established" and recent_pool and roll > 0.95:
                source = "recent"
            pool = {
                "farming": farming_pool,
                "recent": recent_pool,
                "interest": interest_pool,
            }[source]
            subreddit = self._pick_from_pool(pool, exclude=set(chosen) | set(self._recent_session_subreddits))
            if subreddit is None:
                subreddit = self._pick_from_pool(
                    interest_pool or recent_pool or farming_pool,
                    exclude=set(chosen),
                )
            if subreddit is None:
                continue
            chosen.append(subreddit)
            self._recent_session_subreddits.append(subreddit)
        return chosen

    def _interest_subreddits(self, *, prefer_primary: bool) -> list[str]:
        buckets = ("primary", "secondary") if prefer_primary else ("secondary", "primary")
        items: list[str] = []
        for bucket in buckets:
            for entry in self.seed_config.get(bucket, []):
                for subreddit in entry.get("subreddits", []):
                    normalized = self._normalize(subreddit)
                    if normalized:
                        items.append(normalized)
        return list(dict.fromkeys(items))

    def _recent_action_subreddits(self) -> list[str]:
        items: list[str] = []
        for action in self.store.list_actions(limit=50, days=30):
            normalized = self._normalize(action.subreddit)
            if normalized:
                items.append(normalized)
        return list(dict.fromkeys(items))

    @staticmethod
    def _pick_from_pool(pool: list[str], *, exclude: set[str]) -> str | None:
        candidates = [item for item in pool if item not in exclude]
        if not candidates:
            candidates = list(pool)
        if not candidates:
            return None
        return random.choice(candidates)

    @staticmethod
    def _normalize(subreddit: str | None) -> str | None:
        normalized = normalize_subreddit_name(subreddit)
        return f"r/{normalized}" if normalized else None

    @staticmethod
    def _merge_execution_state(plan: DailyPlan, execution: dict[str, Any]) -> DailyPlan:
        sessions_state = execution.get("sessions", []) if isinstance(execution, dict) else []
        for session, state in zip(plan.sessions, sessions_state):
            if not isinstance(state, dict):
                continue
            session.executed = bool(state.get("executed"))
            task_state = state.get("tasks", [])
            for task, task_payload in zip(session.tasks, task_state):
                if not isinstance(task_payload, dict):
                    continue
                task.executed = bool(task_payload.get("executed"))
                task.result = str(task_payload.get("result") or "")
        return plan

    @staticmethod
    def _serialize_execution(plan: DailyPlan) -> dict[str, Any]:
        return {
            "phase": plan.phase,
            "sessions": [
                {
                    "session_type": session.session_type,
                    "executed": session.executed,
                    "tasks": [
                        {
                            "task_type": task.task_type,
                            "executed": task.executed,
                            "result": task.result,
                        }
                        for task in session.tasks
                    ],
                }
                for session in plan.sessions
            ],
        }

    @staticmethod
    def _plan_entry(plan: DailyPlan, *, executed_actions: dict[str, Any]) -> Any:
        from storage import ScheduleLogEntry

        return ScheduleLogEntry(
            day=plan.date,
            planned_actions=plan.to_dict(),
            executed_actions=executed_actions,
            skipped_reason=plan.skip_reason or None,
        )

    @staticmethod
    def _jittered_time(
        today: date,
        hour: int,
        minute: int,
        *,
        sigma: int,
        min_offset: int,
        max_offset: int,
    ) -> time:
        base = datetime.combine(today, time(hour=hour, minute=minute))
        offset = max(min_offset, min(max_offset, int(round(random.gauss(0, sigma)))))
        return (base + timedelta(minutes=offset)).time().replace(second=0, microsecond=0)

