from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from account_care import DailyReporter, HealthMonitor, HealthReport, KarmaStrategy
from collector import FeedCrawler, InterestMatcher
from poster import DraftManager
from reddit_memory import CommunityIntelligence, CommunityPlaybook, InterestProfiler
from replier import EngagementFinder, OutcomeTracker, ReplyContext, ReplyGenerator, ThreadTracker
from storage import AccountSnapshot, ActionLog, TrackedPost
from utils import extract_preview

from .behavior import BehaviorProfile
from .planner import DailyPlan, DailyPlanner, PlannedSession, PlannedTask


class SchedulerContext(Protocol):
    settings: Any
    store: Any
    browser: Any
    article_store: Any

    def seed_config(self) -> dict[str, Any]:
        ...


@dataclass(slots=True)
class SessionExecution:
    session: PlannedSession
    health_report: HealthReport | None = None


class RedditScheduler:
    """Long-running scheduler that reuses existing Reddit workflow services."""

    def __init__(
        self,
        context: SchedulerContext,
        planner: DailyPlanner,
        behavior: BehaviorProfile,
    ) -> None:
        self.context = context
        self.planner = planner
        self.behavior = behavior
        self.current_plan: DailyPlan | None = None
        self.running = True
        self.reporter = DailyReporter(
            context.store,
            output_dir=context.settings.report_output_dir,
            email_to=context.settings.report_email_to,
        )
        self.health_monitor = HealthMonitor(
            context.store,
            context.browser,
            username=context.settings.reddit_username,
            karma_decline_days=context.settings.health_karma_decline_days,
            removal_rate_threshold=context.settings.health_removal_rate_threshold,
        )
        self.draft_manager = DraftManager()
        self._last_health_day: datetime.date | None = None

    def run_forever(self) -> None:
        while self.running:
            now = datetime.now(self.planner.tz)
            if self.current_plan is None or self.current_plan.date != now.date():
                self.current_plan = self.planner.generate_plan(now.date())

            if self.current_plan.skip_today:
                self.reporter.write_report(
                    self.current_plan.date,
                    plan_payload=self.current_plan.to_dict(),
                )
                self._sleep(self._seconds_until(self._next_day_start(now)))
                continue

            self._mark_expired_sessions(now)
            due_session = self._find_due_session(now)
            if due_session is not None:
                self.execute_session(due_session)
                continue

            if self._all_sessions_complete():
                self.reporter.write_report(
                    self.current_plan.date,
                    plan_payload=self.current_plan.to_dict(),
                )
                self._sleep(self._seconds_until(self._next_day_start(now)))
                continue

            next_moment = self._next_check_time(now)
            self._sleep(self._seconds_until(next_moment))

    def execute_session(
        self,
        session: PlannedSession,
        *,
        sleep_between_tasks: bool = True,
    ) -> SessionExecution:
        health_report: HealthReport | None = None
        for index, task in enumerate(session.tasks):
            if task.executed:
                continue
            if not self.behavior.should_be_active_now():
                task.executed = True
                task.result = "skipped_inactive_window"
                self._persist_current_plan()
                continue
            task.result = self.dispatch_task(task)
            task.executed = True
            self._persist_current_plan()
            if sleep_between_tasks and index < len(session.tasks) - 1:
                self._sleep(min(self.behavior.inter_task_delay(), 45.0))

        session.executed = True
        if session.session_type.endswith("evening_active") or session.session_type.endswith("evening"):
            today = self.current_plan.date if self.current_plan else datetime.now(self.planner.tz).date()
            if self._last_health_day != today:
                health_report = self.health_monitor.run_health_check()
                self._last_health_day = today
        self._persist_current_plan()
        today = self.current_plan.date if self.current_plan else datetime.now(self.planner.tz).date()
        self.reporter.write_report(
            today,
            plan_payload=self.current_plan.to_dict() if self.current_plan else None,
            health_report=health_report,
        )
        return SessionExecution(session=session, health_report=health_report)

    def dispatch_task(self, task: PlannedTask) -> str:
        handler_map = {
            "browse": lambda: self._do_browse(task.subreddit),
            "collect": lambda: self._do_collect(task.subreddit),
            "reply_check": self._do_reply_check,
            "reply_auto": self._do_reply_auto,
            "engage": lambda: self._do_engage(task.subreddit),
            "post": lambda: self._do_post(task.subreddit),
            "vote": self._do_vote,
            "intel": lambda: self._do_intel(task.subreddit),
            "snapshot": self._do_snapshot,
        }
        try:
            handler = handler_map[task.task_type]
        except KeyError:
            return f"unsupported_task:{task.task_type}"
        try:
            return handler()
        except Exception as exc:
            return f"failed:{type(exc).__name__}:{exc}"

    def shutdown(self) -> None:
        self.running = False
        self._persist_current_plan()

    def _do_browse(self, subreddit: str | None) -> str:
        chosen = subreddit or self._choose_fallback_subreddit("browse")
        if not chosen:
            return "no_subreddit"
        if not self.context.browser.ensure_logged_in():
            return "login_missing"
        actions = self.context.browser.browse_and_engage(chosen, scroll_count=random.randint(2, 4))
        for action in actions:
            self.context.store.log_action(
                ActionLog(
                    action_type="vote" if action.action == "upvote" else "browse",
                    subreddit=action.subreddit,
                    target_url=action.target_url,
                    content_preview=action.note or action.action,
                )
            )
        return f"{chosen} actions={len(actions)}"

    def _do_collect(self, subreddit: str | None) -> str:
        chosen = subreddit or self._choose_fallback_subreddit("collect")
        if not chosen:
            return "no_subreddit"
        seed_config = self.context.seed_config()
        profiler = InterestProfiler(self.context.store, seed_config=seed_config)
        profiler.build_from_history(reset=False)
        matcher = InterestMatcher(
            interest_vector=profiler.get_interest_vector(),
            seed_config=seed_config,
        )
        crawler = FeedCrawler(
            self.context.browser,
            self.context.article_store,
            matcher,
            sqlite_store=self.context.store,
        )
        result = crawler.collect_subreddit(
            chosen,
            sort="hot" if random.random() < 0.7 else "new",
            limit=25,
        )
        return (
            f"{result.subreddit} fetched={result.fetched} new={result.new_posts} "
            f"matched={result.matched} comments={result.stored_comments}"
        )

    def _do_reply_check(self) -> str:
        if not self.context.settings.reddit_username:
            return "username_missing"
        tracker = ThreadTracker(
            self.context.browser,
            self.context.store,
            own_username=self.context.settings.reddit_username,
        )
        replies = tracker.check_new_replies()
        self.context.store.log_action(
            ActionLog(
                action_type="reply_check",
                content_preview=f"pending_new_replies={len(replies)}",
            )
        )
        return f"new_replies={len(replies)}"

    def _do_reply_auto(self) -> str:
        if not self.context.settings.reddit_username:
            return "username_missing"
        tracker = ThreadTracker(
            self.context.browser,
            self.context.store,
            own_username=self.context.settings.reddit_username,
        )
        pending = tracker.list_pending_replies(refresh=True)
        if not pending:
            return "pending_replies=0"
        generator = ReplyGenerator(
            use_llm=self._reply_mode() == "llm",
            llm_provider=self.context.settings.llm_provider,
            llm_base_url=self.context.settings.llm_base_url,
            llm_model=self.context.settings.llm_model,
        )
        posted = 0
        for item in pending[:3]:
            should_reply, _ = generator.should_reply(item)
            if not should_reply:
                continue
            reply_text = generator.generate_reply(
                ReplyContext(
                    subreddit=item.post.subreddit,
                    post=item.post,
                    comment=item.comment,
                    context_chain=item.context_chain,
                    is_direct_reply=item.is_direct_reply,
                )
            )
            reply_url = self.context.browser.submit_comment(
                item.post_url,
                reply_text,
                parent_comment_id=item.comment.id,
            )
            tracker.mark_replied(
                item.comment.id,
                reply_comment_id=self._extract_comment_id(reply_url),
            )
            self.context.store.log_action(
                ActionLog(
                    action_type="comment",
                    subreddit=item.post.subreddit,
                    target_url=reply_url,
                    content_preview=extract_preview(reply_text, max_length=200),
                )
            )
            posted += 1
        if posted:
            playbook = CommunityPlaybook(self.context.store)
            OutcomeTracker(self.context.browser, self.context.store, playbook).track_recent_actions(hours=24)
        return f"auto_replies={posted}"

    def _do_engage(self, subreddit: str | None) -> str:
        chosen = subreddit or self._choose_fallback_subreddit("engage")
        if not chosen:
            return "no_subreddit"
        seed_config = self.context.seed_config()
        profiler = InterestProfiler(self.context.store, seed_config=seed_config)
        profiler.build_from_history(reset=False)
        finder = EngagementFinder(
            self.context.browser,
            InterestMatcher(
                interest_vector=profiler.get_interest_vector(),
                seed_config=seed_config,
            ),
        )
        opportunities = finder.find_opportunities(chosen, limit=6)
        if not opportunities:
            return f"{chosen} opportunities=0"
        phase_config = KarmaStrategy.get_phase_config(
            self.current_plan.phase if self.current_plan else "established",
            farming_subreddits=self.context.settings.farming_subreddits,
        )
        max_replies = min(
            len(opportunities),
            random.randint(*phase_config["engage_per_session"]),
        )
        if max_replies <= 0:
            return f"{chosen} opportunities={len(opportunities)} skipped"
        generator = ReplyGenerator(
            use_llm=self._reply_mode() == "llm",
            llm_provider=self.context.settings.llm_provider,
            llm_base_url=self.context.settings.llm_base_url,
            llm_model=self.context.settings.llm_model,
        )
        posted = 0
        for opportunity in opportunities[:max_replies]:
            comment_text = generator.generate_engagement_reply(
                subreddit=opportunity.post.subreddit,
                post=opportunity.post,
                post_detail=opportunity.post_detail,
                suggested_angle=opportunity.suggested_angle,
            )
            reply_url = self.context.browser.submit_comment(opportunity.post.url, comment_text)
            self.context.store.log_action(
                ActionLog(
                    action_type="comment",
                    subreddit=opportunity.post.subreddit,
                    target_url=reply_url,
                    content_preview=extract_preview(comment_text, max_length=200),
                )
            )
            posted += 1
            time.sleep(min(self.behavior.typing_delay(len(comment_text)), 45.0))
        return f"{chosen} engaged={posted}/{len(opportunities)}"

    def _do_post(self, subreddit: str | None) -> str:
        if not self.context.browser.ensure_logged_in():
            return "login_missing"
        draft = self.draft_manager.pick_next(preferred_subreddit=subreddit)
        if draft is None:
            return "no_pending_draft"
        target_url = self.context.browser.submit_post(
            draft.subreddit,
            draft.title,
            draft.body,
            submit=True,
        )
        self.draft_manager.mark_posted(draft, post_url=target_url)
        self.context.store.log_action(
            ActionLog(
                action_type="post",
                subreddit=draft.subreddit,
                target_url=target_url,
                content_preview=extract_preview(f"{draft.title}\n\n{draft.body}", max_length=200),
            )
        )
        self.context.store.track_post(
            TrackedPost(
                url=target_url,
                subreddit=draft.subreddit,
                title=draft.title,
                posted_at=datetime.now(timezone.utc),
            )
        )
        return f"posted {draft.subreddit}"

    def _do_vote(self) -> str:
        candidates = [post.url for post in self.context.store.list_cached_posts(limit=25)]
        candidates.extend(
            tracked.url for tracked in self.context.store.list_tracked_posts(active_only=False, days=30)
        )
        seen: set[str] = set()
        unique_candidates = [item for item in candidates if not (item in seen or seen.add(item))]
        if not unique_candidates:
            return "no_vote_target"
        target_url = random.choice(unique_candidates[:10])
        active = self.context.browser.upvote(target_url)
        self.context.store.log_action(
            ActionLog(
                action_type="vote",
                target_url=target_url,
                content_preview="scheduler upvote" if active else "scheduler vote noop",
            )
        )
        return "upvote_active" if active else "upvote_noop"

    def _do_intel(self, subreddit: str | None) -> str:
        chosen = subreddit or self._choose_fallback_subreddit("intel")
        if not chosen:
            return "no_subreddit"
        intel = CommunityIntelligence(self.context.browser, self.context.store)
        collected = intel.collect_snapshot(chosen)
        power_users = intel.identify_power_users(chosen)
        revisited = intel.revisit_snapshots(hours_ago=0)
        self.context.store.log_action(
            ActionLog(
                action_type="intel",
                subreddit=chosen,
                content_preview=f"collected={collected} revisited={revisited}",
            )
        )
        return f"{chosen} collected={collected} power_users={len(power_users)} revisited={revisited}"

    def _do_snapshot(self) -> str:
        username = self.context.settings.reddit_username
        if not username:
            return "username_missing"
        profile = self.context.browser.get_user_profile(username)
        actions = self.context.store.list_actions(limit=5000, days=365)
        snapshot = AccountSnapshot(
            day=datetime.now(self.planner.tz).date(),
            karma_post=profile.karma_post,
            karma_comment=profile.karma_comment,
            karma_total=profile.karma_total,
            active_subreddits=len({action.subreddit for action in actions if action.subreddit}),
            total_posts=sum(1 for action in actions if action.action_type == "post"),
            total_comments=sum(1 for action in actions if action.action_type == "comment"),
        )
        self.context.store.record_account_snapshot(snapshot)
        self.context.store.log_action(
            ActionLog(
                action_type="snapshot",
                target_url=f"https://www.reddit.com/user/{username}/",
                content_preview=f"karma_total={profile.karma_total}",
            )
        )
        return f"karma_total={profile.karma_total}"

    def _persist_current_plan(self) -> None:
        if self.current_plan is not None:
            self.planner.persist_plan(self.current_plan)

    def _mark_expired_sessions(self, now: datetime) -> None:
        if self.current_plan is None:
            return
        for session in self.current_plan.sessions:
            if session.executed:
                continue
            end_at = datetime.combine(self.current_plan.date, session.window_end, tzinfo=self.planner.tz)
            if now <= end_at:
                continue
            for task in session.tasks:
                if not task.executed:
                    task.executed = True
                    task.result = "missed_window"
            session.executed = True
        self._persist_current_plan()

    def _find_due_session(self, now: datetime) -> PlannedSession | None:
        if self.current_plan is None:
            return None
        for session in self.current_plan.sessions:
            if session.executed:
                continue
            start_at = datetime.combine(self.current_plan.date, session.window_start, tzinfo=self.planner.tz)
            end_at = datetime.combine(self.current_plan.date, session.window_end, tzinfo=self.planner.tz)
            if start_at <= now <= end_at:
                return session
        return None

    def _all_sessions_complete(self) -> bool:
        return bool(self.current_plan and all(session.executed for session in self.current_plan.sessions))

    def _choose_fallback_subreddit(self, session_type: str) -> str | None:
        picks = self.planner._select_subreddits_for_session(  # noqa: SLF001
            session_type,
            self.current_plan.phase if self.current_plan else "established",
            1,
        )
        return picks[0] if picks else None

    def _reply_mode(self) -> str:
        phase = self.current_plan.phase if self.current_plan else "established"
        return str(
            KarmaStrategy.get_phase_config(
                phase,
                farming_subreddits=self.context.settings.farming_subreddits,
            )["reply_mode"]
        )

    def _next_day_start(self, now: datetime) -> datetime:
        target = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), tzinfo=self.planner.tz)
        return target.replace(hour=6, minute=0) + timedelta(
            minutes=self.behavior.jitter_minutes(0, 20, min_value=-15, max_value=25)
        )

    def _next_check_time(self, now: datetime) -> datetime:
        return now + timedelta(
            minutes=max(
                1,
                self.context.settings.scheduler_check_interval_min
                + self.behavior.jitter_minutes(0, 2, min_value=-2, max_value=2),
            )
        )

    @staticmethod
    def _seconds_until(target: datetime) -> float:
        return max(5.0, (target - datetime.now(target.tzinfo)).total_seconds())

    def _sleep(self, seconds: float) -> None:
        remaining = max(0.0, seconds)
        while self.running and remaining > 0:
            chunk = min(remaining, 30.0)
            time.sleep(chunk)
            remaining -= chunk

    @staticmethod
    def _extract_comment_id(target_url: str) -> str | None:
        from urllib import parse

        parsed = parse.urlparse(target_url)
        if parsed.fragment.startswith("t1_"):
            return parsed.fragment
        segments = [segment for segment in parsed.path.split("/") if segment]
        for index, segment in enumerate(segments):
            if segment == "comment" and index + 1 < len(segments):
                value = segments[index + 1]
                return value if value.startswith("t1_") else f"t1_{value}"
        return None
