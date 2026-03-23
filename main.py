from __future__ import annotations

import argparse
import signal
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib import parse

from collector import ArticleStore, FeedCrawler, InterestMatcher
from reddit_browser import RedditBrowser, RedditBrowserError, RedditParser, RateLimiter
from reddit_memory import (
    CommunityIntelligence,
    CommunityPlaybook,
    InterestProfiler,
    MemorySeedLoader,
)
from replier import EngagementFinder, OutcomeTracker, ReplyContext, ReplyGenerator, ThreadTracker
from settings import Settings
from storage import AccountSnapshot, ActionLog, SQLiteStore, TrackedPost
from utils import extract_preview, normalize_subreddit_name


@dataclass(slots=True)
class AppContext:
    settings: Settings
    store: SQLiteStore
    browser: RedditBrowser
    article_store: ArticleStore
    _seed_config: dict[str, Any] | None = None

    def seed_config(self) -> dict[str, Any]:
        if self._seed_config is None:
            base_config = self.settings.load_interest_seeds()
            self._seed_config = MemorySeedLoader(
                claude_projects_dir=self.settings.claude_projects_dir,
                openclaw_memory_dir=self.settings.openclaw_memory_dir,
            ).augment_seed_config(base_config)
        return self._seed_config


def build_context() -> AppContext:
    settings = Settings.from_env()
    store = SQLiteStore(settings.sqlite_db_path)
    browser = RedditBrowser(
        parser=RedditParser(),
        rate_limiter=RateLimiter(),
        user_agent=settings.reddit_user_agent,
        cdp_endpoint=f"http://127.0.0.1:{settings.cdp_port}",
    )
    return AppContext(
        settings=settings,
        store=store,
        browser=browser,
        article_store=ArticleStore(store),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="grow-in-reddit bootstrap CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="bootstrap routine entrypoint")
    run_parser.add_argument(
        "--dry-run", action="store_true", help="show what would run"
    )

    collect_parser = subparsers.add_parser("collect", help="collect subreddit content")
    collect_parser.add_argument(
        "--subreddit",
        action="append",
        help="target subreddit, can be passed multiple times",
    )
    collect_parser.add_argument("--sort", default="hot", help="hot/new/top/rising")
    collect_parser.add_argument(
        "--limit", type=int, default=25, help="posts per subreddit"
    )
    collect_parser.add_argument(
        "--threshold", type=float, help="interest match threshold"
    )
    collect_parser.add_argument(
        "--dry-run", action="store_true", help="fetch only, no writes"
    )

    post_parser = subparsers.add_parser("post", help="submit or draft a text post")
    post_parser.add_argument("--subreddit")
    post_parser.add_argument("--title")
    post_parser.add_argument("--body")
    post_parser.add_argument("--idea")
    post_parser.add_argument("--file")
    post_parser.add_argument("--review-only", action="store_true")

    reply_parser = subparsers.add_parser("reply", help="reply workflow placeholder")
    reply_parser.add_argument("--check", action="store_true")
    reply_parser.add_argument("--engage", action="store_true")
    reply_parser.add_argument("--auto", action="store_true")
    reply_parser.add_argument("--llm", action="store_true")
    reply_parser.add_argument("subreddit", nargs="?")

    nurture_parser = subparsers.add_parser("nurture", help="account care placeholder")
    nurture_parser.add_argument("--phase")

    comment_parser = subparsers.add_parser("comment", help="submit a manual comment")
    comment_parser.add_argument("--url", required=True)
    comment_parser.add_argument("--text", required=True)
    comment_parser.add_argument("--parent-comment-id")

    vote_parser = subparsers.add_parser("vote", help="upvote a post or comment")
    vote_parser.add_argument("--url", required=True)

    browse_parser = subparsers.add_parser("browse", help="simulate a browse session")
    browse_parser.add_argument("subreddit")
    browse_parser.add_argument("--scroll-count", type=int, default=3)

    subparsers.add_parser("snapshot", help="record the current account snapshot")

    intel_parser = subparsers.add_parser("intel", help="community intel workflows")
    intel_parser.add_argument("subreddit", nargs="?")
    intel_parser.add_argument("--revisit", action="store_true")
    intel_parser.add_argument("--report", action="store_true")

    status_parser = subparsers.add_parser("status", help="show local project state")
    status_parser.add_argument(
        "--history",
        type=int,
        default=30,
        help="lookback window in days for action counters",
    )
    status_parser.add_argument(
        "--shadowban",
        action="store_true",
        help="placeholder for future shadowban check",
    )
    return parser


def handle_run(args: argparse.Namespace, context: AppContext) -> int:
    from scheduler import BehaviorProfile, DailyPlanner, RedditScheduler

    planner = DailyPlanner(
        context.store,
        seed_config=context.seed_config(),
        timezone=context.settings.reddit_timezone,
        farming_subreddits=context.settings.farming_subreddits,
    )
    behavior = BehaviorProfile(timezone=context.settings.reddit_timezone)
    scheduler = RedditScheduler(context, planner, behavior)

    if args.dry_run:
        plan = planner.generate_plan(force=True)
        _print_plan(plan)
        return 0

    signal.signal(signal.SIGINT, lambda *_: scheduler.shutdown())
    signal.signal(signal.SIGTERM, lambda *_: scheduler.shutdown())
    print(f"Scheduler started. Timezone: {context.settings.reddit_timezone}")
    print("Press Ctrl+C to stop.")
    scheduler.run_forever()
    print("Scheduler stopped.")
    return 0


def handle_collect(args: argparse.Namespace, context: AppContext) -> int:
    seed_config = context.seed_config()
    profiler = InterestProfiler(context.store, seed_config=seed_config)
    profiler.build_from_history(reset=True)
    matcher = InterestMatcher(
        interest_vector=profiler.get_interest_vector(),
        seed_config=seed_config,
    )
    crawler = FeedCrawler(
        context.browser,
        context.article_store,
        matcher,
        sqlite_store=context.store,
    )

    configured_subreddits = _seed_subreddits(seed_config)
    requested = args.subreddit or configured_subreddits
    subreddits = [
        normalize_subreddit_name(subreddit) for subreddit in requested if subreddit
    ]
    subreddits = sorted({subreddit for subreddit in subreddits if subreddit})
    if not subreddits:
        print(
            "No subreddits configured. Pass `--subreddit r/name` or populate the interests file."
        )
        return 2

    exit_code = 0
    for subreddit in subreddits:
        try:
            result = crawler.collect_subreddit(
                subreddit,
                sort=args.sort,
                limit=args.limit,
                threshold=args.threshold,
                dry_run=args.dry_run,
            )
        except RedditBrowserError as exc:
            exit_code = 1
            print(f"r/{subreddit}: fetch failed: {exc}")
            continue

        print(
            f"{result.subreddit}: fetched={result.fetched} new={result.new_posts} "
            f"matched={result.matched} stored_posts={result.stored_posts} "
            f"stored_comments={result.stored_comments} detail_failures={result.detail_failures}"
        )
        for error_message in result.errors[:5]:
            print(f"  detail error: {error_message}")

    return exit_code


def handle_status(args: argparse.Namespace, context: AppContext) -> int:
    print(f"SQLite: {context.settings.sqlite_db_path}")
    print(f"Cached posts: {context.store.count_cached_posts()}")
    print(
        f"Tracked posts: {len(context.store.list_tracked_posts(active_only=False, days=None))}"
    )

    snapshot = context.store.get_latest_account_snapshot()
    if snapshot is None:
        print("Account snapshot: none")
    else:
        print(
            "Account snapshot: "
            f"{snapshot.day} total_karma={snapshot.karma_total} "
            f"posts={snapshot.total_posts} comments={snapshot.total_comments}"
        )

    action_counts = context.store.get_action_counts(days=args.history)
    if action_counts:
        formatted = ", ".join(
            f"{key}={value}" for key, value in sorted(action_counts.items())
        )
        print(f"Recent actions ({args.history}d): {formatted}")
    else:
        print(f"Recent actions ({args.history}d): none")

    top_interests = context.store.list_interest_topics(limit=8, min_weight=0.05)
    if top_interests:
        formatted = ", ".join(
            f"{item.topic}:{item.weight:.2f}" for item in top_interests
        )
        print(f"Top interests: {formatted}")
    else:
        print("Top interests: none")

    playbooks = context.store.list_community_playbooks(limit=5)
    if playbooks:
        for entry in playbooks:
            print(
                f"Playbook {entry.subreddit}: avg_post_karma={entry.avg_post_karma:.1f} "
                f"avg_comment_karma={entry.avg_comment_karma:.1f} "
                f"pitfalls={len(entry.known_pitfalls)}"
            )
    else:
        print("Playbooks: none")

    if args.shadowban:
        if not context.settings.reddit_username:
            print("Shadowban check requires REDDIT_USERNAME.")
        else:
            visible = context.browser.is_profile_publicly_visible(
                context.settings.reddit_username
            )
            print(
                "Shadowban check: profile visible publicly"
                if visible
                else "Shadowban check: profile not visible publicly"
            )

    return 0


def handle_post(args: argparse.Namespace, context: AppContext) -> int:
    if not args.subreddit:
        print("`post` requires --subreddit.")
        return 2
    if not context.browser.ensure_logged_in():
        print("Reddit login session was not detected in the CDP browser.")
        return 2

    title = (args.title or args.idea or "").strip()
    body = (args.body or "").strip()
    if args.file:
        body = Path(args.file).read_text(encoding="utf-8").strip()
    if not title:
        print("`post` requires --title or --idea.")
        return 2

    try:
        target_url = context.browser.submit_post(
            args.subreddit,
            title,
            body,
            submit=not args.review_only,
        )
    except RedditBrowserError as exc:
        print(f"Post flow failed: {exc}")
        return 1

    if args.review_only:
        print(f"Draft filled successfully for r/{normalize_subreddit_name(args.subreddit)} at {target_url}")
        return 0

    context.store.log_action(
        ActionLog(
            action_type="post",
            subreddit=f"r/{normalize_subreddit_name(args.subreddit)}",
            target_url=target_url,
            content_preview=extract_preview(f"{title}\n\n{body}", max_length=200),
        )
    )
    context.store.track_post(
        TrackedPost(
            url=target_url,
            subreddit=f"r/{normalize_subreddit_name(args.subreddit)}",
            title=title,
            posted_at=datetime.now(timezone.utc),
            comment_count_at_post=0,
            comment_count_latest=0,
            is_active=True,
        )
    )
    print(f"Post submitted successfully: {target_url}")
    return 0


def handle_comment(args: argparse.Namespace, context: AppContext) -> int:
    if not context.browser.ensure_logged_in():
        print("Reddit login session was not detected in the CDP browser.")
        return 2
    try:
        target_url = context.browser.submit_comment(
            args.url,
            args.text,
            parent_comment_id=args.parent_comment_id,
        )
    except RedditBrowserError as exc:
        print(f"Comment flow failed: {exc}")
        return 1

    context.store.log_action(
        ActionLog(
            action_type="comment",
            target_url=target_url,
            content_preview=extract_preview(args.text, max_length=200),
        )
    )
    print(f"Comment submitted successfully: {target_url}")
    return 0


def handle_vote(args: argparse.Namespace, context: AppContext) -> int:
    if not context.browser.ensure_logged_in():
        print("Reddit login session was not detected in the CDP browser.")
        return 2
    try:
        active = context.browser.upvote(args.url)
    except RedditBrowserError as exc:
        print(f"Vote flow failed: {exc}")
        return 1

    context.store.log_action(
        ActionLog(
            action_type="vote",
            target_url=args.url,
            content_preview="Upvoted via CLI" if active else "Vote no-op",
        )
    )
    print("Upvote is active." if active else "Target was not upvoted.")
    return 0


def handle_browse(args: argparse.Namespace, context: AppContext) -> int:
    if not context.browser.ensure_logged_in():
        print("Reddit login session was not detected in the CDP browser.")
        return 2
    try:
        actions = context.browser.browse_and_engage(
            args.subreddit,
            scroll_count=args.scroll_count,
        )
    except RedditBrowserError as exc:
        print(f"Browse flow failed: {exc}")
        return 1

    for action in actions:
        context.store.log_action(
            ActionLog(
                action_type="vote" if action.action == "upvote" else "browse",
                subreddit=action.subreddit,
                target_url=action.target_url,
                content_preview=action.note or action.action,
            )
        )
    summary = ", ".join(action.action for action in actions)
    print(f"Browse session complete: {summary}")
    return 0


def handle_snapshot(args: argparse.Namespace, context: AppContext) -> int:
    try:
        snapshot = _record_account_snapshot(context)
    except RedditBrowserError as exc:
        print(f"Snapshot failed: {exc}")
        return 1
    print(
        f"Snapshot recorded for u/{context.settings.reddit_username}: "
        f"post={snapshot.karma_post} comment={snapshot.karma_comment} total={snapshot.karma_total}"
    )
    return 0


def handle_reply(args: argparse.Namespace, context: AppContext) -> int:
    if args.engage:
        if not args.subreddit:
            print("`reply --engage` requires a subreddit.")
            return 2
        seed_config = context.seed_config()
        profiler = InterestProfiler(context.store, seed_config=seed_config)
        profiler.build_from_history(reset=False)
        finder = EngagementFinder(
            context.browser,
            InterestMatcher(
                interest_vector=profiler.get_interest_vector(),
                seed_config=seed_config,
            ),
        )
        opportunities = finder.find_opportunities(args.subreddit)
        if not opportunities:
            print("No reply opportunities found.")
            return 0
        for item in opportunities[:10]:
            print(
                f"{item.priority:.2f} {item.opportunity_type} {item.post.url} | "
                f"{item.suggested_angle}"
            )
        return 0

    if not (args.check or args.auto):
        print("Use `reply --check`, `reply --auto`, or `reply --engage <subreddit>`.")
        return 2
    if not context.settings.reddit_username:
        print("REDDIT_USERNAME is required for reply workflows.")
        return 2

    tracker = ThreadTracker(
        context.browser,
        context.store,
        own_username=context.settings.reddit_username,
    )
    generator = ReplyGenerator(
        use_llm=args.llm,
        llm_provider=context.settings.llm_provider,
        llm_base_url=context.settings.llm_base_url,
        llm_model=context.settings.llm_model,
    )
    replies = tracker.list_pending_replies(refresh=True) if args.auto else tracker.check_new_replies()
    if not replies:
        print("No new replies detected.")
        return 0

    exit_code = 0
    for new_reply in replies:
        should_reply, reason = generator.should_reply(new_reply)
        print(
            f"{new_reply.comment.id} direct={new_reply.is_direct_reply} "
            f"should_reply={should_reply} reason={reason}"
        )
        if not args.auto or not should_reply:
            continue
        reply_text = generator.generate_reply(
            ReplyContext(
                subreddit=new_reply.post.subreddit,
                post=new_reply.post,
                comment=new_reply.comment,
                context_chain=new_reply.context_chain,
                is_direct_reply=new_reply.is_direct_reply,
            )
        )
        try:
            reply_url = context.browser.submit_comment(
                new_reply.post_url,
                reply_text,
                parent_comment_id=new_reply.comment.id,
            )
        except RedditBrowserError as exc:
            context.store.mark_seen_comment_replied(
                new_reply.comment.id,
                reply_status="failed",
            )
            print(f"  auto-reply failed: {exc}")
            exit_code = 1
            continue

        tracker.mark_replied(
            new_reply.comment.id,
            reply_comment_id=_extract_comment_id(reply_url),
        )
        context.store.log_action(
            ActionLog(
                action_type="comment",
                subreddit=new_reply.post.subreddit,
                target_url=reply_url,
                content_preview=extract_preview(reply_text, max_length=200),
            )
        )
        print(f"  replied: {reply_url}")

    playbook = CommunityPlaybook(context.store)
    tracked_count = OutcomeTracker(context.browser, context.store, playbook).track_recent_actions(hours=24)
    if tracked_count:
        print(f"Tracked outcomes updated: {tracked_count}")
    return exit_code


def handle_intel(args: argparse.Namespace, context: AppContext) -> int:
    intel = CommunityIntelligence(context.browser, context.store)
    if args.revisit:
        updated = intel.revisit_snapshots()
        print(f"Revisited snapshots: {updated}")
        return 0
    if not args.subreddit:
        print("`intel` requires a subreddit unless `--revisit` is used.")
        return 2
    if args.report:
        report = intel.build_report(args.subreddit)
        print(f"Subreddit: {report['subreddit']}")
        print(f"Snapshots: {report['snapshots']}")
        print(f"Removed: {report['removed']}")
        power_users = report["power_users"]
        if power_users:
            for entry in power_users:
                print(
                    f"Power user {entry.username}: posts={entry.post_count} "
                    f"avg_score={entry.avg_score:.1f}"
                )
        else:
            print("Power users: none")
        return 0

    collected = intel.collect_snapshot(args.subreddit)
    power_users = intel.identify_power_users(args.subreddit)
    print(f"Snapshot collected: {collected}")
    print(f"Power users refreshed: {len(power_users)}")
    return 0


def handle_placeholder(command: str) -> int:
    print(f"`{command}` is scaffolded in the CLI but not implemented in Phase 1 yet.")
    return 0


def handle_nurture(args: argparse.Namespace, context: AppContext) -> int:
    from scheduler import BehaviorProfile, DailyPlan, DailyPlanner, RedditScheduler

    planner = DailyPlanner(
        context.store,
        seed_config=context.seed_config(),
        timezone=context.settings.reddit_timezone,
        farming_subreddits=context.settings.farming_subreddits,
    )
    phase = args.phase or planner.get_current_phase()
    session = planner.build_immediate_session(phase=phase)
    scheduler = RedditScheduler(
        context,
        planner,
        BehaviorProfile(timezone=context.settings.reddit_timezone),
    )
    scheduler.current_plan = DailyPlan(
        date=date.today(),
        phase=phase,
        skip_today=False,
        skip_reason="",
        sessions=[session],
    )
    execution = scheduler.execute_session(session, sleep_between_tasks=False)
    print(f"Phase: {phase}")
    print(f"Session: {execution.session.session_type}")
    for task in execution.session.tasks:
        target = task.subreddit or "(auto)"
        print(f"  {task.task_type:12s} {target:18s} {task.result}")
    if execution.health_report is not None:
        print(
            f"Health: {execution.health_report.recommended_action} "
            f"warnings={len(execution.health_report.warnings)}"
        )
    return 0


def _seed_subreddits(seed_config: dict[str, Any]) -> list[str]:
    subreddits: set[str] = set()
    for bucket in ("primary", "secondary"):
        for entry in seed_config.get(bucket, []):
            for subreddit in entry.get("subreddits", []):
                normalized = normalize_subreddit_name(subreddit)
                if normalized:
                    subreddits.add(normalized)
    return sorted(subreddits)


def _extract_comment_id(target_url: str) -> str | None:
    parsed = parse.urlparse(target_url)
    if parsed.fragment.startswith("t1_"):
        return parsed.fragment
    segments = [segment for segment in parsed.path.split("/") if segment]
    for index, segment in enumerate(segments):
        if segment == "comment" and index + 1 < len(segments):
            value = segments[index + 1]
            return value if value.startswith("t1_") else f"t1_{value}"
    return None


def _record_account_snapshot(context: AppContext) -> AccountSnapshot:
    if not context.settings.reddit_username:
        raise RedditBrowserError("REDDIT_USERNAME is required for snapshot.")
    profile = context.browser.get_user_profile(context.settings.reddit_username)
    actions = context.store.list_actions(limit=5000, days=365)
    active_subreddits = len({action.subreddit for action in actions if action.subreddit})
    snapshot = AccountSnapshot(
        day=datetime.now(timezone.utc).date(),
        karma_post=profile.karma_post,
        karma_comment=profile.karma_comment,
        karma_total=profile.karma_total,
        active_subreddits=active_subreddits,
        total_posts=sum(1 for action in actions if action.action_type == "post"),
        total_comments=sum(1 for action in actions if action.action_type == "comment"),
    )
    context.store.record_account_snapshot(snapshot)
    context.store.log_action(
        ActionLog(
            action_type="snapshot",
            subreddit=None,
            target_url=f"https://www.reddit.com/user/{profile.username}/",
            content_preview=f"karma_total={profile.karma_total}",
        )
    )
    return snapshot


def _print_plan(plan: Any) -> None:
    print(f"Date: {plan.date}")
    print(f"Phase: {plan.phase}")
    if plan.skip_today:
        print(f"SKIP: {plan.skip_reason}")
        return
    for index, session in enumerate(plan.sessions, start=1):
        print(
            f"\nSession {index}: {session.session_type} "
            f"({session.window_start.isoformat(timespec='minutes')} - "
            f"{session.window_end.isoformat(timespec='minutes')})"
        )
        for task in session.tasks:
            target = task.subreddit or "(auto)"
            print(f"  [{task.priority}] {task.task_type:15s} -> {target}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    context = build_context()
    try:
        if args.command == "run":
            return handle_run(args, context)
        if args.command == "collect":
            return handle_collect(args, context)
        if args.command == "post":
            return handle_post(args, context)
        if args.command == "comment":
            return handle_comment(args, context)
        if args.command == "vote":
            return handle_vote(args, context)
        if args.command == "browse":
            return handle_browse(args, context)
        if args.command == "reply":
            return handle_reply(args, context)
        if args.command == "snapshot":
            return handle_snapshot(args, context)
        if args.command == "intel":
            return handle_intel(args, context)
        if args.command == "status":
            return handle_status(args, context)
        if args.command == "nurture":
            return handle_nurture(args, context)
        parser.error(f"Unsupported command: {args.command}")
    finally:
        context.store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
