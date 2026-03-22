from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

from collector import ArticleStore, FeedCrawler, InterestMatcher
from reddit_browser import RedditBrowser, RedditBrowserError, RedditParser, RateLimiter
from reddit_memory import InterestProfiler
from settings import Settings
from storage import SQLiteStore
from utils import normalize_subreddit_name


@dataclass(slots=True)
class AppContext:
    settings: Settings
    store: SQLiteStore
    browser: RedditBrowser
    article_store: ArticleStore
    _seed_config: dict[str, Any] | None = None

    def seed_config(self) -> dict[str, Any]:
        if self._seed_config is None:
            self._seed_config = self.settings.load_interest_seeds()
        return self._seed_config


def build_context() -> AppContext:
    settings = Settings.from_env()
    store = SQLiteStore(settings.sqlite_db_path)
    browser = RedditBrowser(
        parser=RedditParser(),
        rate_limiter=RateLimiter(),
        user_agent=settings.reddit_user_agent,
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

    post_parser = subparsers.add_parser("post", help="post workflow placeholder")
    post_parser.add_argument("--idea")
    post_parser.add_argument("--file")
    post_parser.add_argument("--review-only", action="store_true")

    reply_parser = subparsers.add_parser("reply", help="reply workflow placeholder")
    reply_parser.add_argument("--check", action="store_true")
    reply_parser.add_argument("--engage", action="store_true")

    nurture_parser = subparsers.add_parser("nurture", help="account care placeholder")
    nurture_parser.add_argument("--phase")

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
    print("Phase-1 bootstrap is installed.")
    if args.dry_run:
        print("Scheduler is not implemented yet.")
        print("Use `collect` to pull content and `status` to inspect local state.")
    else:
        print("`run` is still a placeholder. Use `collect` or `status` for now.")
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
        print("Shadowban check is not implemented yet.")

    return 0


def handle_placeholder(command: str) -> int:
    print(f"`{command}` is scaffolded in the CLI but not implemented in Phase 1 yet.")
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    context = build_context()
    try:
        if args.command == "run":
            return handle_run(args, context)
        if args.command == "collect":
            return handle_collect(args, context)
        if args.command == "status":
            return handle_status(args, context)
        if args.command in {"post", "reply", "nurture"}:
            return handle_placeholder(args.command)
        parser.error(f"Unsupported command: {args.command}")
    finally:
        context.store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
