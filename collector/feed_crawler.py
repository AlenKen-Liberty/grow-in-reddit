from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from reddit_browser import RedditBrowser
from storage import ActionLog, SQLiteStore
from utils import extract_preview, normalize_subreddit_name

from .article_store import ArticleStore
from .interest_matcher import InterestMatcher


@dataclass(slots=True)
class CrawlResult:
    subreddit: str
    sort: str
    fetched: int = 0
    new_posts: int = 0
    matched: int = 0
    stored_posts: int = 0
    stored_comments: int = 0
    detail_failures: int = 0
    errors: list[str] = field(default_factory=list)


class FeedCrawler:
    def __init__(
        self,
        browser: RedditBrowser,
        article_store: ArticleStore,
        matcher: InterestMatcher,
        *,
        sqlite_store: SQLiteStore | None = None,
    ):
        self.browser = browser
        self.article_store = article_store
        self.matcher = matcher
        self.sqlite_store = sqlite_store

    def collect_subreddit(
        self,
        subreddit: str,
        *,
        sort: str = "hot",
        limit: int = 25,
        threshold: float | None = None,
        dry_run: bool = False,
    ) -> CrawlResult:
        normalized = normalize_subreddit_name(subreddit)
        result = CrawlResult(subreddit=f"r/{normalized}", sort=sort)
        posts = self.browser.get_subreddit_feed(normalized, sort=sort, limit=limit)
        result.fetched = len(posts)

        unseen_posts = [
            post for post in posts if not self.article_store.has_post(post.url)
        ]
        result.new_posts = len(unseen_posts)
        cutoff = self.matcher.default_threshold if threshold is None else threshold

        for post in unseen_posts:
            post.interest_score = self.matcher.match_interest(post)
            if post.interest_score >= cutoff:
                result.matched += 1

            if dry_run:
                continue

            self.article_store.store_post(post)
            result.stored_posts += 1

            if post.interest_score >= cutoff:
                try:
                    detail = self.browser.get_post_detail(post.url)
                    _, stored_comments = self.article_store.store_post_detail(detail)
                    result.stored_comments += stored_comments
                except Exception as exc:  # pragma: no cover - depends on live payloads
                    result.detail_failures += 1
                    result.errors.append(f"{post.url}: {exc}")

        if self.sqlite_store is not None and not dry_run:
            self.sqlite_store.log_action(
                ActionLog(
                    action_type="browse",
                    subreddit=f"r/{normalized}",
                    target_url=f"https://www.reddit.com/r/{normalized}/{sort}",
                    content_preview=extract_preview(
                        f"Fetched {result.fetched} posts, stored {result.stored_posts}, matched {result.matched}"
                    ),
                )
            )

        return result

    def collect_many(
        self,
        subreddits: Iterable[str],
        *,
        sort: str = "hot",
        limit: int = 25,
        threshold: float | None = None,
        dry_run: bool = False,
    ) -> list[CrawlResult]:
        return [
            self.collect_subreddit(
                subreddit,
                sort=sort,
                limit=limit,
                threshold=threshold,
                dry_run=dry_run,
            )
            for subreddit in subreddits
        ]
