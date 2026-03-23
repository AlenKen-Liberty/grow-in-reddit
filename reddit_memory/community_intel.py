from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from statistics import median

from reddit_browser import RedditBrowser, RedditBrowserError
from storage import CommunityPowerUser, CommunitySnapshot, SQLiteStore
from storage.models import utc_now
from utils import extract_preview, normalize_subreddit_name


class CommunityIntelligence:
    """Collect and revisit subreddit snapshots for lightweight competitive intel."""

    BOT_USERNAMES = {"automoderator", "modmailbot", "botdefense", "[deleted]"}

    def __init__(self, browser: RedditBrowser, sqlite_store: SQLiteStore):
        self.browser = browser
        self.sqlite_store = sqlite_store

    def collect_snapshot(self, subreddit: str) -> int:
        normalized = normalize_subreddit_name(subreddit)
        if not normalized:
            raise ValueError("subreddit is required")
        hot_posts = self.browser.get_subreddit_feed(normalized, sort="hot", limit=50)
        new_posts = self.browser.get_subreddit_feed(normalized, sort="new", limit=30)
        unique_posts = {post.url: post for post in [*hot_posts, *new_posts]}
        count = 0
        for post in unique_posts.values():
            self.sqlite_store.upsert_community_snapshot(
                CommunitySnapshot(
                    subreddit=f"r/{normalized}",
                    post_url=post.url,
                    title=post.title,
                    author=post.author,
                    flair=post.flair,
                    score_at_capture=post.score,
                    comment_count_at_capture=post.num_comments,
                    posted_at=post.created_utc,
                    body_preview=extract_preview(post.body, max_length=240),
                )
            )
            count += 1
        return count

    def revisit_snapshots(self, hours_ago: int = 24) -> int:
        updated = 0
        candidates = self.sqlite_store.list_snapshot_candidates_for_revisit(
            hours_ago=hours_ago
        )
        for snapshot in candidates:
            try:
                detail = self.browser.get_post_detail(snapshot.post_url)
            except RedditBrowserError:
                self.sqlite_store.update_community_snapshot_revisit(
                    snapshot.id or 0,
                    was_removed=True,
                    removal_detected_at=utc_now(),
                )
                updated += 1
                continue
            self.sqlite_store.update_community_snapshot_revisit(
                snapshot.id or 0,
                score_after_24h=detail.post.score,
                comment_count_after_24h=detail.post.num_comments,
                was_removed=False,
                mod_comment=self._detect_mod_comment(detail.comments),
            )
            updated += 1
        return updated

    def detect_removals(self, subreddit: str, days: int = 7) -> list[dict]:
        cutoff = utc_now() - timedelta(days=days)
        results: list[dict] = []
        for snapshot in self.sqlite_store.list_community_snapshots(
            subreddit=f"r/{normalize_subreddit_name(subreddit)}",
            removed_only=True,
            limit=1000,
        ):
            if snapshot.captured_at < cutoff:
                continue
            results.append(
                {
                    "post_url": snapshot.post_url,
                    "title": snapshot.title,
                    "author": snapshot.author,
                    "removal_type": "removed",
                    "mod_comment": snapshot.mod_comment,
                    "captured_at": snapshot.captured_at,
                }
            )
        return results

    def identify_power_users(self, subreddit: str, days: int = 30) -> list[dict]:
        normalized = f"r/{normalize_subreddit_name(subreddit)}"
        cutoff = utc_now() - timedelta(days=days)
        grouped: defaultdict[str, list[CommunitySnapshot]] = defaultdict(list)
        for snapshot in self.sqlite_store.list_community_snapshots(
            subreddit=normalized,
            limit=5000,
        ):
            if snapshot.captured_at < cutoff:
                continue
            author = (snapshot.author or "").strip()
            if not author or author.lower() in self.BOT_USERNAMES:
                continue
            grouped[author].append(snapshot)

        average_scores = [
            sum((entry.score_at_capture or 0) for entry in snapshots) / len(snapshots)
            for snapshots in grouped.values()
            if snapshots
        ]
        score_floor = median(average_scores) if average_scores else 0.0
        results: list[dict] = []
        for author, snapshots in grouped.items():
            post_count = len(snapshots)
            avg_score = sum((entry.score_at_capture or 0) for entry in snapshots) / post_count
            if post_count < 3 or avg_score <= score_floor:
                continue
            estimated_karma = None
            try:
                estimated_karma = self.browser.get_user_profile(author).karma_total
            except Exception:
                estimated_karma = None
            profile = CommunityPowerUser(
                subreddit=normalized,
                username=author,
                estimated_karma=estimated_karma,
                post_count=post_count,
                avg_score=round(avg_score, 2),
                notes="Top contributor from community_snapshot",
            )
            self.sqlite_store.upsert_community_power_user(profile)
            results.append(
                {
                    "username": author,
                    "post_count": post_count,
                    "avg_score": round(avg_score, 2),
                    "estimated_karma": estimated_karma,
                }
            )
        return sorted(results, key=lambda item: item["avg_score"], reverse=True)[:10]

    def build_report(self, subreddit: str) -> dict[str, object]:
        normalized = f"r/{normalize_subreddit_name(subreddit)}"
        snapshots = self.sqlite_store.list_community_snapshots(
            subreddit=normalized,
            limit=500,
        )
        removed = [snapshot for snapshot in snapshots if snapshot.was_removed]
        return {
            "subreddit": normalized,
            "snapshots": len(snapshots),
            "removed": len(removed),
            "power_users": self.sqlite_store.list_community_power_users(
                subreddit=normalized,
                limit=10,
            ),
        }

    @staticmethod
    def _detect_mod_comment(comments: list[object]) -> str | None:
        for comment in comments:
            author = (getattr(comment, "author", "") or "").lower()
            if "mod" in author or author == "automoderator":
                return extract_preview(getattr(comment, "body", ""), max_length=240)
        return None
