from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from collector import InterestMatcher
from reddit_browser import RedditBrowser
from storage import Post, PostDetail
from storage.models import utc_now


@dataclass(slots=True)
class Opportunity:
    post: Post
    post_detail: PostDetail
    opportunity_type: str
    suggested_angle: str
    priority: float


class EngagementFinder:
    """Find relatively low-risk reply opportunities in a target subreddit."""

    def __init__(
        self,
        browser: RedditBrowser,
        matcher: InterestMatcher | None = None,
    ):
        self.browser = browser
        self.matcher = matcher

    def find_opportunities(
        self, subreddit: str, *, limit: int = 10
    ) -> list[Opportunity]:
        opportunities: list[Opportunity] = []
        feed = self.browser.get_subreddit_feed(subreddit, sort="new", limit=limit)
        now = utc_now()
        for post in feed:
            age = now - post.created_utc
            if age < timedelta(hours=1) or age > timedelta(hours=6):
                continue
            if post.num_comments > 20 or post.score <= 0:
                continue
            if self.matcher and self.matcher.match_interest(post) < 0.35:
                continue
            detail = self.browser.get_post_detail(post.url)
            gap = self._classify_gap(detail)
            if gap is None:
                continue
            opportunity_type, angle = gap
            freshness_bonus = max(0.0, 1.0 - (age.total_seconds() / 21_600))
            priority = round(
                min(
                    1.0,
                    0.35
                    + freshness_bonus * 0.25
                    + min(post.num_comments, 10) * 0.02
                    + min(max(post.score, 0), 20) * 0.01,
                ),
                4,
            )
            opportunities.append(
                Opportunity(
                    post=post,
                    post_detail=detail,
                    opportunity_type=opportunity_type,
                    suggested_angle=angle,
                    priority=priority,
                )
            )
        return sorted(opportunities, key=lambda item: item.priority, reverse=True)

    def _classify_gap(self, detail: PostDetail) -> tuple[str, str] | None:
        post_text = " ".join(
            value for value in [detail.post.title, detail.post.body] if value
        ).strip()
        if not self._has_reply_gap(detail):
            return None
        if "?" in post_text:
            return "unanswered_question", "Answer the core question directly and keep it practical."
        if any("?" in comment.body for comment in detail.comments):
            return "add_perspective", "Add a concrete follow-up where the thread is still open."
        return "share_experience", "Contribute one concrete experience or example."

    def _has_reply_gap(self, detail: PostDetail) -> bool:
        if not detail.comments:
            return True
        replies_by_parent = {
            comment.parent_id for comment in detail.comments if comment.parent_id
        }
        for comment in detail.comments:
            if "?" in comment.body and comment.id not in replies_by_parent:
                return True
        return detail.post.num_comments <= 3
