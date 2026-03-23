from __future__ import annotations

from datetime import timedelta
from urllib import parse

from reddit_browser import RedditBrowser, RedditBrowserError
from reddit_memory import CommunityPlaybook
from storage import ActionOutcome, SQLiteStore
from storage.models import utc_now


class OutcomeTracker:
    """Track recent post/comment outcomes and feed them back into the playbook."""

    def __init__(
        self,
        browser: RedditBrowser,
        sqlite_store: SQLiteStore,
        playbook: CommunityPlaybook,
    ):
        self.browser = browser
        self.sqlite_store = sqlite_store
        self.playbook = playbook

    def track_recent_actions(self, hours: int = 24) -> int:
        cutoff = utc_now() - timedelta(hours=hours)
        actions = [
            action
            for action in self.sqlite_store.list_actions(limit=5000, days=max(2, hours // 24 + 1))
            if action.action_type in {"post", "comment"}
            and action.timestamp >= cutoff
            and action.target_url
        ]
        tracked = 0
        for action in actions:
            try:
                detail = self.browser.get_post_detail(action.target_url or "")
            except RedditBrowserError as exc:
                outcome = ActionOutcome(
                    subreddit=action.subreddit or "",
                    action_type=action.action_type,
                    content_summary=action.content_preview,
                    karma_final=0,
                    was_removed=True,
                    removal_reason=str(exc),
                )
                self.playbook.record_outcome(
                    action.subreddit or "",
                    action.action_type,
                    action.content_preview,
                    outcome,
                )
                tracked += 1
                continue

            if action.action_type == "post":
                outcome = ActionOutcome(
                    subreddit=action.subreddit or detail.post.subreddit,
                    action_type="post",
                    content_summary=action.content_preview,
                    title=detail.post.title,
                    post_type="self" if detail.post.is_self else "link",
                    karma_final=detail.post.score,
                    comment_count=detail.post.num_comments,
                )
            else:
                target_comment = self._locate_comment(detail.comments, action.target_url or "")
                outcome = ActionOutcome(
                    subreddit=action.subreddit or detail.post.subreddit,
                    action_type="comment",
                    content_summary=action.content_preview,
                    title=detail.post.title,
                    post_type="comment",
                    karma_final=target_comment.score if target_comment else 0,
                    comment_count=detail.post.num_comments,
                    was_removed=target_comment is None,
                    removal_reason=None if target_comment else "comment_not_found",
                )
            self.playbook.record_outcome(
                outcome.subreddit,
                outcome.action_type,
                action.content_preview,
                outcome,
            )
            tracked += 1
        return tracked

    @staticmethod
    def _locate_comment(comments: list[object], target_url: str) -> object | None:
        parsed = parse.urlparse(target_url)
        comment_id = parsed.fragment or ""
        if not comment_id:
            segments = [segment for segment in parsed.path.split("/") if segment]
            for index, segment in enumerate(segments):
                if segment == "comment" and index + 1 < len(segments):
                    comment_id = segments[index + 1]
                    break
        if comment_id and not comment_id.startswith("t1_"):
            comment_id = f"t1_{comment_id}"
        for comment in comments:
            if getattr(comment, "id", None) == comment_id:
                return comment
        return None
