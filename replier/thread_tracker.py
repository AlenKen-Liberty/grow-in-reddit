from __future__ import annotations

from dataclasses import dataclass

from reddit_browser import RedditBrowser
from storage import Comment, Post, SeenComment, SQLiteStore
from utils import extract_preview


@dataclass(slots=True)
class NewReply:
    post_url: str
    post: Post
    comment: Comment
    is_direct_reply: bool
    context_chain: list[Comment]


class ThreadTracker:
    """Track new comments on our active posts."""

    def __init__(
        self, browser: RedditBrowser, sqlite_store: SQLiteStore, *, own_username: str
    ):
        self.browser = browser
        self.sqlite_store = sqlite_store
        self.own_username = own_username.strip().lower()

    def check_new_replies(self) -> list[NewReply]:
        replies: list[NewReply] = []
        tracked_posts = self.sqlite_store.list_tracked_posts(
            active_only=True,
            days=None,
        )
        for tracked in tracked_posts:
            detail = self.browser.get_post_detail(tracked.url)
            seen_ids = self.sqlite_store.get_seen_comment_ids(tracked.url)
            comment_by_id = {
                comment.id: comment for comment in detail.comments if comment.id
            }
            own_comment_ids = {
                comment.id
                for comment in detail.comments
                if (comment.author or "").strip().lower() == self.own_username
            }
            post_id = detail.post.id
            for comment in detail.comments:
                if not comment.id or comment.id in seen_ids:
                    continue
                if (comment.author or "").strip().lower() == self.own_username:
                    continue
                is_direct_reply = bool(
                    comment.depth == 0
                    or (post_id and comment.parent_id == post_id)
                    or comment.parent_id in own_comment_ids
                )
                self.sqlite_store.upsert_seen_comment(
                    SeenComment(
                        comment_id=comment.id,
                        post_url=tracked.url,
                        author=comment.author,
                        body_preview=extract_preview(comment.body, max_length=200),
                        is_direct_reply=is_direct_reply,
                    )
                )
                replies.append(
                    NewReply(
                        post_url=tracked.url,
                        post=detail.post,
                        comment=comment,
                        is_direct_reply=is_direct_reply,
                        context_chain=self._build_context_chain(comment, comment_by_id),
                    )
                )
            self.sqlite_store.mark_tracked_post_checked(
                tracked.url,
                comment_count_latest=max(detail.post.num_comments, len(detail.comments)),
                is_active=detail.post.num_comments > 0 or tracked.is_active,
            )
        return sorted(replies, key=lambda item: item.comment.created_utc)

    def mark_replied(
        self, comment_id: str, *, reply_comment_id: str | None = None
    ) -> None:
        self.sqlite_store.mark_seen_comment_replied(
            comment_id,
            reply_comment_id=reply_comment_id,
        )

    @staticmethod
    def _build_context_chain(
        comment: Comment, comment_by_id: dict[str, Comment]
    ) -> list[Comment]:
        chain: list[Comment] = []
        current = comment
        visited: set[str] = set()
        while current.id and current.id not in visited:
            visited.add(current.id)
            chain.append(current)
            if not current.parent_id or current.parent_id not in comment_by_id:
                break
            current = comment_by_id[current.parent_id]
        chain.reverse()
        return chain
