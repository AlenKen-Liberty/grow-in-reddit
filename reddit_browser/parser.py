from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from storage.models import Comment, Post, PostDetail, UserProfile

BASE_URL = "https://www.reddit.com"


def _utc_from_timestamp(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


def _absolute_url(value: str | None, fallback: str | None = None) -> str:
    raw = value or fallback or ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return f"{BASE_URL}{raw}"
    return raw


class RedditParser:
    def parse_feed_json(self, json_data: dict[str, Any]) -> list[Post]:
        children = ((json_data or {}).get("data") or {}).get("children") or []
        posts: list[Post] = []
        for child in children:
            post = self._parse_post_node(child)
            if post is not None:
                posts.append(post)
        return posts

    def parse_feed_dom(self, page: Any) -> list[Post]:
        raise NotImplementedError("DOM parsing fallback is not implemented yet.")

    def parse_feed_old(self, page: Any) -> list[Post]:
        raise NotImplementedError("old.reddit parsing fallback is not implemented yet.")

    def parse_post_detail(self, json_data: list[dict[str, Any]]) -> PostDetail:
        if not isinstance(json_data, list) or len(json_data) < 2:
            raise ValueError("Unexpected post detail payload.")

        post_listing = json_data[0]
        comments_listing = json_data[1]
        post_children = ((post_listing or {}).get("data") or {}).get("children") or []
        if not post_children:
            raise ValueError("Post detail payload does not include a post record.")

        post = self._parse_post_node(post_children[0])
        if post is None:
            raise ValueError("Unable to parse post detail payload.")

        comments: list[Comment] = []
        for child in ((comments_listing or {}).get("data") or {}).get("children") or []:
            comments.extend(self._parse_comment_node(child, post.url, depth=0))
        return PostDetail(post=post, comments=comments)

    def parse_user_profile(self, json_data: dict[str, Any]) -> UserProfile:
        data = (json_data or {}).get("data") or {}
        trophies = []
        for trophy in (data.get("subreddit") or {}).get("trophies") or []:
            if isinstance(trophy, dict) and trophy.get("name"):
                trophies.append(trophy["name"])
        return UserProfile(
            username=data.get("name") or "",
            karma_post=int(data.get("link_karma") or 0),
            karma_comment=int(data.get("comment_karma") or 0),
            cake_day=_utc_from_timestamp(data.get("created_utc")).date(),
            is_premium=bool(data.get("is_gold") or data.get("is_premium")),
            trophies=trophies,
        )

    def _parse_post_node(self, node: dict[str, Any]) -> Post | None:
        if not isinstance(node, dict) or node.get("kind") != "t3":
            return None
        data = node.get("data") or {}
        permalink = data.get("permalink")
        url = _absolute_url(
            data.get("url_overridden_by_dest") or data.get("url"),
            fallback=permalink,
        )
        return Post(
            url=url,
            subreddit=data.get("subreddit_name_prefixed")
            or data.get("subreddit")
            or "",
            title=data.get("title") or "",
            body=data.get("selftext") or "",
            author=data.get("author") or "[deleted]",
            score=int(data.get("score") or 0),
            num_comments=int(data.get("num_comments") or 0),
            created_utc=_utc_from_timestamp(data.get("created_utc")),
            flair=data.get("link_flair_text"),
            is_self=bool(data.get("is_self", True)),
        )

    def _parse_comment_node(
        self, node: dict[str, Any], post_url: str, *, depth: int
    ) -> list[Comment]:
        if not isinstance(node, dict):
            return []
        kind = node.get("kind")
        if kind != "t1":
            return []

        data = node.get("data") or {}
        comment = Comment(
            id=data.get("name") or data.get("id") or "",
            post_url=post_url,
            author=data.get("author") or "[deleted]",
            body=data.get("body") or "",
            score=int(data.get("score") or 0),
            created_utc=_utc_from_timestamp(data.get("created_utc")),
            parent_id=data.get("parent_id"),
            depth=depth,
        )
        comments = [comment]
        replies = data.get("replies")
        if isinstance(replies, dict):
            children = ((replies.get("data") or {}).get("children")) or []
            for child in children:
                comments.extend(
                    self._parse_comment_node(child, post_url=post_url, depth=depth + 1)
                )
        return comments
