from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from storage.models import Comment, Post, PostDetail, UserProfile

BASE_URL = "https://www.reddit.com"
COMPACT_NUMBER_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<suffix>[kmb])?", re.I)


def _utc_from_timestamp(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


def _parse_reddit_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if len(text) >= 5 and text[-5] in {"+", "-"} and text[-3] != ":":
        text = text[:-2] + ":" + text[-2:]
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _absolute_url(value: str | None, fallback: str | None = None) -> str:
    raw = value or fallback or ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return f"{BASE_URL}{raw}"
    return raw


def _parse_compact_number(value: str | None) -> int:
    if not value:
        return 0
    cleaned = value.replace(",", "").strip().lower()
    match = COMPACT_NUMBER_RE.search(cleaned)
    if not match:
        return 0
    number = float(match.group("value"))
    suffix = (match.group("suffix") or "").lower()
    multiplier = {"": 1, "k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[suffix]
    return int(number * multiplier)


class RedditParser:
    def parse_feed_json(self, json_data: dict[str, Any]) -> list[Post]:
        children = ((json_data or {}).get("data") or {}).get("children") or []
        posts: list[Post] = []
        for child in children:
            post = self._parse_post_node(child)
            if post is not None:
                posts.append(post)
        return posts

    def parse_feed_dom(self, page: Any, limit: int | None = None) -> list[Post]:
        records = page.evaluate(
            """
            (limit) => {
              const posts = Array.from(document.querySelectorAll('shreddit-post'));
              return posts.slice(0, limit ?? posts.length).map((post) => {
                const permalink = post.getAttribute('permalink') || '';
                const bodyNode =
                  post.querySelector('[slot="text-body"]') ||
                  post.querySelector('[slot="body"]') ||
                  post.querySelector('p');
                const flairNode =
                  post.querySelector('[slot="flair"]') ||
                  post.querySelector('[data-testid="post-flair"]');
                return {
                  url: permalink ? new URL(permalink, location.origin).href : location.href,
                  id:
                    post.getAttribute('id') ||
                    post.getAttribute('thingid') ||
                    post.getAttribute('post-id') ||
                    '',
                  subreddit:
                    post.getAttribute('subreddit-prefixed-name') ||
                    post.getAttribute('subreddit-name') ||
                    '',
                  title:
                    post.getAttribute('post-title') ||
                    post.querySelector('h3')?.innerText ||
                    '',
                  body: bodyNode?.innerText || '',
                  author: post.getAttribute('author') || '',
                  score: Number(post.getAttribute('score') || 0),
                  num_comments: Number(post.getAttribute('comment-count') || 0),
                  created_utc: post.getAttribute('created-timestamp') || '',
                  flair: flairNode?.innerText || '',
                  is_self:
                    !post.getAttribute('content-href') ||
                    (post.getAttribute('domain') || '').includes('reddit.com'),
                };
              });
            }
            """,
            limit,
        )
        return [self._post_from_dom_record(record) for record in records]

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

    def parse_post_detail_dom(self, page: Any) -> PostDetail:
        payload = page.evaluate(
            """
            () => {
              const post = document.querySelector('shreddit-post');
              const postRecord = post
                ? {
                    url: post.getAttribute('permalink')
                      ? new URL(post.getAttribute('permalink'), location.origin).href
                      : location.href,
                    id:
                      post.getAttribute('id') ||
                      post.getAttribute('thingid') ||
                      post.getAttribute('post-id') ||
                      '',
                    subreddit:
                      post.getAttribute('subreddit-prefixed-name') ||
                      post.getAttribute('subreddit-name') ||
                      '',
                    title:
                      post.getAttribute('post-title') ||
                      post.querySelector('h1, h3')?.innerText ||
                      document.title ||
                      '',
                    body:
                      post.querySelector('[slot="text-body"]')?.innerText ||
                      post.querySelector('[slot="body"]')?.innerText ||
                      '',
                    author: post.getAttribute('author') || '',
                    score: Number(post.getAttribute('score') || 0),
                    num_comments: Number(post.getAttribute('comment-count') || 0),
                    created_utc: post.getAttribute('created-timestamp') || '',
                    flair:
                      post.querySelector('[slot="flair"]')?.innerText ||
                      post.querySelector('[data-testid="post-flair"]')?.innerText ||
                      '',
                    is_self:
                      !post.getAttribute('content-href') ||
                      (post.getAttribute('domain') || '').includes('reddit.com'),
                  }
                : null;

              const comments = Array.from(document.querySelectorAll('shreddit-comment')).map((comment) => {
                const lines = (comment.innerText || '')
                  .split('\\n')
                  .map((line) => line.trim())
                  .filter(Boolean);
                let body = lines.join('\\n');
                if (lines.length >= 4 && /ago$/.test(lines[2])) {
                  body = lines.slice(3).join('\\n');
                } else if (lines.length >= 3 && /ago$/.test(lines[1])) {
                  body = lines.slice(2).join('\\n');
                }
                const parentComment =
                  comment.parentElement?.closest('shreddit-comment') ||
                  comment.closest('[data-parent-comment-id]')?.closest('shreddit-comment');
                const parentId =
                  comment.getAttribute('parentid') ||
                  comment.getAttribute('parent-comment-id') ||
                  comment.getAttribute('data-parent-comment-id') ||
                  comment.dataset.parentId ||
                  parentComment?.getAttribute('thingid') ||
                  post?.getAttribute('id') ||
                  post?.getAttribute('thingid') ||
                  post?.getAttribute('post-id') ||
                  null;
                return {
                  id: comment.getAttribute('thingid') || '',
                  post_url: location.href,
                  author: comment.getAttribute('author') || '',
                  body,
                  score: Number(comment.getAttribute('score') || 0),
                  created_utc: comment.getAttribute('created') || '',
                  parent_id: parentId,
                  depth: Number(comment.getAttribute('depth') || 0),
                };
              });

              return { post: postRecord, comments };
            }
            """
        )

        if not payload or not payload.get("post"):
            raise ValueError("Unable to parse DOM detail payload.")

        post = self._post_from_dom_record(payload["post"])
        comments = [self._comment_from_dom_record(record) for record in payload["comments"]]
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

    def parse_user_profile_dom(self, page: Any, username: str) -> UserProfile:
        payload = page.evaluate(
            """
            () => ({
              username:
                document.querySelector('h1')?.innerText ||
                document.querySelector('[data-testid="profile-name"]')?.innerText ||
                '',
              bodyText: document.body?.innerText || '',
              isPremium: Boolean(
                document.querySelector('[data-testid="user-profile-premium"]') ||
                Array.from(document.querySelectorAll('*')).some((node) =>
                  /reddit premium/i.test(node.textContent || '')
                )
              ),
            })
            """
        )
        body_text = (payload or {}).get("bodyText") or ""
        trophies = sorted(
            {
                line.strip()
                for line in body_text.splitlines()
                if "trophy" in line.lower() and line.strip()
            }
        )[:10]
        cake_day = None
        cake_match = re.search(
            r"Cake day\s+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            body_text,
            re.IGNORECASE,
        )
        if cake_match:
            with_value = cake_match.group(1)
            for fmt in ("%b %d, %Y", "%B %d, %Y"):
                try:
                    cake_day = datetime.strptime(with_value, fmt).date()
                    break
                except ValueError:
                    continue
        post_match = re.search(
            r"(\d[\d,]*(?:\.\d+)?[kmb]?)\s+Post karma",
            body_text,
            re.IGNORECASE,
        )
        comment_match = re.search(
            r"(\d[\d,]*(?:\.\d+)?[kmb]?)\s+Comment karma",
            body_text,
            re.IGNORECASE,
        )
        return UserProfile(
            username=(payload or {}).get("username") or username,
            karma_post=_parse_compact_number(post_match.group(1) if post_match else None),
            karma_comment=_parse_compact_number(
                comment_match.group(1) if comment_match else None
            ),
            cake_day=cake_day,
            is_premium=bool((payload or {}).get("isPremium")),
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
            id=data.get("name") or data.get("id"),
            body=data.get("selftext") or "",
            author=data.get("author") or "[deleted]",
            author_karma=data.get("author_total_karma"),
            score=int(data.get("score") or 0),
            num_comments=int(data.get("num_comments") or 0),
            created_utc=_utc_from_timestamp(data.get("created_utc")),
            flair=data.get("link_flair_text"),
            is_self=bool(data.get("is_self", True)),
        )

    def _post_from_dom_record(self, record: dict[str, Any]) -> Post:
        return Post(
            url=_absolute_url(record.get("url")),
            subreddit=record.get("subreddit") or "",
            title=record.get("title") or "",
            id=record.get("id") or None,
            body=record.get("body") or "",
            author=record.get("author") or "[deleted]",
            score=int(record.get("score") or 0),
            num_comments=int(record.get("num_comments") or 0),
            created_utc=_parse_reddit_datetime(record.get("created_utc")),
            flair=record.get("flair") or None,
            is_self=bool(record.get("is_self", True)),
        )

    def _comment_from_dom_record(self, record: dict[str, Any]) -> Comment:
        return Comment(
            id=record.get("id") or "",
            post_url=_absolute_url(record.get("post_url")),
            author=record.get("author") or "[deleted]",
            body=record.get("body") or "",
            score=int(record.get("score") or 0),
            created_utc=_parse_reddit_datetime(record.get("created_utc")),
            parent_id=record.get("parent_id"),
            depth=int(record.get("depth") or 0),
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
