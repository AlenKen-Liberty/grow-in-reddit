from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request

from storage.models import Post, PostDetail, UserProfile
from utils import normalize_subreddit_name

from .parser import BASE_URL, RedditParser
from .rate_limiter import RateLimiter


class RedditBrowserError(RuntimeError):
    pass


@dataclass(slots=True)
class RedditBrowser:
    parser: RedditParser | None = None
    rate_limiter: RateLimiter | None = None
    base_url: str = BASE_URL
    request_timeout: float = 20.0
    user_agent: str = "grow-in-reddit/0.1"

    def __post_init__(self) -> None:
        if self.parser is None:
            self.parser = RedditParser()
        if self.rate_limiter is None:
            self.rate_limiter = RateLimiter()

    def ensure_logged_in(self) -> bool:
        return False

    def get_subreddit_feed(
        self, subreddit: str, sort: str = "hot", limit: int = 25
    ) -> list[Post]:
        normalized = normalize_subreddit_name(subreddit)
        if not normalized:
            raise ValueError("subreddit is required")
        safe_sort = sort if sort in {"hot", "new", "top", "rising"} else "hot"
        self.rate_limiter.wait("browse")
        url = f"{self.base_url}/r/{normalized}/{safe_sort}.json?" + parse.urlencode(
            {"limit": limit, "raw_json": 1}
        )
        payload = self._fetch_json(url)
        return self.parser.parse_feed_json(payload)

    def get_post_detail(self, post_url: str) -> PostDetail:
        self.rate_limiter.wait("browse")
        payload = self._fetch_json(self._ensure_json_url(post_url))
        return self.parser.parse_post_detail(payload)

    def submit_post(
        self, subreddit: str, title: str, body: str, flair: str | None = None
    ) -> str:
        raise NotImplementedError(
            "Submitting posts requires CDP browser automation and is not implemented yet."
        )

    def submit_comment(
        self, post_url: str, comment: str, parent_comment_id: str | None = None
    ) -> str:
        raise NotImplementedError(
            "Submitting comments requires CDP browser automation and is not implemented yet."
        )

    def upvote(self, target_url: str) -> bool:
        raise NotImplementedError(
            "Voting requires CDP browser automation and is not implemented yet."
        )

    def get_user_profile(self, username: str) -> UserProfile:
        if not username:
            raise ValueError("username is required")
        self.rate_limiter.wait("browse")
        url = f"{self.base_url}/user/{username}/about.json?raw_json=1"
        payload = self._fetch_json(url)
        return self.parser.parse_user_profile(payload)

    def _fetch_json(self, url: str) -> Any:
        req = request.Request(url, headers={"User-Agent": self.user_agent})
        try:
            with request.urlopen(req, timeout=self.request_timeout) as response:
                return json.load(response)
        except error.HTTPError as exc:
            raise RedditBrowserError(f"HTTP {exc.code} while fetching {url}") from exc
        except error.URLError as exc:
            raise RedditBrowserError(f"Failed to fetch {url}: {exc.reason}") from exc

    def _ensure_json_url(self, post_url: str) -> str:
        parsed = parse.urlparse(post_url)
        if not parsed.scheme:
            parsed = parse.urlparse(parse.urljoin(self.base_url, post_url))
        path = parsed.path.rstrip("/")
        if not path.endswith(".json"):
            path = path + ".json"
        query = dict(parse.parse_qsl(parsed.query, keep_blank_values=True))
        query["raw_json"] = "1"
        return parse.urlunparse(
            parsed._replace(path=path, query=parse.urlencode(query))
        )
