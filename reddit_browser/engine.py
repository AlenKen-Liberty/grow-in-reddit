from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from typing import Any
from urllib import error, parse, request

from browser_core import CdpBrowser
from storage.models import BrowseAction, Post, PostDetail, UserProfile
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
    cdp_endpoint: str = "http://127.0.0.1:9222"
    use_browser_fallback: bool = True
    _cdp_browser: CdpBrowser = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.parser is None:
            self.parser = RedditParser()
        if self.rate_limiter is None:
            self.rate_limiter = RateLimiter()
        self._cdp_browser = CdpBrowser(
            cdp_endpoint=self.cdp_endpoint,
            timeout_ms=int(self.request_timeout * 1000),
        )

    def ensure_logged_in(self) -> bool:
        if not self.use_browser_fallback:
            return False
        try:
            return self._cdp_browser.is_logged_in()
        except Exception:
            return False

    def get_subreddit_feed(
        self, subreddit: str, sort: str = "hot", limit: int = 25
    ) -> list[Post]:
        normalized = normalize_subreddit_name(subreddit)
        if not normalized:
            raise ValueError("subreddit is required")
        safe_sort = sort if sort in {"hot", "new", "top", "rising"} else "hot"
        self.rate_limiter.wait("browse")
        dom_url = f"{self.base_url}/r/{normalized}/{safe_sort}/"
        with self._cdp_browser.open_page(dom_url, wait_selector="shreddit-post") as page:
            return self.parser.parse_feed_dom(page, limit=limit)

    def get_post_detail(self, post_url: str) -> PostDetail:
        self.rate_limiter.wait("browse")
        dom_url = self._ensure_html_url(post_url)
        with self._cdp_browser.open_page(dom_url, wait_selector="shreddit-post") as page:
            return self.parser.parse_post_detail_dom(page)

    def submit_post(
        self,
        subreddit: str,
        title: str,
        body: str,
        flair: str | None = None,
        *,
        submit: bool = True,
    ) -> str:
        normalized = normalize_subreddit_name(subreddit)
        if not normalized:
            raise ValueError("subreddit is required")
        if not title.strip():
            raise ValueError("title is required")
        if flair is not None:
            raise NotImplementedError("Flair selection is not implemented yet.")

        self.rate_limiter.wait("post")
        submit_url = f"{self.base_url}/r/{normalized}/submit?type=TEXT"
        with self._cdp_browser.open_page(
            submit_url, wait_selector='textarea[name="title"]'
        ) as page:
            title_box = page.locator('textarea[name="title"]').first
            body_box = page.get_by_label("Post body text field").first
            post_button = page.locator("#inner-post-submit-button").first

            title_box.fill(title.strip())
            if body:
                body_box.click()
                body_box.fill(body)

            page.wait_for_timeout(750)
            if post_button.is_disabled():
                raise RedditBrowserError(self._collect_submit_errors(page))

            if not submit:
                return page.url

            post_button.click()
            page.wait_for_timeout(1500)
            if "/comments/" in page.url:
                return page.url

            permalink = self._locate_recent_post_permalink(page, normalized, title)
            if permalink:
                return permalink

            raise RedditBrowserError(self._collect_submit_errors(page))

    def submit_comment(
        self, post_url: str, comment: str, parent_comment_id: str | None = None
    ) -> str:
        text = comment.strip()
        if not text:
            raise ValueError("comment is required")
        self.rate_limiter.wait("comment")
        dom_url = self._ensure_html_url(post_url)
        with self._cdp_browser.open_page(dom_url, wait_selector="shreddit-post") as page:
            if self._is_commenting_blocked(page):
                raise RedditBrowserError("This post appears to be locked or archived.")

            composer_scope = self._ensure_comment_composer(
                page, parent_comment_id=parent_comment_id
            )
            editor = self._find_first_locator(
                composer_scope or page,
                [
                    'div[contenteditable="true"]',
                    'div[contenteditable="plaintext-only"]',
                    "textarea",
                    'shreddit-composer textarea',
                ],
            )
            if editor is None:
                raise RedditBrowserError("Could not locate a Reddit comment editor.")

            editor.scroll_into_view_if_needed()
            editor.click()
            self._type_comment_like_human(page, editor, text)
            page.wait_for_timeout(random.randint(300, 800))

            submit_button = self._find_first_locator(
                composer_scope or page,
                [
                    'button[slot="submit-button"]',
                    'button[type="submit"]:has-text("Comment")',
                    'button:has-text("Comment")',
                ],
            )
            if submit_button is None:
                raise RedditBrowserError("Could not locate the Comment submit button.")
            if submit_button.is_disabled():
                raise RedditBrowserError(self._collect_submit_errors(page))

            submit_button.click()
            page.wait_for_timeout(1500)
            permalink = self._locate_submitted_comment_permalink(page, text)
            if permalink:
                return permalink

            error_message = self._collect_submit_errors(page)
            raise RedditBrowserError(
                error_message
                or "Comment submit completed but the new comment was not detected."
            )

    def upvote(self, target_url: str) -> bool:
        self.rate_limiter.wait("vote")
        dom_url = self._ensure_html_url(target_url)
        with self._cdp_browser.open_page(dom_url, wait_selector="body") as page:
            return self._upvote_open_page(page, target_url)

    def browse_and_engage(
        self, subreddit: str, *, scroll_count: int = 3
    ) -> list[BrowseAction]:
        normalized = normalize_subreddit_name(subreddit)
        if not normalized:
            raise ValueError("subreddit is required")
        actions: list[BrowseAction] = []
        subreddit_url = f"{self.base_url}/r/{normalized}/"
        self.rate_limiter.wait("browse")
        with self._cdp_browser.open_page(subreddit_url, wait_selector="shreddit-post") as page:
            actions.append(
                BrowseAction(
                    action="visit_subreddit",
                    target_url=subreddit_url,
                    subreddit=f"r/{normalized}",
                )
            )
            for _ in range(max(scroll_count, 1)):
                page.wait_for_timeout(random.randint(2_000, 5_000))
                page.mouse.wheel(0, random.randint(700, 1_600))
                actions.append(
                    BrowseAction(
                        action="scroll_feed",
                        target_url=page.url,
                        subreddit=f"r/{normalized}",
                    )
                )
                candidate_urls = page.evaluate(
                    """
                    () => Array.from(document.querySelectorAll('shreddit-post'))
                      .map((post) => post.getAttribute('permalink'))
                      .filter(Boolean)
                      .slice(0, 8)
                      .map((value) => new URL(value, location.origin).href)
                    """
                )
                if not candidate_urls:
                    continue
                target_url = random.choice(candidate_urls[: min(len(candidate_urls), 3)])
                page.goto(
                    target_url,
                    wait_until="domcontentloaded",
                    timeout=self._cdp_browser.timeout_ms,
                )
                page.locator("shreddit-post").first.wait_for(
                    state="visible", timeout=self._cdp_browser.timeout_ms
                )
                actions.append(
                    BrowseAction(
                        action="open_post",
                        target_url=page.url,
                        subreddit=f"r/{normalized}",
                    )
                )
                page.wait_for_timeout(random.randint(10_000, 18_000))
                if random.random() < 0.20:
                    if self._upvote_open_page(page, page.url):
                        actions.append(
                            BrowseAction(
                                action="upvote",
                                target_url=page.url,
                                subreddit=f"r/{normalized}",
                            )
                        )
                page.go_back(
                    wait_until="domcontentloaded", timeout=self._cdp_browser.timeout_ms
                )
                page.locator("shreddit-post").first.wait_for(
                    state="visible", timeout=self._cdp_browser.timeout_ms
                )
        return actions

    def get_user_profile(self, username: str) -> UserProfile:
        if not username:
            raise ValueError("username is required")
        self.rate_limiter.wait("browse")
        profile_url = f"{self.base_url}/user/{username}/"
        with self._cdp_browser.open_page(profile_url, wait_selector="body") as page:
            payload = page.evaluate(
                """
                async (user) => {
                  try {
                    const response = await fetch(`/user/${user}/about.json?raw_json=1`, {
                      credentials: 'include',
                      headers: { 'x-requested-with': 'XMLHttpRequest' },
                    });
                    if (!response.ok) {
                      return { ok: false, status: response.status };
                    }
                    return { ok: true, data: await response.json() };
                  } catch (error) {
                    return { ok: false, error: String(error) };
                  }
                }
                """,
                username,
            )
            if payload.get("ok") and payload.get("data"):
                return self.parser.parse_user_profile(payload["data"])
            return self.parser.parse_user_profile_dom(page, username)

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

    def _ensure_html_url(self, post_url: str) -> str:
        parsed = parse.urlparse(post_url)
        if not parsed.scheme:
            parsed = parse.urlparse(parse.urljoin(self.base_url, post_url))
        path = parsed.path.rstrip("/")
        if path.endswith(".json"):
            path = path[:-5]
        return parse.urlunparse(parsed._replace(path=path, query=""))

    def _collect_submit_errors(self, page: object) -> str:
        helpers = page.locator("faceplate-form-helper-text").all_inner_texts()
        messages = [message.strip() for message in helpers if message.strip()]
        if messages:
            return " | ".join(messages)
        return "Post form is still invalid or blocked by an interstitial."

    def _locate_recent_post_permalink(
        self, page: object, subreddit: str, title: str
    ) -> str | None:
        page.goto(
            f"{self.base_url}/r/{subreddit}/new/",
            wait_until="domcontentloaded",
            timeout=self._cdp_browser.timeout_ms,
        )
        page.locator("shreddit-post").first.wait_for(
            state="visible", timeout=self._cdp_browser.timeout_ms
        )
        page.wait_for_timeout(1500)
        permalink = page.evaluate(
            """
            (wantedTitle) => {
              const match = Array.from(document.querySelectorAll('shreddit-post')).find((post) => {
                return (post.getAttribute('post-title') || '').trim() === wantedTitle.trim();
              });
              return match ? match.getAttribute('permalink') : null;
            }
            """,
            title,
        )
        if not permalink:
            return None
        return parse.urljoin(self.base_url, permalink)

    def _ensure_comment_composer(
        self, page: object, *, parent_comment_id: str | None
    ) -> object | None:
        if parent_comment_id:
            comment_root = self._find_comment_root(page, parent_comment_id)
            if comment_root is None:
                raise RedditBrowserError(
                    f"Parent comment {parent_comment_id} was not found on the page."
                )
            reply_button = self._find_first_locator(
                comment_root,
                [
                    'button[aria-label="Reply"]',
                    'button:has-text("Reply")',
                    '[noun="reply"] button',
                ],
            )
            if reply_button is None:
                raise RedditBrowserError("Could not locate the Reply button.")
            reply_button.click()
            page.wait_for_timeout(600)
            composer = self._find_first_locator(
                comment_root,
                ["shreddit-composer", '[data-testid="comment-composer"]'],
            )
            if composer is not None:
                return composer

        composer = self._find_first_locator(
            page,
            [
                "shreddit-composer#comment-body-composer",
                'shreddit-composer[slot="comment-body"]',
                "#comment-body-composer",
                '[data-testid="comment-composer"]',
            ],
        )
        if composer is not None:
            return composer

        launcher = self._find_first_locator(
            page,
            [
                'button:has-text("Add a comment")',
                'button:has-text("Comment")',
            ],
        )
        if launcher is not None:
            launcher.click()
            page.wait_for_timeout(600)
            return self._find_first_locator(
                page,
                [
                    "shreddit-composer#comment-body-composer",
                    'shreddit-composer[slot="comment-body"]',
                    '[data-testid="comment-composer"]',
                ],
            )
        return None

    def _find_comment_root(self, page: object, comment_id: str) -> object | None:
        normalized = comment_id if comment_id.startswith("t1_") else f"t1_{comment_id}"
        return self._find_first_locator(
            page,
            [
                f'shreddit-comment[thingid="{normalized}"]',
                f'shreddit-comment[thingid="{comment_id}"]',
                f'shreddit-comment[id="{comment_id}"]',
            ],
        )

    def _type_comment_like_human(
        self, page: object, editor: object, comment: str
    ) -> None:
        is_textarea = bool(
            editor.evaluate(
                "(node) => ['textarea', 'input'].includes(node.tagName.toLowerCase())"
            )
        )
        delay = random.randint(50, 120)
        if is_textarea:
            editor.type(comment, delay=delay)
            return
        page.keyboard.type(comment, delay=delay)

    def _locate_submitted_comment_permalink(
        self, page: object, comment_text: str
    ) -> str | None:
        snippet = " ".join(comment_text.split())[:80].lower()
        for _ in range(10):
            payload = page.evaluate(
                """
                (wantedText) => {
                  const comments = Array.from(document.querySelectorAll('shreddit-comment'));
                  const match = comments.find((comment) => {
                    const text = (comment.innerText || '').toLowerCase().replace(/\\s+/g, ' ');
                    return wantedText && text.includes(wantedText);
                  });
                  if (!match) {
                    return null;
                  }
                  const thingId = match.getAttribute('thingid') || match.id || '';
                  const shortId = thingId.replace(/^t1_/, '');
                  const anchor =
                    match.querySelector(`a[href*="/comment/${shortId}"]`) ||
                    match.querySelector(`a[href*="${shortId}"]`) ||
                    match.querySelector('a[href*="/comments/"]');
                  return { thingId, permalink: anchor?.href || null };
                }
                """,
                snippet,
            )
            if payload:
                if payload.get("permalink"):
                    return payload["permalink"]
                if payload.get("thingId"):
                    return f"{page.url}#{payload['thingId']}"
                return page.url
            page.wait_for_timeout(500)
        return None

    def _is_commenting_blocked(self, page: object) -> bool:
        body_text = page.locator("body").inner_text(timeout=self._cdp_browser.timeout_ms)
        lowered = body_text.lower()
        if "comments are locked" in lowered or "archived post" in lowered:
            return True
        return page.locator('[icon-name="lock-fill"]').count() > 0

    def _upvote_open_page(self, page: object, target_url: str) -> bool:
        button = self._locate_upvote_button(page, target_url)
        if button is None:
            raise RedditBrowserError("Could not locate an upvote button for this target.")
        if button.is_disabled():
            raise RedditBrowserError("The upvote button is disabled on this page.")
        if self._is_vote_active(button):
            return True
        button.scroll_into_view_if_needed()
        button.click()
        page.wait_for_timeout(1200)
        return self._is_vote_active(button)

    def _locate_upvote_button(self, page: object, target_url: str) -> object | None:
        comment_id = self._extract_comment_id(target_url)
        if comment_id:
            comment_root = self._find_comment_root(page, comment_id)
            if comment_root is not None:
                button = self._find_first_locator(
                    comment_root,
                    ['button[upvote]', 'button[aria-label*="upvote" i]'],
                )
                if button is not None:
                    return button
        post_root = self._find_first_locator(page, ["shreddit-post"])
        if post_root is None:
            return None
        return self._find_first_locator(
            post_root,
            ['button[upvote]', 'button[aria-label*="upvote" i]'],
        )

    @staticmethod
    def _is_vote_active(button: object) -> bool:
        state = button.evaluate(
            """
            (node) => {
              const ariaPressed = node.getAttribute('aria-pressed');
              if (ariaPressed === 'true') return true;
              const classes = node.className || '';
              return /upvoted|is-active|selected/i.test(String(classes));
            }
            """
        )
        return bool(state)

    @staticmethod
    def _extract_comment_id(target_url: str) -> str | None:
        parsed = parse.urlparse(target_url)
        if parsed.fragment.startswith("t1_"):
            return parsed.fragment
        segments = [segment for segment in parsed.path.split("/") if segment]
        for index, segment in enumerate(segments):
            if segment == "comment" and index + 1 < len(segments):
                return segments[index + 1]
        return None

    @staticmethod
    def _find_first_locator(scope: object, selectors: list[str]) -> object | None:
        for selector in selectors:
            try:
                locator = scope.locator(selector)
                if locator.count() > 0:
                    return locator.first
            except Exception:
                continue
        return None
