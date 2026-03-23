from __future__ import annotations

from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import Iterator


class CdpBrowserError(RuntimeError):
    pass


@dataclass(slots=True)
class CdpBrowser:
    cdp_endpoint: str = "http://127.0.0.1:9222"
    timeout_ms: int = 30_000
    settle_time_ms: int = 1_500

    @contextmanager
    def open_page(
        self,
        url: str,
        *,
        wait_selector: str | None = None,
        wait_until: str = "domcontentloaded",
        isolated: bool = False,
    ) -> Iterator[object]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - runtime dependency
            raise CdpBrowserError(
                "Playwright is required for CDP fallback. Install requirements.txt first."
            ) from exc

        with sync_playwright() as playwright:
            browser = playwright.chromium.connect_over_cdp(self.cdp_endpoint)
            created_context = None
            if isolated:
                created_context = browser.new_context()
                context = created_context
            else:
                if not browser.contexts:
                    browser.close()
                    raise CdpBrowserError(
                        f"No browser context found at {self.cdp_endpoint}. Start Chromium with remote debugging first."
                    )
                context = browser.contexts[0]
            page = context.new_page()
            try:
                page.set_default_timeout(self.timeout_ms)
                page.goto(url, wait_until=wait_until, timeout=self.timeout_ms)
                if wait_selector:
                    page.locator(wait_selector).first.wait_for(
                        state="visible", timeout=self.timeout_ms
                    )
                if self.settle_time_ms:
                    page.wait_for_timeout(self.settle_time_ms)
                yield page
            except Exception as exc:  # pragma: no cover - live browser interaction
                raise CdpBrowserError(str(exc)) from exc
            finally:
                with suppress(Exception):
                    page.close()
                with suppress(Exception):
                    if created_context is not None:
                        created_context.close()
                with suppress(Exception):
                    browser.close()

    def is_logged_in(self) -> bool:
        with self.open_page("https://www.reddit.com/", wait_selector="body") as page:
            if page.locator("shreddit-post[user-logged-in]").count() > 0:
                return True
            body_text = page.locator("body").inner_text(timeout=self.timeout_ms)
            return any(
                marker in body_text
                for marker in ("Open inbox", "Create post", "Expand user menu")
            )
