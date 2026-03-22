from __future__ import annotations

from storage import Post, PostDetail, SQLiteStore


class ArticleStore:
    """
    Phase-1 local storage adapter.

    The design targets ES + Qdrant, but the bootstrap uses SQLite cache tables so
    collection and inspection can run before those services are wired in.
    """

    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def has_post(self, url: str) -> bool:
        return self.sqlite_store.has_cached_post(url)

    def store_post(self, post: Post) -> None:
        self.sqlite_store.upsert_collected_post(post)

    def store_post_detail(self, detail: PostDetail) -> tuple[int, int]:
        self.store_post(detail.post)
        stored_comments = self.sqlite_store.upsert_collected_comments(detail.comments)
        return 1, stored_comments

    def list_posts(
        self, *, subreddit: str | None = None, limit: int = 50
    ) -> list[Post]:
        return self.sqlite_store.list_cached_posts(subreddit=subreddit, limit=limit)
