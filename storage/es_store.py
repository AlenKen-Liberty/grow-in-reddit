from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ElasticsearchStore:
    url: str
    posts_index: str
    comments_index: str

    def ensure_indexes(self) -> None:
        raise NotImplementedError(
            "Elasticsearch integration is not wired in this bootstrap yet."
        )
