from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class QdrantStore:
    url: str
    api_key: str | None
    posts_collection: str
    chunks_collection: str

    def ensure_collections(self) -> None:
        raise NotImplementedError(
            "Qdrant integration is not wired in this bootstrap yet."
        )
