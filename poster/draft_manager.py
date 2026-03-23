from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from utils import normalize_subreddit_name


@dataclass(slots=True)
class Draft:
    path: Path
    subreddit: str
    title: str
    body: str
    scheduled_after: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class DraftManager:
    """Manage user-reviewed post drafts stored on disk."""

    DRAFTS_DIR = Path("data/drafts")
    POSTED_DIR = Path("data/drafts/posted")

    def __init__(
        self,
        *,
        drafts_dir: Path | None = None,
        posted_dir: Path | None = None,
    ) -> None:
        self.drafts_dir = drafts_dir or self.DRAFTS_DIR
        self.posted_dir = posted_dir or self.POSTED_DIR

    def list_pending(self) -> list[Draft]:
        self.drafts_dir.mkdir(parents=True, exist_ok=True)
        drafts: list[Draft] = []
        for path in sorted(self.drafts_dir.glob("*.y*ml"), key=lambda item: item.stat().st_mtime):
            draft = self._load_draft(path)
            if draft is not None:
                drafts.append(draft)
        return drafts

    def pick_next(self, *, preferred_subreddit: str | None = None) -> Draft | None:
        now = datetime.now()
        pending = [
            draft
            for draft in self.list_pending()
            if draft.scheduled_after is None or draft.scheduled_after <= now
        ]
        if preferred_subreddit:
            normalized = normalize_subreddit_name(preferred_subreddit)
            pending.sort(
                key=lambda draft: (
                    normalize_subreddit_name(draft.subreddit) != normalized,
                    draft.scheduled_after is None,
                    draft.scheduled_after or datetime.min,
                    draft.path.stat().st_mtime,
                )
            )
        else:
            pending.sort(
                key=lambda draft: (
                    draft.scheduled_after is None,
                    draft.scheduled_after or datetime.min,
                    draft.path.stat().st_mtime,
                )
            )
        return pending[0] if pending else None

    def mark_posted(self, draft: Draft, *, post_url: str) -> Path:
        self.posted_dir.mkdir(parents=True, exist_ok=True)
        payload = dict(draft.metadata)
        payload.update(
            {
                "subreddit": draft.subreddit,
                "title": draft.title,
                "body": draft.body,
                "posted_url": post_url,
                "posted_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        target = self.posted_dir / draft.path.name
        self._dump_yaml(target, payload)
        if draft.path.exists():
            draft.path.unlink()
        return target

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("PyYAML is required for draft files.") from exc
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        return payload if isinstance(payload, dict) else {}

    def _load_draft(self, path: Path) -> Draft | None:
        payload = self._load_yaml(path)
        subreddit = payload.get("subreddit")
        title = payload.get("title")
        body = payload.get("body")
        if not subreddit or not title or not body:
            return None
        scheduled_after = payload.get("scheduled_after")
        scheduled_dt = (
            datetime.fromisoformat(str(scheduled_after))
            if scheduled_after
            else None
        )
        return Draft(
            path=path,
            subreddit=str(subreddit),
            title=str(title).strip(),
            body=str(body).strip(),
            scheduled_after=scheduled_dt,
            metadata=payload,
        )

    @staticmethod
    def _dump_yaml(path: Path, payload: dict[str, Any]) -> None:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("PyYAML is required for draft files.") from exc
        path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
