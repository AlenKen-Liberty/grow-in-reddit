from __future__ import annotations

import copy
from pathlib import Path
from typing import Any


class MemorySeedLoader:
    """Augment static interests with local long-term memory cues."""

    CLAUDE_TOPICS = [
        {
            "topic": "ai agents",
            "subreddits": ["r/LocalLLaMA", "r/MachineLearning", "r/OpenAI", "r/ClaudeAI"],
            "keywords": [
                "llm",
                "agent",
                "prompt",
                "rag",
                "embedding",
                "automation",
                "browser",
                "cdp",
                "playwright",
            ],
        },
        {
            "topic": "programming automation",
            "subreddits": ["r/programming", "r/Python", "r/webscraping", "r/javascript"],
            "keywords": [
                "python",
                "javascript",
                "api",
                "browser automation",
                "playwright",
                "patchright",
                "oracle",
            ],
        },
    ]

    OPENCLAW_TOPICS = [
        {
            "topic": "swim recruiting",
            "subreddits": [
                "r/Swimming",
                "r/swimming",
                "r/swimmingcoach",
                "r/ApplyingToCollege",
                "r/college",
                "r/NCAA",
            ],
            "keywords": [
                "ncaa",
                "recruiting",
                "scholarship",
                "stanford",
                "princeton",
                "harvard",
                "swimming",
                "college recruiting",
                "coach contact",
            ],
        },
        {
            "topic": "canadian swimming",
            "subreddits": ["r/Swimming", "r/Olympics", "r/CanadaSports"],
            "keywords": [
                "canada",
                "national team",
                "trials",
                "montreal",
                "pan pacs",
                "olympic standard",
                "lcm",
            ],
        },
    ]

    def __init__(
        self,
        *,
        claude_projects_dir: Path,
        openclaw_memory_dir: Path,
        max_claude_files: int = 12,
        max_openclaw_files: int = 20,
    ):
        self.claude_projects_dir = claude_projects_dir
        self.openclaw_memory_dir = openclaw_memory_dir
        self.max_claude_files = max_claude_files
        self.max_openclaw_files = max_openclaw_files

    def augment_seed_config(self, seed_config: dict[str, Any]) -> dict[str, Any]:
        merged = copy.deepcopy(seed_config)
        merged.setdefault("primary", [])
        merged.setdefault("secondary", [])
        merged["similarity_threshold"] = min(
            float(merged.get("similarity_threshold", 0.40)),
            0.40,
        )

        claude_text = self._collect_claude_text()
        openclaw_text = self._collect_openclaw_text()
        if claude_text:
            for entry in self.CLAUDE_TOPICS:
                self._merge_entry(merged["secondary"], entry)
        if openclaw_text:
            for entry in self.OPENCLAW_TOPICS:
                self._merge_entry(merged["primary"], entry)
        return merged

    def _collect_claude_text(self) -> str:
        if not self.claude_projects_dir.exists():
            return ""
        files = sorted(
            self.claude_projects_dir.rglob("*.jsonl"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[: self.max_claude_files]
        snippets: list[str] = []
        for path in files:
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            lowered = text.lower()
            if any(
                needle in lowered
                for needle in (
                    "llm",
                    "playwright",
                    "cdp",
                    "browser automation",
                    "agent",
                    "programming",
                    "python",
                    "javascript",
                    "oracle",
                )
            ):
                snippets.append(text[:20_000])
        return "\n".join(snippets)

    def _collect_openclaw_text(self) -> str:
        if not self.openclaw_memory_dir.exists():
            return ""
        files = sorted(
            self.openclaw_memory_dir.glob("*.md"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[: self.max_openclaw_files]
        snippets: list[str] = []
        for path in files:
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            lowered = text.lower()
            if any(
                needle in lowered
                for needle in (
                    "swimming",
                    "recruit",
                    "scholarship",
                    "ncaa",
                    "canada",
                    "trial",
                    "stanford",
                    "princeton",
                    "harvard",
                )
            ):
                snippets.append(text[:20_000])
        return "\n".join(snippets)

    @staticmethod
    def _merge_entry(existing_entries: list[dict[str, Any]], candidate: dict[str, Any]) -> None:
        for entry in existing_entries:
            if str(entry.get("topic", "")).strip().lower() != candidate["topic"]:
                continue
            merged_subreddits = {
                str(value) for value in entry.get("subreddits", []) if str(value).strip()
            }
            merged_keywords = {
                str(value) for value in entry.get("keywords", []) if str(value).strip()
            }
            merged_subreddits.update(candidate["subreddits"])
            merged_keywords.update(candidate["keywords"])
            entry["subreddits"] = sorted(merged_subreddits)
            entry["keywords"] = sorted(merged_keywords)
            return
        existing_entries.append(copy.deepcopy(candidate))
