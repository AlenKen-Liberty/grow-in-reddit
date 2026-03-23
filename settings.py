from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency at bootstrap time
    load_dotenv = None


def _default_interest_config() -> dict[str, Any]:
    return {"primary": [], "secondary": [], "similarity_threshold": 0.40}


@dataclass(slots=True)
class Settings:
    cdp_port: int
    display: str
    reddit_username: str | None
    reddit_timezone: str
    reddit_user_agent: str
    sqlite_db_path: Path
    interests_file: Path
    claude_projects_dir: Path
    openclaw_memory_dir: Path
    es_url: str | None
    es_index_posts: str
    es_index_comments: str
    qdrant_url: str | None
    qdrant_api_key: str | None
    qdrant_posts_collection: str
    qdrant_chunks_collection: str
    ollama_url: str | None
    ollama_model: str
    llm_provider: str
    llm_base_url: str
    llm_model: str
    scheduler_check_interval_min: int
    health_karma_decline_days: int
    health_removal_rate_threshold: float
    report_email_to: str | None
    report_output_dir: Path
    farming_subreddits: list[str]

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> "Settings":
        if load_dotenv is not None:
            load_dotenv(dotenv_path=env_file, override=False)

        cwd = Path.cwd()
        sqlite_db_path = _resolve_path(
            os.getenv("SQLITE_DB_PATH", "./data/grow_reddit.db"), cwd
        )
        interests_file = _resolve_path(
            os.getenv("INTERESTS_FILE", "./config/interests.example.yaml"), cwd
        )
        claude_projects_dir = _resolve_path(
            os.getenv("CLAUDE_PROJECTS_DIR", "/home/ubuntu/.claude/projects"), cwd
        )
        openclaw_memory_dir = _resolve_path(
            os.getenv(
                "OPENCLAW_MEMORY_DIR", "/home/ubuntu/.openclaw/workspace/memory"
            ),
            cwd,
        )
        report_output_dir = _resolve_path(
            os.getenv("REPORT_OUTPUT_DIR", "./logs/daily_reports"), cwd
        )
        return cls(
            cdp_port=int(os.getenv("CDP_PORT", "9222")),
            display=os.getenv("DISPLAY", ":1"),
            reddit_username=os.getenv("REDDIT_USERNAME"),
            reddit_timezone=os.getenv("REDDIT_TIMEZONE", "America/New_York"),
            reddit_user_agent=os.getenv("REDDIT_USER_AGENT", "grow-in-reddit/0.1"),
            sqlite_db_path=sqlite_db_path,
            interests_file=interests_file,
            claude_projects_dir=claude_projects_dir,
            openclaw_memory_dir=openclaw_memory_dir,
            es_url=os.getenv("ES_URL"),
            es_index_posts=os.getenv("ES_INDEX_POSTS", "reddit_posts"),
            es_index_comments=os.getenv("ES_INDEX_COMMENTS", "reddit_comments"),
            qdrant_url=os.getenv("QDRANT_URL"),
            qdrant_api_key=os.getenv("QDRANT_API_KEY"),
            qdrant_posts_collection=os.getenv(
                "QDRANT_POSTS_COLLECTION", "reddit_posts_dense"
            ),
            qdrant_chunks_collection=os.getenv(
                "QDRANT_CHUNKS_COLLECTION", "reddit_chunks_dense"
            ),
            ollama_url=os.getenv("OLLAMA_URL"),
            ollama_model=os.getenv("OLLAMA_MODEL", "qwen3-embedding:0.6b"),
            llm_provider=os.getenv("LLM_PROVIDER", "chat2api"),
            llm_base_url=os.getenv("LLM_BASE_URL", "http://127.0.0.1:7860"),
            llm_model=os.getenv("LLM_MODEL", "gemini-thinking"),
            scheduler_check_interval_min=int(
                os.getenv("SCHEDULER_CHECK_INTERVAL_MIN", "5")
            ),
            health_karma_decline_days=int(
                os.getenv("HEALTH_KARMA_DECLINE_DAYS", "3")
            ),
            health_removal_rate_threshold=float(
                os.getenv("HEALTH_REMOVAL_RATE_THRESHOLD", "0.2")
            ),
            report_email_to=os.getenv("REPORT_EMAIL_TO", "liuyl.david@gmail.com"),
            report_output_dir=report_output_dir,
            farming_subreddits=_parse_csv(
                os.getenv(
                    "FARMING_SUBREDDITS",
                    "AskReddit,todayilearned,LifeProTips,NoStupidQuestions",
                )
            ),
        )

    def load_interest_seeds(self) -> dict[str, Any]:
        return load_interest_config(self.interests_file)


def _resolve_path(raw_path: str, cwd: Path) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return cwd / path


def _parse_csv(raw: str) -> list[str]:
    return [value.strip() for value in raw.split(",") if value.strip()]


def load_interest_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _default_interest_config()

    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - handled in runtime environments
        raise RuntimeError(
            "Loading YAML interests requires PyYAML. Install requirements.txt first."
        ) from exc

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    interests = payload.get("interests") or payload
    return {
        "primary": interests.get("primary", []),
        "secondary": interests.get("secondary", []),
        "similarity_threshold": float(interests.get("similarity_threshold", 0.40)),
    }
