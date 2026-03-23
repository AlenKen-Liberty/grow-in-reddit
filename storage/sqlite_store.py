from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import suppress
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .models import (
    ActionLog,
    ActionOutcome,
    AccountSnapshot,
    Comment,
    CommunityPowerUser,
    CommunitySnapshot,
    ContentInsight,
    InterestTopic,
    PlaybookEntry,
    Post,
    ScheduleLogEntry,
    SeenComment,
    TrackedPost,
    SubredditProfile,
)
from .models import utc_now
from utils import clamp

UTC = timezone.utc

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS action_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        action_type TEXT NOT NULL,
        subreddit TEXT,
        target_url TEXT,
        content_preview TEXT,
        karma_before INTEGER,
        karma_after INTEGER,
        status TEXT NOT NULL DEFAULT 'success'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS account_snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        karma_post INTEGER NOT NULL,
        karma_comment INTEGER NOT NULL,
        karma_total INTEGER NOT NULL,
        active_subreddits INTEGER,
        total_posts INTEGER,
        total_comments INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tracked_posts (
        url TEXT PRIMARY KEY,
        subreddit TEXT NOT NULL,
        title TEXT NOT NULL,
        posted_at TEXT NOT NULL,
        last_checked TEXT,
        comment_count_at_post INTEGER DEFAULT 0,
        comment_count_latest INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS seen_comments (
        comment_id TEXT PRIMARY KEY,
        post_url TEXT NOT NULL,
        author TEXT,
        body_preview TEXT,
        first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
        is_direct_reply BOOLEAN DEFAULT 0,
        replied_at TEXT,
        reply_comment_id TEXT,
        reply_status TEXT NOT NULL DEFAULT 'pending'
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_seen_post
    ON seen_comments(post_url)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_seen_status
    ON seen_comments(reply_status)
    """,
    """
    CREATE TABLE IF NOT EXISTS subreddit_profile (
        name TEXT PRIMARY KEY,
        subscribers INTEGER,
        rules_json TEXT,
        allowed_flairs_json TEXT,
        best_post_hours_json TEXT,
        last_updated TEXT,
        our_karma INTEGER DEFAULT 0,
        our_post_count INTEGER DEFAULT 0,
        our_comment_count INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS schedule_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        planned_actions_json TEXT,
        executed_actions_json TEXT,
        skipped_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS interest_profile (
        topic TEXT PRIMARY KEY,
        weight REAL NOT NULL DEFAULT 0.5,
        source TEXT NOT NULL,
        evidence_count INTEGER DEFAULT 0,
        first_seen TEXT NOT NULL,
        last_updated TEXT NOT NULL,
        decay_rate REAL DEFAULT 0.95
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS interest_changelog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        topic TEXT NOT NULL,
        old_weight REAL,
        new_weight REAL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS community_playbook (
        subreddit TEXT PRIMARY KEY,
        total_posts INTEGER DEFAULT 0,
        total_comments INTEGER DEFAULT 0,
        posts_removed INTEGER DEFAULT 0,
        comments_removed INTEGER DEFAULT 0,
        avg_post_karma REAL DEFAULT 0,
        avg_comment_karma REAL DEFAULT 0,
        best_hours_json TEXT,
        best_post_types_json TEXT,
        worst_post_types_json TEXT,
        known_pitfalls_json TEXT,
        tips_json TEXT,
        mod_notes TEXT,
        last_incident TEXT,
        last_incident_date TEXT,
        last_reviewed TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS action_outcome (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        subreddit TEXT NOT NULL,
        action_type TEXT NOT NULL,
        content_hash TEXT,
        content_summary TEXT,
        title TEXT,
        post_type TEXT,
        karma_1h INTEGER,
        karma_24h INTEGER,
        karma_final INTEGER,
        was_removed BOOLEAN DEFAULT 0,
        removal_reason TEXT,
        mod_action TEXT,
        comment_count INTEGER DEFAULT 0,
        FOREIGN KEY (subreddit) REFERENCES community_playbook(subreddit)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS community_snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subreddit TEXT NOT NULL,
        post_url TEXT NOT NULL,
        title TEXT NOT NULL,
        author TEXT,
        flair TEXT,
        score_at_capture INTEGER,
        score_after_24h INTEGER,
        comment_count_at_capture INTEGER,
        comment_count_after_24h INTEGER,
        posted_at TEXT,
        captured_at TEXT NOT NULL,
        was_removed BOOLEAN DEFAULT 0,
        removal_detected_at TEXT,
        mod_comment TEXT,
        body_preview TEXT,
        UNIQUE(subreddit, post_url, captured_at)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_snapshot_sub_time
    ON community_snapshot(subreddit, captured_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_snap_revisit
    ON community_snapshot(score_after_24h, captured_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS community_power_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subreddit TEXT NOT NULL,
        username TEXT NOT NULL,
        role TEXT DEFAULT 'contributor',
        estimated_karma INTEGER,
        post_count INTEGER DEFAULT 0,
        avg_score REAL DEFAULT 0,
        post_frequency TEXT,
        content_style TEXT,
        typical_topics TEXT,
        notes TEXT,
        last_updated TEXT,
        UNIQUE(subreddit, username)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS content_insights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'self',
        category TEXT NOT NULL,
        subreddit TEXT,
        insight TEXT NOT NULL,
        evidence TEXT,
        confidence REAL DEFAULT 0.5,
        sample_size INTEGER DEFAULT 1,
        is_active BOOLEAN DEFAULT 1,
        superseded_by INTEGER,
        FOREIGN KEY (superseded_by) REFERENCES content_insights(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS golden_samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subreddit TEXT NOT NULL,
        is_ours BOOLEAN DEFAULT 0,
        post_type TEXT NOT NULL,
        title TEXT NOT NULL,
        author TEXT,
        body_preview TEXT,
        karma INTEGER NOT NULL,
        comment_count INTEGER,
        posted_at TEXT NOT NULL,
        why_it_worked TEXT,
        reusable_pattern TEXT,
        collected_at TEXT NOT NULL
    )
    """,
    # Phase-1 bootstrap cache before ES/Qdrant are wired in.
    """
    CREATE TABLE IF NOT EXISTS collected_post_cache (
        url TEXT PRIMARY KEY,
        subreddit TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT,
        author TEXT,
        score INTEGER DEFAULT 0,
        num_comments INTEGER DEFAULT 0,
        created_utc TEXT NOT NULL,
        flair TEXT,
        is_self BOOLEAN DEFAULT 1,
        interest_score REAL,
        collected_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collected_comment_cache (
        id TEXT PRIMARY KEY,
        post_url TEXT NOT NULL,
        author TEXT,
        body TEXT,
        score INTEGER DEFAULT 0,
        created_utc TEXT NOT NULL,
        parent_id TEXT,
        depth INTEGER DEFAULT 0,
        collected_at TEXT NOT NULL
    )
    """,
]


def _to_iso(value: date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    assert isinstance(value, datetime)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if " " in text and "T" not in text:
        text = text.replace(" ", "T")
        if "+" not in text and text.count("-") >= 2:
            text += "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


class SQLiteStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self.initialize_schema()

    def close(self) -> None:
        with suppress(sqlite3.Error):
            self._conn.close()

    def initialize_schema(self) -> None:
        with self._lock:
            for statement in SCHEMA_STATEMENTS:
                self._conn.execute(statement)
            self._apply_migrations()
            self._conn.commit()

    def _apply_migrations(self) -> None:
        self._ensure_column("community_power_users", "post_count", "INTEGER DEFAULT 0")
        self._ensure_column("community_power_users", "avg_score", "REAL DEFAULT 0")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {
            row["name"]
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column in columns:
            return
        self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def log_action(self, action: ActionLog) -> int:
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO action_log (
                    timestamp, action_type, subreddit, target_url,
                    content_preview, karma_before, karma_after, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _to_iso(action.timestamp),
                    action.action_type,
                    action.subreddit,
                    action.target_url,
                    action.content_preview,
                    action.karma_before,
                    action.karma_after,
                    action.status,
                ),
            )
            self._conn.commit()
            return int(cursor.lastrowid)

    def list_actions(
        self,
        *,
        limit: int = 100,
        days: int | None = None,
        action_type: str | None = None,
    ) -> list[ActionLog]:
        clauses: list[str] = []
        args: list[Any] = []
        if days is not None:
            clauses.append("timestamp >= ?")
            args.append(_to_iso(utc_now() - timedelta(days=days)))
        if action_type:
            clauses.append("action_type = ?")
            args.append(action_type)
        sql = "SELECT * FROM action_log"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_action_log(row) for row in rows]

    def get_action_counts(self, *, days: int | None = None) -> dict[str, int]:
        clauses: list[str] = []
        args: list[Any] = []
        if days is not None:
            clauses.append("timestamp >= ?")
            args.append(_to_iso(utc_now() - timedelta(days=days)))
        sql = "SELECT action_type, COUNT(*) AS count FROM action_log"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " GROUP BY action_type"
        rows = self._conn.execute(sql, args).fetchall()
        return {row["action_type"]: int(row["count"]) for row in rows}

    def get_oldest_action(self) -> ActionLog | None:
        row = self._conn.execute(
            "SELECT * FROM action_log ORDER BY timestamp ASC LIMIT 1"
        ).fetchone()
        return None if row is None else self._row_to_action_log(row)

    def record_account_snapshot(self, snapshot: AccountSnapshot) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO account_snapshot (
                    date, karma_post, karma_comment, karma_total,
                    active_subreddits, total_posts, total_comments
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    karma_post = excluded.karma_post,
                    karma_comment = excluded.karma_comment,
                    karma_total = excluded.karma_total,
                    active_subreddits = excluded.active_subreddits,
                    total_posts = excluded.total_posts,
                    total_comments = excluded.total_comments
                """,
                (
                    _to_iso(snapshot.day),
                    snapshot.karma_post,
                    snapshot.karma_comment,
                    snapshot.karma_total,
                    snapshot.active_subreddits,
                    snapshot.total_posts,
                    snapshot.total_comments,
                ),
            )
            self._conn.commit()

    def get_latest_account_snapshot(self) -> AccountSnapshot | None:
        row = self._conn.execute(
            "SELECT * FROM account_snapshot ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return AccountSnapshot(
            id=int(row["id"]),
            day=_parse_date(row["date"]) or date.today(),
            karma_post=int(row["karma_post"]),
            karma_comment=int(row["karma_comment"]),
            karma_total=int(row["karma_total"]),
            active_subreddits=int(row["active_subreddits"] or 0),
            total_posts=int(row["total_posts"] or 0),
            total_comments=int(row["total_comments"] or 0),
        )

    def list_account_snapshots(
        self, *, days: int | None = None, limit: int = 365
    ) -> list[AccountSnapshot]:
        clauses: list[str] = []
        args: list[Any] = []
        if days is not None:
            cutoff = (utc_now() - timedelta(days=days)).date().isoformat()
            clauses.append("date >= ?")
            args.append(cutoff)
        sql = "SELECT * FROM account_snapshot"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY date DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [
            AccountSnapshot(
                id=int(row["id"]),
                day=_parse_date(row["date"]) or date.today(),
                karma_post=int(row["karma_post"]),
                karma_comment=int(row["karma_comment"]),
                karma_total=int(row["karma_total"]),
                active_subreddits=int(row["active_subreddits"] or 0),
                total_posts=int(row["total_posts"] or 0),
                total_comments=int(row["total_comments"] or 0),
            )
            for row in rows
        ]

    def upsert_schedule_log(self, entry: ScheduleLogEntry) -> int:
        row = self._conn.execute(
            "SELECT id FROM schedule_log WHERE date = ? ORDER BY id DESC LIMIT 1",
            (_to_iso(entry.day),),
        ).fetchone()
        with self._lock:
            if row is None:
                cursor = self._conn.execute(
                    """
                    INSERT INTO schedule_log (
                        date, planned_actions_json, executed_actions_json, skipped_reason
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        _to_iso(entry.day),
                        _json_dumps(entry.planned_actions or {}),
                        _json_dumps(entry.executed_actions or {}),
                        entry.skipped_reason,
                    ),
                )
                self._conn.commit()
                return int(cursor.lastrowid)
            schedule_id = int(row["id"])
            self._conn.execute(
                """
                UPDATE schedule_log
                SET planned_actions_json = ?,
                    executed_actions_json = ?,
                    skipped_reason = ?
                WHERE id = ?
                """,
                (
                    _json_dumps(entry.planned_actions or {}),
                    _json_dumps(entry.executed_actions or {}),
                    entry.skipped_reason,
                    schedule_id,
                ),
            )
            self._conn.commit()
            return schedule_id

    def get_schedule_log(self, day: date) -> ScheduleLogEntry | None:
        row = self._conn.execute(
            "SELECT * FROM schedule_log WHERE date = ? ORDER BY id DESC LIMIT 1",
            (_to_iso(day),),
        ).fetchone()
        return None if row is None else self._row_to_schedule_log(row)

    def list_schedule_logs(
        self, *, limit: int = 30, days: int | None = None
    ) -> list[ScheduleLogEntry]:
        clauses: list[str] = []
        args: list[Any] = []
        if days is not None:
            clauses.append("date >= ?")
            args.append((utc_now() - timedelta(days=days)).date().isoformat())
        sql = "SELECT * FROM schedule_log"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY date DESC, id DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_schedule_log(row) for row in rows]

    def track_post(self, tracked_post: TrackedPost) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO tracked_posts (
                    url, subreddit, title, posted_at, last_checked,
                    comment_count_at_post, comment_count_latest, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    subreddit = excluded.subreddit,
                    title = excluded.title,
                    posted_at = excluded.posted_at,
                    last_checked = excluded.last_checked,
                    comment_count_at_post = excluded.comment_count_at_post,
                    comment_count_latest = excluded.comment_count_latest,
                    is_active = excluded.is_active
                """,
                (
                    tracked_post.url,
                    tracked_post.subreddit,
                    tracked_post.title,
                    _to_iso(tracked_post.posted_at),
                    _to_iso(tracked_post.last_checked),
                    tracked_post.comment_count_at_post,
                    tracked_post.comment_count_latest,
                    int(tracked_post.is_active),
                ),
            )
            self._conn.commit()

    def list_tracked_posts(
        self, *, active_only: bool = True, days: int | None = 7
    ) -> list[TrackedPost]:
        clauses: list[str] = []
        args: list[Any] = []
        if active_only:
            clauses.append("is_active = 1")
        if days is not None:
            clauses.append("posted_at >= ?")
            args.append(_to_iso(utc_now() - timedelta(days=days)))
        sql = "SELECT * FROM tracked_posts"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY posted_at DESC"
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_tracked_post(row) for row in rows]

    def mark_tracked_post_checked(
        self,
        url: str,
        *,
        comment_count_latest: int,
        is_active: bool | None = None,
        checked_at: datetime | None = None,
    ) -> None:
        assignments = ["last_checked = ?", "comment_count_latest = ?"]
        args: list[Any] = [_to_iso(checked_at or utc_now()), comment_count_latest]
        if is_active is not None:
            assignments.append("is_active = ?")
            args.append(int(is_active))
        args.append(url)
        with self._lock:
            self._conn.execute(
                f"UPDATE tracked_posts SET {', '.join(assignments)} WHERE url = ?",
                args,
            )
            self._conn.commit()

    def upsert_seen_comment(self, seen_comment: SeenComment) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO seen_comments (
                    comment_id, post_url, author, body_preview, first_seen_at,
                    is_direct_reply, replied_at, reply_comment_id, reply_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(comment_id) DO UPDATE SET
                    post_url = excluded.post_url,
                    author = excluded.author,
                    body_preview = excluded.body_preview,
                    is_direct_reply = excluded.is_direct_reply,
                    replied_at = COALESCE(excluded.replied_at, seen_comments.replied_at),
                    reply_comment_id = COALESCE(
                        excluded.reply_comment_id, seen_comments.reply_comment_id
                    ),
                    reply_status = CASE
                        WHEN seen_comments.reply_status = 'replied'
                        THEN seen_comments.reply_status
                        ELSE excluded.reply_status
                    END
                """,
                (
                    seen_comment.comment_id,
                    seen_comment.post_url,
                    seen_comment.author,
                    seen_comment.body_preview,
                    _to_iso(seen_comment.first_seen_at),
                    int(seen_comment.is_direct_reply),
                    _to_iso(seen_comment.replied_at),
                    seen_comment.reply_comment_id,
                    seen_comment.reply_status,
                ),
            )
            self._conn.commit()

    def get_seen_comment(self, comment_id: str) -> SeenComment | None:
        row = self._conn.execute(
            "SELECT * FROM seen_comments WHERE comment_id = ?",
            (comment_id,),
        ).fetchone()
        return None if row is None else self._row_to_seen_comment(row)

    def get_seen_comment_ids(self, post_url: str) -> set[str]:
        rows = self._conn.execute(
            "SELECT comment_id FROM seen_comments WHERE post_url = ?",
            (post_url,),
        ).fetchall()
        return {str(row["comment_id"]) for row in rows}

    def list_seen_comments(
        self,
        *,
        post_url: str | None = None,
        reply_status: str | None = None,
        limit: int = 500,
    ) -> list[SeenComment]:
        clauses: list[str] = []
        args: list[Any] = []
        if post_url:
            clauses.append("post_url = ?")
            args.append(post_url)
        if reply_status:
            clauses.append("reply_status = ?")
            args.append(reply_status)
        sql = "SELECT * FROM seen_comments"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY first_seen_at DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_seen_comment(row) for row in rows]

    def mark_seen_comment_replied(
        self,
        comment_id: str,
        *,
        reply_comment_id: str | None = None,
        reply_status: str = "replied",
        replied_at: datetime | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE seen_comments
                SET replied_at = ?, reply_comment_id = ?, reply_status = ?
                WHERE comment_id = ?
                """,
                (
                    _to_iso(replied_at or utc_now()),
                    reply_comment_id,
                    reply_status,
                    comment_id,
                ),
            )
            self._conn.commit()

    def upsert_subreddit_profile(self, profile: SubredditProfile) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO subreddit_profile (
                    name, subscribers, rules_json, allowed_flairs_json,
                    best_post_hours_json, last_updated, our_karma,
                    our_post_count, our_comment_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    subscribers = excluded.subscribers,
                    rules_json = excluded.rules_json,
                    allowed_flairs_json = excluded.allowed_flairs_json,
                    best_post_hours_json = excluded.best_post_hours_json,
                    last_updated = excluded.last_updated,
                    our_karma = excluded.our_karma,
                    our_post_count = excluded.our_post_count,
                    our_comment_count = excluded.our_comment_count
                """,
                (
                    profile.name,
                    profile.subscribers,
                    _json_dumps(profile.rules),
                    _json_dumps(profile.allowed_flairs),
                    _json_dumps(profile.best_post_hours),
                    _to_iso(profile.last_updated or utc_now()),
                    profile.our_karma,
                    profile.our_post_count,
                    profile.our_comment_count,
                ),
            )
            self._conn.commit()

    def list_subreddit_profiles(self, *, limit: int = 100) -> list[SubredditProfile]:
        rows = self._conn.execute(
            "SELECT * FROM subreddit_profile ORDER BY name ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_subreddit_profile(row) for row in rows]

    def get_subreddit_profile(self, name: str) -> SubredditProfile | None:
        row = self._conn.execute(
            "SELECT * FROM subreddit_profile WHERE name = ?",
            (name,),
        ).fetchone()
        return None if row is None else self._row_to_subreddit_profile(row)

    def set_interest_topic(
        self,
        topic: str,
        weight: float,
        *,
        source: str,
        evidence_count: int = 0,
        decay_rate: float = 0.95,
        reason: str | None = None,
        observed_at: datetime | None = None,
    ) -> InterestTopic:
        now = observed_at or utc_now()
        row = self._conn.execute(
            "SELECT * FROM interest_profile WHERE topic = ?",
            (topic,),
        ).fetchone()
        old_weight = float(row["weight"]) if row else None
        bounded = clamp(weight)
        with self._lock:
            if row:
                self._conn.execute(
                    """
                    UPDATE interest_profile
                    SET weight = ?, source = ?, evidence_count = ?,
                        last_updated = ?, decay_rate = ?
                    WHERE topic = ?
                    """,
                    (
                        bounded,
                        source,
                        evidence_count or int(row["evidence_count"]),
                        _to_iso(now),
                        decay_rate,
                        topic,
                    ),
                )
            else:
                self._conn.execute(
                    """
                    INSERT INTO interest_profile (
                        topic, weight, source, evidence_count,
                        first_seen, last_updated, decay_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        topic,
                        bounded,
                        source,
                        evidence_count,
                        _to_iso(now),
                        _to_iso(now),
                        decay_rate,
                    ),
                )
            if reason and old_weight != bounded:
                self._record_interest_change(topic, old_weight, bounded, reason, now)
            self._conn.commit()
        return self.get_interest_topic(topic) or InterestTopic(
            topic=topic,
            weight=bounded,
            source=source,
            evidence_count=evidence_count,
            first_seen=now,
            last_updated=now,
            decay_rate=decay_rate,
        )

    def increment_interest(
        self,
        topic: str,
        delta: float,
        *,
        source: str,
        reason: str | None = None,
        observed_at: datetime | None = None,
        evidence_increment: int = 1,
        decay_rate: float = 0.95,
    ) -> InterestTopic:
        now = observed_at or utc_now()
        row = self._conn.execute(
            "SELECT * FROM interest_profile WHERE topic = ?",
            (topic,),
        ).fetchone()
        if row is None:
            return self.set_interest_topic(
                topic,
                max(0.05, delta),
                source=source,
                evidence_count=max(1, evidence_increment),
                decay_rate=decay_rate,
                reason=reason,
                observed_at=now,
            )

        old_weight = float(row["weight"])
        new_weight = clamp(old_weight + delta)
        new_evidence = int(row["evidence_count"]) + evidence_increment
        with self._lock:
            self._conn.execute(
                """
                UPDATE interest_profile
                SET weight = ?, source = ?, evidence_count = ?,
                    last_updated = ?, decay_rate = ?
                WHERE topic = ?
                """,
                (
                    new_weight,
                    source,
                    new_evidence,
                    _to_iso(now),
                    decay_rate,
                    topic,
                ),
            )
            if reason and old_weight != new_weight:
                self._record_interest_change(topic, old_weight, new_weight, reason, now)
            self._conn.commit()
        return self.get_interest_topic(topic) or InterestTopic(
            topic=topic,
            weight=new_weight,
            source=source,
            evidence_count=new_evidence,
            first_seen=now,
            last_updated=now,
            decay_rate=decay_rate,
        )

    def get_interest_topic(self, topic: str) -> InterestTopic | None:
        row = self._conn.execute(
            "SELECT * FROM interest_profile WHERE topic = ?",
            (topic,),
        ).fetchone()
        return None if row is None else self._row_to_interest_topic(row)

    def list_interest_topics(
        self, *, limit: int = 100, min_weight: float = 0.0
    ) -> list[InterestTopic]:
        rows = self._conn.execute(
            """
            SELECT * FROM interest_profile
            WHERE weight >= ?
            ORDER BY weight DESC, last_updated DESC
            LIMIT ?
            """,
            (min_weight, limit),
        ).fetchall()
        return [self._row_to_interest_topic(row) for row in rows]

    def clear_interest_profile(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM interest_profile")
            self._conn.execute("DELETE FROM interest_changelog")
            self._conn.commit()

    def record_action_outcome(self, outcome: ActionOutcome) -> int:
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO action_outcome (
                    timestamp, subreddit, action_type, content_hash,
                    content_summary, title, post_type, karma_1h,
                    karma_24h, karma_final, was_removed, removal_reason,
                    mod_action, comment_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _to_iso(outcome.timestamp),
                    outcome.subreddit,
                    outcome.action_type,
                    outcome.content_hash,
                    outcome.content_summary,
                    outcome.title,
                    outcome.post_type,
                    outcome.karma_1h,
                    outcome.karma_24h,
                    outcome.karma_final,
                    int(outcome.was_removed),
                    outcome.removal_reason,
                    outcome.mod_action,
                    outcome.comment_count,
                ),
            )
            self._conn.commit()
            return int(cursor.lastrowid)

    def list_action_outcomes(
        self, *, subreddit: str | None = None, limit: int = 500
    ) -> list[ActionOutcome]:
        sql = "SELECT * FROM action_outcome"
        args: list[Any] = []
        if subreddit:
            sql += " WHERE subreddit = ?"
            args.append(subreddit)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_action_outcome(row) for row in rows]

    def upsert_community_playbook(self, entry: PlaybookEntry) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO community_playbook (
                    subreddit, total_posts, total_comments, posts_removed,
                    comments_removed, avg_post_karma, avg_comment_karma,
                    best_hours_json, best_post_types_json, worst_post_types_json,
                    known_pitfalls_json, tips_json, mod_notes, last_incident,
                    last_incident_date, last_reviewed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subreddit) DO UPDATE SET
                    total_posts = excluded.total_posts,
                    total_comments = excluded.total_comments,
                    posts_removed = excluded.posts_removed,
                    comments_removed = excluded.comments_removed,
                    avg_post_karma = excluded.avg_post_karma,
                    avg_comment_karma = excluded.avg_comment_karma,
                    best_hours_json = excluded.best_hours_json,
                    best_post_types_json = excluded.best_post_types_json,
                    worst_post_types_json = excluded.worst_post_types_json,
                    known_pitfalls_json = excluded.known_pitfalls_json,
                    tips_json = excluded.tips_json,
                    mod_notes = excluded.mod_notes,
                    last_incident = excluded.last_incident,
                    last_incident_date = excluded.last_incident_date,
                    last_reviewed = excluded.last_reviewed
                """,
                (
                    entry.subreddit,
                    entry.total_posts,
                    entry.total_comments,
                    entry.posts_removed,
                    entry.comments_removed,
                    entry.avg_post_karma,
                    entry.avg_comment_karma,
                    _json_dumps(entry.best_hours),
                    _json_dumps(entry.best_post_types),
                    _json_dumps(entry.worst_post_types),
                    _json_dumps(entry.known_pitfalls),
                    _json_dumps(entry.tips),
                    entry.mod_notes,
                    entry.last_incident,
                    _to_iso(entry.last_incident_date),
                    _to_iso(entry.last_reviewed or utc_now()),
                ),
            )
            self._conn.commit()

    def get_community_playbook(self, subreddit: str) -> PlaybookEntry | None:
        row = self._conn.execute(
            "SELECT * FROM community_playbook WHERE subreddit = ?",
            (subreddit,),
        ).fetchone()
        return None if row is None else self._row_to_playbook_entry(row)

    def list_community_playbooks(self, *, limit: int = 50) -> list[PlaybookEntry]:
        rows = self._conn.execute(
            "SELECT * FROM community_playbook ORDER BY subreddit ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_playbook_entry(row) for row in rows]

    def upsert_community_snapshot(self, snapshot: CommunitySnapshot) -> int:
        captured_day = snapshot.captured_at.date().isoformat()
        existing = self._conn.execute(
            """
            SELECT id FROM community_snapshot
            WHERE subreddit = ? AND post_url = ? AND date(captured_at) = ?
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            (snapshot.subreddit, snapshot.post_url, captured_day),
        ).fetchone()
        with self._lock:
            if existing is None:
                cursor = self._conn.execute(
                    """
                    INSERT INTO community_snapshot (
                        subreddit, post_url, title, author, flair,
                        score_at_capture, score_after_24h,
                        comment_count_at_capture, comment_count_after_24h,
                        posted_at, captured_at, was_removed,
                        removal_detected_at, mod_comment, body_preview
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.subreddit,
                        snapshot.post_url,
                        snapshot.title,
                        snapshot.author,
                        snapshot.flair,
                        snapshot.score_at_capture,
                        snapshot.score_after_24h,
                        snapshot.comment_count_at_capture,
                        snapshot.comment_count_after_24h,
                        _to_iso(snapshot.posted_at),
                        _to_iso(snapshot.captured_at),
                        int(snapshot.was_removed),
                        _to_iso(snapshot.removal_detected_at),
                        snapshot.mod_comment,
                        snapshot.body_preview,
                    ),
                )
                self._conn.commit()
                return int(cursor.lastrowid)

            snapshot_id = int(existing["id"])
            self._conn.execute(
                """
                UPDATE community_snapshot
                SET title = ?, author = ?, flair = ?, score_at_capture = ?,
                    posted_at = ?, comment_count_at_capture = ?,
                    body_preview = ?, captured_at = ?
                WHERE id = ?
                """,
                (
                    snapshot.title,
                    snapshot.author,
                    snapshot.flair,
                    snapshot.score_at_capture,
                    _to_iso(snapshot.posted_at),
                    snapshot.comment_count_at_capture,
                    snapshot.body_preview,
                    _to_iso(snapshot.captured_at),
                    snapshot_id,
                ),
            )
            self._conn.commit()
            return snapshot_id

    def list_snapshot_candidates_for_revisit(
        self, *, hours_ago: int = 24, limit: int = 500
    ) -> list[CommunitySnapshot]:
        cutoff = utc_now() - timedelta(hours=hours_ago)
        rows = self._conn.execute(
            """
            SELECT * FROM community_snapshot
            WHERE captured_at <= ?
              AND score_after_24h IS NULL
              AND was_removed = 0
            ORDER BY captured_at ASC
            LIMIT ?
            """,
            (_to_iso(cutoff), limit),
        ).fetchall()
        return [self._row_to_community_snapshot(row) for row in rows]

    def update_community_snapshot_revisit(
        self,
        snapshot_id: int,
        *,
        score_after_24h: int | None = None,
        comment_count_after_24h: int | None = None,
        was_removed: bool = False,
        mod_comment: str | None = None,
        removal_detected_at: datetime | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE community_snapshot
                SET score_after_24h = ?,
                    comment_count_after_24h = ?,
                    was_removed = ?,
                    mod_comment = COALESCE(?, mod_comment),
                    removal_detected_at = ?
                WHERE id = ?
                """,
                (
                    score_after_24h,
                    comment_count_after_24h,
                    int(was_removed),
                    mod_comment,
                    _to_iso(removal_detected_at),
                    snapshot_id,
                ),
            )
            self._conn.commit()

    def list_community_snapshots(
        self,
        *,
        subreddit: str | None = None,
        removed_only: bool = False,
        limit: int = 500,
    ) -> list[CommunitySnapshot]:
        clauses: list[str] = []
        args: list[Any] = []
        if subreddit:
            clauses.append("subreddit = ?")
            args.append(subreddit)
        if removed_only:
            clauses.append("was_removed = 1")
        sql = "SELECT * FROM community_snapshot"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY captured_at DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_community_snapshot(row) for row in rows]

    def upsert_community_power_user(self, profile: CommunityPowerUser) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO community_power_users (
                    subreddit, username, role, estimated_karma, post_count,
                    avg_score, content_style, notes, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subreddit, username) DO UPDATE SET
                    role = excluded.role,
                    estimated_karma = excluded.estimated_karma,
                    post_count = excluded.post_count,
                    avg_score = excluded.avg_score,
                    content_style = excluded.content_style,
                    notes = excluded.notes,
                    last_updated = excluded.last_updated
                """,
                (
                    profile.subreddit,
                    profile.username,
                    profile.role,
                    profile.estimated_karma,
                    profile.post_count,
                    profile.avg_score,
                    profile.content_style,
                    profile.notes,
                    _to_iso(profile.last_updated),
                ),
            )
            self._conn.commit()

    def list_community_power_users(
        self, *, subreddit: str | None = None, limit: int = 50
    ) -> list[CommunityPowerUser]:
        sql = "SELECT * FROM community_power_users"
        args: list[Any] = []
        if subreddit:
            sql += " WHERE subreddit = ?"
            args.append(subreddit)
        sql += " ORDER BY avg_score DESC, post_count DESC, username ASC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_community_power_user(row) for row in rows]

    def add_content_insight(self, insight: ContentInsight) -> int:
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO content_insights (
                    created_at, source, category, subreddit, insight,
                    evidence, confidence, sample_size, is_active, superseded_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _to_iso(insight.created_at),
                    insight.source,
                    insight.category,
                    insight.subreddit,
                    insight.insight,
                    insight.evidence,
                    insight.confidence,
                    insight.sample_size,
                    int(insight.is_active),
                    insight.superseded_by,
                ),
            )
            self._conn.commit()
            return int(cursor.lastrowid)

    def list_content_insights(
        self,
        *,
        subreddit: str | None = None,
        category: str | None = None,
        active_only: bool = True,
        limit: int = 50,
    ) -> list[ContentInsight]:
        clauses: list[str] = []
        args: list[Any] = []
        if subreddit:
            clauses.append("subreddit = ?")
            args.append(subreddit)
        if category:
            clauses.append("category = ?")
            args.append(category)
        if active_only:
            clauses.append("is_active = 1")
        sql = "SELECT * FROM content_insights"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_content_insight(row) for row in rows]

    def upsert_collected_post(self, post: Post) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO collected_post_cache (
                    url, subreddit, title, body, author, score,
                    num_comments, created_utc, flair, is_self,
                    interest_score, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    subreddit = excluded.subreddit,
                    title = excluded.title,
                    body = excluded.body,
                    author = excluded.author,
                    score = excluded.score,
                    num_comments = excluded.num_comments,
                    created_utc = excluded.created_utc,
                    flair = excluded.flair,
                    is_self = excluded.is_self,
                    interest_score = excluded.interest_score,
                    collected_at = excluded.collected_at
                """,
                (
                    post.url,
                    post.subreddit,
                    post.title,
                    post.body,
                    post.author,
                    post.score,
                    post.num_comments,
                    _to_iso(post.created_utc),
                    post.flair,
                    int(post.is_self),
                    post.interest_score,
                    _to_iso(utc_now()),
                ),
            )
            self._conn.commit()

    def upsert_collected_comments(self, comments: Iterable[Comment]) -> int:
        comment_list = list(comments)
        if not comment_list:
            return 0
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO collected_comment_cache (
                    id, post_url, author, body, score, created_utc,
                    parent_id, depth, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    post_url = excluded.post_url,
                    author = excluded.author,
                    body = excluded.body,
                    score = excluded.score,
                    created_utc = excluded.created_utc,
                    parent_id = excluded.parent_id,
                    depth = excluded.depth,
                    collected_at = excluded.collected_at
                """,
                [
                    (
                        comment.id,
                        comment.post_url,
                        comment.author,
                        comment.body,
                        comment.score,
                        _to_iso(comment.created_utc),
                        comment.parent_id,
                        comment.depth,
                        _to_iso(utc_now()),
                    )
                    for comment in comment_list
                ],
            )
            self._conn.commit()
        return len(comment_list)

    def has_cached_post(self, url: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM collected_post_cache WHERE url = ? LIMIT 1",
            (url,),
        ).fetchone()
        return row is not None

    def count_cached_posts(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS count FROM collected_post_cache"
        ).fetchone()
        return int(row["count"]) if row else 0

    def list_cached_posts(
        self, *, subreddit: str | None = None, limit: int = 50
    ) -> list[Post]:
        sql = "SELECT * FROM collected_post_cache"
        args: list[Any] = []
        if subreddit:
            sql += " WHERE subreddit = ?"
            args.append(subreddit)
        sql += " ORDER BY collected_at DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_post(row) for row in rows]

    def _record_interest_change(
        self,
        topic: str,
        old_weight: float | None,
        new_weight: float,
        reason: str,
        observed_at: datetime,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO interest_changelog (timestamp, topic, old_weight, new_weight, reason)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_to_iso(observed_at), topic, old_weight, new_weight, reason),
        )

    @staticmethod
    def _row_to_action_log(row: sqlite3.Row) -> ActionLog:
        return ActionLog(
            id=int(row["id"]),
            timestamp=_parse_datetime(row["timestamp"]) or utc_now(),
            action_type=row["action_type"],
            subreddit=row["subreddit"],
            target_url=row["target_url"],
            content_preview=row["content_preview"] or "",
            karma_before=row["karma_before"],
            karma_after=row["karma_after"],
            status=row["status"],
        )

    @staticmethod
    def _row_to_tracked_post(row: sqlite3.Row) -> TrackedPost:
        return TrackedPost(
            url=row["url"],
            subreddit=row["subreddit"],
            title=row["title"],
            posted_at=_parse_datetime(row["posted_at"]) or utc_now(),
            last_checked=_parse_datetime(row["last_checked"]),
            comment_count_at_post=int(row["comment_count_at_post"] or 0),
            comment_count_latest=int(row["comment_count_latest"] or 0),
            is_active=bool(row["is_active"]),
        )

    @staticmethod
    def _row_to_schedule_log(row: sqlite3.Row) -> ScheduleLogEntry:
        return ScheduleLogEntry(
            id=int(row["id"]),
            day=_parse_date(row["date"]) or date.today(),
            planned_actions=dict(_json_loads(row["planned_actions_json"], {})),
            executed_actions=dict(_json_loads(row["executed_actions_json"], {})),
            skipped_reason=row["skipped_reason"],
        )

    @staticmethod
    def _row_to_seen_comment(row: sqlite3.Row) -> SeenComment:
        return SeenComment(
            comment_id=row["comment_id"],
            post_url=row["post_url"],
            author=row["author"],
            body_preview=row["body_preview"] or "",
            first_seen_at=_parse_datetime(row["first_seen_at"]) or utc_now(),
            is_direct_reply=bool(row["is_direct_reply"]),
            replied_at=_parse_datetime(row["replied_at"]),
            reply_comment_id=row["reply_comment_id"],
            reply_status=row["reply_status"] or "pending",
        )

    @staticmethod
    def _row_to_subreddit_profile(row: sqlite3.Row) -> SubredditProfile:
        return SubredditProfile(
            name=row["name"],
            subscribers=int(row["subscribers"] or 0),
            rules=list(_json_loads(row["rules_json"], [])),
            allowed_flairs=list(_json_loads(row["allowed_flairs_json"], [])),
            best_post_hours=list(_json_loads(row["best_post_hours_json"], [])),
            last_updated=_parse_datetime(row["last_updated"]),
            our_karma=int(row["our_karma"] or 0),
            our_post_count=int(row["our_post_count"] or 0),
            our_comment_count=int(row["our_comment_count"] or 0),
        )

    @staticmethod
    def _row_to_interest_topic(row: sqlite3.Row) -> InterestTopic:
        first_seen = _parse_datetime(row["first_seen"]) or utc_now()
        last_updated = _parse_datetime(row["last_updated"]) or first_seen
        return InterestTopic(
            topic=row["topic"],
            weight=float(row["weight"]),
            source=row["source"],
            evidence_count=int(row["evidence_count"] or 0),
            first_seen=first_seen,
            last_updated=last_updated,
            decay_rate=float(row["decay_rate"] or 0.95),
        )

    @staticmethod
    def _row_to_action_outcome(row: sqlite3.Row) -> ActionOutcome:
        return ActionOutcome(
            id=int(row["id"]),
            timestamp=_parse_datetime(row["timestamp"]) or utc_now(),
            subreddit=row["subreddit"],
            action_type=row["action_type"],
            content_hash=row["content_hash"],
            content_summary=row["content_summary"] or "",
            title=row["title"],
            post_type=row["post_type"],
            karma_1h=row["karma_1h"],
            karma_24h=row["karma_24h"],
            karma_final=row["karma_final"],
            was_removed=bool(row["was_removed"]),
            removal_reason=row["removal_reason"],
            mod_action=row["mod_action"],
            comment_count=int(row["comment_count"] or 0),
        )

    @staticmethod
    def _row_to_playbook_entry(row: sqlite3.Row) -> PlaybookEntry:
        return PlaybookEntry(
            subreddit=row["subreddit"],
            total_posts=int(row["total_posts"] or 0),
            total_comments=int(row["total_comments"] or 0),
            posts_removed=int(row["posts_removed"] or 0),
            comments_removed=int(row["comments_removed"] or 0),
            avg_post_karma=float(row["avg_post_karma"] or 0.0),
            avg_comment_karma=float(row["avg_comment_karma"] or 0.0),
            best_hours=list(_json_loads(row["best_hours_json"], [])),
            best_post_types=list(_json_loads(row["best_post_types_json"], [])),
            worst_post_types=list(_json_loads(row["worst_post_types_json"], [])),
            known_pitfalls=list(_json_loads(row["known_pitfalls_json"], [])),
            tips=list(_json_loads(row["tips_json"], [])),
            mod_notes=row["mod_notes"],
            last_incident=row["last_incident"],
            last_incident_date=_parse_datetime(row["last_incident_date"]),
            last_reviewed=_parse_datetime(row["last_reviewed"]),
        )

    @staticmethod
    def _row_to_community_snapshot(row: sqlite3.Row) -> CommunitySnapshot:
        return CommunitySnapshot(
            id=int(row["id"]),
            subreddit=row["subreddit"],
            post_url=row["post_url"],
            title=row["title"],
            author=row["author"],
            flair=row["flair"],
            score_at_capture=row["score_at_capture"],
            score_after_24h=row["score_after_24h"],
            comment_count_at_capture=row["comment_count_at_capture"],
            comment_count_after_24h=row["comment_count_after_24h"],
            posted_at=_parse_datetime(row["posted_at"]),
            captured_at=_parse_datetime(row["captured_at"]) or utc_now(),
            was_removed=bool(row["was_removed"]),
            removal_detected_at=_parse_datetime(row["removal_detected_at"]),
            mod_comment=row["mod_comment"],
            body_preview=row["body_preview"] or "",
        )

    @staticmethod
    def _row_to_community_power_user(row: sqlite3.Row) -> CommunityPowerUser:
        return CommunityPowerUser(
            id=int(row["id"]),
            subreddit=row["subreddit"],
            username=row["username"],
            role=row["role"] or "contributor",
            estimated_karma=row["estimated_karma"],
            post_count=int(row["post_count"] or 0),
            avg_score=float(row["avg_score"] or 0.0),
            content_style=row["content_style"],
            notes=row["notes"],
            last_updated=_parse_datetime(row["last_updated"]) or utc_now(),
        )

    @staticmethod
    def _row_to_content_insight(row: sqlite3.Row) -> ContentInsight:
        return ContentInsight(
            id=int(row["id"]),
            created_at=_parse_datetime(row["created_at"]) or utc_now(),
            source=row["source"],
            category=row["category"],
            subreddit=row["subreddit"],
            insight=row["insight"],
            evidence=row["evidence"],
            confidence=float(row["confidence"] or 0.5),
            sample_size=int(row["sample_size"] or 1),
            is_active=bool(row["is_active"]),
            superseded_by=row["superseded_by"],
        )

    @staticmethod
    def _row_to_post(row: sqlite3.Row) -> Post:
        return Post(
            url=row["url"],
            subreddit=row["subreddit"],
            title=row["title"],
            id=None,
            body=row["body"] or "",
            author=row["author"] or "",
            author_karma=None,
            score=int(row["score"] or 0),
            num_comments=int(row["num_comments"] or 0),
            created_utc=_parse_datetime(row["created_utc"]) or utc_now(),
            flair=row["flair"],
            is_self=bool(row["is_self"]),
            interest_score=row["interest_score"],
        )
