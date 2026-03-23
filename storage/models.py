from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone

UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class Post:
    url: str
    subreddit: str
    title: str
    id: str | None = None
    body: str = ""
    author: str = ""
    author_karma: int | None = None
    score: int = 0
    num_comments: int = 0
    created_utc: datetime = field(default_factory=utc_now)
    flair: str | None = None
    is_self: bool = True
    interest_score: float | None = None


@dataclass(slots=True)
class Comment:
    id: str
    post_url: str
    author: str
    body: str
    score: int = 0
    created_utc: datetime = field(default_factory=utc_now)
    parent_id: str | None = None
    depth: int = 0


@dataclass(slots=True)
class PostDetail:
    post: Post
    comments: list[Comment] = field(default_factory=list)


@dataclass(slots=True)
class UserProfile:
    username: str
    karma_post: int = 0
    karma_comment: int = 0
    cake_day: date | None = None
    is_premium: bool = False
    trophies: list[str] = field(default_factory=list)

    @property
    def karma_total(self) -> int:
        return self.karma_post + self.karma_comment


@dataclass(slots=True)
class ActionLog:
    action_type: str
    subreddit: str | None = None
    target_url: str | None = None
    content_preview: str = ""
    karma_before: int | None = None
    karma_after: int | None = None
    status: str = "success"
    timestamp: datetime = field(default_factory=utc_now)
    id: int | None = None


@dataclass(slots=True)
class BrowseAction:
    action: str
    target_url: str
    subreddit: str | None = None
    note: str | None = None
    timestamp: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class AccountSnapshot:
    day: date
    karma_post: int
    karma_comment: int
    karma_total: int
    active_subreddits: int = 0
    total_posts: int = 0
    total_comments: int = 0
    id: int | None = None


@dataclass(slots=True)
class ScheduleLogEntry:
    day: date
    planned_actions: dict[str, object] | None = None
    executed_actions: dict[str, object] | None = None
    skipped_reason: str | None = None
    id: int | None = None


@dataclass(slots=True)
class TrackedPost:
    url: str
    subreddit: str
    title: str
    posted_at: datetime
    last_checked: datetime | None = None
    comment_count_at_post: int = 0
    comment_count_latest: int = 0
    is_active: bool = True


@dataclass(slots=True)
class SeenComment:
    comment_id: str
    post_url: str
    author: str | None = None
    body_preview: str = ""
    first_seen_at: datetime = field(default_factory=utc_now)
    is_direct_reply: bool = False
    replied_at: datetime | None = None
    reply_comment_id: str | None = None
    reply_status: str = "pending"


@dataclass(slots=True)
class SubredditProfile:
    name: str
    subscribers: int = 0
    rules: list[str] = field(default_factory=list)
    allowed_flairs: list[str] = field(default_factory=list)
    best_post_hours: list[int] = field(default_factory=list)
    last_updated: datetime | None = None
    our_karma: int = 0
    our_post_count: int = 0
    our_comment_count: int = 0


@dataclass(slots=True)
class InterestTopic:
    topic: str
    weight: float
    source: str
    evidence_count: int = 0
    first_seen: datetime = field(default_factory=utc_now)
    last_updated: datetime = field(default_factory=utc_now)
    decay_rate: float = 0.95


@dataclass(slots=True)
class ActionOutcome:
    subreddit: str
    action_type: str
    content_summary: str
    title: str | None = None
    post_type: str | None = None
    karma_1h: int | None = None
    karma_24h: int | None = None
    karma_final: int | None = None
    was_removed: bool = False
    removal_reason: str | None = None
    mod_action: str | None = None
    comment_count: int = 0
    content_hash: str | None = None
    timestamp: datetime = field(default_factory=utc_now)
    id: int | None = None


@dataclass(slots=True)
class PlaybookEntry:
    subreddit: str
    total_posts: int = 0
    total_comments: int = 0
    posts_removed: int = 0
    comments_removed: int = 0
    avg_post_karma: float = 0.0
    avg_comment_karma: float = 0.0
    best_hours: list[int] = field(default_factory=list)
    best_post_types: list[str] = field(default_factory=list)
    worst_post_types: list[str] = field(default_factory=list)
    known_pitfalls: list[str] = field(default_factory=list)
    tips: list[str] = field(default_factory=list)
    mod_notes: str | None = None
    last_incident: str | None = None
    last_incident_date: datetime | None = None
    last_reviewed: datetime | None = None


@dataclass(slots=True)
class ContentInsight:
    category: str
    insight: str
    source: str = "self"
    subreddit: str | None = None
    evidence: str | None = None
    confidence: float = 0.5
    sample_size: int = 1
    is_active: bool = True
    created_at: datetime = field(default_factory=utc_now)
    superseded_by: int | None = None
    id: int | None = None


@dataclass(slots=True)
class CommunitySnapshot:
    subreddit: str
    post_url: str
    title: str
    author: str | None = None
    flair: str | None = None
    score_at_capture: int | None = None
    score_after_24h: int | None = None
    comment_count_at_capture: int | None = None
    comment_count_after_24h: int | None = None
    posted_at: datetime | None = None
    captured_at: datetime = field(default_factory=utc_now)
    was_removed: bool = False
    removal_detected_at: datetime | None = None
    mod_comment: str | None = None
    body_preview: str = ""
    id: int | None = None


@dataclass(slots=True)
class CommunityPowerUser:
    subreddit: str
    username: str
    role: str = "contributor"
    estimated_karma: int | None = None
    post_count: int = 0
    avg_score: float = 0.0
    content_style: str | None = None
    notes: str | None = None
    last_updated: datetime = field(default_factory=utc_now)
    id: int | None = None
