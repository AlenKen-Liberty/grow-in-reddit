from __future__ import annotations

from collections import Counter, defaultdict

from storage import ActionOutcome, ContentInsight, PlaybookEntry, SQLiteStore


class CommunityPlaybook:
    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def record_outcome(
        self,
        subreddit: str,
        action_type: str,
        content_summary: str,
        outcome: ActionOutcome,
    ) -> PlaybookEntry:
        normalized = ActionOutcome(
            subreddit=subreddit,
            action_type=action_type,
            content_summary=content_summary,
            title=outcome.title,
            post_type=outcome.post_type,
            karma_1h=outcome.karma_1h,
            karma_24h=outcome.karma_24h,
            karma_final=outcome.karma_final,
            was_removed=outcome.was_removed,
            removal_reason=outcome.removal_reason,
            mod_action=outcome.mod_action,
            comment_count=outcome.comment_count,
            content_hash=outcome.content_hash,
            timestamp=outcome.timestamp,
        )
        self.sqlite_store.record_action_outcome(normalized)
        entry = self.analyze_subreddit(subreddit)
        self.sqlite_store.upsert_community_playbook(entry)
        return entry

    def analyze_subreddit(self, subreddit: str) -> PlaybookEntry:
        outcomes = self.sqlite_store.list_action_outcomes(
            subreddit=subreddit, limit=1000
        )
        post_scores: list[int] = []
        comment_scores: list[int] = []
        post_type_scores: defaultdict[str, list[int]] = defaultdict(list)
        hour_counter: Counter[int] = Counter()
        pitfalls: Counter[str] = Counter()
        mod_actions: Counter[str] = Counter()
        last_incident: tuple[str, object] | None = None

        total_posts = 0
        total_comments = 0
        posts_removed = 0
        comments_removed = 0

        for outcome in outcomes:
            final_karma = int(outcome.karma_final or 0)
            hour_counter[outcome.timestamp.hour] += max(final_karma, 0) + 1
            if outcome.action_type == "post":
                total_posts += 1
                post_scores.append(final_karma)
                if outcome.was_removed:
                    posts_removed += 1
            elif outcome.action_type == "comment":
                total_comments += 1
                comment_scores.append(final_karma)
                if outcome.was_removed:
                    comments_removed += 1

            if outcome.post_type:
                post_type_scores[outcome.post_type].append(final_karma)
            if outcome.removal_reason:
                pitfalls[outcome.removal_reason] += 1
            if outcome.mod_action:
                mod_actions[outcome.mod_action] += 1
            if outcome.was_removed or outcome.mod_action:
                description = (
                    outcome.removal_reason or outcome.mod_action or "negative outcome"
                )
                if last_incident is None or outcome.timestamp > last_incident[1]:
                    last_incident = (description, outcome.timestamp)

        avg_scores = {
            post_type: sum(scores) / len(scores)
            for post_type, scores in post_type_scores.items()
            if scores
        }
        best_post_types = [
            post_type
            for post_type, _ in sorted(
                avg_scores.items(), key=lambda item: item[1], reverse=True
            )[:3]
        ]
        worst_post_types = [
            post_type
            for post_type, _ in sorted(avg_scores.items(), key=lambda item: item[1])[:3]
        ]
        tips = []
        if best_post_types:
            tips.append(f"Lean into {', '.join(best_post_types)} formats.")
        if hour_counter:
            best_hours = [hour for hour, _ in hour_counter.most_common(3)]
            tips.append(
                "Best recent posting windows (UTC): "
                + ", ".join(str(hour) for hour in best_hours)
            )
        else:
            best_hours = []
        if post_scores and sum(post_scores) / len(post_scores) >= 5:
            tips.append(
                "Posts in this community have been net positive; keep quality high and stay consistent."
            )
        if comment_scores and sum(comment_scores) / len(comment_scores) >= 3:
            tips.append("Helpful comments are compounding well here.")

        return PlaybookEntry(
            subreddit=subreddit,
            total_posts=total_posts,
            total_comments=total_comments,
            posts_removed=posts_removed,
            comments_removed=comments_removed,
            avg_post_karma=(
                (sum(post_scores) / len(post_scores)) if post_scores else 0.0
            ),
            avg_comment_karma=(
                (sum(comment_scores) / len(comment_scores)) if comment_scores else 0.0
            ),
            best_hours=best_hours,
            best_post_types=best_post_types,
            worst_post_types=worst_post_types,
            known_pitfalls=[reason for reason, _ in pitfalls.most_common(5)],
            tips=tips,
            mod_notes=", ".join(action for action, _ in mod_actions.most_common(3))
            or None,
            last_incident=last_incident[0] if last_incident else None,
            last_incident_date=last_incident[1] if last_incident else None,
        )

    def get_do_and_dont(self, subreddit: str) -> tuple[list[str], list[str]]:
        entry = self.sqlite_store.get_community_playbook(subreddit)
        if entry is None:
            return [], []
        dos = list(entry.tips)
        if entry.best_post_types:
            dos.append("Best known formats: " + ", ".join(entry.best_post_types))
        donts = list(entry.known_pitfalls)
        if entry.worst_post_types:
            donts.append("Weak formats so far: " + ", ".join(entry.worst_post_types))
        if entry.last_incident:
            donts.append("Latest incident: " + entry.last_incident)
        return dos, donts

    def auto_review(self) -> list[PlaybookEntry]:
        reviewed: list[PlaybookEntry] = []
        subreddits = {
            outcome.subreddit
            for outcome in self.sqlite_store.list_action_outcomes(limit=5000)
            if outcome.subreddit
        }
        for subreddit in sorted(subreddits):
            entry = self.analyze_subreddit(subreddit)
            self.sqlite_store.upsert_community_playbook(entry)
            if entry.known_pitfalls:
                self.sqlite_store.add_content_insight(
                    ContentInsight(
                        category="pitfall",
                        subreddit=subreddit,
                        source="self",
                        insight=f"Avoid: {entry.known_pitfalls[0]}",
                        evidence=f"{subreddit} action_outcome analysis",
                        confidence=0.7,
                        sample_size=max(
                            entry.posts_removed + entry.comments_removed, 1
                        ),
                    )
                )
            reviewed.append(entry)
        return reviewed
