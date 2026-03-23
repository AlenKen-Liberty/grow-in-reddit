from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable
from urllib import error, request

from storage import Comment, Post
from storage.models import utc_now
from utils import extract_preview

from .thread_tracker import NewReply


@dataclass(slots=True)
class ReplyContext:
    subreddit: str
    post: Post
    comment: Comment
    context_chain: list[Comment]
    is_direct_reply: bool = False


class ReplyGenerator:
    """Template-first reply generation for Phase 2."""

    LOW_SIGNAL_MESSAGES = {
        "this",
        "same",
        "lol",
        "nice",
        "agreed",
        "yup",
        "+1",
    }
    TROLL_MARKERS = {"idiot", "moron", "stupid", "shut up", "hate you"}

    def __init__(
        self,
        *,
        use_llm: bool = False,
        llm_provider: str = "chat2api",
        llm_base_url: str = "http://127.0.0.1:7860",
        llm_model: str = "gemini-thinking",
        request_timeout: float = 45.0,
        completion_client: Callable[[list[dict[str, str]]], str] | None = None,
    ):
        self.use_llm = use_llm
        self.llm_provider = llm_provider
        self.llm_base_url = llm_base_url.rstrip("/")
        self.llm_model = llm_model
        self.request_timeout = request_timeout
        self._completion_client = completion_client

    def generate_reply(self, context: ReplyContext) -> str:
        if self.use_llm:
            llm_reply = self._generate_reply_with_llm(context)
            if llm_reply:
                return llm_reply

        return self._generate_reply_from_template(context)

    def _generate_reply_from_template(self, context: ReplyContext) -> str:
        comment_text = (context.comment.body or "").strip()
        post_hint = extract_preview(context.post.title or context.post.body, max_length=80)
        if "?" in comment_text:
            return (
                f"Good question. From my side, {post_hint.lower()} is usually where I start."
            )
        if context.is_direct_reply:
            return (
                f"Thanks for adding that. I appreciate the extra context around {post_hint.lower()}."
            )
        if self._shares_experience(comment_text):
            return "That lines up with what I have seen too. The small details tend to matter most."
        return "Appreciate the reply. That is a useful angle to keep in mind."

    def _generate_reply_with_llm(self, context: ReplyContext) -> str | None:
        messages = self._build_llm_messages(context)
        try:
            if self._completion_client is not None:
                content = self._completion_client(messages)
            else:
                content = self._request_chat_completion(messages)
        except Exception:
            return None
        cleaned = self._normalize_llm_reply(content)
        return cleaned or None

    def should_reply(self, new_reply: NewReply) -> tuple[bool, str]:
        text = (new_reply.comment.body or "").strip()
        lowered = text.lower()
        if lowered in {"[deleted]", "[removed]"}:
            return False, "deleted_or_removed"
        if not text:
            return False, "empty"
        if lowered in self.LOW_SIGNAL_MESSAGES or len(text) <= 3:
            return False, "low_signal"
        if any(marker in lowered for marker in self.TROLL_MARKERS):
            return False, "troll_marker"
        if new_reply.comment.created_utc < utc_now() - timedelta(hours=48):
            return False, "stale"
        return True, "reply"

    @staticmethod
    def _shares_experience(comment_text: str) -> bool:
        lowered = comment_text.lower()
        return any(
            phrase in lowered
            for phrase in ("i had", "i have", "for me", "in my case", "i found")
        )

    def _build_llm_messages(self, context: ReplyContext) -> list[dict[str, str]]:
        chain_lines = []
        for item in context.context_chain:
            chain_lines.append(f"{item.author}: {extract_preview(item.body, max_length=220)}")
        chain_text = "\n".join(chain_lines) if chain_lines else "(no prior thread context)"
        prompt = (
            f"Subreddit: {context.subreddit}\n"
            f"Post title: {context.post.title}\n"
            f"Post body: {extract_preview(context.post.body, max_length=500)}\n"
            f"New comment from {context.comment.author}: {extract_preview(context.comment.body, max_length=400)}\n"
            f"Context chain:\n{chain_text}\n"
            f"Direct reply to me: {'yes' if context.is_direct_reply else 'no'}\n\n"
            "Write a natural Reddit reply.\n"
            "Constraints:\n"
            "- 1 to 3 sentences\n"
            "- practical, specific, and human\n"
            "- no emoji\n"
            '- do not mention being an AI\n'
            "- avoid generic gratitude-only filler\n"
            "- plain text only"
        )
        return [
            {
                "role": "system",
                "content": (
                    "You are a normal Reddit user replying in-thread. "
                    "Be helpful, concise, and context-aware."
                ),
            },
            {"role": "user", "content": prompt},
        ]

    def _request_chat_completion(self, messages: list[dict[str, str]]) -> str:
        payload = {
            "model": self.llm_model,
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 180,
            "stream": False,
        }
        req = request.Request(
            f"{self.llm_base_url}/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.request_timeout) as response:
                result = json.load(response)
        except error.HTTPError as exc:
            raise RuntimeError(f"LLM HTTP {exc.code}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"LLM request failed: {exc.reason}") from exc

        choices = result.get("choices") or []
        if not choices:
            raise RuntimeError("LLM response did not contain choices")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            texts = [
                part.get("text", "")
                for part in content
                if isinstance(part, dict) and part.get("type") == "text"
            ]
            return "\n".join(text for text in texts if text).strip()
        raise RuntimeError("LLM response content was empty")

    @staticmethod
    def _normalize_llm_reply(content: str) -> str:
        text = " ".join((content or "").split()).strip()
        if not text:
            return ""
        if text.startswith('"') and text.endswith('"') and len(text) >= 2:
            text = text[1:-1].strip()
        return text
