from __future__ import annotations

import unittest

from replier import ReplyContext, ReplyGenerator
from storage import Comment, Post, PostDetail


def _build_context() -> ReplyContext:
    post = Post(
        url="https://reddit.test/r/swimming/comments/post1",
        subreddit="r/Swimming",
        title="How do you fix crossover in freestyle?",
        body="I keep drifting on longer sets and I am looking for cues.",
        author="me",
    )
    comment = Comment(
        id="t1_reply1",
        post_url=post.url,
        author="helper",
        body="Have you tried slowing down and focusing on hand entry?",
    )
    return ReplyContext(
        subreddit=post.subreddit,
        post=post,
        comment=comment,
        context_chain=[comment],
        is_direct_reply=True,
    )


class ReplyGeneratorTest(unittest.TestCase):
    def test_llm_reply_is_used_when_available(self) -> None:
        generator = ReplyGenerator(
            use_llm=True,
            completion_client=lambda messages: "Try a shorter stroke count for a few sets and see if the hand entry stays cleaner.",
        )

        reply = generator.generate_reply(_build_context())

        self.assertIn("stroke count", reply)

    def test_template_fallback_is_used_when_llm_fails(self) -> None:
        def _raise(messages: list[dict[str, str]]) -> str:
            raise RuntimeError("boom")

        generator = ReplyGenerator(use_llm=True, completion_client=_raise)

        reply = generator.generate_reply(_build_context())

        self.assertTrue(reply)
        self.assertNotIn("boom", reply)

    def test_engagement_reply_falls_back_to_template(self) -> None:
        generator = ReplyGenerator(use_llm=False)
        post = Post(
            url="https://reddit.test/r/swimming/comments/post2",
            subreddit="r/Swimming",
            title="What helped you stop crossing over in freestyle?",
            body="Looking for a cue that worked in real practice.",
        )
        detail = PostDetail(
            post=post,
            comments=[
                Comment(
                    id="t1_one",
                    post_url=post.url,
                    author="helper",
                    body="A coach told me to widen the entry.",
                )
            ],
        )

        reply = generator.generate_engagement_reply(
            subreddit=post.subreddit,
            post=post,
            post_detail=detail,
            suggested_angle="Answer the core question directly and keep it practical.",
        )

        self.assertTrue(reply)
        self.assertIn("starting point", reply.lower())


if __name__ == "__main__":
    unittest.main()
