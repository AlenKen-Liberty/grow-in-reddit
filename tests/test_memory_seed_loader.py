from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from reddit_memory import MemorySeedLoader


class MemorySeedLoaderTest(unittest.TestCase):
    def test_augment_seed_config_uses_claude_and_openclaw_memories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_name:
            tmpdir = Path(tmpdir_name)
            claude_dir = tmpdir / "claude"
            openclaw_dir = tmpdir / "openclaw"
            claude_dir.mkdir()
            openclaw_dir.mkdir()
            (claude_dir / "session.jsonl").write_text(
                '{"message":"Working on AI agent browser automation with Playwright and CDP."}\n',
                encoding="utf-8",
            )
            (openclaw_dir / "swimming.md").write_text(
                "Daughter swimming recruiting, NCAA, Stanford, Princeton, Harvard.\n",
                encoding="utf-8",
            )
            loader = MemorySeedLoader(
                claude_projects_dir=claude_dir,
                openclaw_memory_dir=openclaw_dir,
            )

            seed_config = loader.augment_seed_config(
                {"primary": [], "secondary": [], "similarity_threshold": 0.8}
            )

        self.assertEqual(seed_config["similarity_threshold"], 0.40)
        primary_topics = {entry["topic"] for entry in seed_config["primary"]}
        secondary_topics = {entry["topic"] for entry in seed_config["secondary"]}
        self.assertIn("swim recruiting", primary_topics)
        self.assertIn("ai agents", secondary_topics)


if __name__ == "__main__":
    unittest.main()
