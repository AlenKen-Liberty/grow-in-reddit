# Grow in Reddit

A Reddit community engagement tool designed for individuals with limited English proficiency or language barriers. Helps users naturally interact, browse, and grow their Reddit presence with LLM assistance.

## Core Philosophy
Language shouldn't be a barrier to participating in global communities. This tool is **entirely free** — it uses your local [chat2api](http://127.0.0.1:7860) instance as the LLM backend (OpenAI-compatible), so **no external API keys are needed**.

## Technical Features
- **Memory Capabilities:** Local profile of user interests, past interactions, and account snapshots for long-term consistency.
- **Interactive Scripting (CDP Browser):** Connects to Chrome via Chrome DevTools Protocol for browsing, upvoting, and commenting under your own session — no Reddit API needed.
- **Community Intelligence:** Monitors target subreddits to detect trending topics, community norms, and removals over a 24-hour cycle.
- **AI-Powered Replier:** Tracks your posts for new replies and generates contextual responses (templates or LLM).
- **Local Storage:** SQLite for all data; optional Elasticsearch and Qdrant for future vector search.

## CLI Commands

| Command | Description |
|---------|-------------|
| `run` | Start the automated scheduler (main entry point) |
| `collect` | Collect subreddit content and match interests |
| `post` | Submit or draft a text post |
| `reply --check` | Check your posts for new replies |
| `reply --engage <sub>` | Find reply opportunities in a subreddit |
| `reply --auto [--llm]` | Auto-reply to new comments (template or LLM) |
| `comment --url URL --text TEXT` | Submit a manual comment |
| `vote --url URL` | Upvote a post or comment |
| `browse <subreddit>` | Simulate a natural browsing session |
| `snapshot` | Record current account karma snapshot |
| `intel <subreddit>` | Collect community intelligence snapshot |
| `intel --revisit` | Revisit 24h-old snapshots for outcome data |
| `intel --report <sub>` | Generate community analysis report |
| `status` | Show local project state |
| `nurture` | Run an account care session |

## Getting Started

### Prerequisites
- Python 3.10+
- Chrome Browser (run with `--remote-debugging-port=9222`)
- chat2api running on `http://127.0.0.1:7860` (for LLM features)

### Installation
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and set your `REDDIT_USERNAME`.
4. Start the CLI: `python3 main.py --help`

### LLM Configuration
This project uses **chat2api** as the sole LLM provider — a local, OpenAI-compatible proxy. No Claude, Gemini, or OpenAI API keys are required.

Configuration in `.env`:
```
LLM_PROVIDER=chat2api
LLM_BASE_URL=http://127.0.0.1:7860
LLM_MODEL=gemini-thinking
```
