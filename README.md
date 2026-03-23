# Grow in Reddit

This project is a Reddit automation and community engagement tool designed specifically for individuals with limited English proficiency or language barriers. It helps users natively interact, browse, and grow their presence on Reddit with the assistance of LLMs.

## Core Philosophy
We believe that language shouldn't be a barrier to participating in global communities. This tool is **entirely free** (except for your own LLM API costs) and is built to help you understand community context and engage meaningfully.

## Technical Features
- **Memory Capabilities:** Maintains a local profile of user interests, past interactions, and account snapshots to ensure long-term consistency.
- **Interactive Scripting (CDP Browser):** Directly connects to Chrome via Chrome DevTools Protocol to safely handle browsing, upvoting, and commenting under your own session, avoiding traditional API limits.
- **Community Intelligence:** Actively monitors and takes snapshots of target subreddits to detect trending topics, community rules, and context over a 24-hour cycle.
- **AI-Powered Replier:** Tracks your posts for new replies and can generate intelligent, contextual responses using a template system or LLMs.
- **Local Storage:** Utilizes SQLite and vector databases to securely maintain your history.

## Phase 2 Features (Recently Implemented)
- **Interest Matcher Redesign:** A robust three-layer scoring system prioritizing subreddit relevance, keyword matching, and post quality signals.
- **End-to-End Engagement Commands:** New CLI actions to interact naturally:
  - `submit_comment` & `upvote`: Reply to posts/comments and upvote content directly.
  - `browse_and_engage`: Simulates a natural browsing session to warm up the account.
- **Thread Tracker & Reply Generator:** Automatically detects new replies to your posts/comments and provides suggested templates or LLM responses.
- **Engagement Finder:** Discovers high-value reply opportunities in target communities based on your configured expertise.
- **Account Snapshots & Outcome Tracker:** Periodically logs karma changes and tracks the 24-hour performance of your actions.

## Getting Started

### Prerequisites
- Python 3.10+
- Chrome Browser (run with `--remote-debugging-port=9222`)

### Installation
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in your Reddit username and API keys.
4. Start the CLI tool: `python main.py --help`
