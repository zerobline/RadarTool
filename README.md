# Reddit Scraper Tool — Unified v9

A desktop GUI application for scraping Reddit posts and comments, with sentiment analysis and pain-score filtering.

## Features

- **Subreddit mode** — browse a specific subreddit (PRAW)
- **Search mode** — keyword search across all of Reddit (PRAW)
- **Arctic Shift mode** — historical archive scraping, any date range (no API key required)
- VADER sentiment analysis (Positive / Neutral / Negative)
- Pain score — custom engagement anomaly metric
- Configurable filters: min comments, min upvotes, upvote ratio, pain score range
- Comment scraping with configurable depth
- Deduplication across runs
- Output to formatted `.txt` files

## Requirements

- Python 3.9+
- tkinter (included in standard Python on Windows/macOS; on Linux: `sudo apt install python3-tk`)

## Installation

```bash
git clone https://github.com/zerobline/reddit_scrapper_tool.git
cd reddit_scrapper_tool
pip install -r requirements.txt
```

## Configuration

1. Copy the example env file:

   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your Reddit API credentials:

   ```
   REDDIT_CLIENT_ID=your_client_id_here
   REDDIT_CLIENT_SECRET=your_client_secret_here
   REDDIT_USER_AGENT=UnifiedScraper
   ```

   Create a Reddit app at <https://www.reddit.com/prefs/apps> (choose **script** type).

3. The app also lets you enter credentials directly in the Settings panel — they are saved to `reddit_unified_config.json` (excluded from git).

## Usage

```bash
python "RedditScraper_Unified_23 01.py"
```

### Scrape modes

| Mode | What it does |
|---|---|
| Subreddit | Fetches top/new/hot/rising posts from a single subreddit via PRAW |
| Search | Keyword search across Reddit via PRAW |
| Arctic Shift | Pulls from the Arctic Shift historical archive — supports date ranges, no PRAW needed |

### Output

Scraped data is saved as `.txt` files in your configured output directory (default: `~/RedditScrapping/`). A matching `.ids` file tracks seen post IDs for deduplication across runs.

## Presets

| Preset | Posts | Comment depth |
|---|---|---|
| Quick Scan | 100 | 0 |
| Standard | 300 | 2 |
| Deep Dive | 1000 | 5 |
| Custom | configurable | configurable |

## Platform support

Runs on Windows, macOS, and Linux. The "Open file" button uses the native file opener for each OS.

Windows toast notifications require the optional `win10toast` package:

```bash
pip install win10toast
```

## License

MIT — see [LICENSE](LICENSE).
