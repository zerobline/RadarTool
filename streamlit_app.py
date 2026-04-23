import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, date, timedelta

import streamlit as st

try:
    import praw
    PRAW_OK = True
except ImportError:
    PRAW_OK = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    ANALYZER = SentimentIntensityAnalyzer()
    VADER_OK = True
except ImportError:
    VADER_OK = False


def get_sentiment(text: str) -> str:
    if not VADER_OK or not text:
        return "Neutral"
    score = ANALYZER.polarity_scores(text).get("compound", 0.0)
    if score >= 0.05:
        return "Positive"
    if score <= -0.05:
        return "Negative"
    return "Neutral"


def calc_pain_score(post_ts, comments, post_score, post_n_comments, post_sentiment):
    cph_1h = 0.0
    if comments:
        ctimes = []
        for c in comments:
            try:
                ctimes.append(datetime.strptime(c["date"], "%Y-%m-%d %H:%M").timestamp())
            except Exception:
                pass
        if ctimes:
            age_h = max((datetime.utcnow().timestamp() - post_ts) / 3600, 0.01)
            c1h = sum(1 for t in ctimes if t <= post_ts + 3600)
            cph_1h = c1h / min(age_h, 1)

    sent_w = {"Negative": 1.5, "Neutral": 1.0, "Positive": 0.6}.get(post_sentiment, 1.0)
    upvotes = max(post_score, 1)
    eng_ratio = post_n_comments / upvotes
    eng_score = min(30, eng_ratio * 60)
    vpen = 0.2 if post_score > 50000 else 0.5 if post_score > 10000 else 0.8 if post_score > 1000 else 1.0
    d1 = sum(1 for c in comments if c.get("depth", 0) == 1)
    depth_sc = min(20, (d1 / max(len(comments), 1)) * 40)
    vel_sc = min(30, cph_1h * 2) * vpen
    pain = min(100, int((vel_sc + eng_score + depth_sc) * sent_w))
    pattern = (
        "STRONG PAIN" if pain >= 70 else
        "CLEAR PAIN" if pain >= 45 else
        "MILD PAIN" if pain >= 25 else
        "WEAK SIGNAL" if pain >= 10 else
        "NOISE"
    )
    return {"score": pain, "pattern": pattern}


def process_comments_praw(post, depth: int, max_write: int):
    fetch_cap = max_write * 4
    post.comments.replace_more(limit=depth)
    out = []
    for c in post.comments.list():
        if not isinstance(c, praw.models.Comment):
            continue
        if len(out) >= fetch_cap:
            break
        cdepth = getattr(c, "_depth", 0)
        out.append({
            "id": c.id,
            "body": c.body,
            "score": c.score,
            "depth": cdepth,
            "date": datetime.utcfromtimestamp(c.created_utc).strftime("%Y-%m-%d %H:%M"),
        })
    return out


def fetch_arctic_posts(query, subreddit, date_from, date_to, sort, limit):
    params = {"limit": 100, "sort": sort}
    if query:
        params["q"] = query
    if subreddit:
        params["subreddit"] = subreddit
    if date_from:
        params["after"] = date_from
    if date_to:
        params["before"] = date_to

    got = []
    cursor = None
    while len(got) < limit:
        page_params = dict(params)
        if cursor is not None:
            if sort == "desc":
                page_params["before"] = cursor
            else:
                page_params["after"] = cursor
        url = f"https://arctic-shift.photon-reddit.com/api/posts/search?{urllib.parse.urlencode(page_params)}"
        req = urllib.request.Request(url, headers={"User-Agent": "UnifiedScraper/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="ignore")[:300]
            except Exception:
                pass
            raise RuntimeError(f"Arctic Shift request failed ({exc.code} {exc.reason}). {body}".strip()) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Arctic Shift network error: {exc.reason}") from exc
        except TimeoutError as exc:
            raise RuntimeError("Arctic Shift request timed out.") from exc

        if "error" in payload:
            raise RuntimeError(f"Arctic Shift API error: {payload['error']}")
        batch = payload.get("data", [])
        if not batch:
            break
        got.extend(batch)
        ts = int(float(batch[-1].get("created_utc", 0)))
        cursor = str(ts - 1) if sort == "desc" else str(ts + 1)
        time.sleep(0.2)
    return got[:limit]


def build_rows_from_posts(posts, comment_depth, max_comments, progress_fn=None):
    rows = []
    pos = neg = neutral = 0
    for i, post in enumerate(posts, start=1):
        comments = post.get("_comments", [])
        full_text = f"{post['title']}\n\n{post['body']}"
        post_sent = get_sentiment(full_text)
        pain = calc_pain_score(post["created_utc"], comments, post["upvotes"], post["n_comments"], post_sent)
        row = {
            "post_id": post["post_id"],
            "title": post["title"],
            "subreddit": post["subreddit"],
            "author": post["author"],
            "upvotes": post["upvotes"],
            "upvote_ratio": post["upvote_ratio"],
            "n_comments": post["n_comments"],
            "date": datetime.utcfromtimestamp(post["created_utc"]).strftime("%Y-%m-%d %H:%M"),
            "post_sentiment": post_sent,
            "pain_score": pain["score"],
            "pain_pattern": pain["pattern"],
            "url": post["url"],
            "body": post["body"][:1200],
        }
        rows.append(row)
        if post_sent == "Positive":
            pos += 1
        elif post_sent == "Negative":
            neg += 1
        else:
            neutral += 1
        if progress_fn:
            progress_fn(i, len(posts))
    return rows, {"positive": pos, "negative": neg, "neutral": neutral}


def main():
    st.set_page_config(page_title="Reddit Scraper (Streamlit)", layout="wide")
    st.title("Reddit Scraper — Streamlit")
    st.caption("Web version for Subreddit, Search, and Arctic Shift scraping.")

    with st.sidebar:
        st.header("Mode")
        mode = st.radio("Scrape mode", ["subreddit", "search", "arctic"], index=0)

        st.header("Shared settings")
        post_limit = st.slider("Post limit", min_value=10, max_value=1000, value=100, step=10)
        comment_depth = st.slider("Comment depth (PRAW modes)", min_value=0, max_value=10, value=2)
        max_comments = st.slider("Max comments to parse", min_value=5, max_value=200, value=30, step=5)

        client_id = st.text_input("Reddit client id", value=os.environ.get("REDDIT_CLIENT_ID", ""))
        client_secret = st.text_input("Reddit client secret", value=os.environ.get("REDDIT_CLIENT_SECRET", ""), type="password")
        user_agent = st.text_input("User agent", value=os.environ.get("REDDIT_USER_AGENT", "UnifiedScraper"))

        if mode == "subreddit":
            subreddit = st.text_input("Subreddit", value="python")
            post_type = st.selectbox("Post type", ["top", "new", "hot", "rising", "controversial"], index=0)
            time_filter = st.selectbox("Time filter", ["hour", "day", "week", "month", "year", "all"], index=4)
            query = ""
            sort = "relevance"
            as_query = as_subreddit = ""
            as_date_from = as_date_to = ""
            as_sort = "desc"
        elif mode == "search":
            query = st.text_input("Search query", value="developer pain points")
            sort = st.selectbox("Sort", ["relevance", "hot", "top", "new", "comments"], index=0)
            time_filter = st.selectbox("Time filter", ["hour", "day", "week", "month", "year", "all"], index=4)
            subreddit = ""
            post_type = "top"
            as_query = as_subreddit = ""
            as_date_from = as_date_to = ""
            as_sort = "desc"
        else:
            as_query = st.text_input("Arctic query", value="developer pain points")
            as_subreddit = st.text_input("Arctic subreddit (optional)", value="")
            as_sort = st.selectbox("Arctic sort", ["desc", "asc"], index=0)
            as_date_to = st.date_input("Date to", value=date.today()).strftime("%Y-%m-%d")
            as_date_from = st.date_input("Date from", value=date.today() - timedelta(days=365)).strftime("%Y-%m-%d")
            subreddit = ""
            post_type = "top"
            query = ""
            sort = "relevance"
            time_filter = "year"

    if st.button("Run scrape", type="primary"):
        posts = []
        if mode in ("subreddit", "search"):
            if not PRAW_OK:
                st.error("praw is not installed. Run `pip install -r requirements.txt`.")
                st.stop()
            reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
            if mode == "subreddit":
                sub = reddit.subreddit(re.sub(r"^/?r/", "", subreddit.strip(), flags=re.I))
                generators = {
                    "new": sub.new,
                    "hot": sub.hot,
                    "rising": sub.rising,
                    "top": lambda **kw: sub.top(time_filter=time_filter, **kw),
                    "controversial": lambda **kw: sub.controversial(time_filter=time_filter, **kw),
                }
                iterator = generators[post_type](limit=post_limit)
            else:
                iterator = reddit.subreddit("all").search(query, sort=sort, time_filter=time_filter, limit=post_limit)

            for p in iterator:
                try:
                    body = p.selftext if p.is_self else "[Link Post]"
                    comments = process_comments_praw(p, comment_depth, max_comments)
                    posts.append({
                        "post_id": p.id,
                        "title": p.title,
                        "body": body,
                        "subreddit": p.subreddit.display_name,
                        "author": str(p.author),
                        "upvotes": p.score,
                        "upvote_ratio": p.upvote_ratio,
                        "n_comments": p.num_comments,
                        "url": p.url,
                        "created_utc": float(p.created_utc),
                        "_comments": comments,
                    })
                except Exception:
                    continue
        else:
            with st.spinner("Fetching Arctic Shift posts..."):
                if as_date_from > as_date_to:
                    st.error("Date from must be earlier than or equal to date to.")
                    st.stop()
                try:
                    arctic = fetch_arctic_posts(
                        as_query,
                        as_subreddit,
                        as_date_from,
                        as_date_to,
                        as_sort,
                        post_limit,
                    )
                except RuntimeError as exc:
                    st.error(str(exc))
                    st.stop()
            for p in arctic:
                posts.append({
                    "post_id": p.get("id", ""),
                    "title": p.get("title", ""),
                    "body": p.get("selftext", "") or "[Link Post]",
                    "subreddit": p.get("subreddit", ""),
                    "author": p.get("author", "[deleted]"),
                    "upvotes": int(p.get("score", 0)),
                    "upvote_ratio": float(p.get("upvote_ratio", 0.5)),
                    "n_comments": int(p.get("num_comments", 0)),
                    "url": p.get("url", ""),
                    "created_utc": float(p.get("created_utc", 0.0)),
                    "_comments": [],
                })

        if not posts:
            st.warning("No posts were collected.")
            st.stop()

        bar = st.progress(0)

        def _progress(i, total):
            bar.progress(min(i / total, 1.0))

        rows, stats = build_rows_from_posts(posts, comment_depth, max_comments, progress_fn=_progress)

        st.success(f"Collected {len(rows)} posts")
        c1, c2, c3 = st.columns(3)
        c1.metric("Positive", stats["positive"])
        c2.metric("Neutral", stats["neutral"])
        c3.metric("Negative", stats["negative"])

        st.dataframe(rows, use_container_width=True)
        st.download_button(
            "Download JSON",
            data=json.dumps(rows, indent=2),
            file_name=f"reddit_scrape_{mode}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
        )


if __name__ == "__main__":
    main()
