"""Scrape r/wallstreetbets ticker mentions + FinBERT sentiment into DuckDB.

Phase 21 of Quantgress, and the second phase (after Phase 14 pageviews) that
is an *attention* signal rather than a disclosure record. Treat it the same
way: logged and queryable, not a strategy. The vault's own testing of exactly
this data -- [[Reddit WSB Sentiment Trading Bot - Oxford Paper Findings]] --
found that every mention- and sentiment-based strategy lost money once a
look-ahead bug was fixed (full-year 2025: -49%). Nothing here feeds
networth.py or any derived signal, and it should not.

Two things this does NOT do, deliberately:
  * No entities.py hookup. The ticker comes straight out of the post text by
    regex, like the bot in that write-up did -- there is no company name to
    resolve, so the resolution engine has nothing to add. What it needs
    instead is a stoplist (see STOPWORDS): the known failure of that bot was
    "tech", "bull" and "meme" matching real ETF tickers.
  * No sentiment column on any other table, and no join into `trades`.

Usage:
    py scrape_wsb.py --selftest              # offline checks, no network, no model
    py scrape_wsb.py                          # last 2 days of /new, scored
    py scrape_wsb.py --days 7 --pages 10      # wider walk
    py scrape_wsb.py --no-sentiment           # mention counts only, skips FinBERT
    py scrape_wsb.py --subreddit stocks

Resumable: every post id actually scored goes in `wsb_posts`, and re-running
adds only unseen posts, folding their counts into the existing (date, ticker)
row rather than replacing it. So a day's totals grow across runs instead of
being overwritten by whatever the last walk happened to see.

Reddit's /new listing only pages back 1000 posts (a hard cap on every listing
endpoint, not a flag this script sets), which on r/wallstreetbets is roughly
two days. A gap longer than that is not recoverable from this endpoint --
run it daily.

Credentials are approval-gated, unlike every other key in this project:
Reddit's Responsible Builder Policy requires explicit approval before any
API access at all. https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy
That same policy bars sharing or commercializing Reddit data, which is why
nothing here stores post text, titles, usernames or served post ids -- only
per-day, per-ticker counts and a mean score. Keep it that way.
"""

import datetime
import os
import re
import sys
import time

import duckdb
import requests

from envfile import load_dotenv
from schema import DB_PATH

TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
API = "https://oauth.reddit.com/r/{sub}/new"
API_UA = "quantgress/0.1 (personal research)"
RATE_LIMIT_SECS = 1.0  # OAuth clients get 100 requests/minute; this stays well under
PAGE_SIZE = 100        # Reddit's own maximum per listing page
DAYS_DEFAULT = 2       # about as far back as /new reaches anyway
PAGES_DEFAULT = 5
MODEL = "ProsusAI/finbert"
MAX_CHARS = 2000       # FinBERT truncates at 512 tokens; trimming first keeps batches small

TABLE = "wsb_mentions"

# A cashtag ($NVDA) is unambiguous; a bare all-caps word is a guess. Case
# matters: matching case-insensitively is what made "tech"/"bull"/"meme" read
# as TECH/BULL/MEME in the write-up's bot.
TICKER_RE = re.compile(r"\$?\b([A-Z]{1,5})\b")

# ponytail: a stoplist, not a lookup against SEC company_tickers.json. The
# valid-symbol check would be more precise and is one entities.py call away,
# but it cuts the *other* way too -- A, IT, SO, ON, ALL, GO, EV and AI are all
# real listed symbols and all overwhelmingly English on this subreddit, so a
# validity check alone would keep exactly the noise this is trying to drop.
# Swap in a validity check *plus* this list if false negatives ever matter more.
STOPWORDS = {
    # forum vocabulary
    "WSB", "DD", "YOLO", "ATH", "FD", "OTM", "ITM", "IV", "PT", "TA", "EOD",
    "EOW", "TLDR", "TL", "DR", "IMO", "IMHO", "AFAIK", "LOL", "LMAO", "WTF",
    "RIP", "GG", "HODL", "FOMO", "MOON", "BULL", "BEAR", "PUT", "PUTS", "CALL",
    "CALLS", "LEAP", "LEAPS", "GAIN", "LOSS", "PORN", "APE", "APES", "STONK",
    "STONKS", "MEME", "SHORT", "LONG", "BUY", "SELL", "HOLD", "PUMP", "DUMP",
    "BAG", "BTFD", "ER", "EPS", "PE", "PEG", "ROI", "YTD",
    # finance/market words that are also symbols
    "CEO", "CFO", "COO", "CTO", "IPO", "ETF", "ETFS", "SPAC", "IRA", "HSA",
    "FED", "FOMC", "CPI", "PPI", "GDP", "SEC", "IRS", "FDA", "DOJ", "FTC",
    "NYSE", "OTC", "AH", "PM", "USD", "EUR", "GBP", "JPY", "CAD", "US", "USA",
    "UK", "EU", "AI", "EV", "TECH",
    # plain English that survives an all-caps filter
    "A", "I", "AN", "AND", "ARE", "AS", "AT", "BE", "BEEN", "BUT", "BY", "CAN",
    "DID", "DO", "DOES", "FOR", "FROM", "GET", "GO", "GOT", "HAS", "HAD",
    "HAVE", "HE", "HER", "HIM", "HIS", "HOW", "IF", "IN", "IS", "IT", "ITS",
    "JUST", "ME", "MY", "NEW", "NO", "NOT", "NOW", "OF", "OK", "ON", "ONE",
    "OR", "OUR", "OUT", "OVER", "SO", "SOME", "THAN", "THAT", "THE", "THEM",
    "THEN", "THEY", "THIS", "TO", "UP", "VERY", "WAS", "WE", "WERE", "WHAT",
    "WHEN", "WHO", "WHY", "WILL", "WITH", "YES", "YET", "YOU", "YOUR",
    "ALL", "ANY", "BIG", "DAY", "END", "EVEN", "HERE", "HIGH", "LIKE", "LOW",
    "MORE", "MOST", "MUCH", "NEXT", "ONLY", "REAL", "RIGHT", "SAME", "SEE",
    "STILL", "SUCH", "SURE", "TIME", "TOO", "WEEK", "WELL", "WHILE", "YEAR",
}


def ensure_tables(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        date VARCHAR, ticker VARCHAR,
        mention_count INTEGER, bullish_count INTEGER, bearish_count INTEGER,
        sentiment_score DOUBLE,
        PRIMARY KEY (date, ticker))""")
    # The resume log. Counts are additive, so the same post must never be
    # counted twice -- this is what makes a re-run safe, not a done-set of
    # (date, ticker) like scrape_pageviews uses, since a day's row is still
    # growing while that day is live.
    con.execute("CREATE TABLE IF NOT EXISTS wsb_posts (post_id VARCHAR PRIMARY KEY)")


def extract_tickers(text):
    """All-caps 1-5 letter tokens, minus STOPWORDS. Returns a set: a post
    that says NVDA six times is one post mentioning NVDA, not six mentions."""
    return {t for t in TICKER_RE.findall(text or "") if t not in STOPWORDS}


# ------------------------------------------------------------------ sentiment

def finbert_classifier():
    """Lazy: importing transformers pulls in torch and, on first call,
    downloads ~440MB of weights. --selftest and --no-sentiment never touch it.

    Returns classify(texts) -> [(label, score)] where score is FinBERT's own
    positive_prob - negative_prob, in [-1, 1]."""
    from transformers import pipeline  # deliberately not module-level

    pipe = pipeline("text-classification", model=MODEL, top_k=None,
                    truncation=True, max_length=512)

    def classify(texts):
        out = []
        for scores in pipe(texts, batch_size=8):
            p = {s["label"].lower(): s["score"] for s in scores}
            label = max(p, key=p.get)
            out.append((label, p.get("positive", 0.0) - p.get("negative", 0.0)))
        return out

    return classify


def neutral_classifier(texts):
    """--no-sentiment: mention counts only. Every post scores neutral/0.0, so
    bullish_count and bearish_count stay 0 and sentiment_score stays 0.0 --
    tell that apart from a genuinely balanced day by those two being 0."""
    return [("neutral", 0.0)] * len(texts)


def aggregate(posts, classify):
    """[post] -> [(date, ticker, mentions, bullish, bearish, mean_score)].

    One classification per post, applied to every ticker in it -- FinBERT
    scores a document, not a mention, so a post hyping NVDA while dunking on
    INTC counts bullish for both. A real limitation of doing it this way; the
    fix is per-sentence scoring, which is more machinery than an attention
    dataset warrants.
    """
    texts = [((p.get("title") or "") + "\n" + (p.get("selftext") or ""))[:MAX_CHARS]
             for p in posts]
    if not texts:
        return []
    agg = {}
    for text, post, (label, score) in zip(texts, posts, classify(texts)):
        date = datetime.datetime.fromtimestamp(
            post["created_utc"], datetime.timezone.utc).date().isoformat()
        for ticker in extract_tickers(text):
            row = agg.setdefault((date, ticker), [0, 0, 0, 0.0])
            row[0] += 1
            row[1] += label == "positive"
            row[2] += label == "negative"
            row[3] += score
    return [(d, t, n, bull, bear, total / n)
            for (d, t), (n, bull, bear, total) in sorted(agg.items())]


# --------------------------------------------------------------------- reddit

def _session():
    """Application-only OAuth2 (client_credentials). A script app is a
    confidential client, which Reddit's OAuth2 docs allow this grant for, so
    no account password is involved. No PRAW either: the whole client here is
    one token POST and one paginated GET, less code than configuring the
    library.

    Anonymous www.reddit.com/*.json is not a fallback -- unauthenticated
    automated access is barred outright, so a keyless run has nowhere to go."""
    load_dotenv()
    cid = os.environ.get("REDDIT_CLIENT_ID")
    secret = os.environ.get("REDDIT_CLIENT_SECRET")
    if not (cid and secret):
        sys.exit("REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set -- Reddit "
                 "rejects unauthenticated automated reads. Create an app "
                 "(type: script) at https://www.reddit.com/prefs/apps, then "
                 "add both to .env, or export them before running. Note that "
                 "API access now needs prior approval under Reddit's "
                 "Responsible Builder Policy -- this is not an instant key "
                 "like the USPTO/FEC/OpenSky ones. --selftest still runs "
                 "without them; --no-sentiment skips FinBERT, not Reddit.")
    tok = requests.post(TOKEN_URL, timeout=30, auth=(cid, secret),
                        headers={"User-Agent": API_UA},
                        data={"grant_type": "client_credentials"})
    tok.raise_for_status()
    s = requests.Session()
    s.headers["User-Agent"] = API_UA
    s.headers["Authorization"] = "Bearer " + tok.json()["access_token"]
    return s


def parse_listing(payload):
    """Reddit listing JSON -> (rows, after cursor). `after` is None on the
    last page, which is also how the ~1000-post cap shows up."""
    data = payload["data"]
    rows = [{"post_id": c["data"]["id"], "title": c["data"].get("title"),
             "selftext": c["data"].get("selftext"),
             "created_utc": c["data"]["created_utc"]}
            for c in data["children"]]
    return rows, data.get("after")


def _get(s, url, params, tries=4):
    for attempt in range(tries):
        time.sleep(RATE_LIMIT_SECS)
        try:
            r = s.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (401, 403):  # bad/expired token, not a blip
            sys.exit(f"Reddit rejected the request ({r.status_code}). Check "
                     "REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET in .env.")
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


def fetch_posts(s, sub, pages, cutoff_ts):
    """Walk /new until `pages` pages, the listing ends, or posts predate
    cutoff_ts. /new is strictly newest-first, so the first old post means
    every later one is older too -- stop there rather than paging to the cap."""
    posts, after = [], None
    for _ in range(pages):
        params = {"limit": PAGE_SIZE}
        if after:
            params["after"] = after
        rows, after = parse_listing(_get(s, API.format(sub=sub), params).json())
        fresh = [p for p in rows if p["created_utc"] >= cutoff_ts]
        posts.extend(fresh)
        if len(fresh) < len(rows) or not after:
            break
    return posts


# ----------------------------------------------------------------------- main

UPSERT = f"""INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT (date, ticker) DO UPDATE SET
        mention_count = {TABLE}.mention_count + excluded.mention_count,
        bullish_count = {TABLE}.bullish_count + excluded.bullish_count,
        bearish_count = {TABLE}.bearish_count + excluded.bearish_count,
        -- mention-weighted mean, so a later run's handful of posts don't
        -- count as much as the whole day's average already stored
        sentiment_score = ({TABLE}.sentiment_score * {TABLE}.mention_count
                           + excluded.sentiment_score * excluded.mention_count)
                          / ({TABLE}.mention_count + excluded.mention_count)"""


def store(con, posts, rows):
    for row in rows:
        con.execute(UPSERT, list(row))
    for p in posts:
        con.execute("INSERT INTO wsb_posts VALUES (?) ON CONFLICT DO NOTHING",
                    [p["post_id"]])


def main(sub, days, pages, classify):
    con = duckdb.connect(DB_PATH)
    try:
        ensure_tables(con)
        seen = {r[0] for r in con.execute("SELECT post_id FROM wsb_posts").fetchall()}
        cutoff = (datetime.datetime.now(datetime.timezone.utc)
                  - datetime.timedelta(days=days)).timestamp()

        fetched = fetch_posts(_session(), sub, pages, cutoff)
        posts = [p for p in fetched if p["post_id"] not in seen]
        print(f"r/{sub}: {len(fetched)} posts in the last {days}d, "
              f"{len(posts)} new since the last run")
        if not posts:
            return
        rows = aggregate(posts, classify)
        store(con, posts, rows)
        total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
        print(f"{len(rows)} (date, ticker) rows written; {total} rows in {TABLE}")
        for d, t, n, bull, bear, score in sorted(rows, key=lambda r: -r[2])[:10]:
            print(f"  {d} {t:<5} {n:>4} mentions  +{bull}/-{bear}  score {score:+.2f}")
    finally:
        con.close()


def selftest():
    assert extract_tickers("$NVDA to the moon, GME too") == {"NVDA", "GME"}
    # the write-up's actual data-quality gotcha: these are real ETF tickers and
    # must not come back from ordinary lowercase prose
    assert extract_tickers("tech stocks are a bull meme") == set()
    # ...and are dropped even shouted, since the caps form is pure noise here
    assert extract_tickers("TECH IS A BULL MEME") == set()
    assert extract_tickers("I think IT and AI are DD gains") == set()
    assert extract_tickers("NVDA NVDA NVDA") == {"NVDA"}  # a set, not a tally
    assert extract_tickers("TOOLONG and $BRK.B") == {"BRK", "B"}  # known: no dotted classes

    posts = [
        {"post_id": "a", "title": "$NVDA earnings look great", "selftext": "",
         "created_utc": 1767225600},  # 2026-01-01 00:00 UTC
        {"post_id": "b", "title": "NVDA and GME both dumping", "selftext": "",
         "created_utc": 1767225601},
    ]
    verdicts = {"a": ("positive", 0.9), "b": ("negative", -0.5)}
    rows = aggregate(posts, lambda ts: [verdicts[p["post_id"]] for p in posts])
    assert {(d, t): r for d, t, *r in rows} == {
        ("2026-01-01", "NVDA"): [2, 1, 1, (0.9 - 0.5) / 2],
        ("2026-01-01", "GME"): [1, 0, 1, -0.5],
    }, rows
    assert aggregate([], None) == []

    # a post on a UTC date boundary lands on the UTC day, not the local one
    edge = aggregate([{"post_id": "c", "title": "MSFT", "selftext": "",
                       "created_utc": 1767225599}], lambda ts: [("neutral", 0.0)])
    assert edge[0][0] == "2025-12-31", edge

    # counts accumulate across runs and sentiment_score stays a mention-weighted
    # mean -- the whole reason this isn't an INSERT OR REPLACE
    con = duckdb.connect(":memory:")
    ensure_tables(con)
    store(con, posts, [("2026-01-01", "NVDA", 2, 1, 1, 0.2)])
    store(con, posts, [("2026-01-01", "NVDA", 3, 3, 0, 0.7)])  # same ids, new counts
    got = con.execute(f"SELECT * FROM {TABLE}").fetchall()
    assert got == [("2026-01-01", "NVDA", 5, 4, 1, (0.2 * 2 + 0.7 * 3) / 5)], got
    assert con.execute("SELECT count(*) FROM wsb_posts").fetchone()[0] == 2
    con.close()

    listing = {"data": {"after": "t3_xyz", "children": [
        {"kind": "t3", "data": {"id": "abc", "title": "GME", "selftext": "yolo",
                                "created_utc": 1767225600}}]}}
    rows, after = parse_listing(listing)
    assert after == "t3_xyz" and rows == [
        {"post_id": "abc", "title": "GME", "selftext": "yolo",
         "created_utc": 1767225600}]

    # the walk stops at the first page containing anything older than the
    # cutoff, instead of paging to Reddit's ~1000-post cap
    def page(ids, created):
        return {"data": {"after": "next", "children": [
            {"kind": "t3", "data": {"id": i, "title": "NVDA", "selftext": "",
                                    "created_utc": created}} for i in ids]}}

    class _Pages:
        seq = [page(["a", "b"], 2000), page(["c"], 1000)]
        calls = 0

        def get(self, *a, **k):
            i = _Pages.calls
            _Pages.calls += 1
            return type("R", (), {"status_code": 200,
                                  "json": staticmethod(lambda: _Pages.seq[i])})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        got = fetch_posts(_Pages(), "wallstreetbets", 5, cutoff_ts=1500)
        assert [p["post_id"] for p in got] == ["a", "b"], got
        assert _Pages.calls == 2  # stopped on the stale page, didn't burn all 5

        class _Flaky:
            calls = 0

            def get(self, *a, **k):
                _Flaky.calls += 1
                if _Flaky.calls < 3:
                    raise requests.exceptions.ConnectionError("simulated blip")
                return type("R", (), {"status_code": 200})()

        assert _get(_Flaky(), "http://example.invalid", {}).status_code == 200
        assert _Flaky.calls == 3
    finally:
        time.sleep = real_sleep

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main(arg("--subreddit", "wallstreetbets"),
             int(arg("--days", DAYS_DEFAULT)),
             int(arg("--pages", PAGES_DEFAULT)),
             neutral_classifier if "--no-sentiment" in sys.argv else finbert_classifier())
