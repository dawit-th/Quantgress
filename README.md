# Quantgress

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![DuckDB](https://img.shields.io/badge/database-DuckDB-fff000.svg)
![FastAPI](https://img.shields.io/badge/api-FastAPI-009688.svg)
![Data](https://img.shields.io/badge/data-public%20domain-brightgreen.svg)

**A self-hosted, open-source alternative to Quiver Quantitative.** Congressional stock trading disclosures plus 17 adjacent public-data feeds (lobbying, insider trades, 13F holdings, short volume, patents, campaign donations, executive pay, net worth, corporate jet movements, retail forum attention, and more), scraped from primary government sources, normalized into a single [DuckDB](https://duckdb.org/) file, and served over a read-only REST API.

Every dataset here is public U.S. government or public-domain disclosure data: SEC, Senate/House ethics offices, FEC, FINRA, USAspending, USPTO, Wikimedia, FAA. Nothing is scraped from a paid source. Two feeds are free but account-gated — OpenSky flight history (Phase 20) and Reddit (Phase 21) — and they are the only non-government dependencies in the project, and the only ones that will not run anonymously.

```
py q.py "SELECT tkr, count(*) AS n, sum(amount_low) AS min_dollars
         FROM trades WHERE tkr IS NOT NULL
         GROUP BY tkr ORDER BY n DESC LIMIT 5"
```

---

## Table of Contents

- [Why Quantgress](#why-quantgress)
- [Datasets](#datasets)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Quick Start](#quick-start)
- [The API](#the-api)
- [Data Model](#data-model)
- [Scheduling](#scheduling)
- [Legal & Compliance](#legal--compliance)
- [Known Limitations](#known-limitations)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why Quantgress

Products like Quiver Quantitative and CapitolTrades package congressional trading disclosures (and related alternative datasets) into a paid API. All of the underlying data is public. Quantgress is the build-it-yourself version: own scrapers against primary sources, one local database, and a thin API layer over it, auditable end to end. Every recovered or inferred value, like a ticker guessed from free text, is kept separate from what was actually scraped, so it stays reversible.

The project was scoped originally against congressional trades (Senate + House Periodic Transaction Reports under the STOCK Act) and grew, phase by phase, into a near-complete rebuild of Quiver's public dataset catalog.

## Datasets

21 build phases, each independently runnable. ✅ = built and verified against live data.

| # | Dataset | Source | Auth | Cadence | Module |
|---|---|---|---|---|---|
| 1 | Senate stock trades (PTRs) | [efdsearch.senate.gov](https://efdsearch.senate.gov/) | session only | daily | `scrape_senate.py` |
| 2 | House stock trades (PTRs) | [disclosures-clerk.house.gov](https://disclosures-clerk.house.gov/) | none | daily | `scrape_house.py` |
| 3 | Ticker resolution (congress trades) | embedded in filing text | — | — | `entities.py` |
| 4 | Daily incremental run | — | — | daily (cron) | `daily.py` |
| 5 | Read-only REST API | — | — | — | `api.py` |
| 6 | Corporate lobbying | [LDA.gov](https://lda.gov/api/) | free key or anon | as needed | `scrape_lobbying.py` |
| 7 | Government contracts | [USAspending v2](https://api.usaspending.gov/) | none | rolling 7-day | `scrape_contracts.py` |
| 8 | Entity resolution engine | SEC `company_tickers.json` | contact-email UA | — | `entities.py` |
| 9 | Insider trades (Form 4) | SEC bulk data sets + EDGAR daily index | none (UA header) | daily gap-fill | `scrape_insiders.py` |
| 10 | 13F institutional holdings | [SEC Form 13F data sets](https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets) | none (UA header) | quarterly | `scrape_13f.py` |
| 11 | Off-exchange short volume | [FINRA daily files](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data) | none | daily | `scrape_short_volume.py` |
| 12 | Patents | [USPTO Open Data Portal](https://data.uspto.gov/) | API key (ID.me) | as needed | `scrape_patents.py` |
| 13 | Corporate/PAC donations | [OpenFEC](https://api.open.fec.gov/) | free API key | as needed | `scrape_donors.py` |
| 14 | Wikipedia pageviews | [Wikimedia REST API](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html) | none | as needed | `scrape_pageviews.py` |
| 15 | Politician net worth (floor estimate) | derived — `trades` × live prices | none | on demand | `networth.py` |
| 16 | Executive compensation (Pay vs. Performance) | [SEC XBRL Frames API](https://www.sec.gov/edgar/sec-api-documentation) | none (UA header) | as needed | `scrape_execcomp.py` |
| 17 | Donald Trump 278-T trades | [ProPublica DocumentCloud mirror](https://projects.propublica.org/trump-team-financial-disclosures/) | none | manual | `scrape_trump.py` |
| 18 | Senate Annual Financial Disclosure (accurate net worth) | efdsearch.senate.gov | session only | annual | `scrape_senate_annual.py` |
| 19 | House Annual Financial Disclosure (accurate net worth) | disclosures-clerk.house.gov | none (UA header) | annual | `scrape_house_annual.py` |
| 20 | Corporate jet registry + flight history | [FAA Releasable Aircraft DB](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download) + [OpenSky](https://opensky-network.org/) | none / OAuth2 client | monthly / daily | `scrape_flights.py` |
| 21 | r/wallstreetbets mentions + sentiment | [Reddit API](https://www.reddit.com/dev/api/) + [FinBERT](https://huggingface.co/ProsusAI/finbert) | OAuth2 client | daily | `scrape_wsb.py` |

Every scraper is independently runnable, resumable (safe to `Ctrl-C` and re-run), and ships with a `--selftest` flag that validates its parsing logic offline against captured sample data — no network call required.

## Architecture

```
Primary sources (SEC, Senate/House, FEC, FINRA, USPTO, Wikimedia, USAspending)
        │
        ▼
  scrape_*.py  ──── one script per dataset, own table, own resume logic
        │
        ▼
  entities.py  ──── cross-dataset ticker/company resolution
        │             (writes *_guess / *_guess_how, never touches scraped values)
        ▼
  congress_trades.duckdb  ──── single-file embedded OLAP database
        │
        ├── daily.py   ──── scheduled incremental driver (Task Scheduler / cron)
        │
        └── api.py     ──── FastAPI read-only layer, one route per dataset
                              │
                              ▼
                        REST clients / dashboards
```

**Design choices that shape the codebase:**

- **DuckDB, not Postgres.** Single-file, zero-server, embedded OLAP engine. Right fit for a personal research dataset that's queried far more than it's written. 
- **One declarative route table, not one function per dataset.** `api.py`'s `RELATIONS` dict maps a route name to `(relation, filter columns, default order)`; a single generic `/{dataset}` handler serves all of them. Adding a dataset means adding a dict entry, not a new function. Same pattern in `entities.py`'s `SOURCES` list.
- **Guesses are always reversible.** Any value `entities.py` infers (e.g. a ticker recovered from free text) is written to a separate `*_guess` / `*_guess_how` column. The original scraped value is never overwritten, and every inferred class of guess can be audited or reversed with one `UPDATE`.
- **Fresh DB connection per API request.** A long-lived read-only DuckDB connection won't see rows written by another process after it connects, so `api.py` opens and closes a connection per request rather than pooling one at startup. This keeps every response consistent with whatever `daily.py` last committed.

## Project Structure

```
Quantgress/
├── schema.py                 # table DDL + the trades view, shared by both chambers
├── scrape_senate.py          # Phase 1 — Senate PTR scraper
├── scrape_house.py           # Phase 2 — House PTR scraper (PDF parsing)
├── entities.py                # Phase 3/8 — cross-dataset ticker/company resolution
├── daily.py                   # Phase 4 — scheduled incremental driver
├── api.py                     # Phase 5 — read-only FastAPI layer
├── scrape_lobbying.py         # Phase 6 — LDA.gov lobbying filings
├── scrape_contracts.py        # Phase 7 — USAspending contract awards
├── scrape_insiders.py         # Phase 9 — SEC Form 4 insider transactions
├── scrape_13f.py              # Phase 10 — SEC 13F institutional holdings
├── scrape_short_volume.py     # Phase 11 — FINRA off-exchange short volume
├── scrape_patents.py          # Phase 12 — USPTO granted patents
├── scrape_donors.py           # Phase 13 — OpenFEC corporate/PAC donations
├── scrape_pageviews.py        # Phase 14 — Wikimedia pageviews
├── networth.py                # Phase 15/18/19 — derived net worth (floor + annual)
├── scrape_execcomp.py         # Phase 16 — SEC Pay vs. Performance (XBRL)
├── scrape_trump.py            # Phase 17 — Trump OGE 278-T trades
├── scrape_senate_annual.py    # Phase 18 — Senate Annual Financial Disclosure
├── scrape_house_annual.py     # Phase 19 — House Annual Financial Disclosure
├── scrape_flights.py          # Phase 20 — FAA aircraft registry + OpenSky flights
├── scrape_wsb.py              # Phase 21 — r/wallstreetbets mentions + FinBERT sentiment
├── q.py                       # query helper (sidesteps PowerShell quoting issues)
├── congress_trades.duckdb     # the database (gitignored in a production checkout)
├── requirements.txt
└── .env                       # USPTO / FEC / OpenSky credentials (gitignored)
```

## Getting Started

### Prerequisites

- Python 3.10+ (Windows: use `py`, not `python`; `python` resolves to the Microsoft Store stub)
- No database server — DuckDB ships as a library dependency

### Installation

```bash
git clone <this-repo>
cd Quantgress
py -m pip install -r requirements.txt
```

### API keys (optional, only needed for Phases 12, 13, 20 & 21)

Every other dataset needs no key — either a fully open endpoint or a descriptive `User-Agent`. Patents (USPTO), corporate donations (OpenFEC), the flight-history half of Phase 20 (OpenSky), and Reddit (Phase 21) each need free credentials. Create a `.env` file in the project root:

```
USPTO_API_KEY=<from data.uspto.gov, requires ID.me verification>
FEC_API_KEY=<from api.data.gov/signup, instant>
OPENSKY_CLIENT_ID=<from opensky-network.org/my-opensky, instant>
OPENSKY_CLIENT_SECRET=<same page>
REDDIT_CLIENT_ID=<from reddit.com/prefs/apps, app type "script" — see the approval note below>
REDDIT_CLIENT_SECRET=<same page>
```

**Reddit credentials are not self-service any more.** Under Reddit's [Responsible Builder Policy](https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy), "you must request access and get explicit approval before accessing any Reddit data through our API." Unlike the USPTO/FEC/OpenSky keys above — all instant — Phase 21 needs a request that a human at Reddit approves, with no committed turnaround. Everything else in this project runs without asking anyone's permission; this one phase does not. `--selftest` and `--no-sentiment` still work regardless.

All four scrapers still run `--selftest` with no key, no network, and (for Phase 21) no model download at all. Phase 20's registry half (`--registry`) needs no credentials either — only `--flights` does, and OpenSky rejects anonymous requests to it outright with a 403. Reddit is the same story: unauthenticated automated reads of `reddit.com/*.json` are blocked, so Phase 21 has no anonymous fallback.

Phase 21's sentiment half also needs `transformers` + `torch` (~2GB installed) for FinBERT, which downloads ~440MB of weights on first run. Both are imported lazily — `py scrape_wsb.py --no-sentiment` collects mention counts without either installed.

## Quick Start

```bash
# 1. Validate every parser offline (instant, no network)
py scrape_senate.py --selftest
py scrape_house.py --selftest

# 2. Bounded live runs to confirm access before a full backfill
py scrape_senate.py --limit 20
py scrape_house.py --year 2026 --limit 25

# 3. Resolve tickers embedded in the free-text asset names
py entities.py --dry     # preview, writes nothing
py entities.py           # write recovered tickers

# 4. Query
py q.py                                  # per-senator summary
py q.py --types                          # asset-type breakdown
py q.py "SELECT * FROM trades LIMIT 10"  # arbitrary SQL

# 5. Serve it (every data route needs a key -- see "Using the API" below)
py -m uvicorn api:app --reload
QG_KEY=$(py auth.py issue you@example.com)
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/trades?tkr=NVDA&limit=5"
```

A full historical backfill of just Senate + House trades is ~1.3 hours against 2,411 filings at a self-imposed 2-second rate limit. Every scraper accepts `--limit N` for a bounded test run. Run that before an unbounded one.

The full command reference (every flag, every gotcha, every verified query) lives in [`08 Reference/Quantgress - Command Reference.md`](../Second-Brain/08%20Reference/Quantgress%20-%20Command%20Reference.md) in the accompanying project wiki.

## The API

`api.py` is a read-only FastAPI layer over the database: one generic route per dataset, driven by a declarative `RELATIONS` table, plus three hand-built routes for the original congressional-trades scope.

```bash
py api.py --selftest                             # offline route checks, no server
py -m uvicorn api:app --reload                    # dev server, http://127.0.0.1:8000
py -m uvicorn api:app --host 0.0.0.0 --port 8000   # LAN-visible, trusted networks only
```

| Endpoint | Description |
|---|---|
| `GET /` | Dataset names and row counts |
| `GET /trades` | The unified congress-trades view. Filters: `tkr`, `last_name`, `chamber` |
| `GET /politician/{name}` | Per-politician summary + trade listing (substring match on last name) |
| `GET /ticker/{symbol}` | All activity for one ticker (exact, case-insensitive) |
| `POST /signup` | Public, no key needed: `{"email": ...}` in, an API key out (5/day per IP, one active key per email) |
| `GET /lobbying` `/contracts` `/insiders` `/13f-positions` `/13f-changes` `/13f-top-holders` `/patents` `/donors` `/pageviews` `/exec-comp` `/trump-trades` `/senate-assets` `/senate-liabilities` `/aircraft` `/corp-flights` `/wsb-mentions` | Generic filtered listing per dataset. `/senate-assets` and `/senate-liabilities` carry both chambers since Phase 19 — filter with `chamber=S` / `chamber=H`. See `RELATIONS` in `api.py` for exact filter columns |

Every listing route accepts `?limit=` (default 100, max 1000) and `?offset=`. Filter columns use one of three match modes chosen per column: exact (codes/IDs/years), case-insensitive exact (tickers), or case-insensitive substring (names). Not a uniform strategy: exact-matching a name column or substring-matching a ticker column both produce wrong results. Interactive docs are auto-generated by FastAPI at `/docs`.

`short_volume` is deliberately **not** served over HTTP (FINRA's terms bar redistributing it) — query it locally with `q.py` instead.

**Auth and limits:** every route except `POST /signup` requires an `X-API-Key` header (missing or revoked key → `401`). Keys are stored only as SHA-256 hashes in the `api_keys` table. Requests are rate-limited to 500/day per key (`429` past that), and CORS only allows the marketing site to call `/signup` from a browser. See [Legal & Compliance](#legal--compliance) before considering a public deployment.

### Using the API

**1. Get a key.** Either mint one locally (prints the key once — save it):

```bash
py auth.py issue you@example.com      # create + print a key
py auth.py list                       # every issued key + status
py auth.py revoke <key>               # disable a key
```

or use the public signup route:

```bash
curl -X POST http://127.0.0.1:8000/signup \
  -H "Content-Type: application/json" -d '{"email": "you@example.com"}'
```

Then keep it in a shell variable so the examples below work as-is:

```bash
export QG_KEY=qg_live_...your-key...
```

**2. Query.** Results are JSON, newest first. Append `| python3 -m json.tool` for readable output.

```bash
# What's in the database (dataset names + row counts)
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/"

# Congress trades: latest NVDA trades, Senate only
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/trades?tkr=NVDA&chamber=S&limit=10"

# One politician: summary + trade list (substring match on last name)
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/politician/pelosi"

# Everything for one ticker across both chambers
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/ticker/AAPL"

# Insider trades (Form 4) for a stock -- trans_code P = open-market buy, S = sale
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/insiders?ticker=AAPL&limit=20"
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/insiders?ticker=PFE&trans_code=P"
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/insiders?owner_name=zuckerberg"

# 13F: who holds a stock, and how positions changed quarter over quarter
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/13f-top-holders?issuer_ticker_guess=MSFT"
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/13f-changes?manager_name=berkshire"

# Lobbying, government contracts, patents by company
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/lobbying?client_ticker_guess=MSFT&filing_year=2026"
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/contracts?recipient_name=lockheed"
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/patents?assignee_ticker_guess=NKE"

# Paging: 50 per page, second page
curl -H "X-API-Key: $QG_KEY" "http://127.0.0.1:8000/insiders?issuer_name=nvidia&limit=50&offset=50"
```

A sample `/insiders` row:

```json
{"filed_date": "2026-11-05", "trans_date": "2026-11-03", "issuer_name": "Apple Inc.",
 "ticker": "AAPL", "owner_name": "Newstead Jennifer", "security_title": "Common Stock",
 "trans_code": "S", "acquired_disposed": "D", "shares": 2399.0,
 "price_per_share": 325.75, "shares_owned_following": 29997.0}
```

From Python:

```python
import requests

r = requests.get("http://127.0.0.1:8000/insiders",
                 params={"ticker": "AAPL", "limit": 20},
                 headers={"X-API-Key": "qg_live_...your-key..."})
r.raise_for_status()
for t in r.json():
    print(t["trans_date"], t["owner_name"], t["trans_code"], t["shares"], t["price_per_share"])
```

Every filter name per route is listed in `RELATIONS` in `api.py`, and the interactive docs at `http://127.0.0.1:8000/docs` let you try each route in the browser (paste your key into the `x-api-key` header field on each route).

## Data Model

Query the **`trades`** view, not the raw `senate_trades` / `house_trades` tables. It unions both chambers and resolves three bugs every raw query would otherwise hit:

| Column | What it fixes |
|---|---|
| `chamber` | `'S'` / `'H'`, union of both raw tables |
| `tkr` | `coalesce(ticker, ticker_guess)`; includes tickers recovered from free text |
| `txn_date` / `filed_date` | real `DATE` columns, not `MM/DD/YYYY` text that sorts lexically wrong |
| `tkr_recovered` | flags rows whose ticker came from inference, not the filing itself |

**Two dates matter differently:** `txn_date` is when the trade happened; `filed_date` is when the public could first have known about it. Any signal analysis should key off `filed_date`. Building on `txn_date` bakes in look-ahead bias.

**`amount_low` / `amount_high` are a disclosure bracket, not a price.** The STOCK Act only requires a dollar range, never an exact figure. `sum(amount_low)` is a floor, never a total.

Every non-congress dataset (lobbying, contracts, insider trades, 13F, short volume, patents, donors, pageviews, exec comp, Trump trades, annual disclosures, aircraft/flights, WSB mentions) gets its own standalone table — none of them share `trades`' chamber/ticker shape, so they're not unioned into it.

**Two of those tables are attention data, not disclosure data:** `pageviews` (Phase 14) and `wsb_mentions` (Phase 21). Nobody filed them, they record what people looked at or talked about, and neither is wired into `networth.py` or any other derived signal — deliberately. This project's own testing of `wsb_mentions`-shaped data found no edge in it (see [Known Limitations](#known-limitations)). Query them as research datasets, not as strategies.

## Scheduling

`daily.py` is the incremental driver: Senate (full history, but only new filings actually write), House (current + prior calendar year, to catch year-boundary filings), then `entities.py` to resolve tickers on whatever was just added. Every underlying scraper already resumes from what's stored, so a normal day only touches new filings. `daily.py` adds no new scraping logic of its own, only call order.

```bash
py daily.py --selftest    # offline check, instant
py daily.py                # run it once
```

Scheduled with the OS's native scheduler, not a long-running Python process.

**macOS (launchd)** — same jobs and times as `deploy/cron.d/`, installed as per-user LaunchAgents:

```bash
sh deploy/launchd/install.sh              # write + load the 5 jobs into ~/Library/LaunchAgents
sh deploy/launchd/install.sh --status     # loaded jobs + last exit code
launchctl kickstart gui/$(id -u)/com.quantgress.daily   # run one now
sh deploy/launchd/install.sh --uninstall  # remove them
```

| Job | When (local time) | Runs, in order |
|---|---|---|
| `com.quantgress.daily` | every day 06:00 | `daily.py`, `scrape_short_volume.py`, `scrape_insiders.py --live`, `scrape_wsb.py` |
| `com.quantgress.weekly` | Sunday 07:00 | `scrape_contracts.py`, `scrape_patents.py` |
| `com.quantgress.quarterly` | Jan/Apr/Jul/Oct 1st 08:00 | `scrape_13f.py`, `scrape_lobbying.py` |
| `com.quantgress.annual-jan` | Jan 2nd 09:00 | `scrape_donors.py`, `scrape_execcomp.py`, `scrape_senate_annual.py` |
| `com.quantgress.annual-sep` | Sep 2nd 09:00 | `scrape_house_annual.py` |

Each script logs to `logs/<name>.log` (same names as cron). Unlike the cron files, a cadence group runs its scripts **back to back in one job** rather than staggered 15 minutes apart: DuckDB allows one writer at a time, so a staggered job that starts while a long `daily.py` is still running dies on the file lock. A run time that passes while the Mac is asleep fires once on wake; a powered-off Mac skips it. Paths are taken from wherever the repo lives, so the plists aren't committed — the script generates them.

**Windows (Task Scheduler):**

```
schtasks /create /tn "Quantgress Daily" /sc daily /st 09:00 ^
  /tr "cmd /c cd /d C:\path\to\Quantgress && py daily.py >> daily.log 2>&1" /f
```

STOCK Act disclosures have a 30–45 day filing window, so a daily cadence is comfortably fast enough; a missed day is invisible and self-heals on the next run.

**Beyond `daily.py`:** only Phases 1-3 run on the schedule above. The other 13 phases (lobbying, contracts, insider trades, 13F, short volume, patents, donors, pageviews, exec comp, Trump trades, annual disclosures, aircraft/flights, WSB mentions) are manual-only today (`py scrape_*.py`) — a real gap once any of that data is served publicly, since a paid tier can't silently go stale. `deploy/cron.d/` has the Linux deployment schedule for the Oracle Cloud box (and `deploy/launchd/` the same schedule for a Mac, above): one `/etc/cron.d` file per cadence group (daily/weekly/quarterly/annual, grouped by each source's actual upstream update frequency, not run nightly regardless) — see [`deploy/cron.d/README.md`](deploy/cron.d/README.md) for the install steps and the full phase-to-cadence mapping. Trump trades (Phase 17) is the one deliberate exception, left manual since ProPublica's mirror has no fixed publication schedule to key a cron line off of.

## Legal & Compliance

Every dataset scraped here is public government or public-domain disclosure data, legal to collect and use for personal/research purposes. **Redistributing it commercially is a separate question with real constraints, surfaced while scoping a paid public API for this project:**

- **Corporate/PAC donations (`corporate_donations`)** — 52 U.S.C. § 30111(a)(4) bars commercial use or solicitation of individual FEC contributor data. `api.py` only ever serves `corporate_donations_agg`, a pre-aggregated view with `contributor_name` and `sub_id` stripped out entirely — no individual donor is identifiable through the API.
- **Off-exchange short volume (`short_volume`)** — FINRA's site-wide Terms of Use bar bulk scraping and commercial redistribution; its own API terms bar building a competing product with the data. Currently unresolved whether the daily short-volume catalog's "public dissemination" framing carries a looser license. **Treat this dataset as personal/research use only until confirmed otherwise directly with FINRA.**
- **Corporate jet flights (`flights`)** — the FAA registry half is public domain like everything else, but OpenSky is a volunteer-fed community network, not a government source, and its data is free for **non-commercial and research use only**. `api.py` exposes the `corp_flights` view rather than the raw `flights` table, so a response always carries the owner a movement is attributable to instead of being a redistributable dump of OpenSky's feed. If the paid API tiers ever ship, this route needs the same scrutiny the FINRA and FEC ones got.
- **WSB mentions (`wsb_mentions`)** — the most restricted source in the project, and the only one whose *access* is gated on someone's approval rather than just its use. Reddit's [Responsible Builder Policy](https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy) requires prior approval for any API access at all, and separately bars selling, licensing, sharing, or otherwise commercializing Reddit data without express written approval — explicitly including non-commercial scraping and any use for AI/ML training. Phase 21 is built to sit inside that: no post text, titles, usernames, or post ids are stored in any served table, only a daily per-ticker count and a mean sentiment score, and FinBERT scores locally so nothing is sent anywhere or used to train anything. **The unresolved part is `/wsb-mentions` itself.** Whether serving a derived per-ticker aggregate counts as "sharing Reddit data" is the same open question `short_volume` has with FINRA, and it has not been answered — the difference is that `short_volume`'s public route was cut and this one, per its build spec, was not. Treat `/wsb-mentions` as local/personal like the rest of the API as shipped, and settle this with Reddit before it is ever served publicly or monetized.
- Every other dataset (SEC EDGAR, USAspending, USPTO, LDA.gov, FAA, Senate/House disclosures, Wikimedia) is unrestricted public-domain U.S. government data.

None of the above blocks personal use, local research, or the API as shipped (unauthenticated, `127.0.0.1`-only). It matters only if this is ever exposed publicly or monetized.

## Known Limitations

- **House OCR is not built.** Scanned (image-only) PTR filings are queued (`house_filings.status = 'scanned'`) rather than parsed — roughly an eighth of recent filings, more in older years. `py scrape_house.py --ocr-queue` lists what's waiting.
- **Net worth figures are floor estimates, not real holdings.** Disclosures only ever give a dollar bracket, never a share count — `networth.py`'s default mode sums bracket floors, marked to live prices. `networth.py --annual` (Phase 18, Senate only) is materially more accurate: real Dec-31 asset/liability snapshots from Annual Financial Disclosure Reports, not inferred from trade brackets.
- **Entity resolution coverage is genuinely low for four datasets** (lobbying, contracts, patents, donors) — 2–7% of distinct names match a public ticker on the first pass, by design: most lobbying clients, contract recipients, patent assignees, and donors are private companies, government bodies, or associations with no ticker to have.
- **No `con.close()` in every scraper** (in progress). A completed run's data can sit WAL-only until the connection is cleanly closed. Don't delete a `.duckdb.wal` file without checking whether it holds unflushed data first.
- **Corporate jet ownership is deliberately obscured, and the registry is the weaker half of Phase 20.** Of 24,662 company-registered jets, the single largest registrant is `BANK OF UTAH TRUSTEE` (1,293 airframes), followed by two more ownership-anonymizing trusts — a jet held through a trust or a shell LLC has no public link back to the operating company, so `entities.py` resolves only ~4,900 airframes across 108 names to a ticker. Separately, [Section 803 of the FAA Reauthorization Act of 2024](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/withhold_ownership_data) lets owners request their details be withheld; it covers the PII of **individual** owners, which Phase 20's corporate-registrant filter already excludes, so it should not erode this table directly — but watch it.
- **WSB mentions are an attention dataset with a known-negative track record, and the noisiest table here.** Three separate problems, none fully solved: (1) tickers come from a regex over post text, so an all-caps English word that is also a listed symbol reads as a mention — the stoplist in `scrape_wsb.py` catches the known offenders (`TECH`, `BULL`, `MEME`, `IT`, `AI`, `EV`), at the cost of dropping genuine mentions of those symbols entirely; (2) FinBERT scores a whole post, not a mention, so a post that is bullish on one ticker and bearish on another counts bullish for both; (3) FinBERT is trained on Reuters newswire and the Financial PhraseBank, not forum slang or sarcasm, and benchmarks at 69% on hand-labeled 3-class WSB text. Above all: a full-year 2025 backtest of trading on exactly this signal returned **-49%** once a look-ahead bug was fixed. It is logged because it is interesting, not because it works.
- **Trump 278-T data is OCR'd from scanned forms**, not born-digital text. Query `trump_trades_clean`, not the raw `trump_trades` table, unless specifically auditing the parse. The clean view filters out rows with detectable OCR corruption.

## Roadmap

Not currently planned: Twitter/X data (API now paid). WSB/Reddit sentiment shipped as Phase 21 — as a research dataset, not a signal: it is queryable alongside everything else and is wired into nothing, because the strategy built on it lost half its money in backtest (see Known Limitations). Corporate jet flight tracking shipped as Phase 20 — the "FAA coverage shrinking" objection that had it shelved turned out to be aimed at the wrong half: Section 803 withholding applies to individual owners, not the corporate registrants this phase filters for. The real coverage ceiling is trust/LLC ownership opacity, which predates it (see Known Limitations). Full phase-by-phase implementation notes, live-run numbers, and every gotcha found along the way live in the project wiki.

## License

MIT — see [LICENSE](LICENSE).
