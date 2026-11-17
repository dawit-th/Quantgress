# Deployment cron files

Four `/etc/cron.d` files, one per cadence group (not one per phase — fewer files to install, fewer places to make the `chmod 644` mistake). Install on the target box:

```bash
sudo cp deploy/cron.d/quantgress-* /etc/cron.d/
sudo chmod 644 /etc/cron.d/quantgress-*
mkdir -p /home/ubuntu/quantgress/logs
```

cron silently ignores any `/etc/cron.d` file with group/other write permission — no error anywhere, the job just never runs. `chmod 644` after every copy, not just the first.

Each file is one line per group calling `run_job.py <group>`, which owns the script list (`python run_job.py --list`), runs them back to back, and refreshes the API snapshot after. Logs: `logs/run_job.log` for the summary, `logs/<name>.log` per script.

| File | Phases | Cadence |
|---|---|---|
| `quantgress-daily` | 1-3 + 11 (`daily.py`: Senate, House, tickers, short volume), 9 (`scrape_insiders.py --live`) | daily 06:00, in sequence |
| `quantgress-weekly` | 7 (`scrape_contracts.py`), 12 (`scrape_patents.py`), 17 (`scrape_trump.py`) | weekly, Sunday |
| `quantgress-quarterly` | 10 (`scrape_13f.py`), 6 (`scrape_lobbying.py`) | quarterly (Jan/Apr/Jul/Oct 1st) |
| `quantgress-annual` | 13 (`scrape_donors.py`), 16 (`scrape_execcomp.py`), 18 (`scrape_senate_annual.py`) | annual (Jan 2nd) |
| `quantgress-annual` | 19 (`scrape_house_annual.py`) | annual (Sep 2nd — after the August filing-extension deadline) |

**Phase 21 (`scrape_wsb.py`) is off the schedule until it can run at all** (`requirements-wsb.txt` plus approved Reddit API credentials) — failing every day made the daily job's status meaningless. Once it's added back to `GROUPS["daily"]` in `run_job.py`, it's the one line that genuinely can't be skipped. Reddit's `/new` listing only pages back ~1000 posts, which on r/wallstreetbets is about two days — a longer gap is not recoverable from that endpoint at all, unlike every other phase here, which either re-walks a full index or gap-fills from a dated archive. Put it last in the group: it is also the slowest: FinBERT scores every new post on CPU.

**Phase 17 (`scrape_trump.py`) runs weekly.** ProPublica's DocumentCloud mirror of Trump's OGE 278-T filings has no fixed publication schedule, which was once the reason to leave it manual -- until 7 filings (Apr-Sep 2026) sat unscraped for months. An irregular source is exactly what a cheap poll is for: a week with nothing new is one index request.

**Phase 14 (`scrape_pageviews.py`) has no cron line, deliberately (confirmed live 2026-08-16).** It takes an explicit `--article` watchlist — there's no ticker→Wikipedia-title mapping yet, so an unattended call with no args just fails the same "usage: ..." error every time. It was in `quantgress-weekly` originally; pulled after a live full-scrape run hit exactly that failure. Run it by hand with `--article` until a watchlist exists.

Full reasoning and the "why not just run everything in `daily.py`" tradeoff: see the [Cron for Scheduled Python Scripts](../../../Second-Brain/05%20Patterns/Cron%20for%20Scheduled%20Python%20Scripts.md) wiki page.
