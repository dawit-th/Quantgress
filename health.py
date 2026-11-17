"""Freshness check: newest date and age of every scheduled dataset.

Two silent failures got past every existing check -- short volume dropping
96% of each day's rows, and an insiders run hanging for 17 hours -- because
nothing compared "what's stored" to "what should be there by now". This is
that comparison, in one place for the CLI, api.py's GET /health, and
run_job.py's end-of-job summary.

    python health.py              # table of every dataset, read from the API snapshot
    python health.py --live       # same, against the live DB (fails while a job runs)
    python health.py --selftest   # offline check, temp DB

A dataset is `stale` when its newest date is older than its source's own
cadence allows, `empty`/`missing` when nothing's been scraped at all.
Datasets with no schedule (WSB, flights, pageviews) are
listed as `unscheduled` and never count as stale.
"""

import datetime
import os
import sys

import duckdb

from schema import DB_PATH
from snapshot import SNAPSHOT_PATH

_MDY = "try_strptime({}, '%m/%d/%Y')::DATE"
_ISO = "try_cast(left({}, 10) AS DATE)"

# (dataset, table, SQL for its newest date, max age in days or None if unscheduled).
# Ages follow each source's real cadence plus slack for weekends/holidays/recess:
# trading-day sources 5, congress filings 21 (recess weeks file nothing),
# weekly 14, Trump's irregular 278-Ts 90, quarterly 120, annual 400 (exec comp: fiscal year ends Dec 31,
# filed the following spring, so 550).
CHECKS = [
    ("senate_trades", "trades", "max(filed_date) FILTER (WHERE chamber = 'S')", 21),
    ("house_trades", "trades", "max(filed_date) FILTER (WHERE chamber = 'H')", 21),
    ("short_volume", "short_volume", "max(" + _ISO.format("trade_date") + ")", 5),
    ("insider_trades", "insider_trades", "max(" + _ISO.format("filed_date") + ")", 5),
    ("gov_contracts", "gov_contracts", "max(" + _ISO.format("last_modified_date") + ")", 14),
    ("patents", "patents", "max(" + _ISO.format("grant_date") + ")", 14),
    ("f13_holdings", "f13_holdings", "max(" + _ISO.format("filed_date") + ")", 120),
    ("lobbying", "lobbying_filings", "max(" + _ISO.format("dt_posted") + ")", 120),
    ("donors", "corporate_donations", "max(" + _ISO.format("contribution_date") + ")", 400),
    ("exec_comp", "exec_comp", "max(make_date(fiscal_year, 12, 31))", 550),
    ("senate_annual", "fd_assets", "max(" + _MDY.format("filed_date") + ")", 400),
    ("house_annual", "house_annual_filings", "max(" + _MDY.format("filed") + ")", 400),
    ("wsb_mentions", "wsb_mentions", "max(" + _ISO.format("date") + ")", None),
    ("flights", "flight_scans", "max(" + _ISO.format("day") + ")", None),
    ("pageviews", "pageviews", "max(" + _ISO.format("date") + ")", None),
    # filing date, not trade date: 278-Ts report trades weeks after the fact
    ("trump_trades", "trump_trades_clean", "max(filed_date)", 90),
]


def default_path():
    return SNAPSHOT_PATH if os.path.exists(SNAPSHOT_PATH) else DB_PATH


def check(con, today=None):
    today = today or datetime.date.today()
    have = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables").fetchall()}
    out = []
    for name, table, expr, max_age in CHECKS:
        row = {"dataset": name, "latest": None, "age_days": None, "max_age_days": max_age}
        if table not in have:
            row["status"] = "missing" if max_age is not None else "unscheduled"
            out.append(row)
            continue
        latest = con.execute(f"SELECT {expr} FROM {table}").fetchone()[0]
        if latest is None:
            row["status"] = "empty" if max_age is not None else "unscheduled"
        else:
            row["latest"] = latest.isoformat()
            row["age_days"] = (today - latest).days
            if max_age is None:
                row["status"] = "unscheduled"
            else:
                row["status"] = "ok" if row["age_days"] <= max_age else "stale"
        out.append(row)
    return out


def stale_datasets(path=None):
    """Names of scheduled datasets that aren't ok -- for run_job.py's summary."""
    try:
        con = duckdb.connect(path or default_path(), read_only=True)
    except duckdb.Error as e:
        return [f"(health check couldn't open the DB: {e.__class__.__name__})"]
    try:
        return [r["dataset"] for r in check(con)
                if r["status"] in ("stale", "empty", "missing")]
    finally:
        con.close()


def print_table(rows):
    print(f"{'dataset':<16} {'latest':<11} {'age':>5} {'max':>5}  status")
    for r in rows:
        age = "" if r["age_days"] is None else r["age_days"]
        mx = "" if r["max_age_days"] is None else r["max_age_days"]
        print(f"{r['dataset']:<16} {r['latest'] or '-':<11} {age:>5} {mx:>5}  {r['status']}")


def selftest():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE short_volume (trade_date VARCHAR)")
    con.execute("INSERT INTO short_volume VALUES ('2026-11-10'), ('2026-11-13')")
    con.execute("CREATE TABLE insider_trades (filed_date VARCHAR)")
    con.execute("INSERT INTO insider_trades VALUES ('2026-10-01')")
    con.execute("CREATE TABLE patents (grant_date VARCHAR)")  # scraped table, no rows yet
    con.execute("CREATE TABLE wsb_mentions (date VARCHAR)")
    con.execute("CREATE TABLE fd_assets (filed_date VARCHAR)")
    con.execute("INSERT INTO fd_assets VALUES ('08/13/2026')")
    con.execute("CREATE TABLE lobbying_filings (dt_posted VARCHAR)")
    con.execute("INSERT INTO lobbying_filings VALUES ('2026-08-14T12:49:45-04:00')")
    rows = {r["dataset"]: r for r in check(con, today=datetime.date(2026, 11, 16))}

    assert rows["short_volume"]["latest"] == "2026-11-13"
    assert rows["short_volume"]["age_days"] == 3 and rows["short_volume"]["status"] == "ok"
    assert rows["insider_trades"]["status"] == "stale"       # 46 days > 5
    assert rows["patents"]["status"] == "empty"
    assert rows["gov_contracts"]["status"] == "missing"
    assert rows["wsb_mentions"]["status"] == "unscheduled"   # empty, but not scheduled
    assert rows["senate_annual"]["latest"] == "2026-08-13"   # MM/DD/YYYY parsed
    assert rows["lobbying"]["status"] == "ok"                # ISO timestamp, 94 days <= 120
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        path = DB_PATH if "--live" in sys.argv else default_path()
        con = duckdb.connect(path, read_only=True)
        print(f"freshness of {path}\n")
        print_table(check(con))
        con.close()
