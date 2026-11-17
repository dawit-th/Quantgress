"""Refresh the read-only copy of congress_trades.duckdb that api.py serves.

DuckDB allows one writer per file and no other process can open it while
that writer runs -- not even read-only. Every scraper holds a write
connection for its whole run, so an API reading the live file 500'd on every
request during each scheduled job. The API reads this copy instead; run_job.py
refreshes it after each job group, so it is at most one job old (fine for
sources that update daily at most).

    python snapshot.py             # refresh now (fails if a scraper is running)
    python snapshot.py --selftest  # offline check, temp files only
"""

import os
import shutil
import sys

import duckdb

from schema import DB_PATH

SNAPSHOT_PATH = "congress_trades.read.duckdb"


def refresh(src=DB_PATH, dst=SNAPSHOT_PATH):
    """CHECKPOINT folds the WAL into the main file (and needs the write lock,
    so this raises duckdb.IOException while a scraper is running), then the
    file is copied beside the destination and swapped in with os.replace --
    atomic, so an API request never opens a half-written copy. A request
    already mid-query keeps reading the old file until it closes."""
    con = duckdb.connect(src)
    try:
        con.execute("CHECKPOINT")
    finally:
        con.close()
    base, ext = os.path.splitext(dst)
    tmp = f"{base}.partial{ext}"
    shutil.copyfile(src, tmp)
    # A WAL left beside the old copy would be replayed onto the new one.
    if os.path.exists(dst + ".wal"):
        os.remove(dst + ".wal")
    os.replace(tmp, dst)
    return os.path.getsize(dst)


def selftest():
    import tempfile
    d = tempfile.mkdtemp()
    src, dst = os.path.join(d, "data.duckdb"), os.path.join(d, "data.read.duckdb")

    con = duckdb.connect(src)
    con.execute("CREATE TABLE t AS SELECT 1 AS x")
    con.close()
    refresh(src, dst)
    r = duckdb.connect(dst, read_only=True)
    assert r.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    r.close()

    # new rows reach the copy on the next refresh, even ones still in the WAL
    con = duckdb.connect(src)
    con.execute("INSERT INTO t VALUES (2)")
    con.close()
    refresh(src, dst)
    r = duckdb.connect(dst, read_only=True)
    assert r.execute("SELECT count(*) FROM t").fetchone()[0] == 2
    r.close()
    assert not os.path.exists(os.path.join(d, "data.read.partial.duckdb"))

    # the copy stays readable while the source is locked by a writer
    writer = duckdb.connect(src)
    try:
        r = duckdb.connect(dst, read_only=True)
        assert r.execute("SELECT count(*) FROM t").fetchone()[0] == 2
        r.close()
    finally:
        writer.close()
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        try:
            size = refresh()
        except duckdb.IOException as e:
            sys.exit(f"snapshot not refreshed -- {DB_PATH} is locked by a running job: {e}")
        print(f"{SNAPSHOT_PATH} refreshed ({size / 1e6:.1f} MB)")
