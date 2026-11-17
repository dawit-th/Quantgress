"""Phase 5b: API key gate in front of api.py.

Every route on the `router` in api.py (i.e. everything except POST /signup)
requires a valid, unrevoked key in the X-API-Key header. `signup()` backs
the public self-serve form on the marketing site; `issue_key()` is the
lower-level primitive it (and manual CLI issuance) builds on. Tiers and
Stripe billing are still just plans (03 Concepts/Quantgress API
Monetization.md, Quantgress API Key Portal.md).

    py auth.py issue someone@example.com     # create + print a key (once)
    py auth.py revoke <raw key>               # disable a key
    py auth.py list                           # every issued key + status
    py auth.py --selftest                     # offline check, no server
"""

import hashlib
import os
import secrets
import sys
import time
from datetime import datetime, timezone

import duckdb
from fastapi import Header, HTTPException

from schema import DB_PATH

# Keys live in their own small file, not in congress_trades.duckdb: DuckDB
# allows one writer per file, so while a scraper held the data DB every key
# check (and /signup) failed and api.py couldn't even start. Nothing else
# ever opens this file, so it's never locked for more than one query.
AUTH_DB_PATH = os.environ.get("QG_AUTH_DB", "auth.duckdb")


def ensure_keys_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            key_hash VARCHAR PRIMARY KEY,
            email VARCHAR,
            tier VARCHAR DEFAULT 'free',
            created_at TIMESTAMP,
            revoked_at TIMESTAMP
        )
    """)


def _connect():
    """Every caller opens read-write: DuckDB refuses a read-only and a
    read-write connection to one file at the same moment in one process, so
    a key check overlapping a /signup used to fail. A short retry covers the
    CLI (`auth.py issue`) and the API process touching the file at once --
    each connection lives for a single query."""
    for attempt in range(10):
        try:
            return duckdb.connect(AUTH_DB_PATH)
        except duckdb.IOException:
            if attempt == 9:
                raise
            time.sleep(0.2)


def _migrate_from_data_db(con):
    """One-time copy of keys issued before the split, when api_keys lived in
    the data DB -- otherwise every existing key would stop working. Runs only
    while the auth table is empty; if a scraper holds the data DB right now,
    skip and try again on the next start."""
    if con.execute("SELECT count(*) FROM api_keys").fetchone()[0]:
        return
    if not os.path.exists(DB_PATH):
        return
    try:
        con.execute(f"ATTACH '{DB_PATH}' AS data_db (READ_ONLY)")
    except duckdb.Error as e:
        print(f"auth: couldn't read old keys from {DB_PATH} yet ({e.__class__.__name__}); "
              "will retry next start", file=sys.stderr)
        return
    try:
        has = con.execute("SELECT count(*) FROM information_schema.tables "
                          "WHERE table_catalog = 'data_db' AND table_name = 'api_keys'").fetchone()[0]
        if has:
            con.execute("INSERT INTO api_keys SELECT key_hash, email, tier, created_at, revoked_at "
                        "FROM data_db.api_keys")
    finally:
        con.execute("DETACH data_db")


def init_db():
    """Called once at process start (api.py import) -- idempotent."""
    con = _connect()
    try:
        ensure_keys_table(con)
        _migrate_from_data_db(con)
    finally:
        con.close()


def _hash(raw_key):
    return hashlib.sha256(raw_key.encode()).hexdigest()


def issue_key(email, tier="free"):
    """Returns the raw key. This is the only time it's ever visible --
    only its hash is stored, same as a password."""
    raw = "qg_live_" + secrets.token_urlsafe(32)
    con = _connect()
    try:
        ensure_keys_table(con)
        con.execute(
            "INSERT INTO api_keys VALUES (?, ?, ?, ?, NULL)",
            [_hash(raw), email, tier, datetime.now(timezone.utc)],
        )
    finally:
        con.close()
    return raw


def signup(email):
    """Self-serve signup path (POST /signup in api.py): one active key per
    email. Keys are shown once and never re-displayed, so letting the same
    email mint a second key would just orphan the first with no way back to
    it -- fail instead.
    # ponytail: check-then-insert has a tiny race under a concurrent double
    # -submit of the same email (worst case: two keys for one person,
    # harmless). Add a UNIQUE constraint if that ever actually matters.
    """
    con = _connect()
    try:
        ensure_keys_table(con)
        existing = con.execute(
            "SELECT 1 FROM api_keys WHERE email = ? AND revoked_at IS NULL", [email]
        ).fetchone()
    finally:
        con.close()
    if existing:
        raise ValueError("email already has an active key")
    return issue_key(email)


def revoke_key(raw_key):
    con = _connect()
    try:
        ensure_keys_table(con)
        con.execute(
            "UPDATE api_keys SET revoked_at = ? WHERE key_hash = ?",
            [datetime.now(timezone.utc), _hash(raw_key)],
        )
    finally:
        con.close()


def require_key(x_api_key: str = Header(None)):
    """FastAPI dependency. Wired app-wide in api.py so every route needs a
    key -- add an exemption there (not here) if a public route is ever
    wanted."""
    if not x_api_key:
        raise HTTPException(401, "missing X-API-Key header")
    con = _connect()
    try:
        row = con.execute(
            "SELECT tier, revoked_at FROM api_keys WHERE key_hash = ?",
            [_hash(x_api_key)],
        ).fetchone()
    finally:
        con.close()
    if row is None or row[1] is not None:
        raise HTTPException(401, "invalid or revoked API key")
    return row[0]  # tier -- unused today, there for a future paid-tier check


def selftest():
    # A throwaway file, never the real one -- this used to mint a
    # selftest@example.com key into the live DB on every run.
    import tempfile
    global AUTH_DB_PATH, DB_PATH
    tmp = tempfile.mkdtemp()
    real = AUTH_DB_PATH, DB_PATH
    AUTH_DB_PATH, DB_PATH = os.path.join(tmp, "auth.duckdb"), os.path.join(tmp, "data.duckdb")
    try:
        _selftest()
    finally:
        AUTH_DB_PATH, DB_PATH = real


def _selftest():
    # a key issued before the split (api_keys in the data DB) carries over
    old = "qg_live_issued_before_the_split"
    con = duckdb.connect(DB_PATH)
    ensure_keys_table(con)
    con.execute("INSERT INTO api_keys VALUES (?, 'old@example.com', 'free', now(), NULL)",
                [_hash(old)])
    con.close()
    init_db()
    assert require_key(old) == "free"
    init_db()  # idempotent: a second start doesn't copy the keys again
    con = _connect()
    assert con.execute("SELECT count(*) FROM api_keys").fetchone()[0] == 1
    con.close()

    raw = issue_key("selftest@example.com")
    assert require_key(raw) == "free"
    try:
        require_key("not-a-real-key")
        assert False, "expected 401 on bad key"
    except HTTPException as e:
        assert e.status_code == 401
    revoke_key(raw)
    try:
        require_key(raw)
        assert False, "expected 401 on revoked key"
    except HTTPException as e:
        assert e.status_code == 401
    try:
        require_key(None)
        assert False, "expected 401 on missing key"
    except HTTPException as e:
        assert e.status_code == 401
    print("auth selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif len(sys.argv) > 1 and sys.argv[1] == "issue":
        print(issue_key(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "free"))
    elif len(sys.argv) > 1 and sys.argv[1] == "revoke":
        revoke_key(sys.argv[2])
        print("revoked")
    elif len(sys.argv) > 1 and sys.argv[1] == "list":
        con = _connect()
        for row in con.execute(
            "SELECT email, tier, created_at, revoked_at FROM api_keys ORDER BY created_at"
        ).fetchall():
            print(row)
        con.close()
    else:
        print(__doc__)
