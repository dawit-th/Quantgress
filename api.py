"""Phase 5: read-only FastAPI layer over congress_trades.duckdb.

One route per dataset (`RELATIONS` below) instead of one function per table --
same "declarative list, one engine" shape as entities.py's SOURCES, since
every dataset from Phase 6 on is the identical list/filter/paginate query
with different column names.

Every route opens its own read-only connection and closes it before
returning. Not just Phase 10's con.close() hygiene carried over: a
persistent connection opened once at server startup would keep querying
against the snapshot that existed at startup and never see rows daily.py
adds later, since a read-only DuckDB connection doesn't auto-refresh.
Opening fresh per request means each request sees whatever was last
committed.

    py -m uvicorn api:app --reload                      # dev, port 8000
    py -m uvicorn api:app --host 0.0.0.0 --port 8000     # bind everyone
    py api.py --selftest                                 # offline route checks

Endpoints:
    GET /                     dataset names + row counts
    GET /trades               the trades view
    GET /politician/{name}    one politician's trades + summary (ILIKE on last_name)
    GET /ticker/{symbol}      one ticker's trades
    GET /{dataset}            generic listing -- see RELATIONS keys below for
                               every other table/view and its filter columns
    POST /signup              public, no key needed -- {"email": ...} in, a
                               real API key out (5/day per IP, one active key
                               per email). Everything else above requires
                               X-API-Key.

Every dataset route takes ?limit=&offset=, plus whatever filter columns are
listed for it in RELATIONS. eq_ci filters (tickers/symbols) are
case-insensitive exact match; ilike filters are case-insensitive substring;
eq filters are exact (digit strings are coerced to int for the integer
columns -- filing_year, cycle, fiscal_year, cik).
"""

import os
from datetime import datetime

import duckdb
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from auth import init_db, require_key, revoke_key
from auth import signup as auth_signup
import health
import run_job
from schema import DB_PATH
from snapshot import SNAPSHOT_PATH

LOCAL_HOSTS = {"127.0.0.1", "::1", "localhost"}
UI_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "ui.html")

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
# ponytail: one limit for everyone, no tier differentiation -- auth.py already
# stores a `tier` column for when Quantgress API Monetization's paid tiers
# and Stripe billing actually exist; revisit this constant then.
RATE_LIMIT = "500/day"
SIGNUP_RATE_LIMIT = "5/day"  # stricter -- public, unauthenticated, mints a real key
MARKETING_ORIGIN = "https://quantgress.dhruvmulajkar.me"  # only origin allowed to call /signup from a browser


def _rate_limit_key(request: Request) -> str:
    """Rate-limit by API key, not IP -- an office/NAT of legitimate users
    shouldn't share one bucket, and a key is the unit a future tier applies
    to. Falls back to IP for the pre-auth case, so a request with no/bad key
    is still capped before it ever reaches auth.require_key's 401 --
    otherwise key-guessing has no rate limit of its own."""
    return request.headers.get("x-api-key") or get_remote_address(request)


init_db()  # idempotent -- creates api_keys table if missing
limiter = Limiter(key_func=_rate_limit_key, default_limits=[RATE_LIMIT], headers_enabled=True)
app = FastAPI(title="Quantgress API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(
    CORSMiddleware, allow_origins=[MARKETING_ORIGIN], allow_methods=["POST"], allow_headers=["Content-Type"],
)

# Every data route requires a key (Depends(require_key) below); POST /signup
# is the one public exception, so it lives on the bare `app`, not this
# router -- FastAPI's app-level `dependencies=` has no per-route opt-out.
router = APIRouter(dependencies=[Depends(require_key)])

# name -> (relation, [(column, mode)], default ORDER BY)
# mode: "eq" exact, "eq_ci" case-insensitive exact (tickers/symbols/codes
# with inconsistent casing across sources), "ilike" substring match.
RELATIONS = {
    "trades": ("trades", [
        ("chamber", "eq"), ("tkr", "eq_ci"), ("last_name", "ilike"),
        ("tx_type", "eq_ci"), ("asset_type", "eq_ci"), ("date_suspect", "eq"),
    ], "txn_date DESC"),
    "lobbying": ("lobbying_filings", [
        ("client_name", "ilike"), ("registrant_name", "ilike"),
        ("client_ticker_guess", "eq_ci"), ("filing_year", "eq"),
    ], "dt_posted DESC"),
    "contracts": ("gov_contracts", [
        ("recipient_name", "ilike"), ("awarding_agency", "ilike"),
        ("recipient_ticker_guess", "eq_ci"),
    ], "last_modified_date DESC"),
    "insiders": ("insider_trades", [
        ("ticker", "eq_ci"), ("issuer_name", "ilike"),
        ("owner_name", "ilike"), ("trans_code", "eq"),
    ], "trans_date DESC"),
    "13f-positions": ("f13_positions", [
        ("cusip", "eq"), ("manager_name", "ilike"), ("issuer_name", "ilike"),
        ("issuer_ticker_guess", "eq_ci"), ("period_of_report", "eq"),
    ], "period_of_report DESC"),
    "13f-changes": ("f13_changes", [
        ("cusip", "eq"), ("manager_name", "ilike"),
        ("issuer_ticker_guess", "eq_ci"), ("change_type", "eq"),
    ], "period_of_report DESC"),
    "13f-top-holders": ("f13_top_holders", [
        ("cusip", "eq"), ("issuer_ticker_guess", "eq_ci"),
        ("period_of_report", "eq"),
    ], "rank ASC"),
    # short_volume deliberately not exposed here -- FINRA API Terms of
    # Service Sec 3.3(a)/(e) bar redistributing Licensed Materials to
    # non-Authorized Users or "bulk distributor" use, which a public API
    # matches directly. See 03 Concepts/Quantgress API Monetization.md's
    # Open Questions. scrape_short_volume.py / the short_volume table stay
    # for personal querying -- only the public HTTP route is cut.
    "patents": ("patents", [
        ("assignee_name", "ilike"), ("invention_title", "ilike"),
        ("assignee_ticker_guess", "eq_ci"),
    ], "grant_date DESC"),
    # corporate_donations_agg, not the raw table: ticker/committee/cycle
    # totals only, never a contributor_name or sub_id row -- 52 U.S.C.
    # Sec 30111(a)(4) bars commercial use of raw FEC contributor info, see
    # scrape_donors.py's ensure_agg_view and 03 Concepts/Quantgress API
    # Monetization.md.
    "donors": ("corporate_donations_agg", [
        ("committee_name", "ilike"), ("contributor_ticker_guess", "eq_ci"),
        ("cycle", "eq"),
    ], "total_amount DESC"),
    "pageviews": ("pageviews", [
        ("article", "ilike"), ("date", "eq"),
    ], "date DESC"),
    "exec-comp": ("exec_comp", [
        ("ticker", "eq_ci"), ("company", "ilike"),
        ("fiscal_year", "eq"), ("cik", "eq"),
    ], "fiscal_year DESC"),
    "trump-trades": ("trump_trades_clean", [
        ("tx_type", "eq_ci"), ("asset_class", "eq_ci"), ("description", "ilike"),
    ], "txn_date DESC"),
    # both chambers since Phase 19 -- filter with chamber=S / chamber=H
    "senate-assets": ("fd_assets", [
        ("chamber", "eq"), ("last_name", "ilike"), ("asset_name", "ilike"),
        ("ticker", "eq_ci"), ("filing_year", "eq"),
    ], "filed_date DESC"),
    "senate-liabilities": ("fd_liabilities", [
        ("chamber", "eq"), ("last_name", "ilike"), ("creditor", "ilike"),
        ("filing_year", "eq"),
    ], "filed_date DESC"),
    # Phase 20. `aircraft` is the FAA registry half (public domain, safe to
    # serve). `corp-flights` is the view, not the raw `flights` table, so a
    # response always carries the owner the flight is attributable to -- a bare
    # icao24/timestamp list is both useless to a caller and the shape closest to
    # redistributing OpenSky's feed. OpenSky is free for non-commercial use
    # only; if Quantgress API Monetization's paid tiers ever ship, this route
    # needs the same look the FINRA and FEC ones got.
    "aircraft": ("aircraft", [
        ("owner_name", "ilike"), ("n_number", "eq_ci"), ("icao24", "eq_ci"),
        ("state", "eq_ci"), ("owner_ticker_guess", "eq_ci"),
    ], "year_mfr DESC"),
    "corp-flights": ("corp_flights", [
        ("owner_name", "ilike"), ("tkr", "eq_ci"), ("n_number", "eq_ci"),
        ("dep_airport", "eq_ci"), ("arr_airport", "eq_ci"),
    ], "departed DESC"),
    # Phase 21. An attention dataset like `pageviews`, served the same way and
    # with the same caveat: it is not a signal, and this project's own
    # backtesting of exactly this data lost money. Reddit content is never
    # stored or served -- `wsb_mentions` holds daily per-ticker counts only,
    # no post text, no usernames, no post ids (those stay in `wsb_posts`,
    # which is an internal dedupe log and deliberately not routed).
    "wsb-mentions": ("wsb_mentions", [
        ("ticker", "eq_ci"), ("date", "eq"),
    ], "date DESC, mention_count DESC"),
}


def data_db_path():
    """Which file requests read, decided per request rather than at import,
    so a snapshot that first appears after the server started gets picked
    up: QG_API_DB if set, else snapshot.py's read-only copy (never locked by
    a scraper), else the live file (e.g. before the first snapshot)."""
    if os.environ.get("QG_API_DB"):
        return os.environ["QG_API_DB"]
    return SNAPSHOT_PATH if os.path.exists(SNAPSHOT_PATH) else DB_PATH


def _connect():
    return duckdb.connect(data_db_path(), read_only=True)


def _rows(con, sql, params):
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _coerce(val):
    """Digit-only query strings become int, so eq filters on INTEGER/BIGINT
    columns (filing_year, cycle, fiscal_year, cik) don't need a cast in SQL;
    true/false become bool for BOOLEAN columns (trades.date_suspect)."""
    if val.lower() in ("true", "false"):
        return val.lower() == "true"
    return int(val) if val.isdigit() else val


def _build_where(filters, query_params):
    where, params = [], []
    for col, mode in filters:
        val = query_params.get(col)
        if val is None or val == "":
            continue
        if mode == "eq":
            where.append(f"{col} = ?")
            params.append(_coerce(val))
        elif mode == "eq_ci":
            where.append(f"upper({col}) = upper(?)")
            params.append(val)
        else:  # ilike
            where.append(f"{col} ILIKE ?")
            params.append(f"%{val}%")
    return where, params


def _limit_offset(query_params):
    limit = min(int(query_params.get("limit", DEFAULT_LIMIT)), MAX_LIMIT)
    offset = max(int(query_params.get("offset", 0)), 0)
    return limit, offset


def list_dataset(dataset, query_params):
    """The one function behind every /{dataset} route -- build + run a
    filtered, paginated SELECT against RELATIONS[dataset]."""
    if dataset not in RELATIONS:
        raise HTTPException(404, f"unknown dataset {dataset!r} -- see GET / for the list")
    rel, filters, order = RELATIONS[dataset]
    where, params = _build_where(filters, query_params)
    limit, offset = _limit_offset(query_params)
    sql = f"SELECT * FROM {rel}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {order} LIMIT ? OFFSET ?"
    params = params + [limit, offset]
    con = _connect()
    try:
        return _rows(con, sql, params)
    finally:
        con.close()


@router.get("/")
def root():
    con = _connect()
    try:
        counts = {name: con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
                  for name, (rel, _, _) in RELATIONS.items()}
        return {"datasets": counts,
                # filter columns per dataset, so a client (ui.html) can build
                # its query form from RELATIONS instead of a copy of it
                "filters": {name: [{"column": col, "match": mode} for col, mode in filters]
                            for name, (_, filters, _) in RELATIONS.items()},
                "usage": "GET /{dataset}?<filter col>=<value>&limit=100&offset=0",
                "named_routes": ["/trades", "/politician/{name}", "/ticker/{symbol}"]}
    finally:
        con.close()


@router.get("/politician/{name}")
def politician(name: str, limit: int = DEFAULT_LIMIT):
    con = _connect()
    try:
        summary = _rows(con, """
            SELECT chamber, last_name, count(*) AS txns, count(DISTINCT tkr) AS tickers,
                   min(txn_date) AS first_trade, max(txn_date) AS last_trade
            FROM trades WHERE last_name ILIKE ? GROUP BY chamber, last_name
        """, [f"%{name}%"])
        if not summary:
            raise HTTPException(404, f"no trades found for {name!r}")
        listing = _rows(con, """
            SELECT chamber, last_name, tkr, asset_name, tx_type, amount_low, amount_high,
                   txn_date, filed_date
            FROM trades WHERE last_name ILIKE ? ORDER BY txn_date DESC LIMIT ?
        """, [f"%{name}%", min(limit, MAX_LIMIT)])
        return {"summary": summary, "trades": listing}
    finally:
        con.close()


@router.get("/ticker/{symbol}")
def ticker(symbol: str, limit: int = DEFAULT_LIMIT):
    con = _connect()
    try:
        rows = _rows(con, """
            SELECT chamber, last_name, tkr, asset_name, tx_type, amount_low, amount_high,
                   txn_date, filed_date
            FROM trades WHERE upper(tkr) = upper(?) ORDER BY txn_date DESC LIMIT ?
        """, [symbol, min(limit, MAX_LIMIT)])
        if not rows:
            raise HTTPException(404, f"no trades found for ticker {symbol!r}")
        return rows
    finally:
        con.close()


# Manual job runs (the UI's "Run now"). Registered before /{dataset}, whose
# catch-all would otherwise swallow GET /jobs.
@router.get("/jobs")
def jobs_route():
    """Every schedule group: when it runs, its scripts, and its last/current
    run's status (see run_job.read_state)."""
    return [{"group": g, "schedule": run_job.SCHEDULE[g],
             "scripts": [" ".join(c) for c in cmds], **run_job.read_state(g)}
            for g, cmds in run_job.GROUPS.items()]


@router.post("/jobs/{group}", status_code=202)
def start_job_route(group: str, request: Request):
    """Start a group now, exactly as the scheduler would (same lock, logs and
    snapshot refresh). Needs a key AND a request from this machine: /signup
    hands keys to anyone, and a key shouldn't let a stranger kick off a
    40-minute scrape on your box."""
    if group not in run_job.GROUPS:
        raise HTTPException(404, f"unknown job {group!r} -- one of {', '.join(run_job.GROUPS)}")
    if request.client is None or request.client.host not in LOCAL_HOSTS:
        raise HTTPException(403, "jobs can only be started from the machine the API runs on")
    state = run_job.read_state(group)
    if state["active"]:
        raise HTTPException(409, f"{group} is already {state['status']}")
    return {"group": group, "status": "queued", "pid": run_job.start_detached(group)}


@router.get("/{dataset}")
def dataset_route(dataset: str, request: Request):
    return list_dataset(dataset, dict(request.query_params))


# Public routes, registered before the router: its catch-all /{dataset}
# would otherwise match /health and /ui and demand a key.
@app.get("/health")
def health_route(response: Response):
    """Freshness of every scheduled dataset (see health.py). Public: it
    exposes dates and ages only, so a monitor can poll it without a key.
    "degraded" means at least one scheduled dataset is stale or empty."""
    con = _connect()
    try:
        rows = health.check(con)
    finally:
        con.close()
    bad = [r["dataset"] for r in rows if r["status"] in ("stale", "empty", "missing")]
    return {"status": "degraded" if bad else "ok", "not_ok": bad,
            "serving": os.path.basename(data_db_path()),
            "snapshot_taken": (datetime.fromtimestamp(os.path.getmtime(SNAPSHOT_PATH)).isoformat(timespec="seconds")
                               if os.path.exists(SNAPSHOT_PATH) else None),
            "datasets": rows}


@app.get("/ui", include_in_schema=False)
def ui_route():
    """The browser UI -- a single static page that calls this same API with
    the key the user pastes in (kept in their browser's localStorage)."""
    return FileResponse(UI_PATH, media_type="text/html")


app.include_router(router)


class SignupRequest(BaseModel):
    email: str


@app.post("/signup")
@limiter.limit(SIGNUP_RATE_LIMIT)
def signup_route(request: Request, response: Response, body: SignupRequest):
    """Public, unauthenticated -- the marketing site's key-signup form posts
    here. Issues a real key in the same api_keys table require_key checks,
    so there's exactly one source of truth for what's valid."""
    email = body.email.strip().lower()
    if "@" not in email or "." not in email.rsplit("@", 1)[-1]:
        raise HTTPException(400, "invalid email")
    try:
        key = auth_signup(email)
    except ValueError:
        raise HTTPException(409, "that email already has an active key -- keys aren't re-shown, contact support if lost")
    return {"api_key": key}


def selftest():
    """Offline route checks against the real DB file, no server process --
    fails loudly if a RELATIONS entry names a column/relation that doesn't
    exist, or if the routing/filter/pagination logic breaks."""
    import tempfile

    import auth
    from fastapi.testclient import TestClient
    from auth import issue_key

    # keys go to a throwaway auth DB, not the real one
    auth.AUTH_DB_PATH = os.path.join(tempfile.mkdtemp(), "auth.duckdb")
    auth.init_db()

    c = TestClient(app, headers={"X-API-Key": issue_key("api-selftest@example.com")})

    assert c.get("/", headers={"X-API-Key": ""}).status_code == 401  # gate is on

    r = c.get("/")
    assert r.status_code == 200, r.text
    assert set(r.json()["datasets"]) == set(RELATIONS)

    # every RELATIONS entry: relation resolves, default limit applies, and
    # its first eq_ci/ilike filter column doesn't error even with no matches
    for name, (rel, filters, order) in RELATIONS.items():
        r = c.get(f"/{name}")
        assert r.status_code == 200, f"{name}: {r.text}"
        assert len(r.json()) <= DEFAULT_LIMIT, name
        r = c.get(f"/{name}?limit=2")
        assert r.status_code == 200 and len(r.json()) <= 2, name
        if filters:
            col, _mode = filters[0]
            r = c.get(f"/{name}?{col}=zzz_no_such_value_zzz")
            assert r.status_code == 200 and r.json() == [], f"{name}.{col}: {r.text}"

    assert c.get("/not-a-real-dataset").status_code == 404

    # /jobs lists every group; starting one is refused off-machine (the test
    # client's host is "testclient") and for an unknown group -- neither
    # case starts anything
    r = c.get("/jobs")
    assert r.status_code == 200 and {j["group"] for j in r.json()} == set(run_job.GROUPS), r.text
    assert c.post("/jobs/daily").status_code == 403
    assert c.post("/jobs/not-a-job").status_code == 404
    assert TestClient(app).post("/jobs/daily").status_code == 401  # and it needs a key

    r = c.get("/politician/armstrong")
    assert r.status_code == 200, r.text
    assert r.json()["summary"], "expected at least one chamber/last_name group"
    assert c.get("/politician/zzz_no_such_member_zzz").status_code == 404

    r = c.get("/trades?tkr=aapl&limit=5")
    assert r.status_code == 200
    assert all(row["tkr"] == "AAPL" for row in r.json())

    # Rate limiting: verify the *mechanism* (Limiter + SlowAPIMiddleware +
    # key_func + 429 handler) actually triggers, using a throwaway 2/minute
    # limit on a standalone app -- not the real RATE_LIMIT, which would take
    # hundreds of requests in a test to exhaust.
    # slowapi introspects the key_func's parameter *name* -- it must be
    # literally "request" (not just Request-typed) or slowapi calls it with
    # zero args and TypeErrors. Matches _rate_limit_key's real signature.
    test_limiter = Limiter(key_func=lambda request: "selftest", default_limits=["2/minute"])
    test_app = FastAPI()
    test_app.state.limiter = test_limiter
    test_app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    test_app.add_middleware(SlowAPIMiddleware)

    @test_app.get("/ping")
    def _ping():
        return {"ok": True}

    tc = TestClient(test_app)
    codes = [tc.get("/ping").status_code for _ in range(3)]
    assert codes == [200, 200, 429], f"rate limit wiring broken: {codes}"

    r = c.get("/ticker/aapl?limit=5")
    if r.status_code == 200:
        assert all(row["tkr"] == "AAPL" for row in r.json())
    else:
        assert r.status_code == 404

    # /signup: public (no X-API-Key needed), issues a real, usable key, and
    # rejects a second signup for the same email. Revoke at the end so
    # re-running selftest against the same DB file stays idempotent.
    public = TestClient(app)
    r = public.get("/")
    assert r.status_code == 401  # confirms /signup's lack of a key requirement is the exception, not a general gate hole

    r = public.post("/signup", json={"email": "signup-selftest@example.com"})
    assert r.status_code == 200, r.text
    new_key = r.json()["api_key"]
    assert TestClient(app, headers={"X-API-Key": new_key}).get("/").status_code == 200

    r = public.post("/signup", json={"email": "signup-selftest@example.com"})
    assert r.status_code == 409, r.text

    assert public.post("/signup", json={"email": "not-an-email"}).status_code == 400

    revoke_key(new_key)

    print("selftest OK --", len(RELATIONS), "datasets +", 3, "named routes + /signup")


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        selftest()
    else:
        import uvicorn
        uvicorn.run(app, host="127.0.0.1", port=8000)
