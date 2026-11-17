"""Corporate jet flight tracking: FAA aircraft registry + OpenSky flight history.

Phase 20 of Quantgress. The premise Quiver sells as "Corporate Flights": a
company's jet showing up at an airport it never visits is a movement signal
(site visit, plant tour, counterparty HQ) that lands before any filing does.

Two halves, two tables, one view:

  aircraft   FAA Releasable Aircraft Database (MASTER.txt), filtered to
             corporate jets -- registrant is a corporation/LLC, airframe is a
             fixed-wing multi-engine turbojet/turbofan. ~24k rows. The FAA
             publishes the registrant NAME, which is the whole point: it maps
             a transponder hex code to "EXXON MOBIL CORP". entities.py fills
             owner_ticker_guess from there.
  flights    OpenSky Network /flights/aircraft -- one row per takeoff/landing
             pair, with the estimated departure and arrival ICAO airport.
  corp_flights  the view you query: flights joined to their owner.

Public-data status is the same as every other phase for the registry half:
MASTER.txt is a U.S. government publication, no auth, no terms to accept.
OpenSky is not government data -- it's a community ADS-B network, free for
non-commercial/research use with an account. It is the one dependency here
that is not public domain; read their terms before doing anything commercial
with the flights table.

Usage:
    py scrape_flights.py --selftest              # offline checks, no network
    py scrape_flights.py --registry              # (re)load the FAA registry
    py scrape_flights.py --flights                       # top 25 jets, last 7 days
    py scrape_flights.py --flights --owner EXXON         # one owner's fleet
    py scrape_flights.py --flights --icao24 a004b4 --days 30
    py scrape_flights.py --flights --limit 100 --days 2  # widen, mind the credits

Resumable: every (icao24, day) actually queried is recorded in flight_scans,
so a re-run costs nothing for days already covered -- including days that came
back empty, which is most of them for a jet that flies twice a month.

OpenSky credentials are required -- /flights/aircraft 403s anonymous requests
outright (confirmed live; the endpoint is not merely rate-limited without an
account). Create a free API client at https://opensky-network.org/my-opensky and
put OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET in .env next to the USPTO and FEC
keys. Only --flights needs them; --registry is pure FAA and needs nothing.

# ponytail: one calendar day per request, not the 2-day maximum the endpoint
# allows. OpenSky charges 4-960 credits per flight query depending on how many
# day partitions it crosses, and a standard account gets 4,000/day -- a 1-day
# window is the cheapest possible unit, and it makes the resume log a plain
# (icao24, day) pair instead of interval arithmetic. Batching two days per call
# would halve the request count and roughly double the per-call cost. Not worth
# the resume complexity.

# ponytail: --limit defaults to 25 aircraft, not the whole 24k registry.
# 24,662 jets x 7 days is 172k requests and ~690k credits against a 4,000/day
# budget -- a full sweep is a multi-month backfill, not a run. Scope it with
# --owner/--icao24 to a fleet you actually care about, or raise --limit
# knowing what it costs.
"""

import csv
import datetime
import io
import os
import sys
import time
import zipfile

import duckdb
import requests

from envfile import load_dotenv
from schema import DB_PATH

REGISTRY_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"
OPENSKY_API = "https://opensky-network.org/api/flights/aircraft"
TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/opensky-network"
             "/protocol/openid-connect/token")

# registry.faa.gov sits behind Akamai, which 403s a bare requests/curl UA with
# an "Access Denied" HTML body -- confirmed live. Every other scraper here
# sends a contact UA to be polite; this one has to send a browser-shaped one to
# get a response at all.
REGISTRY_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
API_UA = "quantgress/0.1 (personal research)"

RATE_LIMIT_SECS = 0.5  # OpenSky bills by credit, not rate; this is courtesy

# MASTER.txt code columns, from the FAA's own ardata.pdf dictionary and
# verified against ACFTREF.txt (a Gulfstream GVII-G600 is TYPE-ACFT 5, ENG 5).
TYPE_AIRCRAFT_MULTI_ENGINE = "5"
TYPE_ENGINE_JET = ("4", "5")               # 4 = turbo-jet, 5 = turbo-fan
TYPE_REGISTRANT_COMPANY = ("3", "7", "8")  # corporation, LLC, non-citizen corp
STATUS_VALID = "V"

DAYS_DEFAULT = 7
LIMIT_DEFAULT = 25


def ensure_tables(con):
    con.execute("""CREATE TABLE IF NOT EXISTS aircraft (
        icao24 VARCHAR PRIMARY KEY, n_number VARCHAR, owner_name VARCHAR,
        city VARCHAR, state VARCHAR, mfr_mdl_code VARCHAR, year_mfr VARCHAR,
        type_registrant VARCHAR, type_engine VARCHAR,
        owner_ticker_guess VARCHAR, owner_ticker_guess_how VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS flights (
        icao24 VARCHAR, first_seen BIGINT, last_seen BIGINT, callsign VARCHAR,
        dep_airport VARCHAR, arr_airport VARCHAR,
        PRIMARY KEY (icao24, first_seen))""")
    # what we have actually asked OpenSky for. A day with no flights is a real
    # answer worth remembering -- without this the next run re-buys it.
    con.execute("""CREATE TABLE IF NOT EXISTS flight_scans (
        icao24 VARCHAR, day VARCHAR, PRIMARY KEY (icao24, day))""")
    con.execute("""CREATE OR REPLACE VIEW corp_flights AS
        SELECT a.owner_name, a.owner_ticker_guess AS tkr, a.n_number,
               f.dep_airport, f.arr_airport, f.callsign,
               to_timestamp(f.first_seen)::TIMESTAMP AS departed,
               to_timestamp(f.last_seen)::TIMESTAMP AS arrived,
               (f.last_seen - f.first_seen) / 60 AS minutes,
               a.state AS owner_state, f.icao24
        FROM flights f JOIN aircraft a USING (icao24)""")


# ---------------------------------------------------------------- registry

def is_corporate_jet(row):
    """A company-registered turbine airframe on a valid registration.

    Deliberately not "business jet" by model: a model whitelist would need
    maintaining forever and would still miss an owner-operated 737.
    """
    return (row["TYPE AIRCRAFT"] == TYPE_AIRCRAFT_MULTI_ENGINE
            and row["TYPE ENGINE"] in TYPE_ENGINE_JET
            and row["TYPE REGISTRANT"] in TYPE_REGISTRANT_COMPANY
            and row["STATUS CODE"] == STATUS_VALID)


def parse_master(fh):
    """Yield aircraft dicts from an open MASTER.txt text stream.

    Every field is blank-padded to a fixed width inside the comma-delimited
    row, so everything gets stripped. MODE S CODE HEX is the ADS-B transponder
    address OpenSky keys on -- lowercased here because the endpoint documents
    that all letters must be lower case.
    """
    reader = csv.reader(fh)
    header = [c.strip() for c in next(reader)]
    for raw in reader:
        if len(raw) < len(header):
            continue  # trailing/blank line
        row = {h: raw[i].strip() for i, h in enumerate(header)}
        if not is_corporate_jet(row):
            continue
        hexcode = row["MODE S CODE HEX"].lower()
        if not hexcode:
            continue  # no transponder address, nothing to track it by
        yield {
            "icao24": hexcode,
            "n_number": "N" + row["N-NUMBER"],
            "owner_name": row["NAME"],
            "city": row["CITY"],
            "state": row["STATE"],
            "mfr_mdl_code": row["MFR MDL CODE"],
            "year_mfr": row["YEAR MFR"],
            "type_registrant": row["TYPE REGISTRANT"],
            "type_engine": row["TYPE ENGINE"],
        }


FIELDS = ("icao24", "n_number", "owner_name", "city", "state", "mfr_mdl_code",
          "year_mfr", "type_registrant", "type_engine")


def load_registry(con):
    print(f"downloading {REGISTRY_URL} (~70 MB) ...")
    r = requests.get(REGISTRY_URL, headers={"User-Agent": REGISTRY_UA},
                     timeout=600)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open("MASTER.txt") as fh:
            rows = list(parse_master(io.TextIOWrapper(
                fh, encoding="utf-8-sig", errors="replace")))
    # UPDATE the scraped columns only. A plain DELETE+INSERT refresh would
    # throw away every owner_ticker_guess entities.py has resolved.
    con.executemany(
        f"""INSERT INTO aircraft ({", ".join(FIELDS)})
            VALUES ({", ".join("?" * len(FIELDS))})
            ON CONFLICT (icao24) DO UPDATE SET
        """ + ", ".join(f"{f} = excluded.{f}" for f in FIELDS[1:]),
        [[row[f] for f in FIELDS] for row in rows])
    total = con.execute("SELECT count(*) FROM aircraft").fetchone()[0]
    print(f"{len(rows)} corporate jets parsed; {total} rows in aircraft")


# ----------------------------------------------------------------- flights

def _session():
    """OAuth2 client-credentials session. Not optional: OpenSky answers an
    anonymous /flights/aircraft with 403, so an unauthenticated run has nothing
    to fall back to. Same shape as scrape_patents/scrape_donors' key checks --
    fail up front with the signup URL rather than deep in a loop."""
    load_dotenv()
    cid = os.environ.get("OPENSKY_CLIENT_ID")
    secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    if not (cid and secret):
        sys.exit("OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET not set -- "
                 "/flights/aircraft rejects anonymous requests. Create a free "
                 "API client at https://opensky-network.org/my-opensky, then "
                 "add both to .env, or export them before running.")
    s = requests.Session()
    s.headers["User-Agent"] = API_UA
    # ponytail: fetched once, not refreshed. Tokens last ~30 minutes and a run
    # this size finishes well inside that; if a --limit large enough to outlive
    # a token ever becomes normal, re-mint on the first 401 instead.
    tok = requests.post(TOKEN_URL, timeout=30, data={
        "grant_type": "client_credentials",
        "client_id": cid, "client_secret": secret})
    tok.raise_for_status()
    s.headers["Authorization"] = "Bearer " + tok.json()["access_token"]
    return s


def parse_flights(payload):
    """OpenSky flight objects -> our row shape.

    estDeparture/ArrivalAirport are null when the network had no receiver
    coverage near the ends of the flight, which is common for small private
    fields -- kept as NULL rather than dropped, since "this jet flew, origin
    unknown" is still a data point.
    """
    return [{
        "icao24": f["icao24"].lower(),
        "first_seen": f["firstSeen"],
        "last_seen": f["lastSeen"],
        "callsign": (f.get("callsign") or "").strip() or None,
        "dep_airport": f.get("estDepartureAirport"),
        "arr_airport": f.get("estArrivalAirport"),
    } for f in payload]


def fetch_day(s, icao24, day):
    """One calendar day (UTC) of flights for one aircraft.

    404 is OpenSky's documented "no flights in this interval" -- a real answer,
    not a failure, so it returns [] and is never retried. Same shape as
    scrape_pageviews' _get.
    """
    begin = int(datetime.datetime.combine(
        day, datetime.time(), datetime.timezone.utc).timestamp())
    params = {"icao24": icao24, "begin": begin, "end": begin + 86400}
    for attempt in range(4):
        time.sleep(RATE_LIMIT_SECS)
        try:
            r = s.get(OPENSKY_API, params=params, timeout=60)
        except requests.exceptions.RequestException:
            if attempt == 3:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 404:
            return []
        if r.status_code == 200:
            return parse_flights(r.json())
        if r.status_code == 429:  # out of credits -- backing off won't help
            sys.exit("OpenSky credit limit hit (429). Resume tomorrow; "
                     "everything scanned so far is already saved.")
        if r.status_code in (401, 403):  # bad/expired/absent token, not a blip
            sys.exit(f"OpenSky rejected the request ({r.status_code}). Check "
                     "OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET in .env -- an "
                     "anonymous or stale-token request gets exactly this.")
        if attempt == 3:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))
    return []


# A registrant with a handful of jets is a corporate flight department. One
# with hundreds is an airline (UNITED AIRLINES INC, 1206) or an ownership-
# anonymizing trust (BANK OF UTAH TRUSTEE, 1293; TVPX AIRCRAFT SOLUTIONS INC
# TRUSTEE, 897) -- neither tells you where a company's executives went, and
# both would eat the whole credit budget on the default run.
#
# ponytail: a fleet-size cap, not a blacklist of airline and trust-company
# names. Same result, nothing to maintain as new leasing trusts appear. An
# explicit --owner or --icao24 bypasses it, so you can still pull an airline
# fleet deliberately.
MAX_FLEET = 5

FLEET_FILTER = f"""owner_name IN (
    SELECT owner_name FROM aircraft GROUP BY 1 HAVING count(*) <= {MAX_FLEET})"""


def pick_aircraft(con, owner, icao24s, limit):
    if icao24s:
        return [i.lower() for i in icao24s]
    where, params = f"WHERE {FLEET_FILTER}", []
    if owner:
        # an explicit owner is a deliberate choice -- don't second-guess it
        where, params = "WHERE owner_name ILIKE ?", [f"%{owner}%"]
    # newest airframes first: a 2023 Gulfstream is likelier to still be flying
    # for the registered owner than a 1979 Learjet parked in a desert somewhere.
    return [r[0] for r in con.execute(
        f"SELECT icao24 FROM aircraft {where} ORDER BY year_mfr DESC, icao24 "
        f"LIMIT {int(limit)}", params).fetchall()]


def scrape_flights(con, owner, icao24s, days, limit):
    targets = pick_aircraft(con, owner, icao24s, limit)
    if not targets:
        sys.exit("no aircraft matched -- run --registry first, or widen --owner")
    today = datetime.datetime.now(datetime.timezone.utc).date()
    window = [today - datetime.timedelta(days=d) for d in range(1, days + 1)]
    done = {(r[0], r[1]) for r in
            con.execute("SELECT icao24, day FROM flight_scans").fetchall()}

    s = _session()
    added = scanned = 0
    print(f"{len(targets)} aircraft x {len(window)} days "
          f"= {len(targets) * len(window)} day-slots, minus already scanned")
    for icao24 in targets:
        for day in window:
            key = (icao24, day.isoformat())
            if key in done:
                continue
            rows = fetch_day(s, icao24, day)
            for row in rows:
                con.execute(
                    """INSERT INTO flights (icao24, first_seen, last_seen,
                       callsign, dep_airport, arr_airport)
                       VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""",
                    [row["icao24"], row["first_seen"], row["last_seen"],
                     row["callsign"], row["dep_airport"], row["arr_airport"]])
            con.execute("INSERT INTO flight_scans VALUES (?, ?) "
                        "ON CONFLICT DO NOTHING", list(key))
            added += len(rows)
            scanned += 1
            if rows:
                print(f"  {icao24} {day}: {len(rows)} flight(s)")

    total = con.execute("SELECT count(*) FROM flights").fetchone()[0]
    print(f"\n{total} flight rows in {DB_PATH}; this run scanned {scanned} "
          f"new day-slots and added {added} flights")


# ---------------------------------------------------------------- selftest

SAMPLE_MASTER = (
    "N-NUMBER,SERIAL NUMBER,MFR MDL CODE,ENG MFR MDL,YEAR MFR,TYPE REGISTRANT,"
    "NAME,STREET,STREET2,CITY,STATE,ZIP CODE,REGION,COUNTY,COUNTRY,"
    "LAST ACTION DATE,CERT ISSUE DATE,CERTIFICATION,TYPE AIRCRAFT,TYPE ENGINE,"
    "STATUS CODE,MODE S CODE,FRACT OWNER,AIR WORTH DATE,OTHER NAMES(1),"
    "OTHER NAMES(2),OTHER NAMES(3),OTHER NAMES(4),OTHER NAMES(5),"
    "EXPIRATION DATE,UNIQUE ID,KIT MFR, KIT MODEL,MODE S CODE HEX,\n"
    # a real corporate jet: corporation registrant, multi-engine turbofan
    "100A ,3980050  ,3980050,     ,2023,3,EXXON MOBIL CORP   ,PO BOX 1  , ,"
    "SPRING            ,TX,773891425 ,2,339,US,20230101,20230101,1    ,5,5 ,"
    "V ,50002264, ,20230101, , , , , ,20270430,00600061, , ,A004B4    ,\n"
    # individual-owned piston single: wrong airframe and wrong registrant type
    "100  ,5334     ,7100510,17003,1940,1,BENE MARY D        ,PO BOX 329, ,"
    "KETCHUM           ,OK,743490329 ,2,097,US,20230122,20050506,1    ,4,1 ,"
    "V ,50002263, ,19540430, , , , , ,20270430,00600060, , ,A004B3    ,\n"
    # corporate turbofan, but the registration is no longer valid
    "200B ,9999     ,3980050,     ,1999,3,DEFUNCT AIRWAYS INC,PO BOX 2  , ,"
    "RENO              ,NV,895010000 ,2,031,US,20200101,19990101,1    ,5,5 ,"
    "N ,50002265, ,19990101, , , , , ,20200430,00600062, , ,A004B5    ,\n"
)


def selftest():
    rows = list(parse_master(io.StringIO(SAMPLE_MASTER)))
    assert len(rows) == 1, rows
    assert rows[0]["icao24"] == "a004b4"           # lowercased for the API
    assert rows[0]["owner_name"] == "EXXON MOBIL CORP"
    assert rows[0]["n_number"] == "N100A"          # N prefix restored
    assert rows[0]["state"] == "TX"

    # the two exclusions are the whole filter -- assert on why, not just count
    assert not is_corporate_jet({"TYPE AIRCRAFT": "4", "TYPE ENGINE": "1",
                                 "TYPE REGISTRANT": "3", "STATUS CODE": "V"})
    assert not is_corporate_jet({"TYPE AIRCRAFT": "5", "TYPE ENGINE": "5",
                                 "TYPE REGISTRANT": "1", "STATUS CODE": "V"})
    assert not is_corporate_jet({"TYPE AIRCRAFT": "5", "TYPE ENGINE": "5",
                                 "TYPE REGISTRANT": "3", "STATUS CODE": "N"})
    assert is_corporate_jet({"TYPE AIRCRAFT": "5", "TYPE ENGINE": "4",
                             "TYPE REGISTRANT": "7", "STATUS CODE": "V"})

    flights = parse_flights([{
        "icao24": "A004B4", "firstSeen": 1725000000, "lastSeen": 1725007200,
        "callsign": "N100A  ", "estDepartureAirport": "KIAH",
        "estArrivalAirport": None}])
    assert flights[0] == {"icao24": "a004b4", "first_seen": 1725000000,
                          "last_seen": 1725007200, "callsign": "N100A",
                          "dep_airport": "KIAH", "arr_airport": None}
    # a blank callsign must normalize to NULL, not to the empty string
    assert parse_flights([{"icao24": "a1", "firstSeen": 1, "lastSeen": 2,
                           "callsign": "   ", "estDepartureAirport": None,
                           "estArrivalAirport": None}])[0]["callsign"] is None

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        # a 404 is "no flights that day": returned once, never retried
        class _NotFound:
            calls = 0

            def get(self, *a, **k):
                _NotFound.calls += 1
                return type("R", (), {"status_code": 404})()

        assert fetch_day(_NotFound(), "a004b4", datetime.date(2026, 9, 1)) == []
        assert _NotFound.calls == 1

        # 403 (anonymous or stale token) is terminal -- exit, don't retry it
        class _Forbidden:
            calls = 0

            def get(self, *a, **k):
                _Forbidden.calls += 1
                return type("R", (), {"status_code": 403})()

        try:
            fetch_day(_Forbidden(), "a004b4", datetime.date(2026, 9, 1))
        except SystemExit:
            pass
        else:
            raise AssertionError("403 must abort, not fall through")
        assert _Forbidden.calls == 1

        # the request must ask for exactly one UTC day -- a wider interval is
        # both rejected by OpenSky (>2 days) and billed at a higher credit tier
        seen = {}

        class _Capture:
            def get(self, url, params=None, **k):
                seen.update(params)
                return type("R", (), {"status_code": 404})()

        fetch_day(_Capture(), "a004b4", datetime.date(2026, 9, 1))
    finally:
        time.sleep = real_sleep
    assert seen["end"] - seen["begin"] == 86400
    assert seen["begin"] == 1788220800  # 2026-09-01T00:00:00Z
    assert seen["icao24"] == "a004b4"

    # the schema and the view must actually build before a real run does it
    con = duckdb.connect(":memory:")
    ensure_tables(con)
    con.execute("INSERT INTO aircraft (icao24, owner_name, n_number) "
                "VALUES ('a004b4', 'EXXON MOBIL CORP', 'N100A')")
    con.execute("INSERT INTO flights VALUES "
                "('a004b4', 1725000000, 1725007200, 'N100A', 'KIAH', 'KTEB')")
    got = con.execute("SELECT owner_name, dep_airport, arr_airport, minutes "
                      "FROM corp_flights").fetchone()
    assert got == ("EXXON MOBIL CORP", "KIAH", "KTEB", 120), got

    print("selftest ok")


def args(flag):
    return [sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == flag]


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        sys.exit()
    if "--registry" not in sys.argv and "--flights" not in sys.argv:
        sys.exit(__doc__.split("Usage:")[1].split("Resumable")[0].strip())
    con = duckdb.connect(DB_PATH)
    ensure_tables(con)
    if "--registry" in sys.argv:
        load_registry(con)
    if "--flights" in sys.argv:
        scrape_flights(con, arg("--owner"), args("--icao24"),
                       int(arg("--days", DAYS_DEFAULT)),
                       int(arg("--limit", LIMIT_DEFAULT)))
    con.close()
