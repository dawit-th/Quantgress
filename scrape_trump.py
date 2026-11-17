"""Scrape Donald Trump's OGE Form 278-T periodic transaction reports.

Phase 17 of Quantgress. OGE's own web front end (extapps2.oge.gov) is not a
scrapable filing index -- confirmed live, see the correction below. The real
access path is ProPublica's "Trump Team Financial Disclosures" project: an
appointee's page embeds the site's document index as inline JSON (id,
DocumentCloud URL, form_type, ...), and DocumentCloud serves the underlying
PDF straight off S3 at a stable, derivable URL.

> **Correction (live, before any parsing was written):** the wiki's original
> scoping assumed OGE's own "PAS Index" / dlgDocumentListnew pages were a
> scrapable filing index like Phase 2's House ZIP. They are not. PAS Index
> only lists PAS (Senate-confirmed) appointees -- the President isn't on it
> -- and the President's own filings sit behind Online Form 201, an
> email-request system (5-document cap, 2-business-day turnaround, identity
> fields completed under penalty of 18 U.S.C. 1001). Not scrapable, and not
> something to automate. ProPublica's DocumentCloud mirror is the real
> source, confirmed live: `s3.documentcloud.org/documents/{id}/{slug}.pdf`
> resolves for every doc_id ProPublica's page lists.

Usage:
    py scrape_trump.py --selftest   # offline parser checks
    py scrape_trump.py --limit 3    # bounded run, a few filings
    py scrape_trump.py              # every 278-T on file
    py scrape_trump.py --reparse    # re-fetch + re-parse all, even ones stored ok
    py scrape_trump.py --pdf <OGE URL or local .pdf>   # one filing ProPublica lacks yet

Re-running skips DocumentCloud doc_ids already in trump_filings.

# ponytail: single filer (PERSON below), not every executive-branch 278-T
# filer ProPublica's index covers. Matches what the wiki scoped this phase
# to (Quiver's own "Trump trades" product) -- congress trades already split
# by filer population (Senate vs House) rather than scraping "everyone".
# --filer overrides the slug if a broader crawl is ever actually wanted.

# ponytail: DocumentCloud's text layer here is its own OCR of a scanned
# form, not born-digital text like House's PTRs -- '$' misreads as 's', '0'
# misreads as 'o'/'O' inside amounts. parse_amount_ocr leans on 5 C.F.R.
# 2634 Schedule B being a small fixed set of dollar brackets: the *low* end
# almost always survives OCR intact and uniquely determines the *high* end,
# so a bracket-table lookup beats trying to repair a garbled second number
# character by character. No entities.py ticker resolution registered yet --
# `description` still carries OCR noise fused in from the type/date fields
# on wrapped rows; clean that up first, see implementation notes.
"""

import io
import os
import re
import sys
import time

import duckdb
import pdfplumber
import requests

DB_PATH = "congress_trades.duckdb"
PERSON = "trump-donald-j"
INDEX_PAGE = "https://projects.propublica.org/trump-team-financial-disclosures/appointees/{slug}"
PDF_URL = "https://s3.documentcloud.org/documents/{doc_id}/{slug}.pdf"
RATE_LIMIT_SECS = 1  # ~a dozen requests total; a light courtesy delay is enough

# One filing-document record out of the appointee page's inline SvelteKit
# data (unquoted keys -- not strict JSON, hence a regex instead of a real
# parser). Anchored on the few fields this module needs; tolerant of
# everything else in the object (net worth, agency, pic, ...).
DOC_RE = re.compile(
    r'\{id:"\d+",name:"[^"]*",title:"[^"]*",slug:"(?P<slug>[^"]*)".*?'
    r'did:"(?P<did>\d+)",url:"(?P<url>[^"]*)",file:"(?P<file>[^"]*)",'
    r'form_type:"(?P<form_type>[^"]*)"'
)
DOC_URL_RE = re.compile(r"documents/(\d+)-(.+)/$")

# 5 C.F.R. 2634 Schedule B -- the closed set every PTR dollar amount falls
# into. Same categories the Command Reference documents for Congress.
BRACKETS = {
    1001: 15000, 15001: 50000, 50001: 100000, 100001: 250000,
    250001: 500000, 500001: 1000000, 1000001: 5000000,
    5000001: 25000000, 25000001: 50000000, 50000001: None,
}

# A transaction line ends with type, date, the "notified >30 days late" flag,
# then the amount -- everything before the type word is description, however
# many lines it wrapped across. Type words are badly OCR'd ("lourchoso",
# "PUrchoso", "salo"), so matched by substring rather than an exact set.
TYPE_RE = r"\S*(?:[uo]rch\S*|sal[eo]\S*|xch[ae]ng\S*)"
# The late flag is optional here: some filings (08.12.2025) lay a row out
# as "54 YES" on one line and "<description> Purchase 2/5/2025 $50,001 -
# $100,000" on the next, with no flag between date and amount. A flag token
# never contains '$', which is what lets the regex tell it from the amount.
TAIL_RE = re.compile(
    rf"(?P<type>{TYPE_RE})\s+(?P<date>\d{{1,2}}/\d{{1,2}}/\d{{2,4}})\s+"
    r"(?:(?P<flag>[^\s$]+)\s+)?(?P<amt>[\$sS].*\d.*)$", re.I)
# Row numbers run past 999 -- the 05.08.2026 filing has 1,400+ transactions.
NUM_RE = re.compile(r"^\s*(\d{1,4})\s+")
NUM_ONLY_RE = re.compile(r"^\s*(\d{1,4})\s*$")
# "54 YES" / "10 . . .. YES '": a row number with only the late flag after it.
NUM_FLAG_RE = re.compile(r"^\s*(\d{1,4})\s+(?:[\W_]+\s*)*([vy][eo0]s|n[o0])\W*$", re.I)
# Case-insensitive: 2026 forms print "OGE Received: 6/29/2026".
RECEIVED_RE = re.compile(r"OGE RECEIVED:?\s*(\d{1,2}/\d{1,2}/\d{2,4})", re.I)
# Fallback entry into the table when the received stamp is missing or
# unreadable (05.08.2026 (2) has none): the column header row repeated at
# the top of every transactions page.
TABLE_HEADER_RE = re.compile(r"^#?\s*D\S*cr\S*\s.*Amount\s*$", re.I)
# Row-start marker only (leading number), independent of whether the rest of
# that row goes on to parse cleanly -- some real rows never match TAIL_RE at
# all (a slash dropped from the date, "12/10/2025" OCR'd as "12110/2025",
# turns three groups into two and the date pattern simply can't match). This
# gives an honest denominator for coverage instead of a silent undercount.
ROW_START_RE = re.compile(r"^(\d{1,4})\s+\S")
# Page furniture and form boilerplate -- never part of a description, mangled
# every which way by OCR, so matched on a stable leading fragment per line
# rather than the whole (unrecoverable) line.
SKIP_RE = re.compile(
    r"^(OGE Form 278|If [Yy]o[uy] n[o0]{2}d|N[o0]te[:.·]|Fl[il]e[rc]'?[sc]|"
    r"Trans[au]d?[l1]ion|Notification|Received O|D[hn]crl|Page \d|"
    r"[DO]onald J|[DO]on[ae]ld|Summary of Contents|Privacy Act|U\.S\. [O0]|"
    r"C.OVERNM|Exocutivo Branch|Periodic Transaction|OGE RECEIVED|"
    r"[-_=•]+$)", re.I)


# ── fallback for rows TAIL_RE can't read ──────────────────────────────
# Coverage was 50-90% per filing because TAIL_RE needs a legible type word
# AND date ("l!Ud>ase 3112/2026" is "purchase 3/12/2026"). Nearly every such
# row still starts with its row number and ends in a readable dollar bracket,
# so those two anchors take the row; type/date are filled when legible and
# left NULL when not -- a NULL is honest, a dropped row is a silent undercount.
# Guards against non-rows: the amount must resolve to a real Schedule B
# bracket floor, and there must be a second transaction signal (late flag
# before the amount, a type word, or a date).
AMT_END_RE = re.compile(
    r"(?P<amt>[\$sS§][\s\dOo]?[\dOoIl(][\dOoIl,.\s()]*?\s*[-•·~—]+\s*"
    r"[\$sS§]?\s?[\dOoIl(][\dOoIl,.\s()]*)\s*\W?$")
FLAG_END_RE = re.compile(r"(?:^|\s)([vy][eo0]s|n[o0])\W{0,3}\s*$", re.I)
STRICT_DATE_RE = re.compile(r"(?<![\w/])(\d{1,2})/(\d{1,2})/(\d{4}|\d{2})(?![\w/])")
MATURITY_RE = re.compile(r"(?:DUE|DATED|DTD|MAT)\W{0,3}$", re.I)


def _trade_date(head, max_year):
    """Last legible date in the row that isn't a bond's maturity/dated date
    ("DUE 08/15/2029") and isn't in the future -- or None."""
    best = None
    for m in STRICT_DATE_RE.finditer(head):
        if MATURITY_RE.search(head[:m.start()]):
            continue
        year = int(m.group(3))
        year = year + 2000 if year < 100 else year
        if 2000 <= year <= max_year and 1 <= int(m.group(1)) <= 12 and 1 <= int(m.group(2)) <= 31:
            best = m
    return best


def parse_loose(line, max_year=None):
    """A numbered row TAIL_RE couldn't read, anchored on its row number and
    closing amount. Returns a row dict, or None if it isn't a real row."""
    m = re.match(r"^(\d{1,4})\s+(.+)$", line)
    if not m:
        return None
    a = AMT_END_RE.search(m.group(2))
    if not a:
        return None
    low, high = parse_amount_ocr(a.group("amt"))
    if low not in BRACKETS:
        return None
    head = m.group(2)[:a.start()].rstrip()
    flag = FLAG_END_RE.search(head)
    typ = re.search(TYPE_RE, head, re.I)
    date = _trade_date(head, max_year or time.localtime().tm_year)
    if not (flag or typ or date):
        return None
    cut = min([x.start() for x in (flag, typ, date) if x] + [len(head)])
    return {
        "row_num": int(m.group(1)),
        "description": head[:cut].strip(" -•~"),
        "tx_type": classify_type(typ.group()) if typ else None,
        "tx_date": date.group(0) if date else None,
        "notified_late": classify_late(flag.group(1)) if flag else None,
        "amount_raw": a.group("amt").strip(),
        "amount_low": low, "amount_high": high,
    }


def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (mmulajkar@gmail.com)"
    return s


def clean_amount_ocr(raw):
    """OCR noise inside an amount tail only ever looks like '$'->'s' and
    '0'->'o'/'O' -- an amount field has no legitimate letters at all, so
    both substitutions are safe once isolated to just this text."""
    s = re.sub(r"[oO]", "0", raw)
    s = re.sub(r"(^|[-\s])[sS]+(?=\d)", r"\1$", s)
    # the 2026 scans drop the thousands comma for a space: "$50 001" -> "$50,001"
    s = re.sub(r"(\d)\s(\d{3})(?!\d)", r"\1,\2", s)
    return s.replace(".", ",").replace(";", ",")


def parse_amount_ocr(raw):
    """'$250,001 -$500,000' (or a garbled variant) -> (250001, 500000)."""
    s = clean_amount_ocr(raw)
    nums = [int(n.replace(",", "")) for n in re.findall(r"\d[\d,]*", s)]
    if not nums:
        return None, None
    low = nums[0]
    if low in BRACKETS:
        return low, BRACKETS[low]
    return low, (nums[1] if len(nums) > 1 else None)


def classify_type(token):
    t = token.lower()
    if "sal" in t:
        return "Sale"
    if "xch" in t:
        return "Exchange"
    if re.search(r"[uo]rch", t):
        return "Purchase"
    return None


def classify_late(token):
    t = token.lower()
    if t == "no":
        return False
    if re.match(r"^[vy][eo0]s$", t):
        return True
    return None


def list_documents(session, filer=PERSON):
    """Yield (doc_id, slug, file) for every 278-T on file for `filer`."""
    r = session.get(INDEX_PAGE.format(slug=filer), timeout=30)
    r.raise_for_status()
    seen = set()
    for m in DOC_RE.finditer(r.text):
        if m.group("slug") != filer or m.group("form_type") != "278 Transaction":
            continue
        did = m.group("did")
        if did in seen:
            continue
        seen.add(did)
        um = DOC_URL_RE.search(m.group("url"))
        if um:
            yield um.group(1), um.group(2), m.group("file")


def parse_text(text):
    """(rows, filed_date, expected_n) from a 278-T's OCR'd text. expected_n is
    a row-start marker count -- an upper bound on real transactions, since a
    handful never make it into `rows` at all (see ROW_START_RE)."""
    rows, pending_desc, pending_num, pending_flag, filed = [], [], None, None, None
    expected, seen = set(), set()
    started = False  # pages 1-2 are certification/signature boilerplate, not
    # the transaction table -- their OCR garbage doesn't match any known
    # SKIP pattern (every filing mangles it differently) and was leaking into
    # the first real row's description, corrupting its amount. "OGE
    # RECEIVED:" is the last line before the table starts, so it gates entry.
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        rm = RECEIVED_RE.search(line)
        if rm:
            filed = rm.group(1)
            started = True
            continue
        if TABLE_HEADER_RE.match(line):
            started = True
            pending_desc = []  # page-top furniture isn't part of any row
            continue
        if not started:
            continue
        rsm = ROW_START_RE.match(line)
        if rsm:
            expected.add(int(rsm.group(1)))
        m = TAIL_RE.search(line)
        if m:
            num_m = NUM_RE.match(line)
            head = line[:m.start()].strip()
            row_num = pending_num
            if num_m and head.startswith(num_m.group(1)):
                head = head[len(num_m.group(1)):].strip()
                row_num = int(num_m.group(1))
            pending_desc.append(head)
            low, high = parse_amount_ocr(m.group("amt"))
            rows.append({
                "row_num": row_num,
                "description": " ".join(d for d in pending_desc if d).strip(),
                "tx_type": classify_type(m.group("type")),
                "tx_date": m.group("date"),
                "notified_late": classify_late(m.group("flag") or pending_flag or ""),
                "amount_raw": m.group("amt").strip(),
                "amount_low": low, "amount_high": high,
            })
            seen.add(row_num)
            pending_desc, pending_num, pending_flag = [], None, None
        elif (loose := parse_loose(line)) and loose["row_num"] not in seen:
            if loose["notified_late"] is None and pending_flag:
                loose["notified_late"] = classify_late(pending_flag)
            rows.append(loose)
            seen.add(loose["row_num"])
            pending_desc, pending_num, pending_flag = [], None, None
        elif SKIP_RE.match(line):
            continue
        elif NUM_FLAG_RE.match(line):
            nf = NUM_FLAG_RE.match(line)
            pending_num, pending_flag, pending_desc = int(nf.group(1)), nf.group(2), []
        elif NUM_ONLY_RE.match(line):
            pending_num = int(NUM_ONLY_RE.match(line).group(1))
        else:
            pending_desc.append(line)
    return rows, filed, len(expected)


def ensure_tables(con):
    con.execute("""CREATE TABLE IF NOT EXISTS trump_filings (
        doc_id VARCHAR PRIMARY KEY, file VARCHAR, filed VARCHAR,
        link VARCHAR, status VARCHAR, n_rows INTEGER, n_rows_expected INTEGER)""")
    con.execute("""CREATE TABLE IF NOT EXISTS trump_trades (
        doc_id VARCHAR, row_num INTEGER, description VARCHAR,
        tx_type VARCHAR, tx_date VARCHAR, notified_late BOOLEAN,
        amount_raw VARCHAR, amount_low BIGINT, amount_high BIGINT,
        filed VARCHAR, link VARCHAR)""")
    # entities.py fills these (exact SEC-name match on a cleaned description);
    # created here too so the view below never references a missing column.
    for col in ("ticker_guess", "ticker_guess_how"):
        con.execute(f"ALTER TABLE trump_trades ADD COLUMN IF NOT EXISTS {col} VARCHAR")
    # Two independent tells for "don't trust this row":
    #  1. A description with a stray '$' in it absorbed another failed row's
    #     leftover text (see parse_text) -- a clean single-transaction
    #     description never has one, since the real amount lives in
    #     amount_low/amount_high, not the text.
    #  2. An amount_low that isn't one of the 10 real Schedule B bracket
    #     floors means the amount itself got OCR-mangled (parse_amount_ocr's
    #     fallback path), independent of whether the description is clean.
    # asset_class is best-effort (Schedule B has no asset-type field at all,
    # unlike the House form) -- a coupon-rate '%' in the description is a
    # decent bond/note tell, but a mangled coupon ("0S37S'Xi" for "05.375%")
    # won't match, so the fallback bucket is "couldn't tell," not "equity."
    # Dates: OCR'd years come as 4 or 2 digits ("7/17/25", "9/14/26"), so
    # each is parsed by its own length -- %Y on "26" yields the year 0026.
    # A trade date after the filing's own received date, or in the future,
    # is an OCR misread ("2026" read as "2028") and becomes NULL here; the
    # raw text stays in trump_trades.tx_date, as filed.
    con.execute("""
        CREATE OR REPLACE MACRO qg_mdy(s) AS
            CASE WHEN regexp_matches(s, '/\d{4}$') THEN try_strptime(s, '%m/%d/%Y')::DATE
                 WHEN regexp_matches(s, '/\d{2}$') THEN try_strptime(s, '%m/%d/%y')::DATE END
    """)
    con.execute("""
        CREATE OR REPLACE VIEW trump_trades_clean AS
        SELECT CASE WHEN qg_mdy(tx_date) > coalesce(qg_mdy(filed), current_date)
                      OR qg_mdy(tx_date) > current_date
                    THEN NULL ELSE qg_mdy(tx_date) END AS txn_date,
               tx_type, amount_low, amount_high, notified_late,
               CASE WHEN regexp_matches(description, '[0-9](\\.[0-9]+)?\\s*%')
                    THEN 'Bond/Note' ELSE 'Unclassified' END AS asset_class,
               ticker_guess, ticker_guess_how,
               description, doc_id, qg_mdy(filed) AS filed_date, filed, link
        FROM trump_trades
        WHERE description NOT LIKE '%$%'
          AND amount_low IN (1001, 15001, 50001, 100001, 250001, 500001,
                              1000001, 5000001, 25000001, 50000001)
    """)


def filed_from_name(file):
    """Fallback filing date from the document name ("...-05.08.2026-278T" ->
    "5/8/2026") when the OGE received stamp is missing or unreadable, as on
    the 3,300-row 05.08.2026 (2) filing. That's the report date, which lands
    within days of the received date -- close enough to date the filing."""
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", file or "")
    return f"{int(m.group(1))}/{int(m.group(2))}/{m.group(3)}" if m else None


def done_doc_ids(con):
    """Filings that never need fetching again: only ones that parsed. An
    'empty', 'partial' or 'error' filing is retried every run -- the 2026 filings'
    DocumentCloud OCR is too garbled for parse_text today, and skipping
    anything ever attempted would bury them for good even once the parser
    (or the OCR) improves. A retry is one PDF download each."""
    return {r[0] for r in con.execute(
        "SELECT doc_id FROM trump_filings WHERE status = 'ok'").fetchall()}


def file_key(file):
    """Filing identity across sources: OGE names a file
    "Donald-J-Trump.09.17.2026-278T", ProPublica's copy may differ only in
    punctuation/spacing -- so compare lowercase letters and digits only."""
    return re.sub(r"[^a-z0-9]", "", (file or "").lower())


def store_filing(con, doc_id, file, link, pdf_bytes):
    """Parse one 278-T PDF and (re)write it: its trades and its filing row.
    Shared by the ProPublica crawl and --pdf imports. Returns (n_rows, status)."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as p:
            text = "\n".join(pg.extract_text() or "" for pg in p.pages)
        rows, filed, expected = parse_text(text)
        filed = filed or filed_from_name(file)
        # under half the row markers recovered = mostly unreadable;
        # 'partial' isn't 'ok', so it's retried (done_doc_ids)
        status = ("ok" if len(rows) >= expected / 2 else "partial") if rows else "empty"
    except Exception as e:
        rows, filed, expected, status = [], None, 0, "error"
        print(f"  {doc_id} {file}: {e}")

    con.execute("DELETE FROM trump_trades WHERE doc_id = ?", [doc_id])
    if rows:
        # named columns: entities.py adds ticker_guess/_how to this table
        con.executemany(
            "INSERT INTO trump_trades (doc_id, row_num, description, tx_type, "
            "tx_date, notified_late, amount_raw, amount_low, amount_high, filed, link) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [(doc_id, r["row_num"], r["description"], r["tx_type"],
              r["tx_date"], r["notified_late"], r["amount_raw"],
              r["amount_low"], r["amount_high"], filed, link)
             for r in rows])
    # a retried filing replaces its previous attempt (doc_id is the key)
    con.execute("DELETE FROM trump_filings WHERE doc_id = ?", [doc_id])
    con.execute("INSERT INTO trump_filings VALUES (?,?,?,?,?,?,?)",
                [doc_id, file, filed, link, status, len(rows), expected])
    note = f" (expected ~{expected})" if expected > len(rows) else ""
    print(f"  {file}: {len(rows)} txns [{status}]{note}")
    return len(rows), status


def main(limit=None, filer=PERSON, reparse=False):
    con = duckdb.connect(DB_PATH)
    try:
        ensure_tables(con)
        # --reparse re-fetches every filing, even ones that parsed 'ok' --
        # for after a parser fix (the 08.12.2025 filing was stored with 3 of
        # its ~500 rows before the two-line layout was handled).
        done = set() if reparse else done_doc_ids(con)
        # a filing already imported straight from OGE (--pdf) isn't stored a
        # second time when ProPublica's mirror catches up with it
        imported = {file_key(f) for (f,) in con.execute(
            "SELECT file FROM trump_filings WHERE doc_id LIKE 'oge:%' AND status = 'ok'").fetchall()}

        s = _session()
        docs = list(list_documents(s, filer))
        print(f"{len(docs)} 278-T filings on file for {filer}")

        n = 0
        for doc_id, slug, file in docs:
            if limit is not None and n >= limit:
                break
            if doc_id in done or file_key(file) in imported:
                continue
            time.sleep(RATE_LIMIT_SECS)
            link = f"https://www.documentcloud.org/documents/{doc_id}-{slug}/"
            try:
                pdf = s.get(PDF_URL.format(doc_id=doc_id, slug=slug), timeout=60)
                pdf.raise_for_status()
                content = pdf.content
            except Exception as e:
                print(f"  {doc_id} {file}: {e}")
                content = b""
            store_filing(con, doc_id, file, link, content)
            n += 1

        total = con.execute("SELECT count(*) FROM trump_trades").fetchone()[0]
        print(f"\n{total} Trump transactions in {DB_PATH}")
    finally:
        con.close()


def import_pdf(src):
    """--pdf: one filing ProPublica's mirror doesn't have yet, from an OGE URL
    or a local file. The user's call to fetch it (see the module notes on
    OGE's Form 201 system); this only parses and stores what it's given.
    doc_id is "oge:<OGE document id>" for an OGE URL, else "file:<name>"."""
    if re.match(r"https?://", src):
        r = _session().get(src, timeout=120)
        r.raise_for_status()
        content, link = r.content, src
        m = re.search(r"/([0-9A-F]{32})/\$FILE/", src, re.I)
        file = requests.utils.unquote(src.rsplit("/", 1)[-1]).removesuffix(".pdf")
        doc_id = f"oge:{m.group(1).upper()}" if m else f"url:{file_key(file)}"
    else:
        with open(src, "rb") as f:
            content = f.read()
        file = os.path.basename(src).removesuffix(".pdf")
        link, doc_id = None, f"file:{file_key(file)}"
    con = duckdb.connect(DB_PATH)
    try:
        ensure_tables(con)
        # the same filing already on file under ProPublica's id: replace it
        # rather than storing it twice
        dupes = [d for (d, f) in con.execute("SELECT doc_id, file FROM trump_filings").fetchall()
                 if file_key(f) == file_key(file) and d != doc_id]
        for d in dupes:
            con.execute("DELETE FROM trump_trades WHERE doc_id = ?", [d])
            con.execute("DELETE FROM trump_filings WHERE doc_id = ?", [d])
        store_filing(con, doc_id, file, link, content)
        if dupes:
            print(f"  replaced the earlier copy: {', '.join(dupes)}")
    finally:
        con.close()


def selftest():
    # the loose fallback, on real lines from the 2026 scans
    r = parse_loose("22 FACTSET RESH SYS INC IDUl'CNISO 4/17/2026 Vos S500 001 • $1,000,000", 2026)
    assert r and r["row_num"] == 22 and r["amount_low"] == 500001 and r["amount_high"] == 1000000
    assert r["tx_date"] == "4/17/2026" and r["notified_late"] is True and r["tx_type"] is None
    assert r["description"] == "FACTSET RESH SYS INC IDUl'CNISO"
    r = parse_loose("1 Abbott labs sale 5/512026 Yos $15 001 -$50 000", 2026)
    assert r and r["tx_type"] == "Sale" and r["tx_date"] is None and r["amount_low"] == 15001
    # a bond's maturity date is never taken as the trade date
    r = parse_loose("11 OLIN CORP DUE 08/01/2029 05.825% purchase $1,001 - $15,000", 2026)
    assert r and r["tx_date"] is None and r["tx_type"] == "Purchase"
    # no second signal (flag/type/date) -> not a row
    assert parse_loose("10 SOMETHING DISCRETIONARY ORDER $100,001 - $250,000", 2026) is None
    # an amount off the Schedule B brackets -> not a row
    assert parse_loose("7 ACME CORP purchase 1/2/2026 No $12,345 - $67,890", 2026) is None

    # only parsed filings count as done -- empty/error ones are retried
    mem = duckdb.connect(":memory:")
    ensure_tables(mem)
    for doc_id, status in [("1", "ok"), ("2", "empty"), ("3", "error")]:
        mem.execute("INSERT INTO trump_filings VALUES (?, 'f', NULL, 'l', ?, 0, 0)", [doc_id, status])
    assert done_doc_ids(mem) == {"1"}
    assert filed_from_name("Trump, Donald J.-05.08.2026-278T(2)") == "5/8/2026"
    assert filed_from_name("Donald J. Trump 2.26.2026 278-T (1)") == "2/26/2026"
    assert filed_from_name("no date here") is None
    # the same filing under OGE's and ProPublica's naming is one filing
    assert file_key("Donald-J-Trump.09.17.2026-278T") == file_key("Donald-J-Trump-09.17.2026-278T")
    assert file_key("Donald-J-Trump.09.17.2026-278T") != file_key("Donald-J-Trump-09.8.2026-278T")

    assert parse_amount_ocr("$250,001 - $500,000") == (250001, 500000)
    # OCR turns '$' into 's' and '0' into 'o'/'O' -- verbatim from a real
    # filing (doc 26496956, row 191): "sale 12/19/2025 no s1.ooo,001 -ss,000,000"
    assert parse_amount_ocr("s1.ooo,001 -ss,000,000") == (1000001, 5000000)
    assert parse_amount_ocr("$50,000,001 +") == (50000001, None)
    assert parse_amount_ocr("$999,999 - $1,000,000") == (999999, 1000000), \
        "an unrecognized low end has no bracket to trust -- fall back to the raw second number"

    assert classify_type("lourchoso") == "Purchase"
    assert classify_type("PUrchoso") == "Purchase"
    assert classify_type("salo") == "Sale"
    assert classify_type("sale") == "Sale"
    assert classify_late("VOS") is True
    assert classify_late("ves") is True
    assert classify_late("no") is False
    assert classify_late("???") is None

    # Verbatim pdfplumber/DocumentCloud OCR output, doc 26496956 pages 2-3
    # (Trump's 1.14.2026 278-T): a clean single-line row, then a row whose
    # description wraps and whose row number lands on the *second* line.
    text = (
        "OGE RECEIVED: 1/14/2026OGE Fann 278-T (Updated February 2024)\n"
        "1 MIAMI-DADE CNTY Fl WTR & SR B fN BE/R/ 5 DUE 100133 OTO 120425 FC "
        "040126 2.610% YIELD TO MATURITY lourchoso 11/14/2025 VOS "
        "$250,001 • $500,000\n"
        "HUMBLE TX INDPT SCH T SRA BEIR/ 5 OUE 021529 OTO 011426 FC 021526 "
        "2.590% YIELD TO MATIJRITY UNSOLICITED\n"
        "105 I Purchase 12/10/2025 VOS $250,001 -$500,000\n"
        "191 • .,COREWEAVE INC REGS DUE 06/01/2030 09.250%JO 01 "
        "DISCRETIONARY ORDER IF THIS CONFIRMATION IS IN CONNECTION WITH A "
        "SALE PURSUANT TO REG. 5 sale 12/19/2025 no s1.ooo,001 -ss,000,000\n"
        "OGE Form 278-T (Updotod FebnJery 2024)\n"
    )
    rows, filed, expected = parse_text(text)
    assert filed == "1/14/2026", filed
    assert len(rows) == 3, rows
    assert expected == 3, expected  # rows 1, 105, 191 all carry a leading marker

    a, b, c = rows
    assert a["row_num"] == 1 and a["tx_type"] == "Purchase"
    assert a["tx_date"] == "11/14/2025" and a["notified_late"] is True
    assert (a["amount_low"], a["amount_high"]) == (250001, 500000)

    assert b["row_num"] == 105
    assert "HUMBLE TX" in b["description"], b["description"]
    assert b["tx_type"] == "Purchase" and (b["amount_low"], b["amount_high"]) == (250001, 500000)

    assert c["row_num"] == 191 and c["tx_type"] == "Sale" and c["notified_late"] is False
    assert "COREWEAVE" in c["description"], c["description"]
    assert (c["amount_low"], c["amount_high"]) == (1000001, 5000000), \
        "bracket lookup must recover the high end even from garbled OCR"

    assert parse_text("nothing here at all\n") == ([], None, 0)
    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif "--pdf" in sys.argv:
        import_pdf(arg("--pdf"))
    else:
        n = arg("--limit")
        main(int(n) if n else None, arg("--filer", PERSON), reparse="--reparse" in sys.argv)
