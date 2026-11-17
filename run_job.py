"""Run one schedule group's scrapers in order, then refresh the API snapshot.

The one entry point both schedulers call -- deploy/launchd (macOS) and
deploy/cron.d (Linux) -- so the job list lives here once instead of in two
schedule formats.

    python run_job.py daily        # daily / weekly / quarterly / annual-jan / annual-sep
    python run_job.py --list       # every group and its scripts
    python run_job.py --selftest   # offline checks, no scrapers run

Scripts run back to back, never in parallel: DuckDB allows one writer per
file, so the old cron layout (jobs staggered 15 min apart) crashed any job
that started while a slow one still held the lock. For the same reason a
group that starts while another is running waits for it (a lock file),
e.g. Sunday's weekly job when the daily one runs long.

Each script logs to logs/<name>.log. One that fails doesn't stop the group --
later scripts are independent sources. One that runs past WARN_SECS sends a
warning; past KILL_SECS it is stopped (every scraper resumes where it left
off, so nothing is lost but time). Any failure exits non-zero and, on macOS,
posts a notification -- a scheduled job failing used to be visible only in
`launchctl list`.
"""

import fcntl
import json
import os
import subprocess
import sys
import time
from datetime import datetime

from envfile import load_dotenv

REPO = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(REPO, "logs")

# Insiders run --live: the no-arg bulk mode only re-reads the latest
# quarterly zip, so it adds nothing day to day. Short volume isn't listed --
# daily.py already runs it. WSB (scrape_wsb.py) is left off until it can run
# at all (needs requirements-wsb.txt plus approved Reddit API credentials):
# it failed every run, so the daily job's status was always 1 and a real
# Senate/House failure looked identical to the expected one. Add it back as
# its own line once that's set up.
GROUPS = {
    "daily": [["daily.py"], ["scrape_insiders.py", "--live"]],
    # Trump's 278-Ts post on no fixed schedule (Aug, Oct-Feb, then Apr-Sep
    # 2026 -- gaps up to ~2 months), which is a reason to poll, not to skip:
    # a run with nothing new is one index request.
    "weekly": [["scrape_contracts.py"], ["scrape_patents.py"], ["scrape_trump.py"]],
    "quarterly": [["scrape_13f.py"], ["scrape_lobbying.py"]],
    "annual-jan": [["scrape_donors.py"], ["scrape_execcomp.py"], ["scrape_senate_annual.py"]],
    # Sep 2nd -- after the August House filing-extension deadline
    "annual-sep": [["scrape_house_annual.py"]],
}

# Scripts that exit immediately without an API key. With no key set they're
# skipped and reported, not run-and-failed -- otherwise the group's status is
# a permanent failure (the WSB problem again) and a real one hides behind it.
# health.py still shows the dataset as stale, so the gap stays visible.
REQUIRES = {
    "scrape_patents.py": "USPTO_API_KEY",
    "scrape_donors.py": "FEC_API_KEY",
}

WARN_SECS = 60 * 60
KILL_SECS = 4 * 60 * 60


def log_name(script):
    """daily.py -> daily, scrape_short_volume.py -> short_volume (the log
    names deploy/cron.d always used)."""
    return script.removesuffix(".py").removeprefix("scrape_")


def notify(title, message):
    print(f"[{title}] {message}", flush=True)
    if sys.platform == "darwin":
        script = f"display notification {json.dumps(message)} with title {json.dumps(title)}"
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True)


def run_script(cmd, log_path, warn_secs=WARN_SECS, kill_secs=KILL_SECS, cwd=REPO):
    """Run one scraper; returns its exit code, or "timeout" if it was stopped."""
    with open(log_path, "a") as log:
        log.write(f"=== {datetime.now():%a %b %d %H:%M:%S %Y} {' '.join(cmd)} ===\n")
        log.flush()
        p = subprocess.Popen([sys.executable, "-u", *cmd], cwd=cwd,
                             stdout=log, stderr=subprocess.STDOUT)
        try:
            return p.wait(timeout=warn_secs)
        except subprocess.TimeoutExpired:
            notify("Quantgress", f"{cmd[0]} still running after {warn_secs // 60} min")
        try:
            return p.wait(timeout=kill_secs - warn_secs)
        except subprocess.TimeoutExpired:
            p.terminate()
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
            log.write(f"=== stopped after {kill_secs // 60} min (resumes next run) ===\n")
            return "timeout"


def run_group(group):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(os.path.join(LOG_DIR, "run_job.lock"), "w") as lock:
        start = time.time()
        fcntl.flock(lock, fcntl.LOCK_EX)  # wait for any other group to finish
        if time.time() - start > 1:
            print(f"waited {time.time() - start:.0f}s for another job to finish", flush=True)

        load_dotenv()
        failed = []
        for cmd in GROUPS[group]:
            key = REQUIRES.get(cmd[0])
            if key and not os.environ.get(key):
                print(f"{' '.join(cmd)}: skipped, {key} not set (.env)", flush=True)
                continue
            t = time.time()
            code = run_script(cmd, os.path.join(LOG_DIR, log_name(cmd[0]) + ".log"))
            print(f"{' '.join(cmd)}: exit {code} in {time.time() - t:.0f}s", flush=True)
            if code != 0:
                failed.append(f"{cmd[0]} ({code})")

        # Refresh even after a failure -- whatever the other scripts added is
        # still worth serving. Still inside the lock, so no scraper can be
        # writing while CHECKPOINT runs.
        os.chdir(REPO)
        import snapshot
        try:
            snapshot.refresh()
            print(f"snapshot refreshed: {snapshot.SNAPSHOT_PATH}", flush=True)
        except Exception as e:  # noqa: BLE001 -- report any failure, don't crash the summary
            failed.append(f"snapshot ({e.__class__.__name__})")

        import health
        stale = health.stale_datasets()
        if stale:
            print("stale: " + ", ".join(stale), flush=True)

    if failed:
        notify("Quantgress job failed", f"{group}: " + ", ".join(failed) + " -- see logs/")
        return 1
    return 0


def selftest():
    import tempfile

    # every scheduled script exists, and the two dropped from daily stay dropped
    for group, cmds in GROUPS.items():
        for cmd in cmds:
            assert os.path.exists(os.path.join(REPO, cmd[0])), (group, cmd)
    daily = [c[0] for c in GROUPS["daily"]]
    assert "scrape_wsb.py" not in daily and "scrape_short_volume.py" not in daily
    assert ["scrape_insiders.py", "--live"] in GROUPS["daily"]
    assert log_name("scrape_short_volume.py") == "short_volume"
    assert log_name("daily.py") == "daily"
    for script in REQUIRES:  # a key requirement names a script that's actually scheduled
        assert any(c[0] == script for cmds in GROUPS.values() for c in cmds), script

    d = tempfile.mkdtemp()
    ok, bad, slow = (os.path.join(d, n) for n in ("ok.py", "bad.py", "slow.py"))
    open(ok, "w").write("print('hello')\n")
    open(bad, "w").write("raise SystemExit(3)\n")
    open(slow, "w").write("import time; time.sleep(30)\n")
    log = os.path.join(d, "t.log")

    real_notify = globals()["notify"]
    sent = []
    globals()["notify"] = lambda title, msg: sent.append(msg)
    try:
        assert run_script([ok], log, cwd=d) == 0
        assert "hello" in open(log).read()  # child output lands in the log
        assert run_script([bad], log, cwd=d) == 3  # exit code passes through
        t = time.time()
        assert run_script([slow], log, warn_secs=1, kill_secs=2, cwd=d) == "timeout"
        assert time.time() - t < 10  # actually stopped, not waited out
        assert sent and "still running" in sent[0]
        assert "stopped after" in open(log).read()
    finally:
        globals()["notify"] = real_notify
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif "--list" in sys.argv:
        for g, cmds in GROUPS.items():
            print(f"{g:<11} " + ", ".join(" ".join(c) for c in cmds))
    elif len(sys.argv) == 2 and sys.argv[1] in GROUPS:
        sys.exit(run_group(sys.argv[1]))
    else:
        sys.exit(f"usage: python run_job.py <{'|'.join(GROUPS)}> | --list | --selftest")
