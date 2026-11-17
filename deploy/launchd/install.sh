#!/bin/sh
# Quantgress -- macOS launchd schedule, the Mac equivalent of deploy/cron.d/.
#
#   sh deploy/launchd/install.sh              # write + load every job
#   sh deploy/launchd/install.sh --uninstall  # unload + delete every job
#   sh deploy/launchd/install.sh --status     # loaded jobs + last exit code
#   sh deploy/launchd/install.sh --dry DIR    # write + lint plists into DIR, load nothing
#   launchctl kickstart gui/$(id -u)/com.quantgress.daily   # run one now
#
# Each job just runs `python run_job.py <group>`. Which scripts a group runs,
# in what order, the timeouts, failure notifications and the API snapshot
# refresh all live in run_job.py, shared with the Linux cron files -- this
# script only owns the *when*.
#
# Paths come from wherever this repo lives, so nothing user-specific is
# committed. A job whose time passes while the Mac is asleep runs once on
# wake (launchd's StartCalendarInterval behavior); a Mac that's powered off
# skips it.

set -e
REPO=$(cd "$(dirname "$0")/../.." && pwd)
AGENTS="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"
JOBS="daily weekly quarterly annual-jan annual-sep"

if [ -x "$REPO/.venv/bin/python" ]; then PY="$REPO/.venv/bin/python"
elif [ -x "$REPO/venv/bin/python" ]; then PY="$REPO/venv/bin/python"
else echo "no .venv/ or venv/ in $REPO -- create one first (see README)" >&2; exit 1
fi

# name -> <StartCalendarInterval> body (local time). Same times as deploy/cron.d.
calendar() {
    case $1 in
        daily)      echo "<dict><key>Hour</key><integer>6</integer><key>Minute</key><integer>0</integer></dict>" ;;
        weekly)     echo "<dict><key>Weekday</key><integer>0</integer><key>Hour</key><integer>7</integer><key>Minute</key><integer>0</integer></dict>" ;;
        quarterly)  for m in 1 4 7 10; do
                        printf "<dict><key>Month</key><integer>%s</integer><key>Day</key><integer>1</integer><key>Hour</key><integer>8</integer><key>Minute</key><integer>0</integer></dict>" $m
                    done ;;
        annual-jan) echo "<dict><key>Month</key><integer>1</integer><key>Day</key><integer>2</integer><key>Hour</key><integer>9</integer><key>Minute</key><integer>0</integer></dict>" ;;
        # Sep 2nd -- after the August House filing-extension deadline
        annual-sep) echo "<dict><key>Month</key><integer>9</integer><key>Day</key><integer>2</integer><key>Hour</key><integer>9</integer><key>Minute</key><integer>0</integer></dict>" ;;
    esac
}

unload() {
    launchctl bootout "$DOMAIN/com.quantgress.$1" 2>/dev/null || true
}

case ${1:-} in
    --uninstall)
        for j in $JOBS; do unload "$j"; rm -f "$AGENTS/com.quantgress.$j.plist"; done
        echo "removed: $JOBS"; exit 0 ;;
    --status)
        launchctl list | awk 'NR==1 || /com\.quantgress\./'; exit 0 ;;
    --dry)
        AGENTS=${2:?--dry needs a directory}; DRY=1 ;;
esac

mkdir -p "$AGENTS" "$REPO/logs"
for j in $JOBS; do
    plist="$AGENTS/com.quantgress.$j.plist"
    cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.quantgress.$j</string>
    <key>ProgramArguments</key>
    <array><string>$PY</string><string>-u</string><string>$REPO/run_job.py</string><string>$j</string></array>
    <key>WorkingDirectory</key><string>$REPO</string>
    <key>StartCalendarInterval</key>
    <array>$(calendar "$j")</array>
    <key>StandardOutPath</key><string>$REPO/logs/launchd-$j.log</string>
    <key>StandardErrorPath</key><string>$REPO/logs/launchd-$j.log</string>
</dict>
</plist>
EOF
    plutil -lint -s "$plist"
    [ -n "${DRY:-}" ] && { echo "wrote $plist"; continue; }
    unload "$j"
    launchctl bootstrap "$DOMAIN" "$plist"
    echo "loaded com.quantgress.$j"
done
"$PY" "$REPO/run_job.py" --list
echo "logs: $REPO/logs/"
