#!/usr/bin/env bash
# Integration test for the phi threshold-crossing event (0.60.0):
# a 2-node cluster in --phi-accrual mode, with a WATCH open on the
# survivor.
#  1. Both nodes converge on alive=2.
#  2. Kill node 2 (SIGKILL, an unclean crash): node 1 suspects it
#     on phi, and the WATCH stream carries the SUSPECTED event with
#     a trailing " phi=<value>" — the phi that crossed the
#     threshold, at least 8.0 — while the later DEAD event carries
#     no phi. Since 0.61.0 both lines also carry a trailing
#     " last_seen_ms=<ms>" after the phi (SUSPECTED) or on its own
#     (DEAD), and the patterns below match the full line.
#  3. The same phi rides in the persisted <data-dir>/events file,
#     so the crossing survives a restart and a replay.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-phi-watch-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch.pid" ] && kill "$(cat "$TMP/watch.pid")" 2>/dev/null
    for f in "$TMP"/*.pid; do
        [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null
    done
    sleep 0.2
    rm -rf "$TMP"
}
trap cleanup EXIT

note() {
    if [ "$1" = "ok" ]; then PASS=$((PASS + 1)); echo "ok - $2";
    else FAIL=$((FAIL + 1)); echo "FAIL - $2"; fi
}

# wait_for <logfile> <pattern> <timeout-seconds>
wait_for() {
    local log="$1" pat="$2" timeout="$3" i
    for i in $(seq 1 $((timeout * 5))); do
        if grep -Eq "$pat" "$log" 2>/dev/null; then return 0; fi
        sleep 0.2
    done
    return 1
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17311-17312 (gossip) and 18311-18312 (control) so this test
# never clashes with the other suites.
(cd "$TMP/n1" && exec "$BIN" --port 17311 --control-port 18311 --phi-accrual --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17312 --seed 127.0.0.1:17311 --control-port 18312 --phi-accrual --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge on alive=2 (phi mode)"
else
    note fail "2 nodes converge on alive=2 (phi mode)"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 60 s bounds it if the test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18311" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 2. Kill node 2 — with SIGKILL, an unclean crash: a clean SIGTERM
# would announce its leave and node 1 would mark it DEAD straight
# away, never SUSPECTing it, so no phi crossing would ever be
# measured. Phi needs ~0.7 s of silence to cross 8 here, plus the
# 300 ms suspect timeout, so allow 10 s.
kill -9 "$(cat "$TMP/n2.pid")"
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17312 [0-9]+ [0-9]+ phi=[0-9]+\.[0-9][0-9] last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams SUSPECTED with a phi value"
else
    note fail "WATCH streams SUSPECTED with a phi value"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null; tail -5 "$TMP/n1.log"
fi

# The phi on the event is at least the threshold: a crossing, not
# a stale or zero value.
PHI="$(grep -E '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17312 ' "$TMP/watch1.out" | head -n 1 | grep -o 'phi=[0-9.]*' | cut -d= -f2)"
if [ -n "$PHI" ] && awk -v p="$PHI" 'BEGIN { exit !(p >= 8.0) }'; then
    note ok "SUSPECTED event phi ($PHI) is at least the 8.0 threshold"
else
    note fail "SUSPECTED event phi ($PHI) is at least the 8.0 threshold"
fi

# The DEAD event that follows carries no phi: only a phi-mode
# threshold crossing does.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17312 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "DEAD event carries no phi field"
else
    note fail "DEAD event carries no phi field"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

# 3. The persisted event log carries the same phi: the crossing
# is in <data-dir>/events, so a restart restores it. The file's
# lines start with the sequence number, like the ring's save.
if wait_for "$TMP/n1/events" '^[0-9]+ SUSPECTED 127\.0\.0\.1:17312 [0-9]+ [0-9]+ phi=[0-9]+\.[0-9][0-9] last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "persisted events file carries the phi crossing"
else
    note fail "persisted events file carries the phi crossing"
    cat "$TMP/n1/events" 2>/dev/null
fi

echo "test_phi_watch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
