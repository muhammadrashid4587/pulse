#!/usr/bin/env bash
# Integration test for the suspicion-refutation window (0.19.0): real
# nodes over localhost UDP.
#  1. A 2-node cluster converges (alive=2 on both nodes).
#  2. Node 2 is frozen with SIGSTOP: it stops answering probes, so
#     node 1 suspects it. The suspect timeout is stretched to 2000 ms
#     so the suspicion is still open when node 2 is resumed.
#  3. Node 2 is resumed with SIGCONT: it answers again, node 1 logs
#     ALIVE, and the suspicion is refuted — never DEAD.
#  4. METRICS on node 1 reports suspect_refuted=1 with one measured
#     refute-latency sample (the time the peer sat SUSPECT), and JSON
#     carries the same count and latency group.
#  5. Suspicion duration on the REVIVED event (0.70.0): the ALIVE
#     log line, the WATCH REVIVED event and the persisted events
#     file all carry the same suspect_ms — how long the peer sat
#     SUSPECT before it came back — and it agrees with the
#     refute-latency sample in METRICS, since both measure the
#     same window.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-refute-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    for f in "$TMP"/*.pid; do
        [ -f "$f" ] && kill -CONT "$(cat "$f")" 2>/dev/null
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

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

# event_field <file> <pattern-prefix> <field>: the value of a
# trailing field on the first line matching the prefix, or empty.
event_field() {
    grep -m1 -E "$2" "$1" 2>/dev/null \
        | sed -n "s/.* $3=\\([0-9]*\\).*/\\1/p"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17511-17512 (gossip) and 18511-18512 (control) so this test
# never clashes with the other suites.
(cd "$TMP/n1" && exec "$BIN" --port 17511 --control-port 18511 --suspect-timeout-ms 2000 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17512 --seed 127.0.0.1:17511 --control-port 18512 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. The cluster converges.
if wait_for "$TMP/n1.log" "STATUS alive=2" 8 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 8; then
    note ok "2-node cluster converges (alive=2)"
else
    note fail "2-node cluster converges (alive=2)"
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test, so the REVIVED event streams live instead of
# only landing in the persisted file.
(timeout 120 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18511" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 2. Freeze node 2: it stops answering, so node 1 suspects it. The
# frozen process gets no signal and sends no graceful LEAVE.
kill -STOP "$(cat "$TMP/n2.pid")"

if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17512" 6; then
    note ok "node 1 suspects the frozen node"
else
    note fail "node 1 suspects the frozen node"
fi

# 3. Resume node 2 well inside the 2 s suspect window: it answers
# again and the suspicion is refuted, never confirmed dead.
kill -CONT "$(cat "$TMP/n2.pid")"

if wait_for "$TMP/n1.log" "ALIVE 127.0.0.1:17512" 6; then
    note ok "node 1 marks the resumed node ALIVE again"
else
    note fail "node 1 marks the resumed node ALIVE again"
fi
if grep -q "DEAD 127.0.0.1:17512" "$TMP/n1.log" 2>/dev/null; then
    note fail "the suspicion was never declared DEAD"
else
    note ok "the suspicion was never declared DEAD"
fi

# 4. The refutation is counted, with a measured window, in METRICS
# and JSON.
metrics="$(ctl 18511 METRICS)"
refuted="$(echo "$metrics" | sed -n 's/.*suspect_refuted=\([0-9]*\).*/\1/p')"
if [ "$refuted" = "1" ]; then
    note ok "METRICS reports suspect_refuted=1"
else
    note fail "METRICS reports suspect_refuted=1 (got '$refuted')"
    echo "--- ctl output:"; echo "$metrics"
fi
if echo "$metrics" | grep -qE "refute_latency_ms mean=[0-9]+ max=[0-9]+ samples=1"; then
    note ok "METRICS reports one measured refute-latency sample"
else
    note fail "METRICS reports one measured refute-latency sample"
    echo "--- ctl output:"; echo "$metrics"
fi
json="$(ctl 18511 JSON)"
if echo "$json" | grep -q '"suspect_refuted":1' \
   && echo "$json" | grep -q '"refute_latency_ms":{"mean":[0-9]*,"max":[0-9]*,"samples":1}'; then
    note ok "JSON carries suspect_refuted and the refute-latency group"
else
    note fail "JSON carries suspect_refuted and the refute-latency group"
    echo "--- ctl output:"; echo "$json"
fi

# 5. The REVIVED event carries the suspicion duration (0.70.0):
# the log line, the WATCH event and the persisted file all carry
# the same suspect_ms, and it agrees with the refute-latency
# sample in METRICS — both measure the same SUSPECT window.
if wait_for "$TMP/n1.log" 'ALIVE 127\.0\.0\.1:17512 suspect_ms=[0-9]+' 5; then
    note ok "node 1 logs ALIVE with the suspicion duration"
else
    note fail "node 1 logs ALIVE with the suspicion duration"
    grep ' ALIVE ' "$TMP/n1.log" 2>/dev/null
fi
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17512 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ suspect_ms=[0-9]+$' 10; then
    note ok "WATCH streams a REVIVED event with suspect_ms"
else
    note fail "WATCH streams a REVIVED event with suspect_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi
if wait_for "$TMP/n1/events" 'REVIVED 127\.0\.0\.1:17512 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ suspect_ms=[0-9]+$' 10; then
    note ok "persisted events file carries the same REVIVED line"
else
    note fail "persisted events file carries the same REVIVED line"
    cat "$TMP/n1/events" 2>/dev/null
fi
watch_suspect="$(event_field "$TMP/watch1.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17512 ' suspect_ms)"
file_suspect="$(event_field "$TMP/n1/events" 'REVIVED 127\.0\.0\.1:17512 ' suspect_ms)"
refute_mean="$(echo "$metrics" | sed -n 's/.*refute_latency_ms mean=\([0-9]*\).*/\1/p')"
if [ -n "$watch_suspect" ] && [ "$watch_suspect" = "$file_suspect" ] \
   && [ "$watch_suspect" = "$refute_mean" ]; then
    note ok "suspect_ms agrees across WATCH, file and METRICS ($watch_suspect ms)"
else
    note fail "suspect_ms agrees across WATCH, file and METRICS (watch='$watch_suspect' file='$file_suspect' metrics='$refute_mean')"
fi

echo "refute: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
