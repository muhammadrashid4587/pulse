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
        if grep -q "$pat" "$log" 2>/dev/null; then return 0; fi
        sleep 0.2
    done
    return 1
}

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
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

echo "refute: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
