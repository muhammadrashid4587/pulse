#!/usr/bin/env bash
# Integration test for the adaptive probe interval: real nodes over
# localhost UDP, one killed with SIGKILL so no graceful LEAVE is sent.
#  1. A 3-node cluster converges (alive=3 on all nodes).
#  2. Kill node 3; the survivors suspect it, and while it is SUSPECT
#     they log an ADAPTIVE fast line and run probe cycles at the fast
#     interval — node 1 was started with --probe-interval-fast-ms 40,
#     so it must log probe=40ms; node 2 uses the default half of the
#     100 ms base, probe=50ms.
#  3. METRICS on a survivor reports adaptive_fast_cycles > 0, and
#     JSON carries the same count.
#  4. Once the dead peer is declared DEAD the table is suspect-free
#     again, so the survivors log ADAPTIVE back to the base interval
#     (probe=100ms stable) — the speedup only lasts while the cluster
#     is actually unstable.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-adaptive-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17501-17503 (gossip) and 18501-18503 (control) so this test
# never clashes with the other suites.
(cd "$TMP/n1" && exec "$BIN" --port 17501 --control-port 18501 --probe-interval-fast-ms 40 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17502 --seed 127.0.0.1:17501 --control-port 18502 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17503 --seed 127.0.0.1:17501 --control-port 18503 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. The cluster converges.
if wait_for "$TMP/n1.log" "STATUS alive=3" 8 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 8 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 8; then
    note ok "3-node cluster converges (alive=3)"
else
    note fail "3-node cluster converges (alive=3)"
fi

# A stable cluster never runs fast: no ADAPTIVE line before the kill.
if grep -q "ADAPTIVE" "$TMP/n1.log" 2>/dev/null; then
    note fail "stable cluster logs no ADAPTIVE line before the kill"
else
    note ok "stable cluster logs no ADAPTIVE line before the kill"
fi

# 2. Kill node 3 with SIGKILL: no graceful LEAVE, so the survivors
# must suspect it on their own probe timeouts.
kill -9 "$(cat "$TMP/n3.pid")"

if wait_for "$TMP/n1.log" "ADAPTIVE fast probe=40ms" 6; then
    note ok "node 1 speeds up to its configured 40 ms fast interval while a peer is suspect"
else
    note fail "node 1 speeds up to its configured 40 ms fast interval while a peer is suspect"
fi
if wait_for "$TMP/n2.log" "ADAPTIVE fast probe=50ms" 6; then
    note ok "node 2 speeds up to the default 50 ms fast interval while a peer is suspect"
else
    note fail "node 2 speeds up to the default 50 ms fast interval while a peer is suspect"
fi

# 3. The fast cycles are counted in METRICS and JSON.
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17503" 6; then
    note ok "survivors declare the killed node DEAD"
else
    note fail "survivors declare the killed node DEAD"
fi
metrics="$(ctl 18501 METRICS)"
fast="$(echo "$metrics" | sed -n 's/.*adaptive_fast_cycles=\([0-9]*\).*/\1/p')"
if [ -n "$fast" ] && [ "$fast" -gt 0 ]; then
    note ok "METRICS reports adaptive_fast_cycles=$fast (> 0)"
else
    note fail "METRICS reports adaptive_fast_cycles > 0 (got '$fast')"
    echo "--- ctl output:"; echo "$metrics"
fi
json="$(ctl 18501 JSON)"
if echo "$json" | grep -q '"adaptive_fast_cycles":[1-9]'; then
    note ok "JSON carries adaptive_fast_cycles > 0"
else
    note fail "JSON carries adaptive_fast_cycles > 0"
    echo "--- ctl output:"; echo "$json"
fi

# 4. With the suspect gone the interval returns to base.
if wait_for "$TMP/n1.log" "ADAPTIVE probe=100ms stable" 6; then
    note ok "node 1 returns to the 100 ms base interval once the table is suspect-free"
else
    note fail "node 1 returns to the 100 ms base interval once the table is suspect-free"
fi

echo "adaptive: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
