#!/usr/bin/env bash
# Integration test for seed rejoin: a node that starts while its seed is
# down must eventually join the cluster once the seed comes up.
#  1. Start node 2 with --seed 127.0.0.1:17401 while node 1 is down.
#     Node 2 suspects, deads and GCs the unanswering seed.
#  2. Start node 1. Node 2 re-sends JOIN to the seed every ~5 s
#     (REJOIN log line) until node 1 answers.
#  3. Both nodes converge on alive=2 — the cluster heals instead of
#     leaving node 2 isolated forever.
# Without the rejoin retry, step 3 never happens: node 1 has no reason
# to contact node 2 and node 2 never contacts node 1 again.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-rejoin-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    # Kill the nodes by pid file (the pid file holds the node pid because
    # the start commands below use exec). jobs -p is not enough: the nodes
    # are children of subshells, not of this script.
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

mkdir -p "$TMP/n1" "$TMP/n2"

# exec: the subshell replaces itself with the node, so $! is the node pid.
# Ports 17401-17402 so this test never clashes with the other tests.
# Node 2 starts first; its seed (node 1) is down. Its control port is
# 18402, so the test can read METRICS at the end.
(cd "$TMP/n2" && exec "$BIN" --port 17402 --seed 127.0.0.1:17401 --control-port 18402 --dead-retention 2 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Node 2 suspects, deads and GCs the unanswering seed.
if wait_for "$TMP/n2.log" "GC 127.0.0.1:17401" 10; then
    note ok "node 2 GCs its down seed"
else
    note fail "node 2 GCs its down seed"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 2. Node 1 comes up. Node 2 must re-JOIN it (REJOIN log line) — the
# retry fires every ~5 s, so allow 15 s.
(cd "$TMP/n1" && exec "$BIN" --port 17401 --dead-retention 2 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
if wait_for "$TMP/n2.log" "REJOIN 127.0.0.1:17401" 15; then
    note ok "node 2 re-sends JOIN to the seed once it is GC'd"
else
    note fail "node 2 re-sends JOIN to the seed once it is GC'd"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 3. The cluster heals: node 1 learns about node 2 from the re-JOIN,
# and both nodes report alive=2.
if wait_for "$TMP/n1.log" "JOIN 127.0.0.1:17402" 10; then
    note ok "late seed learns about the waiting node via re-JOIN"
else
    note fail "late seed learns about the waiting node via re-JOIN"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
fi

if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "both nodes converge on alive=2 after the seed comes up"
else
    note fail "both nodes converge on alive=2 after the seed comes up"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 4. The rejoin retry is visible (0.53.0): METRICS on node 2 counts
# the re-JOINs it sent while its seed was down — at least the one
# after the GC, and usually one while the seed was SUSPECT/DEAD in
# the table too. The count is stable now that the seed is ALIVE.
metrics="$("$CTL" 127.0.0.1:18402 METRICS 2>/dev/null)"
if echo "$metrics" | grep -qE "seed_rejoins=[1-9][0-9]*"; then
    note ok "METRICS counts the seed rejoins (seed_rejoins above 0)"
else
    note fail "METRICS counts the seed rejoins (seed_rejoins above 0)"
    echo "--- metrics"; echo "$metrics"
fi

echo "test_rejoin: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
