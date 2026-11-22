#!/usr/bin/env bash
# Integration test for anti-entropy full-table sync: two nodes exchange
# their whole membership tables on a periodic timer (SYNC), not just the
# 16-entry digest, so a node that missed many digests still converges.
#  1. Start a 2-node cluster with --sync-interval-ms 2000.
#  2. Both nodes log a SYNC line within a few seconds — the periodic
#     full-table exchange fires on the timer.
#  3. Both nodes report alive=2 — the exchange converges both sides.
# Without the periodic SYNC, step 2 never happens: the nodes only ever
# send 16-entry digests, and a node that missed updates has to wait for
# the random window to rotate the missing entries through.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-sync-test.XXXXXX)"
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
# Ports 17411-17412 so this test never clashes with the other tests.
(cd "$TMP/n1" && exec "$BIN" --port 17411 --sync-interval-ms 2000 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17412 --seed 127.0.0.1:17411 --sync-interval-ms 2000 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. The cluster converges first (JOIN + digest), so there is a peer to
#    SYNC with.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2-node cluster converges before the first SYNC"
else
    note fail "2-node cluster converges before the first SYNC"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 2. The periodic SYNC fires on both nodes (every 2 s here, allow 15 s).
if wait_for "$TMP/n1.log" "SYNC 127.0.0.1:17412" 15; then
    note ok "node 1 periodically SYNCs its full table to node 2"
else
    note fail "node 1 periodically SYNCs its full table to node 2"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
fi

if wait_for "$TMP/n2.log" "SYNC 127.0.0.1:17411" 15; then
    note ok "node 2 periodically SYNCs its full table to node 1"
else
    note fail "node 2 periodically SYNCs its full table to node 1"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 3. The exchange keeps both sides converged.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "both nodes stay converged on alive=2 after the SYNC exchange"
else
    note fail "both nodes stay converged on alive=2 after the SYNC exchange"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

echo "test_sync: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
