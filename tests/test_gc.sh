#!/usr/bin/env bash
# Integration test for DEAD-entry garbage collection: a real 3-node
# cluster over localhost UDP, with a short --dead-retention so the test
# runs in seconds instead of a minute.
#  1. Three nodes converge on the same membership.
#  2. Kill one node; the survivors mark it DEAD.
#  3. After the retention period both survivors GC the entry (GC log
#     lines) and agree the cluster is now 2 nodes (total=2, dead=0).
#  4. Restart the killed node (same data dir, so its incarnation bumps);
#     the survivors accept it back as a fresh member and the cluster
#     converges to 3 again.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-gc-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# exec: the subshell replaces itself with the node, so $! is the node pid.
# Ports 17201-17203 so this test never clashes with test_cluster.sh.
(cd "$TMP/n1" && exec "$BIN" --port 17201 --dead-retention 2 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17202 --seed 127.0.0.1:17201 --dead-retention 2 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17203 --seed 127.0.0.1:17201 --dead-retention 2 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. Convergence: every node reports alive=3.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 6; then
    note ok "3 nodes converge on alive=3"
else
    note fail "3 nodes converge on alive=3"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
    echo "--- n3.log"; tail -20 "$TMP/n3.log"
fi

# 2. Kill node 3 with SIGKILL — an unclean crash, so the survivors
# must detect it by probing, not by a leave announcement (a clean
# SIGTERM announces its leave; see test_leave.sh).
kill -9 "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17203" 6 \
   && wait_for "$TMP/n2.log" "DEAD 127.0.0.1:17203" 6; then
    note ok "killed node is marked DEAD by both survivors"
else
    note fail "killed node is marked DEAD by both survivors"
fi

# 3. After the 2 s retention both survivors GC the entry and agree the
# cluster is 2 nodes (no dead entries left). Detection ~0.5 s + 2 s
# retention + status cadence ~1 s, so allow 15 s.
if wait_for "$TMP/n1.log" "GC 127.0.0.1:17203" 15 \
   && wait_for "$TMP/n2.log" "GC 127.0.0.1:17203" 15; then
    note ok "both survivors GC the dead entry"
else
    note fail "both survivors GC the dead entry"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

if wait_for "$TMP/n1.log" "STATUS alive=2 suspect=0 dead=0 total=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2 suspect=0 dead=0 total=2" 10; then
    note ok "survivors agree the cluster is 2 nodes (dead=0, total=2)"
else
    note fail "survivors agree the cluster is 2 nodes (dead=0, total=2)"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 4. Restart node 3 in the same data dir (incarnation bumps). The
# survivors must accept it back — as a fresh JOIN, since the entry was
# GC'd from their tables — despite the tombstone, because it rejoins
# at a higher incarnation.
(cd "$TMP/n3" && exec "$BIN" --port 17203 --seed 127.0.0.1:17201 --dead-retention 2 >> "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "JOIN 127.0.0.1:17203" 6 \
   && wait_for "$TMP/n2.log" "JOIN 127.0.0.1:17203" 6; then
    note ok "restarted node is accepted back as a fresh member after GC"
else
    note fail "restarted node is accepted back as a fresh member after GC"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 5. And the cluster converges again.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 6; then
    note ok "cluster converges again after GC + flap"
else
    note fail "cluster converges again after GC + flap"
fi

echo "test_gc: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
