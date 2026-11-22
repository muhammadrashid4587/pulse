#!/usr/bin/env bash
# Integration test: a real 3-node cluster over localhost UDP.
#  1. Three nodes converge on the same membership.
#  2. Kill one node; the other two mark it DEAD within the timeout.
#  3. The survivors agree on the membership (alive=2).
#  4. Restart the killed node (same data dir, so its incarnation bumps);
#     the survivors clear it back to ALIVE.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-cluster-test.XXXXXX)"
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
(cd "$TMP/n1" && exec "$BIN" --port 17101 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17102 --seed 127.0.0.1:17101 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17103 --seed 127.0.0.1:17101 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

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

# 2. Kill node 3; the survivors must mark it DEAD.
kill "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17103" 6 \
   && wait_for "$TMP/n2.log" "DEAD 127.0.0.1:17103" 6; then
    note ok "killed node is marked DEAD by both survivors"
else
    note fail "killed node is marked DEAD by both survivors"
fi

# 3. Survivors agree: alive=2, dead=1.
if wait_for "$TMP/n1.log" "STATUS alive=2 suspect=0 dead=1" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=2 suspect=0 dead=1" 6; then
    note ok "survivors agree on membership (alive=2, dead=1)"
else
    note fail "survivors agree on membership (alive=2, dead=1)"
fi

# 4. Flap: restart node 3 in the same data dir (incarnation bumps).
(cd "$TMP/n3" && exec "$BIN" --port 17103 --seed 127.0.0.1:17101 >> "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "ALIVE 127.0.0.1:17103" 6 \
   && wait_for "$TMP/n2.log" "ALIVE 127.0.0.1:17103" 6; then
    note ok "restarted node is cleared back to ALIVE"
else
    note fail "restarted node is cleared back to ALIVE"
fi

# 5. And the cluster converges again.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6; then
    note ok "cluster converges again after flap"
else
    note fail "cluster converges again after flap"
fi

echo "test_cluster: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
