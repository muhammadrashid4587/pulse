#!/usr/bin/env bash
# Integration test for phi-accrual failure detection: a real 3-node
# cluster over localhost UDP, all nodes started with --phi-accrual.
#  1. Three nodes converge on the same membership.
#  2. Kill one node; the survivors must SUSPECT it with a phi value
#     logged, then mark it DEAD.
#  3. The survivors agree on the membership (alive=2, dead=1).
#  4. Restart the killed node (same data dir, so its incarnation bumps);
#     the survivors clear it back to ALIVE and the cluster converges.
# The fixed-timeout path is covered separately by test_cluster.sh;
# this test proves suspicion here comes from phi, not a timeout.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-phi-test.XXXXXX)"
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
# Ports 17301-17303 so this test never clashes with the other suites.
(cd "$TMP/n1" && exec "$BIN" --port 17301 --phi-accrual > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17302 --seed 127.0.0.1:17301 --phi-accrual > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17303 --seed 127.0.0.1:17301 --phi-accrual > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 0. The nodes report phi mode in their START lines.
if wait_for "$TMP/n1.log" "START .* phi=on threshold=8.0" 6 \
   && wait_for "$TMP/n2.log" "START .* phi=on threshold=8.0" 6; then
    note ok "nodes report phi=on in their START lines"
else
    note fail "nodes report phi=on in their START lines"
fi

# 1. Convergence: every node reports alive=3.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 6; then
    note ok "3 nodes converge on alive=3 (phi mode)"
else
    note fail "3 nodes converge on alive=3 (phi mode)"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
    echo "--- n3.log"; tail -20 "$TMP/n3.log"
fi

# 2. Kill node 3 — with SIGKILL, an unclean crash: a clean SIGTERM
# would announce its leave and the survivors would mark it DEAD
# straight away, never SUSPECTing it, so phi suspicion would never
# be exercised. The survivors must SUSPECT it — at least one of
# them by phi (the first suspicion always originates from a phi
# evaluation; the other may learn it a cycle earlier via gossip) —
# then mark it DEAD. Phi needs ~0.7 s of silence to cross 8 here,
# plus the 300 ms suspect timeout, so allow 10 s.
kill -9 "$(cat "$TMP/n3.pid")"
if (wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17303 phi=" 10 \
     || wait_for "$TMP/n2.log" "SUSPECT 127.0.0.1:17303 phi=" 10) \
   && wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17303" 10 \
   && wait_for "$TMP/n2.log" "SUSPECT 127.0.0.1:17303" 10; then
    note ok "killed node is SUSPECTED with a phi value by a survivor"
else
    note fail "killed node is SUSPECTED with a phi value by a survivor"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17303" 10 \
   && wait_for "$TMP/n2.log" "DEAD 127.0.0.1:17303" 10; then
    note ok "killed node is marked DEAD by both survivors (phi mode)"
else
    note fail "killed node is marked DEAD by both survivors (phi mode)"
fi

# 3. Survivors agree: alive=2, dead=1.
if wait_for "$TMP/n1.log" "STATUS alive=2 suspect=0 dead=1" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=2 suspect=0 dead=1" 6; then
    note ok "survivors agree on membership (alive=2, dead=1, phi mode)"
else
    note fail "survivors agree on membership (alive=2, dead=1, phi mode)"
fi

# 4. Flap: restart node 3 in the same data dir (incarnation bumps).
(cd "$TMP/n3" && exec "$BIN" --port 17303 --seed 127.0.0.1:17301 --phi-accrual >> "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "ALIVE 127.0.0.1:17303" 6 \
   && wait_for "$TMP/n2.log" "ALIVE 127.0.0.1:17303" 6; then
    note ok "restarted node is cleared back to ALIVE (phi mode)"
else
    note fail "restarted node is cleared back to ALIVE (phi mode)"
fi

# 5. And the cluster converges again.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6; then
    note ok "cluster converges again after flap (phi mode)"
else
    note fail "cluster converges again after flap (phi mode)"
fi

echo "test_phi: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
