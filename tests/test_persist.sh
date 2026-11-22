#!/usr/bin/env bash
# Integration test for membership persistence: a node remembers its peers
# across restart, and stale entries are re-probed, not trusted.
#  1. A 3-node cluster converges. Node 3 is stopped cleanly (SIGTERM),
#     which saves its membership to <data-dir>/members.
#  2. Node 3 restarts with NO --seed and the same data dir. It logs
#     RESTORE and re-discovers nodes 1 and 2 from the state file, so
#     all three converge on alive=3 again.
#  3. A fresh node 4 starts from a hand-written members file holding
#     one real peer (node 1) and one bogus peer that never answers
#     (127.0.0.1:17999). Node 4 converges with node 1 — the file is a
#     real source of peers — but the bogus peer is re-probed and goes
#     SUSPECT, DEAD, then GC: a stale entry is never trusted as alive.
# Without persistence, step 2 never happens: node 3 has no seed and no
# reason to contact anyone, so it stays a 1-node cluster forever.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-persist-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3" "$TMP/n4"

# Ports 17501-17504 so this test never clashes with the other tests.
(cd "$TMP/n1" && exec "$BIN" --port 17501 --control-port 18501 --dead-retention 2 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17502 --seed 127.0.0.1:17501 --control-port 18502 --dead-retention 2 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17503 --seed 127.0.0.1:17501 --control-port 18503 --dead-retention 2 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. The 3-node cluster converges.
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 10; then
    note ok "3-node cluster converges on alive=3"
else
    note fail "3-node cluster converges on alive=3"
    echo "--- n1.log"; tail -5 "$TMP/n1.log"
    echo "--- n3.log"; tail -5 "$TMP/n3.log"
fi

# 2a. Clean shutdown of node 3 saves its membership to disk.
kill "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n3.log" "STOP 127.0.0.1:17503" 5; then
    note ok "node 3 shuts down cleanly"
else
    note fail "node 3 shuts down cleanly"
fi
if grep -q "127.0.0.1:17501" "$TMP/n3/members" 2>/dev/null \
   && grep -q "127.0.0.1:17502" "$TMP/n3/members" 2>/dev/null; then
    note ok "shutdown saved both peers to the members file"
else
    note fail "shutdown saved both peers to the members file"
    echo "--- n3/members"; cat "$TMP/n3/members" 2>/dev/null || echo "(no file)"
fi

# 2b. Restart node 3 with NO seed and the same data dir: it must
# restore its peers from the state file and rejoin the cluster.
sleep 1
(cd "$TMP/n3" && exec "$BIN" --port 17503 --control-port 18503 --dead-retention 2 > "$TMP/n3b.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n3b.log" "RESTORE 2 members" 5; then
    note ok "restarted node restores 2 members from the state file"
else
    note fail "restarted node restores 2 members from the state file"
    echo "--- n3b.log"; tail -10 "$TMP/n3b.log"
fi
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n3b.log" "STATUS alive=3" 10; then
    note ok "seedless restart re-discovers its old peers (alive=3)"
else
    note fail "seedless restart re-discovers its old peers (alive=3)"
    echo "--- n1.log"; tail -5 "$TMP/n1.log"
    echo "--- n3b.log"; tail -5 "$TMP/n3b.log"
fi

# 3. Stale entries are re-probed, not trusted: node 4 starts from a
# hand-written members file with one real peer and one bogus peer.
printf '127.0.0.1:17999 1 A 0\n127.0.0.1:17501 1 A 0\n' > "$TMP/n4/members"
(cd "$TMP/n4" && exec "$BIN" --port 17504 --control-port 18504 --dead-retention 2 > "$TMP/n4.log" 2>&1 & echo $! > "$TMP/n4.pid")
if wait_for "$TMP/n4.log" "RESTORE 2 members" 5; then
    note ok "node 4 restores the hand-written members file"
else
    note fail "node 4 restores the hand-written members file"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi
if wait_for "$TMP/n1.log" "JOIN 127.0.0.1:17504" 10; then
    note ok "node 1 learns about node 4 via the restored peer"
else
    note fail "node 1 learns about node 4 via the restored peer"
    echo "--- n1.log"; tail -5 "$TMP/n1.log"
fi
if wait_for "$TMP/n4.log" "SUSPECT 127.0.0.1:17999" 10; then
    note ok "bogus restored peer is suspected, not trusted as alive"
else
    note fail "bogus restored peer is suspected, not trusted as alive"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi
if wait_for "$TMP/n4.log" "DEAD 127.0.0.1:17999" 10; then
    note ok "bogus restored peer is marked dead"
else
    note fail "bogus restored peer is marked dead"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi
if wait_for "$TMP/n4.log" "GC 127.0.0.1:17999" 10; then
    note ok "bogus restored peer is garbage-collected"
else
    note fail "bogus restored peer is garbage-collected"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi
if wait_for "$TMP/n4.log" "STATUS alive=4" 10; then
    note ok "node 4 settles on alive=4 (self + all three real peers)"
else
    note fail "node 4 settles on alive=4 (self + all three real peers)"
    echo "--- n4.log"; tail -5 "$TMP/n4.log"
fi

echo "test_persist: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
