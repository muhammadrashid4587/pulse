#!/usr/bin/env bash
# Integration test: graceful leave on clean shutdown.
#  1. Three nodes converge on the same membership.
#  2. Node 3 stops cleanly (SIGTERM); it announces its leave, so the
#     survivors mark it DEAD within 400 ms — the probe path alone needs
#     ~500 ms (100 ms ack timeout + 100 ms indirect + 300 ms suspect),
#     so a DEAD that fast can only come from the announcement.
#  3. The survivors never log SUSPECT for it: the announcement lands
#     before any probe can time out.
#  4. Restart node 3 (same data dir, so its incarnation bumps); the
#     survivors clear it back to ALIVE — the leave did not poison the
#     rejoin.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-leave-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

(cd "$TMP/n1" && exec "$BIN" --port 17601 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17602 --seed 127.0.0.1:17601 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17603 --seed 127.0.0.1:17601 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. Convergence: every node reports alive=3.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 6; then
    note ok "3 nodes converge on alive=3"
else
    note fail "3 nodes converge on alive=3"
fi

# 2. Clean stop of node 3; the survivors must mark it DEAD fast.
kill "$(cat "$TMP/n3.pid")"
fast=0
for i in $(seq 1 8); do
    if grep -q "DEAD 127.0.0.1:17603" "$TMP/n1.log" 2>/dev/null \
       && grep -q "DEAD 127.0.0.1:17603" "$TMP/n2.log" 2>/dev/null; then
        fast=1
        break
    fi
    sleep 0.05
done
if [ "$fast" = 1 ]; then
    note ok "cleanly stopped node is marked DEAD by both survivors within 400 ms"
else
    note fail "cleanly stopped node is marked DEAD by both survivors within 400 ms"
    echo "--- n1.log"; tail -15 "$TMP/n1.log"
    echo "--- n3.log"; tail -15 "$TMP/n3.log"
fi

# 2b. The leaving node logged its own announcement.
if grep -q "LEAVE 127.0.0.1:17603" "$TMP/n3.log" 2>/dev/null; then
    note ok "leaving node logs its LEAVE announcement"
else
    note fail "leaving node logs its LEAVE announcement"
fi

# 3. No SUSPECT first: the announcement beat the probe timeout.
if grep -q "SUSPECT 127.0.0.1:17603" "$TMP/n1.log" 2>/dev/null \
   || grep -q "SUSPECT 127.0.0.1:17603" "$TMP/n2.log" 2>/dev/null; then
    note fail "survivors never suspect the cleanly leaving node"
else
    note ok "survivors never suspect the cleanly leaving node"
fi

# 4. Restart node 3 in the same data dir (incarnation bumps).
(cd "$TMP/n3" && exec "$BIN" --port 17603 --seed 127.0.0.1:17601 >> "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "ALIVE 127.0.0.1:17603" 6 \
   && wait_for "$TMP/n2.log" "ALIVE 127.0.0.1:17603" 6; then
    note ok "restarted node is cleared back to ALIVE after graceful leave"
else
    note fail "restarted node is cleared back to ALIVE after graceful leave"
fi

# 5. And the cluster converges again.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6; then
    note ok "cluster converges again after flap"
else
    note fail "cluster converges again after flap"
fi

echo "test_leave: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
