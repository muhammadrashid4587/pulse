#!/usr/bin/env bash
# Integration test for WATCH event-log persistence across restarts
# (0.38.0): the node appends each membership event to
# <data-dir>/events as it records it, and on startup loads the file
# back into the ring, so sequence numbers continue across a restart
# instead of resetting to 1 and a WATCH FROM <seq> issued after a
# restart can still replay what happened before it.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. A live WATCH on node 1 records the sequence numbers
#     of node 3's lifecycle: JOINED when it joins through --seed
#     only (a peer-file peer is already in the table at startup,
#     so it never generates a JOINED), SUSPECTED then DEAD when it
#     is killed — and a FLAP event right after the DEAD (0.79.0),
#     since the ALIVE -> SUSPECT -> DEAD leg completes a flap.
#  2. Node 1 is stopped cleanly (SIGTERM): the shutdown compacts the
#     persisted log, and <data-dir>/events holds exactly the four
#     events the ring held.
#  3. Node 1 restarts from the same data dir: it logs
#     "EVENTS 4 restored", and the restored SUSPECT entry for node 3
#     goes DEAD again on its own suspect timeout — a new event, with
#     a sequence number past the pre-restart ones.
#  4. Node 3 restarts: node 1 streams REVIVED live, with a sequence
#     number past the pre-restart DEAD, and a WATCH FROM 1 opened
#     after the restart replays JOINED, SUSPECTED, DEAD and FLAP with the
#     same sequence numbers the live stream carried before the
#     restart — and no GAP line, since nothing was evicted.
# The watchers run under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-events-persist-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch1.pid" ] && kill "$(cat "$TMP/watch1.pid")" 2>/dev/null
    [ -f "$TMP/watch2.pid" ] && kill "$(cat "$TMP/watch2.pid")" 2>/dev/null
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17851-17853 (gossip) and 18851-18853 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17851
127.0.0.1:17852
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17851 --peer-file "$TMP/peers" --control-port 18851 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17852 --peer-file "$TMP/peers" --control-port 18852 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the live WATCH on node 1 in the background: it stays open
# until node 1 is stopped.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18851" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch1.pid")
if ! wait_for "$TMP/watch1.out" "^END$" 10; then
    note fail "live WATCH opens on node 1"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# Node 3 joins through --seed only: the live stream carries
# JOINED for it. Then it is killed: SUSPECTED, then DEAD.
(cd "$TMP/n3" && exec "$BIN" --port 17853 --seed 127.0.0.1:17851 --control-port 18853 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "live WATCH streams JOINED when node 3 joins"
else
    note fail "live WATCH streams JOINED when node 3 joins"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi
kill -9 "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17853" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "live WATCH streams SUSPECTED then DEAD when node 3 is killed"
else
    note fail "live WATCH streams SUSPECTED then DEAD when node 3 is killed"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi

# The sequence numbers the live stream carried for node 3.
JOIN_SEQ="$(grep '^EVENT .* JOINED 127\.0\.0\.1:17853 ' "$TMP/watch1.out" | head -n 1 | awk '{print $2}')"
SUSPECT_SEQ="$(grep '^EVENT .* SUSPECTED 127\.0\.0\.1:17853 ' "$TMP/watch1.out" | head -n 1 | awk '{print $2}')"
DEAD_SEQ="$(grep '^EVENT .* DEAD 127\.0\.0\.1:17853 ' "$TMP/watch1.out" | head -n 1 | awk '{print $2}')"
FLAP_SEQ="$(grep '^EVENT .* FLAP 127\.0\.0\.1:17853 ' "$TMP/watch1.out" | head -n 1 | awk '{print $2}')"

# 2. Node 1 stops cleanly: the shutdown compacts the persisted
# log to exactly the events the ring held.
kill -TERM "$(cat "$TMP/n1.pid")"
for i in $(seq 1 50); do kill -0 "$(cat "$TMP/n1.pid")" 2>/dev/null || break; sleep 0.2; done
if [ -f "$TMP/n1/events" ] && [ "$(wc -l < "$TMP/n1/events")" -eq 4 ] \
   && grep -Eq " FLAP 127.0.0.1:17853 [0-9]+ [0-9]+ .* flaps_10m=1 flap_count=1\$" "$TMP/n1/events"; then
    note ok "clean shutdown leaves <data-dir>/events with the 4 recorded events, FLAP last with flap_count 1"
else
    note fail "clean shutdown leaves <data-dir>/events with the 4 recorded events, FLAP last with flap_count 1"
    ls -l "$TMP/n1/" 2>/dev/null; cat "$TMP/n1/events" 2>/dev/null
fi

# 3. Node 1 restarts from the same data dir: it restores the
# events, and the restored SUSPECT entry for node 3 goes DEAD
# again — a new event past the pre-restart sequences.
(cd "$TMP/n1" && exec "$BIN" --port 17851 --peer-file "$TMP/peers" --control-port 18851 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
if wait_for "$TMP/n1.log" "EVENTS 4 restored" 10; then
    note ok "restarted node 1 restores 4 events from <data-dir>/events"
else
    note fail "restarted node 1 restores 4 events from <data-dir>/events"
    tail -5 "$TMP/n1.log"
fi
# The DEAD line for node 3 appears again in the fresh log: the
# restored SUSPECT entry went DEAD again after the restart.
# (n1.log was truncated on restart, so this is the post-restart
# DEAD, not the pre-restart one.)
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17853" 10; then
    note ok "restored SUSPECT entry goes DEAD again after the restart"
else
    note fail "restored SUSPECT entry goes DEAD again after the restart"
    tail -5 "$TMP/n1.log"
fi

# 4. A WATCH FROM 1 on the restarted node replays the pre-restart
# events with their original sequence numbers, then streams
# REVIVED live when node 3 restarts — with a sequence number past
# the pre-restart DEAD.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18851" WATCH FROM 1 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^END$" 10 \
   && wait_for "$TMP/watch2.out" "^EVENT $JOIN_SEQ JOINED 127\\.0\\.0\\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$" 10 \
   && wait_for "$TMP/watch2.out" "^EVENT $SUSPECT_SEQ SUSPECTED 127\\.0\\.0\\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$" 10 \
   && wait_for "$TMP/watch2.out" "^EVENT $DEAD_SEQ DEAD 127\\.0\\.0\\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$" 10 \
   && wait_for "$TMP/watch2.out" "^EVENT $FLAP_SEQ FLAP 127\\.0\\.0\\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)? flaps_10m=1 flap_count=1$" 10; then
    note ok "WATCH FROM 1 after restart replays JOINED, SUSPECTED, DEAD, FLAP with the original sequence numbers"
else
    note fail "WATCH FROM 1 after restart replays JOINED, SUSPECTED, DEAD, FLAP with the original sequence numbers"
    echo "--- watch2.out"; cat "$TMP/watch2.out"
    echo "join=$JOIN_SEQ suspect=$SUSPECT_SEQ dead=$DEAD_SEQ flap=$FLAP_SEQ"
fi
sleep 1
if [ "$(grep -c '^GAP ' "$TMP/watch2.out")" -eq 0 ]; then
    note ok "replay after restart carries no GAP line"
else
    note fail "replay after restart carries no GAP line"
    grep '^GAP ' "$TMP/watch2.out"
fi

(cd "$TMP/n3" && exec "$BIN" --port 17853 --seed 127.0.0.1:17851 --control-port 18853 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch2.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17853 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "restarted node 3 is REVIVED live on the resumed stream"
else
    note fail "restarted node 3 is REVIVED live on the resumed stream"
    echo "--- watch2.out"; cat "$TMP/watch2.out"
fi
REVIVED_SEQ="$(grep '^EVENT .* REVIVED 127\.0\.0\.1:17853 ' "$TMP/watch2.out" | head -n 1 | awk '{print $2}')"
if [ -n "$REVIVED_SEQ" ] && [ "$REVIVED_SEQ" -gt "$DEAD_SEQ" ]; then
    note ok "REVIVED after the restart has a sequence past the pre-restart DEAD ($REVIVED_SEQ > $DEAD_SEQ)"
else
    note fail "REVIVED after the restart has a sequence past the pre-restart DEAD (revived=$REVIVED_SEQ dead=$DEAD_SEQ)"
fi

# The second watcher is still alive at the end: the stream stayed
# open for the whole test instead of dropping.
if kill -0 "$(cat "$TMP/watch2.pid")" 2>/dev/null; then
    note ok "WATCH connection stayed open for the whole test"
else
    note fail "WATCH connection stayed open for the whole test"
fi

echo "test_watch_events_persist: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
