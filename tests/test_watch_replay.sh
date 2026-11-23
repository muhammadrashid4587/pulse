#!/usr/bin/env bash
# Integration test for WATCH replay / resume (0.31.0): a WATCH
# client can pass a starting sequence number — WATCH FROM <seq> —
# and the server replays the buffered events from that seq, in
# order, after the snapshot and END, before streaming live events.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. A live WATCH on node 1 records the sequence numbers
#     of node 3's lifecycle: JOINED when it joins, SUSPECTED then
#     DEAD when it is killed.
#  2. A second WATCH on node 1, opened after the kill with
#     FROM <join-seq>, replays exactly JOINED, SUSPECTED, DEAD for
#     node 3, in order, with the same sequence numbers the live
#     stream carried — and no GAP line, since nothing was evicted.
#  3. Node 3 restarts: the resumed stream carries REVIVED live,
#     after the replay.
#  4. A WATCH FROM 0 replays from the oldest buffered event: it
#     carries the JOINED for node 3 too.
#  5. A bad FROM is refused up front: WATCH FROM answers
#     "ERR invalid watch filter FROM requires a sequence number"
#     and WATCH FROM xyz answers
#     "ERR invalid watch filter invalid FROM seq xyz".
# The gap path — a FROM below the oldest buffered event, answered
# with a GAP line and a replay from the oldest — needs 128 events
# to evict the ring, so it is covered deterministically in the
# unit test (tests/test_control.c), not here.
# The watchers run under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-replay-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch1.pid" ] && kill "$(cat "$TMP/watch1.pid")" 2>/dev/null
    [ -f "$TMP/watch2.pid" ] && kill "$(cat "$TMP/watch2.pid")" 2>/dev/null
    [ -f "$TMP/watch3.pid" ] && kill "$(cat "$TMP/watch3.pid")" 2>/dev/null
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

# Ports 17841-17843 (gossip) and 18841-18843 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17841
127.0.0.1:17842
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17841 --peer-file "$TMP/peers" --control-port 18841 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17842 --peer-file "$TMP/peers" --control-port 18842 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the live WATCH on node 1 in the background: it stays open
# for the whole test (timeout 60 s bounds it if the test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18841" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch1.pid")
if ! wait_for "$TMP/watch1.out" "^END$" 10; then
    note fail "live WATCH opens on node 1"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# Node 3 joins through --seed only: the live stream carries
# JOINED for it. Then it is killed: SUSPECTED, then DEAD.
(cd "$TMP/n3" && exec "$BIN" --port 17843 --seed 127.0.0.1:17841 --control-port 18843 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17843 1 [0-9]+$' 10; then
    note ok "live WATCH streams JOINED when node 3 joins"
else
    note fail "live WATCH streams JOINED when node 3 joins"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi
kill -9 "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17843" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17843 [0-9]+ [0-9]+$' 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17843 [0-9]+ [0-9]+$' 10; then
    note ok "live WATCH streams SUSPECTED then DEAD when node 3 is killed"
else
    note fail "live WATCH streams SUSPECTED then DEAD when node 3 is killed"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi

# The sequence numbers the live stream carried for node 3's
# lifecycle: the resume point and the expected replay.
JOIN_SEQ="$(grep '^EVENT .* JOINED 127\.0\.0\.1:17843 ' "$TMP/watch1.out" | head -n 1 | awk '{print $2}')"
SUSPECT_SEQ="$(grep '^EVENT .* SUSPECTED 127\.0\.0\.1:17843 ' "$TMP/watch1.out" | head -n 1 | awk '{print $2}')"
DEAD_SEQ="$(grep '^EVENT .* DEAD 127\.0\.0\.1:17843 ' "$TMP/watch1.out" | head -n 1 | awk '{print $2}')"

# 2. A second WATCH, opened after the kill with FROM <join-seq>,
# replays JOINED, SUSPECTED, DEAD for node 3 with the same
# sequence numbers — and no GAP line, since nothing was evicted.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18841" WATCH FROM "$JOIN_SEQ" > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^END$" 10 \
   && wait_for "$TMP/watch2.out" "^EVENT $JOIN_SEQ JOINED 127\\.0\\.0\\.1:17843 1 [0-9]+$" 10 \
   && wait_for "$TMP/watch2.out" "^EVENT $SUSPECT_SEQ SUSPECTED 127\\.0\\.0\\.1:17843 [0-9]+ [0-9]+$" 10 \
   && wait_for "$TMP/watch2.out" "^EVENT $DEAD_SEQ DEAD 127\\.0\\.0\\.1:17843 [0-9]+ [0-9]+$" 10; then
    note ok "WATCH FROM <join-seq> replays JOINED, SUSPECTED, DEAD with the same sequence numbers"
else
    note fail "WATCH FROM <join-seq> replays JOINED, SUSPECTED, DEAD with the same sequence numbers"
    echo "--- watch2.out"; cat "$TMP/watch2.out"
    echo "join=$JOIN_SEQ suspect=$SUSPECT_SEQ dead=$DEAD_SEQ"
fi
sleep 1
if [ "$(grep -c '^GAP ' "$TMP/watch2.out")" -eq 0 ]; then
    note ok "replay with nothing evicted carries no GAP line"
else
    note fail "replay with nothing evicted carries no GAP line"
    grep '^GAP ' "$TMP/watch2.out"
fi

# 3. Node 3 restarts: the resumed stream carries REVIVED live,
# after the replay.
(cd "$TMP/n3" && exec "$BIN" --port 17843 --seed 127.0.0.1:17841 --control-port 18843 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch2.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17843 [0-9]+ [0-9]+$' 10; then
    note ok "resumed WATCH streams REVIVED live after the replay"
else
    note fail "resumed WATCH streams REVIVED live after the replay"
    echo "--- watch2.out"; cat "$TMP/watch2.out"
fi
# The REVIVED comes after the replayed DEAD in the stream.
if [ "$(grep -n "^EVENT $DEAD_SEQ DEAD" "$TMP/watch2.out" | head -n 1 | cut -d: -f1)" -lt \
     "$(grep -n '^EVENT .* REVIVED 127\.0\.0\.1:17843 ' "$TMP/watch2.out" | head -n 1 | cut -d: -f1)" ]; then
    note ok "replayed events come before live events on the resumed stream"
else
    note fail "replayed events come before live events on the resumed stream"
    grep '^EVENT' "$TMP/watch2.out"
fi

# 4. A WATCH FROM 0 replays from the oldest buffered event: it
# carries the JOINED for node 3 too.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18841" WATCH FROM 0 > "$TMP/watch3.out" 2>&1 & echo $! > "$TMP/watch3.pid")
if wait_for "$TMP/watch3.out" "^END$" 10 \
   && wait_for "$TMP/watch3.out" "^EVENT $JOIN_SEQ JOINED 127\\.0\\.0\\.1:17843 1 [0-9]+$" 10; then
    note ok "WATCH FROM 0 replays from the oldest buffered event"
else
    note fail "WATCH FROM 0 replays from the oldest buffered event"
    echo "--- watch3.out"; cat "$TMP/watch3.out"
fi

# 5. A bad FROM is refused up front.
if [ "$(ctl 18841 WATCH FROM)" = "ERR invalid watch filter FROM requires a sequence number" ] \
   && [ "$(ctl 18841 WATCH FROM xyz)" = "ERR invalid watch filter invalid FROM seq xyz" ]; then
    note ok "bad FROM is refused with ERR invalid watch filter"
else
    note fail "bad FROM is refused with ERR invalid watch filter"
    ctl 18841 WATCH FROM
    ctl 18841 WATCH FROM xyz
fi

# The watchers are still alive at the end: the streams stayed open
# for the whole test instead of dropping.
if kill -0 "$(cat "$TMP/watch1.pid")" 2>/dev/null \
   && kill -0 "$(cat "$TMP/watch2.pid")" 2>/dev/null \
   && kill -0 "$(cat "$TMP/watch3.pid")" 2>/dev/null; then
    note ok "WATCH connections stayed open for the whole test"
else
    note fail "WATCH connections stayed open for the whole test"
fi

echo "test_watch_replay: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
