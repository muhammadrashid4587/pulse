#!/usr/bin/env bash
# Integration test for WATCH filtering (0.30.0): a WATCH client can
# subscribe to only certain event types and/or only certain peer
# ids, with the filter applied server-side before sending.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. A filtered WATCH on node 1 — types JOINED,DEAD for
#     peer 127.0.0.1:17833, which has not joined yet — opens with
#     "OK watching" and END but no SNAPSHOT lines at all, and a
#     second WATCH filtered to peer 127.0.0.1:17822 snapshots only
#     that peer.
#  2. Node 3 joins: the first stream carries JOINED for it, the
#     second carries nothing.
#  3. Node 3 is stopped (SIGSTOP): node 1 suspects it, but neither
#     stream carries SUSPECTED — the first stream's type filter
#     drops it, the second stream's peer filter drops it. Resumed
#     (SIGCONT) it comes back, and neither stream carries REVIVED.
#  4. Node 3 is killed: the first stream carries DEAD for it, the
#     second still carries nothing.
#  5. A bad filter is refused up front: WATCH BOGUS answers
#     "ERR invalid watch filter unknown event type BOGUS".
#  6. The first stream's EVENT lines are exactly JOINED then DEAD,
#     in order, with strictly increasing sequence numbers.
# Node 1 runs with a 3 s suspect timeout so the stop/resume window
# stays well inside the suspect-to-dead window: node 3 is suspected
# but never declared DEAD while stopped. Node 2 gets the same
# timeout: with its default sub-second timeout it would declare
# node 3 DEAD while stopped and gossip that DEAD to node 1, whose
# stream would then — correctly, DEAD is in the filter — carry a
# DEAD before the kill step.
# The watchers run under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-filter-test.XXXXXX)"
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

# Ports 17831-17833 (gossip) and 18831-18833 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17831
127.0.0.1:17832
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17831 --peer-file "$TMP/peers" --control-port 18831 --suspect-timeout-ms 3000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17832 --peer-file "$TMP/peers" --control-port 18832 --suspect-timeout-ms 3000 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the two filtered WATCHes on node 1 in the background: they
# stay open for the whole test (timeout 60 s bounds them if the
# test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18831" WATCH JOINED,DEAD 127.0.0.1:17833 > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch1.pid")
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18831" WATCH 127.0.0.1:17832 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")

# 1a. The JOINED,DEAD-for-17833 stream opens with OK and END but
# no SNAPSHOT: the only peer it cares about has not joined yet.
if wait_for "$TMP/watch1.out" "^END$" 10; then
    if [ "$(head -n 1 "$TMP/watch1.out")" = "OK watching" ] \
       && [ "$(grep -c '^SNAPSHOT' "$TMP/watch1.out")" -eq 0 ]; then
        note ok "type+peer filtered WATCH opens with OK and END, no SNAPSHOT for a peer that has not joined"
    else
        note fail "type+peer filtered WATCH opens with OK and END, no SNAPSHOT for a peer that has not joined"
        cat "$TMP/watch1.out"
    fi
else
    note fail "type+peer filtered WATCH opens with OK and END, no SNAPSHOT for a peer that has not joined"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 1b. The 17832-only stream snapshots only that peer.
if wait_for "$TMP/watch2.out" "^END$" 10; then
    if [ "$(head -n 1 "$TMP/watch2.out")" = "OK watching" ] \
       && grep -Eq '^SNAPSHOT 127\.0\.0\.1:17832 1 ALIVE [0-9]+ peer-file$' "$TMP/watch2.out" \
       && [ "$(grep -c '^SNAPSHOT' "$TMP/watch2.out")" -eq 1 ]; then
        note ok "peer-filtered WATCH snapshots only the filtered peer"
    else
        note fail "peer-filtered WATCH snapshots only the filtered peer"
        cat "$TMP/watch2.out"
    fi
else
    note fail "peer-filtered WATCH snapshots only the filtered peer"
    cat "$TMP/watch2.out" 2>/dev/null
fi

# 2. Node 3 joins through --seed only: the first stream carries
# JOINED for it, the second carries nothing.
(cd "$TMP/n3" && exec "$BIN" --port 17833 --seed 127.0.0.1:17831 --control-port 18833 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17833 1 [0-9]+$' 10; then
    note ok "filtered WATCH streams JOINED when the filtered peer joins"
else
    note fail "filtered WATCH streams JOINED when the filtered peer joins"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi
sleep 1
if [ "$(grep -c '^EVENT' "$TMP/watch2.out")" -eq 0 ]; then
    note ok "peer-filtered WATCH stays silent for another peer's JOINED"
else
    note fail "peer-filtered WATCH stays silent for another peer's JOINED"
    cat "$TMP/watch2.out"
fi

# 3. Stop node 3 (SIGSTOP): node 1 suspects it, but SUSPECTED is
# outside the first stream's type filter and outside the second
# stream's peer filter, so neither stream carries it. The filtered
# event is consumed server-side at once, so a 1 s settle is plenty
# before asserting absence.
kill -STOP "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17833" 10; then
    sleep 1
    if [ "$(grep -c 'SUSPECTED 127\.0\.0\.1:17833' "$TMP/watch1.out")" -eq 0 ]; then
        note ok "type-filtered WATCH drops SUSPECTED for the filtered peer"
    else
        note fail "type-filtered WATCH drops SUSPECTED for the filtered peer"
        grep '^EVENT' "$TMP/watch1.out"
    fi
    if [ "$(grep -c '^EVENT' "$TMP/watch2.out")" -eq 0 ]; then
        note ok "peer-filtered WATCH stays silent for another peer's SUSPECTED"
    else
        note fail "peer-filtered WATCH stays silent for another peer's SUSPECTED"
        grep '^EVENT' "$TMP/watch2.out"
    fi
else
    note fail "type-filtered WATCH drops SUSPECTED for the filtered peer"
    note fail "peer-filtered WATCH stays silent for another peer's SUSPECTED"
    tail -5 "$TMP/n1.log"
fi

# Resume node 3: it comes back, but REVIVED is outside the first
# stream's type filter, so neither stream carries it.
kill -CONT "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "STATUS alive=3" 10; then
    sleep 1
    if [ "$(grep -c 'REVIVED 127\.0\.0\.1:17833' "$TMP/watch1.out")" -eq 0 ] \
       && [ "$(grep -c '^EVENT' "$TMP/watch2.out")" -eq 0 ]; then
        note ok "filtered WATCHes drop REVIVED for the filtered peer"
    else
        note fail "filtered WATCHes drop REVIVED for the filtered peer"
        echo "--- watch1.out"; grep '^EVENT' "$TMP/watch1.out"
        echo "--- watch2.out"; grep '^EVENT' "$TMP/watch2.out"
    fi
else
    note fail "filtered WATCHes drop REVIVED for the filtered peer"
    tail -5 "$TMP/n1.log"
fi

# 4. Kill node 3: the first stream carries DEAD for it, the second
# still carries nothing.
kill -9 "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17833" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17833 [0-9]+ [0-9]+$' 10; then
    note ok "filtered WATCH streams DEAD when the filtered peer is killed"
else
    note fail "filtered WATCH streams DEAD when the filtered peer is killed"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi
sleep 1
if [ "$(grep -c '^EVENT' "$TMP/watch2.out")" -eq 0 ]; then
    note ok "peer-filtered WATCH stays silent for another peer's DEAD"
else
    note fail "peer-filtered WATCH stays silent for another peer's DEAD"
    grep '^EVENT' "$TMP/watch2.out"
fi

# 5. A bad filter is refused up front.
if [ "$(ctl 18831 WATCH BOGUS)" = "ERR invalid watch filter unknown event type BOGUS" ]; then
    note ok "bad WATCH filter is refused with ERR invalid watch filter"
else
    note fail "bad WATCH filter is refused with ERR invalid watch filter"
    ctl 18831 WATCH BOGUS
fi

# 6. The first stream's EVENT lines are exactly JOINED then DEAD,
# in order, with strictly increasing sequence numbers.
if [ "$(grep -c '^EVENT' "$TMP/watch1.out")" -eq 2 ] \
   && [ "$(grep '^EVENT' "$TMP/watch1.out" | head -n 1 | awk '{print $3}')" = "JOINED" ] \
   && [ "$(grep '^EVENT' "$TMP/watch1.out" | tail -n 1 | awk '{print $3}')" = "DEAD" ] \
   && awk '/^EVENT / { s=$2; if (s <= prev) exit 1; prev=s } END { exit (NR > 0 && prev > 0) ? 0 : 1 }' "$TMP/watch1.out"; then
    note ok "filtered stream carries exactly JOINED then DEAD, sequence numbers strictly increase"
else
    note fail "filtered stream carries exactly JOINED then DEAD, sequence numbers strictly increase"
    grep '^EVENT' "$TMP/watch1.out"
fi

# Both watchers are still alive at the end: the streams stayed open
# for the whole test instead of dropping.
if kill -0 "$(cat "$TMP/watch1.pid")" 2>/dev/null \
   && kill -0 "$(cat "$TMP/watch2.pid")" 2>/dev/null; then
    note ok "filtered WATCH connections stayed open for the whole test"
else
    note fail "filtered WATCH connections stayed open for the whole test"
fi

echo "test_watch_filter: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
