#!/usr/bin/env bash
# Integration test for WATCH backpressure / pause (0.37.0): a slow
# consumer can stop the flow without closing the connection, and
# resume later with the ring replaying what it missed — or the gap
# signal when the ring has scrolled past it.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. Node 1 runs with --event-log-cap 4, so the gap part
#     of the test needs only a handful of events. With no watcher
#     connected, METRICS reports watch_paused=0 and JSON carries
#     "paused":0 with an empty streams array.
#  2. One WATCH opens on node 1 (session 1). METRICS reports
#     watch_paused=0 and JSON shows session 1 with "paused":false.
#  3. WATCH PAUSE 1 answers OK paused 1 (twice: pausing an
#     already-paused stream answers OK again). METRICS reports
#     watch_paused=1 and JSON shows session 1 with "paused":true.
#     Node 2 is killed: its graceful leave marks it DEAD on node 1
#     (STATUS shows alive=1 dead=1), but no DEAD event reaches the
#     paused stream — the watcher output holds no EVENT line for
#     node 2 after the pause.
#  4. WATCH RESUME 1 answers OK resumed 1, and the held DEAD event
#     arrives on the stream, replayed from the ring. METRICS
#     reports watch_paused=0 again. Resuming a running stream
#     answers OK again.
#  5. Bad pause / resume commands are refused: an unknown session,
#     a non-numeric session, and a missing session.
#  6. The stream is paused again and five events happen while it
#     is paused — node 2 restarts (REVIVED), is killed (DEAD), is
#     force-left (DROPPED), restarts again (JOINED), and is killed
#     again (DEAD) — five events against a ring that holds four,
#     so the first of them has scrolled out. On resume the stream
#     ends with ERR gap: events lost, reconnect, and METRICS
#     reports watch_dropped>=1: the ring's gap signal covers the
#     events missed while paused. The stream is closed, so
#     subscribers falls to 0 while sessions stays at 1.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-backpressure-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch1.pid" ] && kill "$(cat "$TMP/watch1.pid")" 2>/dev/null
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

# wait_metrics <pattern> <timeout-seconds>: poll METRICS on node 1
# until its line matches <pattern>.
wait_metrics() {
    local pat="$1" timeout="$2" i
    for i in $(seq 1 $((timeout * 5))); do
        if ctl 18891 METRICS | grep -Eq "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

# wait_json <pattern> <timeout-seconds>: poll JSON on node 1 until
# its line matches <pattern>.
wait_json() {
    local pat="$1" timeout="$2" i
    for i in $(seq 1 $((timeout * 5))); do
        if ctl 18891 JSON | grep -Eq "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

# wait_status <pattern> <timeout-seconds>: poll STATUS on node 1
# until its line matches <pattern>. The log's STATUS lines are
# stale by design (one a second, accumulating), so a live poll is
# the only way to tell the cluster's current state.
wait_status() {
    local pat="$1" timeout="$2" i
    for i in $(seq 1 $((timeout * 5))); do
        if ctl 18891 STATUS | grep -Eq "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17891-17892 (gossip) and 18891-18892 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17891
127.0.0.1:17892
EOF

start_n2() {
    (cd "$TMP/n2" && exec "$BIN" --port 17892 --peer-file "$TMP/peers" --control-port 18892 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
}

(cd "$TMP/n1" && exec "$BIN" --port 17891 --peer-file "$TMP/peers" --control-port 18891 --event-log-cap 4 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
start_n2

# 1. Both converge from the peer file, with no watcher connected.
if wait_status "alive=2 suspect=0 dead=0 total=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi
if ctl 18891 METRICS | grep -Eq "watch_paused=0 watch_event_log_cap=4"; then
    note ok "METRICS reports 0 paused streams and a ring cap of 4 with no watcher"
else
    note fail "METRICS reports 0 paused streams and a ring cap of 4 with no watcher"
    ctl 18891 METRICS
fi
if ctl 18891 JSON | grep -Fq '"watch":{"subscribers":0,"sessions":0,"delivered":0,"filtered":0,"dropped":0,"paused":0,"event_log_cap":4,"streams":[]}'; then
    note ok "JSON reports 0 paused and no live streams with no watcher"
else
    note fail "JSON reports 0 paused and no live streams with no watcher"
    ctl 18891 JSON
fi

# 2. One WATCH opens on node 1 (session 1).
(timeout 90 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18891" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch1.pid")
if ! wait_for "$TMP/watch1.out" "^END$" 10; then
    note fail "WATCH opens on node 1"
    cat "$TMP/watch1.out" 2>/dev/null
fi
if wait_json '"session":1,"delivered":0,"filtered":0,"dropped":0,"paused":false' 10; then
    note ok "JSON shows session 1 unpaused"
else
    note fail "JSON shows session 1 unpaused"
    ctl 18891 JSON
fi

# 3. Pause it: OK twice (idempotent), METRICS and JSON show it
# paused. Node 2 is killed — node 1 marks it DEAD, confirmed via
# STATUS so the absence on the watcher is meaningful — but no
# DEAD event reaches the paused stream.
if [ "$(ctl 18891 WATCH PAUSE 1)" = "OK paused 1" ]; then
    note ok "WATCH PAUSE 1 answers OK paused 1"
else
    note fail "WATCH PAUSE 1 answers OK paused 1"
    ctl 18891 WATCH PAUSE 1
fi
if [ "$(ctl 18891 WATCH PAUSE 1)" = "OK paused 1" ]; then
    note ok "pausing an already-paused stream answers OK again"
else
    note fail "pausing an already-paused stream answers OK again"
fi
if wait_metrics "watch_paused=1" 10; then
    note ok "METRICS reports 1 paused stream"
else
    note fail "METRICS reports 1 paused stream"
    ctl 18891 METRICS
fi
if wait_json '"session":1,"delivered":0,"filtered":0,"dropped":0,"paused":true' 10; then
    note ok "JSON shows session 1 paused"
else
    note fail "JSON shows session 1 paused"
    ctl 18891 JSON
fi
kill "$(cat "$TMP/n2.pid")" 2>/dev/null
rm -f "$TMP/n2.pid"
if ! wait_status "alive=1 suspect=0 dead=1 total=2" 10; then
    note fail "node 1 marks node 2 DEAD after the kill"
    ctl 18891 STATUS
fi
sleep 2
if grep -Eq "EVENT.*DEAD 127.0.0.1:17892" "$TMP/watch1.out"; then
    note fail "paused WATCH received a DEAD event for node 2"
    cat "$TMP/watch1.out"
else
    note ok "paused WATCH received no DEAD event for node 2"
fi

# 4. Resume it: the held DEAD event arrives, replayed from the
# ring, and the stream is unpaused. Resuming a running stream
# answers OK again.
if [ "$(ctl 18891 WATCH RESUME 1)" = "OK resumed 1" ]; then
    note ok "WATCH RESUME 1 answers OK resumed 1"
else
    note fail "WATCH RESUME 1 answers OK resumed 1"
    ctl 18891 WATCH RESUME 1
fi
if wait_for "$TMP/watch1.out" "EVENT.*DEAD 127.0.0.1:17892" 10; then
    note ok "resumed WATCH replays the held DEAD event for node 2"
else
    note fail "resumed WATCH replays the held DEAD event for node 2"
    cat "$TMP/watch1.out" 2>/dev/null
fi
if wait_metrics "watch_paused=0" 10; then
    note ok "METRICS reports 0 paused streams after the resume"
else
    note fail "METRICS reports 0 paused streams after the resume"
    ctl 18891 METRICS
fi
if [ "$(ctl 18891 WATCH RESUME 1)" = "OK resumed 1" ]; then
    note ok "resuming a running stream answers OK again"
else
    note fail "resuming a running stream answers OK again"
fi

# 5. Bad pause / resume commands are refused.
if [ "$(ctl 18891 WATCH PAUSE 99)" = "ERR unknown watch session 99" ]; then
    note ok "WATCH PAUSE 99 answers ERR unknown watch session 99"
else
    note fail "WATCH PAUSE 99 answers ERR unknown watch session 99"
    ctl 18891 WATCH PAUSE 99
fi
if [ "$(ctl 18891 WATCH RESUME 99)" = "ERR unknown watch session 99" ]; then
    note ok "WATCH RESUME 99 answers ERR unknown watch session 99"
else
    note fail "WATCH RESUME 99 answers ERR unknown watch session 99"
    ctl 18891 WATCH RESUME 99
fi
if [ "$(ctl 18891 WATCH PAUSE xyz)" = "ERR invalid watch session xyz" ]; then
    note ok "WATCH PAUSE xyz answers ERR invalid watch session xyz"
else
    note fail "WATCH PAUSE xyz answers ERR invalid watch session xyz"
    ctl 18891 WATCH PAUSE xyz
fi
if [ "$(ctl 18891 WATCH PAUSE)" = "ERR usage: WATCH PAUSE <session>" ]; then
    note ok "WATCH PAUSE with no session answers ERR usage"
else
    note fail "WATCH PAUSE with no session answers ERR usage"
    ctl 18891 WATCH PAUSE
fi

# 6. Pause again and churn five events past the stream while it
# is paused — restart (REVIVED), kill (DEAD), LEAVE (DROPPED),
# restart (JOINED), kill (DEAD) — five events against a ring
# that holds four, so the first has scrolled out. On resume the
# stream ends with ERR gap, and the lost event counts as
# dropped. Each step waits on node 1's live STATUS, so the
# events are known recorded before the next one is caused.
ctl 18891 WATCH PAUSE 1 >/dev/null
start_n2
if wait_status "alive=2 suspect=0 dead=0 total=2" 10; then
    note ok "node 2 restarts and comes back alive (REVIVED)"
else
    note fail "node 2 restarts and comes back alive (REVIVED)"
fi
kill "$(cat "$TMP/n2.pid")" 2>/dev/null
rm -f "$TMP/n2.pid"
if wait_status "alive=1 suspect=0 dead=1 total=2" 10; then
    note ok "node 2 killed again (DEAD)"
else
    note fail "node 2 killed again (DEAD)"
fi
if [ "$(ctl 18891 LEAVE 127.0.0.1:17892)" = "OK 127.0.0.1:17892" ]; then
    note ok "LEAVE drops node 2 (DROPPED)"
else
    note fail "LEAVE drops node 2 (DROPPED)"
fi
if ! wait_status "alive=1 suspect=0 dead=0 total=1" 10; then
    note fail "node 1 drops node 2 from the table after LEAVE"
    ctl 18891 STATUS
fi
start_n2
if wait_status "alive=2 suspect=0 dead=0 total=2" 10; then
    note ok "node 2 restarts again (JOINED)"
else
    note fail "node 2 restarts again (JOINED)"
fi
kill "$(cat "$TMP/n2.pid")" 2>/dev/null
rm -f "$TMP/n2.pid"
if ! wait_status "alive=1 suspect=0 dead=1 total=2" 10; then
    note fail "node 2 killed a third time (DEAD)"
    ctl 18891 STATUS
fi
if [ "$(ctl 18891 WATCH RESUME 1)" = "OK resumed 1" ]; then
    note ok "WATCH RESUME 1 answers OK resumed 1 after the churn"
else
    note fail "WATCH RESUME 1 answers OK resumed 1 after the churn"
fi
if wait_for "$TMP/watch1.out" "ERR gap: events lost, reconnect" 10; then
    note ok "resumed WATCH ends with ERR gap: the ring scrolled past the resume point"
else
    note fail "resumed WATCH ends with ERR gap: the ring scrolled past the resume point"
    cat "$TMP/watch1.out" 2>/dev/null
fi
if wait_metrics "watch_dropped=[1-9][0-9]*" 10; then
    note ok "METRICS reports at least 1 dropped event"
else
    note fail "METRICS reports at least 1 dropped event"
    ctl 18891 METRICS
fi
if wait_metrics "watch_subscribers=0 watch_sessions=1" 10; then
    note ok "gapped stream closed: 0 subscribers, sessions still 1"
else
    note fail "gapped stream closed: 0 subscribers, sessions still 1"
    ctl 18891 METRICS
fi
if wait_metrics "watch_paused=0" 10; then
    note ok "METRICS reports 0 paused streams after the stream closed"
else
    note fail "METRICS reports 0 paused streams after the stream closed"
    ctl 18891 METRICS
fi

echo "test_watch_backpressure: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
