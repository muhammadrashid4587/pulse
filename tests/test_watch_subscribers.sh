#!/usr/bin/env bash
# Integration test for the WATCH subscriber count (0.33.0): an
# operator polling METRICS or JSON should see how many consumers
# are attached to the event stream, without counting connections
# by hand.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. With no watcher connected, METRICS reports
#     watch_subscribers=0 watch_sessions=0 and JSON carries
#     "watch":{"subscribers":0,"sessions":0}.
#  2. One WATCH opens on node 1: METRICS reports
#     watch_subscribers=1 watch_sessions=1, and JSON carries the
#     same counts.
#  3. A second WATCH opens: subscribers=2, sessions=2.
#  4. The first watcher goes away: subscribers falls back to 1
#     within a few seconds (the server notices a closed client
#     on its next 1 s wait timeout) while sessions stays at 2.
#  5. The second watcher goes away too: subscribers=0, sessions
#     still 2 — the session count only ever grows.
# The watchers run under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-subscribers-test.XXXXXX)"
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

# wait_metrics <pattern> <timeout-seconds>: poll METRICS on node 1
# until its line matches <pattern>. The subscriber count lags a
# watcher going away by up to a second, so an exact read right
# after a close is racy — the poll waits for it to settle.
wait_metrics() {
    local pat="$1" timeout="$2" i
    for i in $(seq 1 $((timeout * 5))); do
        if ctl 18861 METRICS | grep -Eq "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17861-17862 (gossip) and 18861-18862 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17861
127.0.0.1:17862
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17861 --peer-file "$TMP/peers" --control-port 18861 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17862 --peer-file "$TMP/peers" --control-port 18862 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file, with no watcher connected.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi
if ctl 18861 METRICS | grep -Eq "watch_subscribers=0 watch_sessions=0"; then
    note ok "METRICS reports 0 subscribers and 0 sessions with no watcher"
else
    note fail "METRICS reports 0 subscribers and 0 sessions with no watcher"
    ctl 18861 METRICS
fi
if ctl 18861 JSON | grep -Fq '"watch":{"subscribers":0,"sessions":0,"delivered":0,"filtered":0,"dropped":0,"streams":[]}'; then
    note ok "JSON reports 0 subscribers and 0 sessions with no watcher"
else
    note fail "JSON reports 0 subscribers and 0 sessions with no watcher"
    ctl 18861 JSON
fi

# 2. One WATCH opens on node 1.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18861" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch1.pid")
if ! wait_for "$TMP/watch1.out" "^END$" 10; then
    note fail "first WATCH opens on node 1"
    cat "$TMP/watch1.out" 2>/dev/null
fi
if wait_metrics "watch_subscribers=1 watch_sessions=1" 10; then
    note ok "one WATCH: METRICS reports 1 subscriber, 1 session"
else
    note fail "one WATCH: METRICS reports 1 subscriber, 1 session"
    ctl 18861 METRICS
fi
if ctl 18861 JSON | grep -Fq '"watch":{"subscribers":1,"sessions":1,"delivered":0,"filtered":0,"dropped":0,"streams":[{"session":1,"delivered":0,"filtered":0,"dropped":0}]}'; then
    note ok "one WATCH: JSON reports 1 subscriber, 1 session"
else
    note fail "one WATCH: JSON reports 1 subscriber, 1 session"
    ctl 18861 JSON
fi

# 3. A second WATCH opens on the same node.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18861" WATCH > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if ! wait_for "$TMP/watch2.out" "^END$" 10; then
    note fail "second WATCH opens on node 1"
    cat "$TMP/watch2.out" 2>/dev/null
fi
if wait_metrics "watch_subscribers=2 watch_sessions=2" 10; then
    note ok "two WATCHes: METRICS reports 2 subscribers, 2 sessions"
else
    note fail "two WATCHes: METRICS reports 2 subscribers, 2 sessions"
    ctl 18861 METRICS
fi

# 4. The first watcher goes away: subscribers falls to 1, sessions
# stays at 2. Killing the timeout wrapper forwards the signal to
# pulse-ctl, which closes the stream.
kill "$(cat "$TMP/watch1.pid")" 2>/dev/null
rm -f "$TMP/watch1.pid"
if wait_metrics "watch_subscribers=1 watch_sessions=2" 10; then
    note ok "first watcher gone: 1 subscriber left, sessions still 2"
else
    note fail "first watcher gone: 1 subscriber left, sessions still 2"
    ctl 18861 METRICS
fi

# 5. The second watcher goes away too: subscribers=0, sessions
# still 2 — the session count only ever grows.
kill "$(cat "$TMP/watch2.pid")" 2>/dev/null
rm -f "$TMP/watch2.pid"
if wait_metrics "watch_subscribers=0 watch_sessions=2" 10; then
    note ok "both watchers gone: 0 subscribers, sessions still 2"
else
    note fail "both watchers gone: 0 subscribers, sessions still 2"
    ctl 18861 METRICS
fi

echo "test_watch_subscribers: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
