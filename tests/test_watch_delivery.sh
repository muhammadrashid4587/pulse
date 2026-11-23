#!/usr/bin/env bash
# Integration test for the WATCH drop and delivery counts (0.34.0):
# each stream counts the EVENT lines it sent (delivered), the events
# it consumed but skipped because its type/id filter excluded them
# (filtered), and the events it lost to a gap in the bounded event
# ring (dropped) — and METRICS reports the three as lifetime totals
# while JSON carries the same three totals plus a per-stream
# breakdown keyed by session number.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. With no watcher connected, METRICS reports
#     watch_delivered=0 watch_filtered=0 watch_dropped=0 and JSON
#     carries "watch":{"subscribers":0,"sessions":0,"delivered":0,
#     "filtered":0,"dropped":0,"streams":[]}.
#  2. One unfiltered WATCH opens on node 1 (session 1). Node 2 is
#     killed: its graceful leave marks it DEAD on node 1, the
#     watcher streams the DEAD event, and METRICS reports
#     watch_delivered>=1 while JSON shows session 1 with
#     delivered>=1.
#  3. A second WATCH opens, filtered to DEAD only (session 2).
#     Node 2 restarts and comes back REVIVED on node 1 — not a
#     DEAD event, so the filtered stream skips it: METRICS reports
#     watch_filtered>=1 while JSON shows session 2 with
#     delivered=0 and filtered>=1.
#  4. Both watchers go away: subscribers falls to 0, sessions stays
#     at 2, the totals stay (they only ever grow), dropped is
#     still 0 — no gap happened — and JSON lists no live streams.
# The drop count itself — a gap in the event ring — is covered
# deterministically in the unit test (tests/test_control.c): it
# needs 128 events to evict the ring, so the integration test does
# not flood the cluster to force one. The watchers run under
# stdbuf -o0: pulse-ctl's stdout is block-buffered to a file, so
# without it the streamed lines would sit in the buffer and the
# test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-delivery-test.XXXXXX)"
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
# until its line matches <pattern>. The counts lag a stream by up
# to a second, so an exact read right after an event is racy — the
# poll waits for it to settle.
wait_metrics() {
    local pat="$1" timeout="$2" i
    for i in $(seq 1 $((timeout * 5))); do
        if ctl 18871 METRICS | grep -Eq "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

# wait_json <pattern> <timeout-seconds>: poll JSON on node 1 until
# its line matches <pattern> — the per-stream counts in the watch
# object lag a stream the same way the METRICS totals do.
wait_json() {
    local pat="$1" timeout="$2" i
    for i in $(seq 1 $((timeout * 5))); do
        if ctl 18871 JSON | grep -Eq "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17871-17872 (gossip) and 18871-18872 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17871
127.0.0.1:17872
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17871 --peer-file "$TMP/peers" --control-port 18871 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17872 --peer-file "$TMP/peers" --control-port 18872 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file, with no watcher connected.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi
if ctl 18871 METRICS | grep -Eq "watch_delivered=0 watch_filtered=0 watch_dropped=0"; then
    note ok "METRICS reports 0 delivered, 0 filtered, 0 dropped with no watcher"
else
    note fail "METRICS reports 0 delivered, 0 filtered, 0 dropped with no watcher"
    ctl 18871 METRICS
fi
if ctl 18871 JSON | grep -Fq '"watch":{"subscribers":0,"sessions":0,"delivered":0,"filtered":0,"dropped":0,"streams":[]}'; then
    note ok "JSON reports 0 delivered/filtered/dropped and no live streams with no watcher"
else
    note fail "JSON reports 0 delivered/filtered/dropped and no live streams with no watcher"
    ctl 18871 JSON
fi

# 2. One unfiltered WATCH opens on node 1 (session 1). Killing
# node 2 marks it DEAD on node 1 — a graceful leave announces the
# departure, so the DEAD event streams at once — and the watcher
# delivers it.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18871" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch1.pid")
if ! wait_for "$TMP/watch1.out" "^END$" 10; then
    note fail "first WATCH opens on node 1"
    cat "$TMP/watch1.out" 2>/dev/null
fi
kill "$(cat "$TMP/n2.pid")" 2>/dev/null
rm -f "$TMP/n2.pid"
if wait_for "$TMP/watch1.out" "DEAD 127.0.0.1:17872" 10; then
    note ok "unfiltered WATCH streams the DEAD event for node 2"
else
    note fail "unfiltered WATCH streams the DEAD event for node 2"
    cat "$TMP/watch1.out" 2>/dev/null
fi
if wait_metrics "watch_delivered=[1-9][0-9]*" 10; then
    note ok "METRICS reports at least 1 delivered event"
else
    note fail "METRICS reports at least 1 delivered event"
    ctl 18871 METRICS
fi
if wait_json '"session":1,"delivered":[1-9][0-9]*,"filtered":0,"dropped":0' 10; then
    note ok "JSON shows session 1 with at least 1 delivered, 0 filtered, 0 dropped"
else
    note fail "JSON shows session 1 with at least 1 delivered, 0 filtered, 0 dropped"
    ctl 18871 JSON
fi

# 3. A second WATCH opens, filtered to DEAD only (session 2).
# Node 2 restarts — same data dir, so its incarnation bumps — and
# comes back REVIVED on node 1: not a DEAD event, so the filtered
# stream skips it while the unfiltered one delivers it.
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18871" WATCH DEAD > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if ! wait_for "$TMP/watch2.out" "^END$" 10; then
    note fail "filtered WATCH opens on node 1"
    cat "$TMP/watch2.out" 2>/dev/null
fi
(cd "$TMP/n2" && exec "$BIN" --port 17872 --peer-file "$TMP/peers" --control-port 18872 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
if wait_for "$TMP/watch1.out" "REVIVED 127.0.0.1:17872" 10; then
    note ok "node 2 restarts and comes back REVIVED on the unfiltered WATCH"
else
    note fail "node 2 restarts and comes back REVIVED on the unfiltered WATCH"
    cat "$TMP/watch1.out" 2>/dev/null
fi
if wait_metrics "watch_filtered=[1-9][0-9]*" 10; then
    note ok "METRICS reports at least 1 filtered event"
else
    note fail "METRICS reports at least 1 filtered event"
    ctl 18871 METRICS
fi
if wait_json '"session":2,"delivered":0,"filtered":[1-9][0-9]*,"dropped":0' 10; then
    note ok "JSON shows session 2 with 0 delivered and at least 1 filtered"
else
    note fail "JSON shows session 2 with 0 delivered and at least 1 filtered"
    ctl 18871 JSON
fi
if grep -Eq "EVENT.*(JOINED|REVIVED|SUSPECTED) 127.0.0.1:17872" "$TMP/watch2.out"; then
    note fail "filtered WATCH received a non-DEAD event for node 2"
    cat "$TMP/watch2.out"
else
    note ok "filtered WATCH received no non-DEAD event for node 2"
fi

# 4. Both watchers go away: subscribers falls to 0, sessions stays
# at 2, the totals stay (they only ever grow), dropped is still 0
# — no gap happened — and JSON lists no live streams.
kill "$(cat "$TMP/watch1.pid")" 2>/dev/null
rm -f "$TMP/watch1.pid"
kill "$(cat "$TMP/watch2.pid")" 2>/dev/null
rm -f "$TMP/watch2.pid"
if wait_metrics "watch_subscribers=0 watch_sessions=2" 10; then
    note ok "both watchers gone: 0 subscribers, sessions still 2"
else
    note fail "both watchers gone: 0 subscribers, sessions still 2"
    ctl 18871 METRICS
fi
if ctl 18871 METRICS | grep -Eq "watch_delivered=[1-9][0-9]* watch_filtered=[1-9][0-9]* watch_dropped=0"; then
    note ok "totals persist after the watchers go away, dropped still 0"
else
    note fail "totals persist after the watchers go away, dropped still 0"
    ctl 18871 METRICS
fi
if ctl 18871 JSON | grep -Fq '"streams":[]'; then
    note ok "JSON lists no live streams after the watchers go away"
else
    note fail "JSON lists no live streams after the watchers go away"
    ctl 18871 JSON
fi

echo "test_watch_delivery: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
