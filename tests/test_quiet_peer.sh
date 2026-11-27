#!/usr/bin/env bash
# Integration test for the quiet-peer early warning (0.62.0): when an
# ALIVE peer's last-contact age crosses --quiet-peer-ms, the node
# logs one "QUIET <id> age=<ms> threshold=<ms>" line, records one
# QUIET event on the WATCH stream (and in the persisted events
# file), and flags the member "quiet":true in JSON with a
# quiet_peers count in METRICS and JSON and a pulse_quiet_peers
# gauge in PROMETHEUS — while the peer is still ALIVE, before the
# suspicion fires. The warning is once per crossing: a persistently
# quiet peer warns once, not once per probe cycle, and the flag
# clears the moment the peer is suspected.
#  1. Two nodes start from one shared --peer-file with
#     --quiet-peer-ms 300, a long ack timeout (1000 ms, so
#     suspicion fires ~2 s after the last contact, long after the
#     300 ms quiet threshold) and a 10 s suspect timeout, and
#     converge on alive=2, with a WATCH open on node 1.
#  2. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 logs exactly one QUIET line for it,
#     the WATCH stream carries one QUIET event with a
#     last_seen_ms of at least the 300 ms threshold, and while
#     the peer is still ALIVE its JSON says "quiet":true,
#     METRICS says quiet_peers=1 and PROMETHEUS says
#     pulse_quiet_peers 1.
#  3. Once node 1 suspects the peer, the flag clears: its JSON
#     says "quiet":false and METRICS says quiet_peers=0 — a
#     SUSPECT peer is not quiet, the suspicion already says it
#     is silent — and no second QUIET line ever appears.
#  4. The persisted <data-dir>/events file carries the same
#     QUIET line as the WATCH stream did, and a WATCH FROM 0
#     replay shows it too.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-quiet-peer-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch.pid" ] && kill "$(cat "$TMP/watch.pid")" 2>/dev/null
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

# json_member <control-port> <member-id> <field>: print the
# member's field as JSON, parsed from the live JSON reply.
json_member() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(json.dumps(m.get('$3')))
        break
"
}

# wait_json_quiet <timeout-seconds>: poll node 1's JSON until the
# killed peer reads "quiet":true while still ALIVE — the window
# between the quiet threshold (300 ms of silence) and the
# suspicion (~2 s) is over a second wide, and the probe cycle
# runs every 100 ms, so a 100 ms poll catches it.
wait_json_quiet() {
    local timeout="$1" i
    for i in $(seq 1 $((timeout * 10))); do
        if [ "$(json_member 18341 127.0.0.1:17342 quiet)" = "true" ] \
           && [ "$(json_member 18341 127.0.0.1:17342 state)" = '"ALIVE"' ]; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# wait_metrics_quiet <timeout-seconds>: poll node 1's METRICS
# until it reports quiet_peers=1.
wait_metrics_quiet() {
    local timeout="$1" i
    for i in $(seq 1 $((timeout * 10))); do
        if ctl 18341 METRICS | grep -q "quiet_peers=1"; then return 0; fi
        sleep 0.1
    done
    return 1
}

# wait_prom_quiet <timeout-seconds>: poll node 1's PROMETHEUS
# until it reports pulse_quiet_peers 1.
wait_prom_quiet() {
    local timeout="$1" i
    for i in $(seq 1 $((timeout * 10))); do
        if ctl 18341 PROMETHEUS | grep -q "^pulse_quiet_peers 1$"; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# event_seen_ms <file> <pattern>: print the last_seen_ms of the
# first EVENT line in the file matching the pattern.
event_seen_ms() {
    grep -E "$2" "$1" | head -n 1 | grep -o 'last_seen_ms=[0-9]*' | cut -d= -f2
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17341-17342 (gossip) and 18341-18342 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17341
127.0.0.1:17342
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17341 --peer-file "$TMP/peers" --control-port 18341 --quiet-peer-ms 300 --ack-timeout-ms 1000 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17342 --peer-file "$TMP/peers" --control-port 18342 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file, and the START line
# shows the quiet threshold the node is watching with.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

if grep -q "START .* quiet=300ms" "$TMP/n1.log"; then
    note ok "START line shows the quiet threshold (quiet=300ms)"
else
    note fail "START line shows the quiet threshold (quiet=300ms)"
    grep "START" "$TMP/n1.log" | head -2
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 60 s bounds it if the test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18341" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 2. Kill node 2 — with SIGKILL, an unclean crash: a clean SIGTERM
# would announce its leave and node 1 would mark it DEAD straight
# away, never suspecting it, so no silence would ever accumulate.
kill -9 "$(cat "$TMP/n2.pid")"

# The QUIET log line fires once the peer has been silent for the
# threshold, while it is still ALIVE — well before the ~2 s
# suspicion delay.
if wait_for "$TMP/n1.log" 'QUIET 127\.0\.0\.1:17342 age=[0-9]+ms threshold=300ms' 10; then
    note ok "node 1 logs one QUIET line for the silent peer"
else
    note fail "node 1 logs one QUIET line for the silent peer"
    tail -5 "$TMP/n1.log"
fi

# The WATCH stream carries the QUIET event with the age frozen
# on it: at least the 300 ms threshold — the peer was silent
# that long when the threshold was crossed — and under the ~2 s
# suspicion delay, so it fired while the peer was still ALIVE.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ QUIET 127\.0\.0\.1:17342 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams a QUIET event with a last_seen_ms"
else
    note fail "WATCH streams a QUIET event with a last_seen_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

QUIET_SEEN="$(event_seen_ms "$TMP/watch1.out" '^EVENT [0-9]+ QUIET 127\.0\.0\.1:17342 ')"
if [ -n "$QUIET_SEEN" ] && python3 -c "exit(0 if 300 <= int('$QUIET_SEEN') < 2000 else 1)" 2>/dev/null; then
    note ok "QUIET event last_seen_ms is at least the threshold and under the suspicion delay ($QUIET_SEEN ms)"
else
    note fail "QUIET event last_seen_ms is at least the threshold and under the suspicion delay (got '$QUIET_SEEN')"
fi

# While the peer is still ALIVE its JSON flag reads true, METRICS
# counts one quiet peer, and PROMETHEUS gauges one.
if wait_json_quiet 5; then
    note ok "JSON flags the peer quiet while it is still ALIVE"
else
    note fail "JSON flags the peer quiet while it is still ALIVE"
    ctl 18341 JSON | sed -n '1p'
fi

if wait_metrics_quiet 5; then
    note ok "METRICS reports quiet_peers=1 while the peer is quiet"
else
    note fail "METRICS reports quiet_peers=1 while the peer is quiet"
    ctl 18341 METRICS
fi

if wait_prom_quiet 5; then
    note ok "PROMETHEUS reports pulse_quiet_peers 1 while the peer is quiet"
else
    note fail "PROMETHEUS reports pulse_quiet_peers 1 while the peer is quiet"
    ctl 18341 PROMETHEUS | grep -E "quiet"
fi

# 3. Once the peer is suspected the flag clears: a SUSPECT peer
# is not quiet, the suspicion already says it is silent — and
# the QUIET line never fires a second time for this episode.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17342 ' 10; then
    note ok "WATCH streams SUSPECTED for the peer after the QUIET"
else
    note fail "WATCH streams SUSPECTED for the peer after the QUIET"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

if [ "$(json_member 18341 127.0.0.1:17342 quiet)" = "false" ]; then
    note ok "JSON clears the quiet flag once the peer is SUSPECT"
else
    note fail "JSON clears the quiet flag once the peer is SUSPECT"
    ctl 18341 JSON | sed -n '1p'
fi

if ctl 18341 METRICS | grep -q "quiet_peers=0"; then
    note ok "METRICS reports quiet_peers=0 once the peer is SUSPECT"
else
    note fail "METRICS reports quiet_peers=0 once the peer is SUSPECT"
    ctl 18341 METRICS
fi

if [ "$(grep -c ' QUIET 127\.0\.0\.1:17342 ' "$TMP/n1.log")" = "1" ]; then
    note ok "the QUIET line fired exactly once for the episode"
else
    note fail "the QUIET line fired exactly once for the episode"
    grep ' QUIET ' "$TMP/n1.log"
fi

if [ "$(grep -c '^EVENT [0-9]* QUIET 127\.0\.0\.1:17342 ' "$TMP/watch1.out")" = "1" ]; then
    note ok "the QUIET event fired exactly once for the episode"
else
    note fail "the QUIET event fired exactly once for the episode"
    grep '^EVENT.*QUIET' "$TMP/watch1.out" 2>/dev/null
fi

# 4. The persisted event log carries the same QUIET line as the
# WATCH stream did: the warning survives a restart and a replay.
if wait_for "$TMP/n1/events" "^[0-9]+ QUIET 127\\.0\\.0\\.1:17342 [0-9]+ [0-9]+ last_seen_ms=$QUIET_SEEN( probes_sent=[0-9]+ probes_answered=[0-9]+)?\$" 10; then
    note ok "persisted events file carries the same QUIET line ($QUIET_SEEN ms)"
else
    note fail "persisted events file carries the same QUIET line ($QUIET_SEEN ms)"
    cat "$TMP/n1/events" 2>/dev/null
fi

# A replay from the start shows the QUIET event too.
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18341" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ QUIET 127\\.0\\.0\\.1:17342 [0-9]+ [0-9]+ last_seen_ms=$QUIET_SEEN( probes_sent=[0-9]+ probes_answered=[0-9]+)?\$" 10; then
    note ok "replayed WATCH shows the QUIET event with the same age"
else
    note fail "replayed WATCH shows the QUIET event with the same age"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi

echo "test_quiet_peer: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
