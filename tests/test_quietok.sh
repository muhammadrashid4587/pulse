#!/usr/bin/env bash
# Integration test for the quiet-peer recovery event (0.72.0): when
# an ALIVE peer that was flagged quiet is heard from again — its
# last-contact age back at or below --quiet-peer-ms — the node logs
# one "QUIETOK <id> age=<ms> threshold=<ms>" line, records one
# QUIETOK event on the WATCH stream (and in the persisted events
# file), and the quiet flag clears — while the peer is still ALIVE,
# before any suspicion fires. The recovery is once per episode,
# re-armed by the next QUIET: a peer that goes quiet again warns
# again and recovers again.
#  1. Two nodes start from one shared --peer-file with
#     --quiet-peer-ms 300, a long ack timeout (5000 ms, so
#     suspicion fires ~10 s after the last contact, long after the
#     300 ms quiet threshold) and a 10 s suspect timeout, and
#     converge on alive=2, with a WATCH open on node 1.
#  2. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 logs one QUIET line for it and the
#     WATCH stream carries one QUIET event.
#  3. Restart node 2 at once, before the suspicion fires: node 1
#     logs one QUIETOK line for it, the WATCH stream carries one
#     QUIETOK event with a last_seen_ms at or below the 300 ms
#     threshold, and no SUSPECTED event for the peer appears at
#     all — the recovery landed while it was still ALIVE.
#  4. While the peer is back its JSON says "quiet":false and
#     METRICS says quiet_peers=0, the QUIETOK line and event each
#     fired exactly once, and the persisted <data-dir>/events file
#     and a WATCH FROM 0 replay carry the same QUIETOK line.
#  5. Kill node 2 a second time and restart it again: a second
#     QUIET and a second QUIETOK fire, so the recovery re-arms
#     with the next quiet episode instead of firing once ever.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-quietok-test.XXXXXX)"
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

# wait_count <logfile> <pattern> <count> <timeout-seconds>: wait
# until the pattern has matched exactly <count> times — for a
# second episode, where a plain wait_for would match the first
# episode's line at once.
wait_count() {
    local log="$1" pat="$2" want="$3" timeout="$4" i
    for i in $(seq 1 $((timeout * 5))); do
        if [ "$(grep -c "$pat" "$log" 2>/dev/null)" = "$want" ]; then
            return 0
        fi
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

# event_seen_ms <file> <pattern>: print the last_seen_ms of the
# first EVENT line in the file matching the pattern.
event_seen_ms() {
    grep -E "$2" "$1" | head -n 1 | grep -o 'last_seen_ms=[0-9]*' | cut -d= -f2
}

# start_n2: (re)start node 2 on its data dir, recording its pid.
start_n2() {
    (cd "$TMP/n2" && exec "$BIN" --port 17344 --peer-file "$TMP/peers" --control-port 18344 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17343-17344 (gossip) and 18343-18344 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17343
127.0.0.1:17344
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17343 --peer-file "$TMP/peers" --control-port 18343 --quiet-peer-ms 300 --ack-timeout-ms 5000 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
start_n2

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 90 s bounds it if the test hangs).
(timeout 90 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18343" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

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
# threshold, while it is still ALIVE — well before the ~10 s
# suspicion delay.
if wait_for "$TMP/n1.log" 'QUIET 127\.0\.0\.1:17344 age=[0-9]+ms threshold=300ms' 10; then
    note ok "node 1 logs a QUIET line for the silent peer"
else
    note fail "node 1 logs a QUIET line for the silent peer"
    tail -5 "$TMP/n1.log"
fi

if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ QUIET 127\.0\.0\.1:17344 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams a QUIET event with a last_seen_ms"
else
    note fail "WATCH streams a QUIET event with a last_seen_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

# 3. Restart node 2 at once — the suspicion is ~10 s away, the
# restart takes well under a second — and the recovery fires
# while the peer is still ALIVE.
start_n2

if wait_for "$TMP/n1.log" 'QUIETOK 127\.0\.0\.1:17344 age=[0-9]+ms threshold=300ms' 10; then
    note ok "node 1 logs a QUIETOK line for the peer heard from again"
else
    note fail "node 1 logs a QUIETOK line for the peer heard from again"
    tail -5 "$TMP/n1.log"
fi

if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ QUIETOK 127\.0\.0\.1:17344 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams a QUIETOK event with a last_seen_ms"
else
    note fail "WATCH streams a QUIETOK event with a last_seen_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

# The QUIETOK age is the age when the peer was heard from again:
# at or below the 300 ms threshold — the peer answered within one
# probe cycle of the recovery firing — never a stale quiet age.
QUIETOK_SEEN="$(event_seen_ms "$TMP/watch1.out" '^EVENT [0-9]+ QUIETOK 127\.0\.0\.1:17344 ')"
if [ -n "$QUIETOK_SEEN" ] && python3 -c "exit(0 if 0 <= int('$QUIETOK_SEEN') <= 300 else 1)" 2>/dev/null; then
    note ok "QUIETOK event last_seen_ms is at or below the threshold ($QUIETOK_SEEN ms)"
else
    note fail "QUIETOK event last_seen_ms is at or below the threshold (got '$QUIETOK_SEEN')"
fi

# The peer was never suspected: the recovery landed while it was
# still ALIVE, not on the way back from SUSPECT.
if ! grep -q '^EVENT [0-9]* SUSPECTED 127\.0\.0\.1:17344 ' "$TMP/watch1.out"; then
    note ok "no SUSPECTED event for the peer before the QUIETOK"
else
    note fail "no SUSPECTED event for the peer before the QUIETOK"
    grep '^EVENT.*SUSPECTED' "$TMP/watch1.out" 2>/dev/null
fi

# 4. While the peer is back its JSON flag reads false and METRICS
# counts zero quiet peers, and the QUIETOK fired exactly once.
if [ "$(json_member 18343 127.0.0.1:17344 quiet)" = "false" ]; then
    note ok "JSON clears the quiet flag once the peer is heard from again"
else
    note fail "JSON clears the quiet flag once the peer is heard from again"
    ctl 18343 JSON | sed -n '1p'
fi

if ctl 18343 METRICS | grep -q "quiet_peers=0"; then
    note ok "METRICS reports quiet_peers=0 once the peer is heard from again"
else
    note fail "METRICS reports quiet_peers=0 once the peer is heard from again"
    ctl 18343 METRICS
fi

if [ "$(grep -c ' QUIETOK 127\.0\.0\.1:17344 ' "$TMP/n1.log")" = "1" ]; then
    note ok "the QUIETOK line fired exactly once for the episode"
else
    note fail "the QUIETOK line fired exactly once for the episode"
    grep ' QUIETOK ' "$TMP/n1.log"
fi

if [ "$(grep -c '^EVENT [0-9]* QUIETOK 127\.0\.0\.1:17344 ' "$TMP/watch1.out")" = "1" ]; then
    note ok "the QUIETOK event fired exactly once for the episode"
else
    note fail "the QUIETOK event fired exactly once for the episode"
    grep '^EVENT.*QUIETOK' "$TMP/watch1.out" 2>/dev/null
fi

# The persisted event log carries the same QUIETOK line as the
# WATCH stream did: the recovery survives a restart and a replay.
if wait_for "$TMP/n1/events" "^[0-9]+ QUIETOK 127\\.0\\.0\\.1:17344 [0-9]+ [0-9]+ last_seen_ms=$QUIETOK_SEEN( probes_sent=[0-9]+ probes_answered=[0-9]+)?\$" 10; then
    note ok "persisted events file carries the same QUIETOK line ($QUIETOK_SEEN ms)"
else
    note fail "persisted events file carries the same QUIETOK line ($QUIETOK_SEEN ms)"
    cat "$TMP/n1/events" 2>/dev/null
fi

# A replay from the start shows the QUIETOK event too.
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18343" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ QUIETOK 127\\.0\\.0\\.1:17344 [0-9]+ [0-9]+ last_seen_ms=$QUIETOK_SEEN( probes_sent=[0-9]+ probes_answered=[0-9]+)?\$" 10; then
    note ok "replayed WATCH shows the QUIETOK event with the same age"
else
    note fail "replayed WATCH shows the QUIETOK event with the same age"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi

# 5. A second quiet episode re-arms the warning and the recovery:
# kill node 2 again, wait for a second QUIET, restart it, and
# wait for a second QUIETOK — one per episode, not once ever.
kill -9 "$(cat "$TMP/n2.pid")"

if wait_count "$TMP/n1.log" ' QUIET 127\.0\.0\.1:17344 ' 2 10; then
    note ok "a second quiet episode logs a second QUIET line"
else
    note fail "a second quiet episode logs a second QUIET line"
    grep ' QUIET ' "$TMP/n1.log"
fi

start_n2

if wait_count "$TMP/n1.log" ' QUIETOK 127\.0\.0\.1:17344 ' 2 10; then
    note ok "a second quiet episode logs a second QUIETOK line"
else
    note fail "a second quiet episode logs a second QUIETOK line"
    grep ' QUIETOK ' "$TMP/n1.log"
fi

if wait_count "$TMP/watch1.out" '^EVENT [0-9]* QUIETOK 127\.0\.0\.1:17344 ' 2 10; then
    note ok "a second quiet episode streams a second QUIETOK event"
else
    note fail "a second quiet episode streams a second QUIETOK event"
    grep '^EVENT.*QUIETOK' "$TMP/watch1.out" 2>/dev/null
fi

echo "test_quietok: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
