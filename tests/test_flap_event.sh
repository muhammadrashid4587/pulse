#!/usr/bin/env bash
# Integration test for the FLAP event on the WATCH stream (0.79.0):
# when a member completes one flap cycle — ALIVE -> SUSPECT ->
# ALIVE (a refuted suspicion) or ALIVE -> SUSPECT -> DEAD (the
# peer died after being suspected) — the node records one FLAP
# event on the WATCH stream (and in the persisted events file)
# right after the REVIVED or DEAD event for the same transition,
# carrying the lifetime flap count frozen as flap_count and the
# recent count frozen as flaps_10m, so a consumer watching the
# stream sees each flap as it happens instead of only polling
# the counts in JSON / METRICS / PROMETHEUS.
#  1. Two nodes start from one shared --peer-file with a 5 s
#     suspect timeout and converge on alive=2, with a WATCH open
#     on node 1. Before any crash the peer reads flap_count 0 and
#     the stream carries no FLAP event.
#  2. Kill node 2 with SIGKILL, wait for node 1 to suspect it,
#     then restart it inside the 5 s suspect window: the
#     SUSPECT -> ALIVE leg completes a flap, so the stream
#     carries a FLAP event right after the REVIVED, with
#     flap_count 1 and flaps_10m 1 — unless the slow restart
#     lands genuine extra refuted-suspicion flaps of its own, in
#     which case the count is left to settle first and the last
#     FLAP event carries the settled count, with exactly one
#     FLAP event per completed flap.
#  3. Kill node 2 again and do not restart it: the SUSPECT ->
#     DEAD leg completes another flap, so the stream carries one
#     more FLAP event right after the DEAD, with the lifetime
#     count one higher, and JSON agrees on flap_count and
#     flaps_10m.
#  4. The persisted <data-dir>/events file carries the same FLAP
#     lines as the WATCH stream did, a WATCH FROM 0 replay shows
#     them too, and a WATCH FLAP FROM 0 stream carries only the
#     FLAP events — the type filter subscribes to them by name.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-flap-event-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch.pid" ] && kill "$(cat "$TMP/watch.pid")" 2>/dev/null
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

# wait_for_count <logfile> <pattern> <min-count> <timeout-seconds>:
# like wait_for, but waits until the pattern has matched at least
# min-count times — a plain wait_for would match a stale line from
# an earlier cycle of the same peer.
wait_for_count() {
    local log="$1" pat="$2" min="$3" timeout="$4" i c
    for i in $(seq 1 $((timeout * 5))); do
        c=$(grep -cE "$pat" "$log" 2>/dev/null || true)
        if [ "$c" -ge "$min" ]; then return 0; fi
        sleep 0.2
    done
    return 1
}

# wait_for_json_state <control-port> <member-id> <state>
# <timeout-seconds>: poll JSON until the member is in the wanted
# state — the log's "STATUS alive=2" line would match a stale
# line from before the peer was killed, so the log cannot prove
# the restarted peer is ALIVE again.
wait_for_json_state() {
    local port="$1" id="$2" want="$3" timeout="$4" i st
    for i in $(seq 1 $((timeout * 5))); do
        st=$(ctl "$port" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$id':
        print(m['state'])
        break
")
        if [ "$st" = "$want" ]; then return 0; fi
        sleep 0.2
    done
    return 1
}

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

# json_member <control-port> <member-id> <field>: the member's
# field from the live JSON reply, parsed with python3, or empty
# when the member is absent.
json_member() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(m.get('$3', ''))
        break
"
}

# flap_events <file>: "seq flap_count flaps_10m" per FLAP EVENT
# line for the peer, in stream order. The wire order is
# flaps_10m first, flap_count after it.
flap_events() {
    grep -E '^EVENT [0-9]+ FLAP 127\.0\.0\.1:17978 ' "$1" | python3 -c "
import re, sys
for line in sys.stdin:
    m = re.search(r'^EVENT (\d+) FLAP .* flaps_10m=(\d+) flap_count=(\d+)', line)
    if m:
        print(m.group(1), m.group(3), m.group(2))
"
}

# flap_prev_types <file>: the type of the event right before each
# FLAP event for the peer, in stream order — a FLAP must ride
# right after the REVIVED or DEAD that completed the cycle.
flap_prev_types() {
    python3 - "$1" <<'PYEOF'
import re, sys
evs = []
for line in open(sys.argv[1]):
    m = re.match(r'^EVENT (\d+) (\w+) 127\.0\.0\.1:17978 ', line)
    if m:
        evs.append((int(m.group(1)), m.group(2)))
for i, (seq, typ) in enumerate(evs):
    if typ != 'FLAP':
        continue
    prev = evs[i - 1][1] if i > 0 else 'NONE'
    print(prev)
PYEOF
}

start_n2() {
    (cd "$TMP/n2" && exec "$BIN" --port 17978 --peer-file "$TMP/peers" --control-port 18978 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17977-17978 (gossip) and 18977-18978 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17977
127.0.0.1:17978
EOF

# A 5 s suspect timeout: long enough to restart node 2 after the
# SUSPECTED fires and still catch the REVIVED, so the first flap
# completes on the revival leg instead of the death leg.
(cd "$TMP/n1" && exec "$BIN" --port 17977 --peer-file "$TMP/peers" --control-port 18977 --suspect-timeout-ms 5000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
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
# the whole test so every event is captured live.
(timeout 120 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18977" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")
if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# Before any crash the peer has never flapped: flap_count 0 and
# no FLAP event on the stream.
if [ "$(json_member 18977 127.0.0.1:17978 flap_count)" = "0" ] \
   && [ -z "$(grep -E '^EVENT [0-9]+ FLAP ' "$TMP/watch1.out" || true)" ]; then
    note ok "a healthy peer reads flap_count 0 with no FLAP event"
else
    note fail "a healthy peer reads flap_count 0 with no FLAP event"
    ctl 18977 JSON | sed -n '1p'
    grep '^EVENT.*FLAP' "$TMP/watch1.out" || true
fi

# 2. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the silence through probing — wait for the SUSPECTED, then
# restart inside the 5 s suspect window: the SUSPECT -> ALIVE leg
# completes the first flap.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17978" 10; then
    note ok "node 1 suspects the crashed peer"
else
    note fail "node 1 suspects the crashed peer"
    tail -5 "$TMP/n1.log"
fi
start_n2
if wait_for_json_state 18977 127.0.0.1:17978 ALIVE 10; then
    note ok "restarted peer comes back ALIVE after the first crash"
else
    note fail "restarted peer comes back ALIVE after the first crash"
    tail -5 "$TMP/n1.log"
fi
# A slow restart can add genuine refuted-suspicion flaps of its
# own for a second or two after the peer comes back ALIVE, so the
# count is left to settle — unchanged across a 1 s gap, up to 10 s
# — before it is judged, the same settle-then-judge the flap-rate
# test uses for the same transient.
COUNT1="$(json_member 18977 127.0.0.1:17978 flap_count)"
for _ in $(seq 1 10); do
    sleep 1
    COUNT1_NEW="$(json_member 18977 127.0.0.1:17978 flap_count)"
    [ "$COUNT1_NEW" = "$COUNT1" ] && break
    COUNT1="$COUNT1_NEW"
done
RECENT1="$(json_member 18977 127.0.0.1:17978 flaps_10m)"
if python3 -c "exit(0 if int('$COUNT1') >= 1 else 1)" 2>/dev/null \
   && [ "$RECENT1" = "$COUNT1" ]; then
    note ok "after the revival the settled flap_count is $COUNT1 with flaps_10m $RECENT1"
else
    note fail "after the revival the settled flap_count is at least 1 with flaps_10m equal to it (got $COUNT1 / $RECENT1)"
    ctl 18977 JSON | sed -n '1p'
fi
# The stream carries exactly one FLAP event per completed flap,
# so the settled count is the number of FLAP events, the last
# one carries the settled lifetime and recent counts, and every
# FLAP rides right after the REVIVED or DEAD that completed
# its cycle.
FLAP1="$(flap_events "$TMP/watch1.out")"
N_FLAP1="$(echo "$FLAP1" | grep -c . || true)"
LAST1="$(echo "$FLAP1" | tail -n 1)"
PREV1="$(flap_prev_types "$TMP/watch1.out" | tr '\n' ' ')"
if [ "$N_FLAP1" = "$COUNT1" ] \
   && echo "$LAST1" | grep -Eq " $COUNT1 $RECENT1\$" \
   && ! echo "$PREV1" | grep -Eqv "^(REVIVED |DEAD )*$"; then
    note ok "WATCH carries $N_FLAP1 FLAP event(s) after REVIVED, last with flap_count $COUNT1 flaps_10m $RECENT1"
else
    note fail "WATCH carries $COUNT1 FLAP event(s) after REVIVED, last with flap_count $COUNT1 flaps_10m $RECENT1 (got $N_FLAP1 events, last '$LAST1', prev types '$PREV1')"
    grep '^EVENT' "$TMP/watch1.out" | tail -10
fi

# 3. Kill node 2 again and do not restart it: the SUSPECT ->
# DEAD leg completes one more flap, so the stream carries one
# more FLAP event right after the DEAD.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17978" 15; then
    note ok "node 1 marks the crashed peer DEAD on the second crash"
else
    note fail "node 1 marks the crashed peer DEAD on the second crash"
    tail -5 "$TMP/n1.log"
fi
# The count cannot move after the death — no more transitions —
# but poll until it settles anyway, in case the DEAD log line
# lands a cycle before the flap is counted.
COUNT2="$(json_member 18977 127.0.0.1:17978 flap_count)"
for _ in $(seq 1 10); do
    sleep 1
    COUNT2_NEW="$(json_member 18977 127.0.0.1:17978 flap_count)"
    [ "$COUNT2_NEW" = "$COUNT2" ] && break
    COUNT2="$COUNT2_NEW"
done
RECENT2="$(json_member 18977 127.0.0.1:17978 flaps_10m)"
if python3 -c "exit(0 if int('$COUNT2') > int('$COUNT1') else 1)" 2>/dev/null \
   && [ "$RECENT2" = "$COUNT2" ]; then
    note ok "after the death the settled flap_count climbed to $COUNT2 with flaps_10m $RECENT2"
else
    note fail "after the death the settled flap_count climbed past $COUNT1 with flaps_10m equal to it (got $COUNT2 / $RECENT2)"
    ctl 18977 JSON | sed -n '1p'
fi
FLAP2="$(flap_events "$TMP/watch1.out")"
N_FLAP2="$(echo "$FLAP2" | grep -c . || true)"
LAST2="$(echo "$FLAP2" | tail -n 1)"
PREV2="$(flap_prev_types "$TMP/watch1.out" | tr '\n' ' ')"
if [ "$N_FLAP2" = "$COUNT2" ] \
   && echo "$LAST2" | grep -Eq " $COUNT2 $RECENT2\$" \
   && ! echo "$PREV2" | grep -Eqv "^(REVIVED |DEAD )*$" \
   && echo "$PREV2" | grep -Eq "DEAD *\$"; then
    note ok "WATCH carries $N_FLAP2 FLAP event(s), last after DEAD with flap_count $COUNT2 flaps_10m $RECENT2"
else
    note fail "WATCH carries $COUNT2 FLAP event(s), last after DEAD with flap_count $COUNT2 flaps_10m $RECENT2 (got $N_FLAP2 events, last '$LAST2', prev types '$PREV2')"
    grep '^EVENT' "$TMP/watch1.out" | tail -10
fi
# The FLAP counts climb 1, 2, 3, ... in stream order: each event
# is one more completed cycle, never a repeat or a gap.
if echo "$FLAP2" | awk '{print $2}' | python3 -c "
import sys
counts = [int(l) for l in sys.stdin]
exit(0 if counts == list(range(1, len(counts) + 1)) else 1)
"; then
    note ok "FLAP flap_count values climb 1..$COUNT2 in stream order"
else
    note fail "FLAP flap_count values climb 1..$COUNT2 in stream order (got $(echo "$FLAP2" | awk '{print $2}' | tr '\n' ' '))"
    grep '^EVENT.*FLAP' "$TMP/watch1.out" || true
fi

# 4. The persisted <data-dir>/events file carries the same FLAP
# lines as the WATCH stream did, a WATCH FROM 0 replay shows
# them too, and a WATCH FLAP FROM 0 stream carries only FLAP
# events.
if grep -Eq " FLAP 127.0.0.1:17978 [0-9]+ [0-9]+ .* flaps_10m=$RECENT2 flap_count=$COUNT2\$" "$TMP/n1/events" \
   && [ "$(grep -cE ' FLAP 127\.0\.0\.1:17978 ' "$TMP/n1/events")" = "$COUNT2" ]; then
    note ok "persisted events file carries $COUNT2 FLAP line(s), last with flaps_10m $RECENT2 flap_count $COUNT2"
else
    note fail "persisted events file carries $COUNT2 FLAP line(s), last with flaps_10m $RECENT2 flap_count $COUNT2"
    grep 'FLAP' "$TMP/n1/events" 2>/dev/null || echo "(no events file)"
fi
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18977" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ FLAP 127\\.0\\.0\\.1:17978 [0-9]+ [0-9]+ .* flaps_10m=$RECENT2 flap_count=$COUNT2\$" 10; then
    note ok "replayed WATCH shows the last FLAP event with the same counts"
else
    note fail "replayed WATCH shows the last FLAP event with the same counts"
    grep '^EVENT.*FLAP' "$TMP/watch2.out" 2>/dev/null
fi
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18977" WATCH FLAP FROM 0 > "$TMP/watch3.out" 2>&1 & echo $! > "$TMP/watch3.pid")
if wait_for_count "$TMP/watch3.out" '^EVENT [0-9]+ FLAP ' "$COUNT2" 10 \
   && [ "$(grep -c '^EVENT ' "$TMP/watch3.out")" = "$COUNT2" ]; then
    note ok "WATCH FLAP FROM 0 carries only the $COUNT2 FLAP event(s)"
else
    note fail "WATCH FLAP FROM 0 carries only the $COUNT2 FLAP event(s)"
    grep '^EVENT' "$TMP/watch3.out" 2>/dev/null | head -20
fi

echo "test_flap_event: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
