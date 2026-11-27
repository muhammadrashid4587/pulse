#!/usr/bin/env bash
# Integration test for the probe counts in the event log (0.67.0):
# a WATCH EVENT line carries trailing
# " probes_sent=<n> probes_answered=<n>" — how many direct probes
# this node had sent the member and how many were answered when the
# event was recorded, the same values JSON reports as
# "probes_sent" / "probes_answered" and METRICS as a PROBES line,
# frozen at the event instead of recomputed at replay time — so a
# watcher can see how many probes went unanswered before a
# suspicion fired without polling METRICS for counts that have
# already moved on.
#  1. Two nodes start from one shared --peer-file (which also lists
#     a third peer that never starts) and converge on alive=2, with
#     a WATCH open on node 1.
#  2. A third node joins through --seed: the live stream carries its
#     JOINED event with no probe fields at all — the member was
#     just added to the table and this node has not probed it yet,
#     so there is no count to report, never a fabricated zero.
#  3. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 marks it SUSPECT, and the SUSPECTED
#     event carries probe counts with sent at least answered and
#     answered at least 1 — the peer answered probes before it
#     died — and the DEAD event that follows carries a sent count
#     at least as large and an answered count frozen at the same
#     value, the peer having stopped answering.
#  4. The persisted <data-dir>/events file carries the same probe
#     counts on the SUSPECTED line as the WATCH stream did, so the
#     counts survive a restart and a replay.
#  5. A replay from the start shows the same rule at the other end:
#     the SUSPECTED event for the never-started peer carries probe
#     counts with sent at least 1 and answered 0 — it was probed
#     but never answered — and no last_seen_ms field, since it was
#     never heard from.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-probe-counts-watch-test.XXXXXX)"
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

# event_probes <file> <pattern> <field>: print the probes_sent or
# probes_answered of the first EVENT line in the file matching the
# pattern.
event_probes() {
    grep -E "$2" "$1" | head -n 1 | grep -o "$3=[0-9]*" | cut -d= -f2
}

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17351-17353 (gossip) and 18351-18353 (control) so this test
# never clashes with the other suites. 17359 is listed but never
# started: the probed-but-never-answered peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17351
127.0.0.1:17352
127.0.0.1:17359
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17351 --peer-file "$TMP/peers" --control-port 18351 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17352 --peer-file "$TMP/peers" --control-port 18352 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 60 s bounds it if the test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18351" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 2. A third node joins through --seed: the live stream carries its
# JOINED event with no probe fields — this node had not probed it
# yet when the event was recorded.
(cd "$TMP/n3" && exec "$BIN" --port 17353 --seed 127.0.0.1:17351 --control-port 18353 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17353 [0-9]+ [0-9]+' 10; then
    note ok "WATCH streams JOINED for the joining node"
else
    note fail "WATCH streams JOINED for the joining node"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

JOIN_LINE="$(grep -E '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17353 ' "$TMP/watch1.out" | head -n 1)"
if [ -n "$JOIN_LINE" ] && ! printf '%s' "$JOIN_LINE" | grep -q 'probes_sent='; then
    note ok "JOINED event for the just-added member carries no probe counts"
else
    note fail "JOINED event for the just-added member carries no probe counts (got '$JOIN_LINE')"
fi

# 3. Kill node 2 — with SIGKILL, an unclean crash: a clean SIGTERM
# would announce its leave and node 1 would mark it DEAD straight
# away, never SUSPECTing it, so no probe history would accumulate
# on the SUSPECTED event.
kill -9 "$(cat "$TMP/n2.pid")"
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+$' 10; then
    note ok "WATCH streams SUSPECTED with probe counts"
else
    note fail "WATCH streams SUSPECTED with probe counts"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null; tail -5 "$TMP/n1.log"
fi

SUSPECT_SENT="$(event_probes "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17352 ' probes_sent)"
SUSPECT_ANS="$(event_probes "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17352 ' probes_answered)"
if [ -n "$SUSPECT_SENT" ] && [ -n "$SUSPECT_ANS" ] \
   && python3 -c "exit(0 if int('$SUSPECT_SENT') >= int('$SUSPECT_ANS') >= 1 else 1)" 2>/dev/null; then
    note ok "SUSPECTED event probe counts are sane: $SUSPECT_SENT sent, $SUSPECT_ANS answered"
else
    note fail "SUSPECTED event probe counts are sane (sent '$SUSPECT_SENT', answered '$SUSPECT_ANS')"
fi

# The DEAD event that follows carries a sent count at least as
# large — the node kept probing while the peer sat SUSPECT — and
# an answered count frozen at the same value, the peer having
# stopped answering.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+$' 10; then
    note ok "WATCH streams DEAD with probe counts"
else
    note fail "WATCH streams DEAD with probe counts"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

DEAD_SENT="$(event_probes "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17352 ' probes_sent)"
DEAD_ANS="$(event_probes "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17352 ' probes_answered)"
if [ -n "$DEAD_SENT" ] && [ -n "$DEAD_ANS" ] && [ -n "$SUSPECT_SENT" ] && [ -n "$SUSPECT_ANS" ] \
   && python3 -c "exit(0 if int('$DEAD_SENT') >= int('$SUSPECT_SENT') and int('$DEAD_ANS') == int('$SUSPECT_ANS') else 1)" 2>/dev/null; then
    note ok "DEAD event sent ($DEAD_SENT) grew past SUSPECTED ($SUSPECT_SENT) while answered stayed frozen at $DEAD_ANS"
else
    note fail "DEAD event sent grew while answered stayed frozen (dead sent '$DEAD_SENT' answered '$DEAD_ANS', suspected sent '$SUSPECT_SENT' answered '$SUSPECT_ANS')"
fi

# 4. The persisted event log carries the same probe counts on the
# SUSPECTED line as the WATCH stream did: the values recorded at
# the event, not values recomputed later.
if wait_for "$TMP/n1/events" "^[0-9]+ SUSPECTED 127\\.0\\.0\\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=$SUSPECT_SENT probes_answered=$SUSPECT_ANS\$" 10; then
    note ok "persisted events file carries the same probe counts ($SUSPECT_SENT sent, $SUSPECT_ANS answered) on the SUSPECTED line"
else
    note fail "persisted events file carries the same probe counts on the SUSPECTED line"
    cat "$TMP/n1/events" 2>/dev/null
fi

# 5. A replay from the start shows the same rule at the other end:
# the SUSPECTED event for the never-started peer carries probe
# counts with sent at least 1 and answered 0 — it was probed but
# never answered — and no last_seen_ms field, since it was never
# heard from.
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18351" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17359 [0-9]+ [0-9]+ probes_sent=[0-9]+ probes_answered=0$' 10; then
    note ok "replayed SUSPECTED for the never-started peer carries probe counts with 0 answered and no last_seen_ms"
else
    note fail "replayed SUSPECTED for the never-started peer carries probe counts with 0 answered and no last_seen_ms"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi

REPLAY_SENT="$(event_probes "$TMP/watch2.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17359 ' probes_sent)"
if [ -n "$REPLAY_SENT" ] && python3 -c "exit(0 if int('$REPLAY_SENT') >= 1 else 1)" 2>/dev/null; then
    note ok "replayed SUSPECTED for the never-started peer was probed at least once ($REPLAY_SENT sent)"
else
    note fail "replayed SUSPECTED for the never-started peer was probed at least once (got '$REPLAY_SENT')"
fi

echo "test_probe_counts_watch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
