#!/usr/bin/env bash
# Integration test for the last-contact age in the event log (0.61.0):
# a WATCH EVENT line carries a trailing " last_seen_ms=<ms>" — the
# ms since this node last heard from the member when the event was
# recorded, the same value JSON reports as "last_seen_ms", frozen at
# the event instead of recomputed at replay time — so a watcher can
# see how quiet a peer was when a transition happened without
# polling JSON for a value that has already moved on.
#  1. Two nodes start from one shared --peer-file (which also lists
#     a third peer that never starts) and converge on alive=2, with
#     a WATCH open on node 1.
#  2. A third node joins through --seed: the live stream carries its
#     JOINED event with a small last_seen_ms — it was just heard
#     from — while a replay shows the never-started peer's
#     SUSPECTED event with no trailing field at all: never heard
#     from means no value, never a fabricated zero.
#  3. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 marks it SUSPECT, and the SUSPECTED
#     event carries a last_seen_ms of at least one ack timeout —
#     the peer was silent for the whole escalation — and the DEAD
#     event that follows carries a larger one, the silence growing
#     across the suspect timeout.
#  4. The persisted <data-dir>/events file carries the same
#     last_seen_ms on the SUSPECTED line as the WATCH stream did,
#     so the age survives a restart and a replay.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-last-seen-watch-test.XXXXXX)"
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

# event_seen_ms <file> <pattern>: print the last_seen_ms of the
# first EVENT line in the file matching the pattern.
event_seen_ms() {
    grep -E "$2" "$1" | head -n 1 | grep -o 'last_seen_ms=[0-9]*' | cut -d= -f2
}

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17321-17323 (gossip) and 18321-18323 (control) so this test
# never clashes with the other suites. 17329 is listed but never
# started: the never-heard-from peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17321
127.0.0.1:17322
127.0.0.1:17329
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17321 --peer-file "$TMP/peers" --control-port 18321 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17322 --peer-file "$TMP/peers" --control-port 18322 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

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
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18321" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 2. A third node joins through --seed: the live stream carries its
# JOINED event with a small last_seen_ms — it was just heard from.
(cd "$TMP/n3" && exec "$BIN" --port 17323 --seed 127.0.0.1:17321 --control-port 18323 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17323 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams JOINED with a last_seen_ms for the joining node"
else
    note fail "WATCH streams JOINED with a last_seen_ms for the joining node"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

JOIN_SEEN="$(event_seen_ms "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17323 ')"
if [ -n "$JOIN_SEEN" ] && python3 -c "exit(0 if 0 <= int('$JOIN_SEEN') < 1000 else 1)" 2>/dev/null; then
    note ok "JOINED event last_seen_ms is small, the peer was just heard from ($JOIN_SEEN ms)"
else
    note fail "JOINED event last_seen_ms is small, the peer was just heard from (got '$JOIN_SEEN')"
fi

# 3. Kill node 2 — with SIGKILL, an unclean crash: a clean SIGTERM
# would announce its leave and node 1 would mark it DEAD straight
# away, never SUSPECTing it, so no silence would ever accumulate.
# Suspicion needs the direct probe timeout plus the indirect one,
# so the age at SUSPECTED is at least one ack timeout (100 ms).
kill -9 "$(cat "$TMP/n2.pid")"
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17322 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams SUSPECTED with a last_seen_ms"
else
    note fail "WATCH streams SUSPECTED with a last_seen_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null; tail -5 "$TMP/n1.log"
fi

SUSPECT_SEEN="$(event_seen_ms "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17322 ')"
if [ -n "$SUSPECT_SEEN" ] && python3 -c "exit(0 if int('$SUSPECT_SEEN') >= 100 else 1)" 2>/dev/null; then
    note ok "SUSPECTED event last_seen_ms is at least one ack timeout of silence ($SUSPECT_SEEN ms)"
else
    note fail "SUSPECTED event last_seen_ms is at least one ack timeout of silence (got '$SUSPECT_SEEN')"
fi

# The DEAD event that follows carries a larger age: the silence
# kept growing across the 300 ms suspect timeout.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17322 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams DEAD with a last_seen_ms"
else
    note fail "WATCH streams DEAD with a last_seen_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

DEAD_SEEN="$(event_seen_ms "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17322 ')"
if [ -n "$DEAD_SEEN" ] && [ -n "$SUSPECT_SEEN" ] \
   && python3 -c "exit(0 if int('$DEAD_SEEN') > int('$SUSPECT_SEEN') else 1)" 2>/dev/null; then
    note ok "DEAD event last_seen_ms ($DEAD_SEEN ms) is larger than the SUSPECTED one ($SUSPECT_SEEN ms)"
else
    note fail "DEAD event last_seen_ms is larger than the SUSPECTED one (dead '$DEAD_SEEN', suspected '$SUSPECT_SEEN')"
fi

# 4. The persisted event log carries the same age on the
# SUSPECTED line as the WATCH stream did: the value recorded at
# the event, not a value recomputed later.
if wait_for "$TMP/n1/events" "^[0-9]+ SUSPECTED 127\\.0\\.0\\.1:17322 [0-9]+ [0-9]+ last_seen_ms=$SUSPECT_SEEN( probes_sent=[0-9]+ probes_answered=[0-9]+)?\$" 10; then
    note ok "persisted events file carries the same last_seen_ms ($SUSPECT_SEEN ms) on the SUSPECTED line"
else
    note fail "persisted events file carries the same last_seen_ms ($SUSPECT_SEEN ms) on the SUSPECTED line"
    cat "$TMP/n1/events" 2>/dev/null
fi

# 5. A replay from the start shows the same rule at the other end:
# the SUSPECTED event for the never-started peer carries no
# trailing field at all — it was never heard from, so there is no
# age to report — while the JOINED for the node that joined live
# carries its age.
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18321" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17329 [0-9]+ [0-9]+ probes_sent=[0-9]+ probes_answered=0$' 10; then
    note ok "replayed SUSPECTED for the never-started peer carries no last_seen_ms field"
else
    note fail "replayed SUSPECTED for the never-started peer carries no last_seen_ms field"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi
if wait_for "$TMP/watch2.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17323 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "replayed JOINED for the live-joined node carries its last_seen_ms"
else
    note fail "replayed JOINED for the live-joined node carries its last_seen_ms"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi

echo "test_last_seen_watch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
