#!/usr/bin/env bash
# Integration test for the WATCH heartbeat (0.32.0): a long-lived
# WATCH on a quiet cluster is indistinguishable from a dead
# connection unless the stream says it is alive, so the server
# sends a HEARTBEAT line with the current unix time once 5 s pass
# with nothing sent.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. A WATCH on node 1 opens with OK, SNAPSHOTs, END.
#  2. With the cluster quiet, a first HEARTBEAT line arrives within
#     ~10 s, carrying a numeric unix timestamp close to now, and a
#     second one follows about 5 s later — the heartbeat repeats
#     instead of firing once.
#  3. No EVENT line arrives while the cluster is quiet: the stream
#     is alive, nothing happened.
#  4. A third node joining through --seed still streams JOINED on
#     the same connection, after the heartbeats — the heartbeat
#     does not break the event stream, and it carries no sequence
#     number, so the JOINED is the next event after the snapshot
#     with no gap in the numbering.
# The watchers run under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-heartbeat-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch.pid" ] && kill "$(cat "$TMP/watch.pid")" 2>/dev/null
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
# until at least <count> lines match <pattern>.
wait_count() {
    local log="$1" pat="$2" want="$3" timeout="$4" i
    for i in $(seq 1 $((timeout * 5))); do
        if [ "$(grep -c "$pat" "$log" 2>/dev/null)" -ge "$want" ]; then
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17851-17853 (gossip) and 18851-18853 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17851
127.0.0.1:17852
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17851 --peer-file "$TMP/peers" --control-port 18851 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17852 --peer-file "$TMP/peers" --control-port 18852 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 60 s bounds it if the test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18851" WATCH > "$TMP/watch.out" 2>&1 & echo $! > "$TMP/watch.pid")
if ! wait_for "$TMP/watch.out" "^END$" 10; then
    note fail "WATCH opens on node 1"
    cat "$TMP/watch.out" 2>/dev/null
fi

# 2. The cluster is quiet, so the stream sends a HEARTBEAT once
# 5 s pass with nothing sent: the first one lands within ~10 s
# of the open (the 5 s idle mark, plus scheduling slack), and a
# second follows about 5 s after the first.
if wait_for "$TMP/watch.out" '^HEARTBEAT [0-9]+$' 12; then
    note ok "idle WATCH sends a HEARTBEAT line"
else
    note fail "idle WATCH sends a HEARTBEAT line"
    cat "$TMP/watch.out"
fi
HB="$(grep '^HEARTBEAT ' "$TMP/watch.out" | head -n 1 | awk '{print $2}')"
NOW="$(date +%s)"
if [ -n "$HB" ] && [ "$HB" -ge $((NOW - 30)) ] && [ "$HB" -le $((NOW + 30)) ]; then
    note ok "HEARTBEAT carries a unix timestamp close to now"
else
    note fail "HEARTBEAT carries a unix timestamp close to now"
    echo "heartbeat=$HB now=$NOW"
fi
if wait_count "$TMP/watch.out" '^HEARTBEAT ' 2 12; then
    note ok "a second HEARTBEAT follows about 5 s later"
else
    note fail "a second HEARTBEAT follows about 5 s later"
    grep '^HEARTBEAT ' "$TMP/watch.out"
fi

# 3. Nothing happened while the cluster was quiet: no EVENT line
# arrived — the heartbeats are liveness, not events, and they
# carry no sequence number.
if [ "$(grep -c '^EVENT' "$TMP/watch.out")" -eq 0 ]; then
    note ok "no EVENT lines on a quiet cluster, only HEARTBEATs"
else
    note fail "no EVENT lines on a quiet cluster, only HEARTBEATs"
    grep '^EVENT' "$TMP/watch.out"
fi

# 4. A third node joins through --seed only: JOINED still streams
# on the same connection, after the heartbeats.
(cd "$TMP/n3" && exec "$BIN" --port 17853 --seed 127.0.0.1:17851 --control-port 18853 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17853 1 [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams JOINED on the same connection after the heartbeats"
else
    note fail "WATCH streams JOINED on the same connection after the heartbeats"
    echo "--- watch.out"; cat "$TMP/watch.out"
fi
if [ "$(grep -n '^HEARTBEAT ' "$TMP/watch.out" | head -n 1 | cut -d: -f1)" -lt \
     "$(grep -n '^EVENT .* JOINED 127\.0\.0\.1:17853 ' "$TMP/watch.out" | head -n 1 | cut -d: -f1)" ]; then
    note ok "heartbeats come before the JOINED, in stream order"
else
    note fail "heartbeats come before the JOINED, in stream order"
    cat "$TMP/watch.out"
fi

# The watcher is still alive at the end: the stream stayed open
# for the whole test instead of dropping.
if kill -0 "$(cat "$TMP/watch.pid")" 2>/dev/null; then
    note ok "WATCH connection stayed open for the whole test"
else
    note fail "WATCH connection stayed open for the whole test"
fi

echo "test_watch_heartbeat: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
