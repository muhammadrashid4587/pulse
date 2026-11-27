#!/usr/bin/env bash
# Integration test for the configurable WATCH event-log ring size
# (0.36.0): a node started with --event-log-cap N holds that many
# membership events for replay, and METRICS / JSON / PROMETHEUS
# report the cap.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. Node 1 runs with --event-log-cap 4, node 2 with the
#     default. METRICS on node 1 reports watch_event_log_cap=4 and
#     on node 2 watch_event_log_cap=128; JSON carries
#     "event_log_cap":4 / 128 in the "watch" object, and PROMETHEUS
#     carries "pulse_watch_event_log_cap 4" on node 1.
#  2. The small ring actually evicts: killing node 2 twice with
#     SIGKILL (with a restart between) records more than 4 events
#     on node 1 — SUSPECTED + DEAD, REVIVED, SUSPECTED + DEAD — so
#     a WATCH FROM 1 on node 1 answers with a GAP line instead of
#     replaying from seq 1. The kills must be SIGKILL, not a plain
#     SIGTERM: a clean shutdown sends a LEAVE announcement and node
#     1 marks the peer DEAD at once with no SUSPECTED first, so two
#     graceful kills record only DEAD, REVIVED, DEAD — three events,
#     which never evict the 4-event ring, and the GAP never fires
#     (the test then passes only when a restart transient adds a
#     SUSPECTED + REVIVED pair of its own, which is timing, not a
#     guarantee). SIGKILL is an unclean death, so the probe timeout
#     suspects the peer and the suspect timeout declares it dead —
#     one SUSPECTED + DEAD per kill, deterministically. No JOINED
#     is recorded for node 2 either: it arrives from the shared
#     peer file, so it is already in node 1's table at startup.
#  3. A bad cap is a clean startup error: --event-log-cap 0 and
#     --event-log-cap 100001 both exit 2 with an error message.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-event-log-cap-test.XXXXXX)"
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

# wait_count <logfile> <pattern> <min-count> <timeout-seconds>:
# like wait_for, but waits until <pattern> has appeared at least
# <min-count> times — a plain wait_for would match the first kill's
# DEAD line again on the second kill.
wait_count() {
    local log="$1" pat="$2" min="$3" timeout="$4" i n
    for i in $(seq 1 $((timeout * 5))); do
        # grep -c prints 0 on no match but exits 1, so a plain
        # "|| echo 0" would append a second 0 and break the -ge
        # test; swallow the exit status and default an empty
        # count (missing log file) to 0 instead.
        n=$(grep -Ec "$pat" "$log" 2>/dev/null || true)
        n=${n:-0}
        if [ "$n" -ge "$min" ]; then return 0; fi
        sleep 0.2
    done
    return 1
}

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

start_n2() {
    (cd "$TMP/n2" && exec "$BIN" --port 17882 --peer-file "$TMP/peers" --control-port 18882 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17881-17882 (gossip) and 18881-18882 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17881
127.0.0.1:17882
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17881 --peer-file "$TMP/peers" --control-port 18881 --event-log-cap 4 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
start_n2

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi
if ctl 18881 METRICS | grep -Fq "watch_event_log_cap=4"; then
    note ok "METRICS on the --event-log-cap 4 node reports watch_event_log_cap=4"
else
    note fail "METRICS on the --event-log-cap 4 node reports watch_event_log_cap=4"
    ctl 18881 METRICS
fi
if ctl 18882 METRICS | grep -Fq "watch_event_log_cap=128"; then
    note ok "METRICS on the default node reports watch_event_log_cap=128"
else
    note fail "METRICS on the default node reports watch_event_log_cap=128"
    ctl 18882 METRICS
fi
if ctl 18881 JSON | grep -Fq '"event_log_cap":4'; then
    note ok "JSON on the --event-log-cap 4 node reports event_log_cap 4"
else
    note fail "JSON on the --event-log-cap 4 node reports event_log_cap 4"
    ctl 18881 JSON
fi
if ctl 18882 JSON | grep -Fq '"event_log_cap":128'; then
    note ok "JSON on the default node reports event_log_cap 128"
else
    note fail "JSON on the default node reports event_log_cap 128"
    ctl 18882 JSON
fi
if ctl 18881 PROMETHEUS | grep -Fq "pulse_watch_event_log_cap 4"; then
    note ok "PROMETHEUS on the --event-log-cap 4 node reports pulse_watch_event_log_cap 4"
else
    note fail "PROMETHEUS on the --event-log-cap 4 node reports pulse_watch_event_log_cap 4"
    ctl 18881 PROMETHEUS
fi

# 2. The small ring evicts: kill node 2 twice with SIGKILL, with
# a restart between, so node 1 records more than 4 events
# (SUSPECTED + DEAD, REVIVED, SUSPECTED + DEAD — five at least;
# a slow restart can add a transient SUSPECTED + REVIVED of its
# own, which only helps). A WATCH FROM 1 then answers with a GAP
# line instead of replaying from seq 1. SIGKILL, not SIGTERM:
# a graceful kill sends LEAVE and node 1 marks the peer DEAD at
# once with no SUSPECTED first, so two graceful kills record
# only three events and never evict the ring (see the header).
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
rm -f "$TMP/n2.pid"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17882" 10; then
    note ok "node 1 marks node 2 DEAD after the first kill"
else
    note fail "node 1 marks node 2 DEAD after the first kill"
    tail -5 "$TMP/n1.log"
fi
start_n2
# Node 2's log is fresh after the restart (start_n2 truncates it),
# so this really waits for the restart to converge — node 1's log
# already holds a STATUS alive=2 line from before the kill.
if wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "node 2 restarts and the cluster converges again"
else
    note fail "node 2 restarts and the cluster converges again"
    tail -5 "$TMP/n1.log"
fi
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
rm -f "$TMP/n2.pid"
if wait_count "$TMP/n1.log" "DEAD 127.0.0.1:17882" 2 10; then
    note ok "node 1 marks node 2 DEAD after the second kill"
else
    note fail "node 1 marks node 2 DEAD after the second kill"
    tail -5 "$TMP/n1.log"
fi
(timeout 10 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18881" WATCH FROM 1 > "$TMP/watch.out" 2>&1 & echo $! > "$TMP/watch.pid")
if wait_for "$TMP/watch.out" "^GAP 1 [0-9]+$" 10; then
    note ok "WATCH FROM 1 on the 4-event ring answers with a GAP line"
else
    note fail "WATCH FROM 1 on the 4-event ring answers with a GAP line"
    cat "$TMP/watch.out" 2>/dev/null
fi
kill "$(cat "$TMP/watch.pid")" 2>/dev/null
rm -f "$TMP/watch.pid"

# 3. A bad cap is a clean startup error, not a silent default.
if "$BIN" --port 17883 --event-log-cap 0 --data-dir "$TMP/bad" 2> "$TMP/bad0.err"; then
    note fail "--event-log-cap 0 exits nonzero"
else
    if grep -Fq -- "--event-log-cap must be between 1 and 100000" "$TMP/bad0.err"; then
        note ok "--event-log-cap 0 exits 2 with an error message"
    else
        note fail "--event-log-cap 0 exits 2 with an error message"
        cat "$TMP/bad0.err"
    fi
fi
if "$BIN" --port 17883 --event-log-cap 100001 --data-dir "$TMP/bad" 2> "$TMP/bad1.err"; then
    note fail "--event-log-cap 100001 exits nonzero"
else
    if grep -Fq -- "--event-log-cap must be between 1 and 100000" "$TMP/bad1.err"; then
        note ok "--event-log-cap 100001 exits 2 with an error message"
    else
        note fail "--event-log-cap 100001 exits 2 with an error message"
        cat "$TMP/bad1.err"
    fi
fi

echo "test_watch_event_log_cap: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
