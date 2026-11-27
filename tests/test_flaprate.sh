#!/usr/bin/env bash
# Integration test for the flap-rate alert (0.76.0): when an ALIVE
# peer's completed flap cycles in the last 10 minutes reach
# --flap-rate-threshold, the node logs one "FLAPRATE <id> flaps=<n>
# threshold=<t>" line, records one FLAPRATE event on the WATCH
# stream (and in the persisted events file) carrying the count
# frozen as flaps_10m, and flags the member "flap_rate":true in
# JSON with a flap_rate_peers count in METRICS and JSON, a
# FLAPRATE line in METRICS, and a pulse_flap_rate_peers gauge in
# PROMETHEUS — while the peer is still ALIVE, before the next
# suspicion fires. The warning is once per crossing: a peer that
# keeps flapping warns once, not once per probe cycle.
#  1. Two nodes start from one shared --peer-file with
#     --flap-rate-threshold 3 and converge on alive=2, with a
#     WATCH open on node 1. Before any crash the peer reads
#     "flap_rate":false with flap_count 0, METRICS says
#     flap_rate_peers=0 with no FLAPRATE line, and PROMETHEUS
#     says pulse_flap_rate_peers 0.
#  2. Kill node 2 with SIGKILL and restart it: one completed
#     cycle, so the peer reads flap_count at least 1 — but still
#     "flap_rate":false with no FLAPRATE line anywhere: one flap
#     is below the threshold of 3. (A slow restart can add a
#     genuine refuted-suspicion flap of its own, so the count
#     after one cycle is 1 or 2 — still below 3 either way.)
#  3. Kill and restart node 2 again, up to two more times, until
#     the FLAPRATE fires: each kill is one more completed cycle,
#     so three kills guarantee at least 3 recent flaps, and a
#     restart flap can only bring the firing earlier. Node 1 logs
#     exactly one FLAPRATE line, the WATCH stream carries exactly
#     one FLAPRATE event with flaps_10m at least 3, and while the
#     peer is ALIVE its JSON says "flap_rate":true, METRICS says
#     flap_rate_peers=1 with a FLAPRATE line whose count agrees
#     with JSON, and PROMETHEUS says pulse_flap_rate_peers 1.
#  4. The persisted <data-dir>/events file carries the same
#     FLAPRATE line as the WATCH stream did, and a WATCH FROM 0
#     replay shows it too — the frozen flaps_10m survives a
#     restart and a replay.
# FLAPOK is not covered end to end here: the recovery fires when
# flaps age out of the 10-minute window, which no short test can
# wait for — the predicate, including the ageing, is covered at
# the unit level in test_member.c instead.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-flaprate-test.XXXXXX)"
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

# json_metrics <control-port> <field>: a field of the metrics
# object from the live JSON reply.
json_metrics() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
print(json.load(sys.stdin)['metrics'].get('$2', ''))
"
}

# metrics_flaprate <control-port> <member-id>: the FLAPRATE line
# for the member from the live METRICS reply, or empty when there
# is none.
metrics_flaprate() {
    ctl "$1" METRICS | grep "^FLAPRATE $2 " || true
}

# prom_gauge <control-port> <series>: the value of the unlabelled
# gauge from the live PROMETHEUS reply, or empty when there is no
# sample. Anchored to the start of the line: the "# HELP ..." line
# for the same family contains the name too, and an unanchored
# grep would take its last word instead of the value.
prom_gauge() {
    ctl "$1" PROMETHEUS | grep -E "^$2 " | head -n 1 | awk '{print $NF}' || true
}

# prom_member_gauge <control-port> <member-id> <series>: the value
# of the member's gauge from the live PROMETHEUS reply.
prom_member_gauge() {
    ctl "$1" PROMETHEUS | grep -F "$3{member=\"$2\"} " | sed 's/.*} //' || true
}

# flaprate_event_flaps <file>: the flaps_10m of the first FLAPRATE
# EVENT line in the file, or empty when there is none.
flaprate_event_flaps() {
    grep -E '^EVENT [0-9]+ FLAPRATE ' "$1" | head -n 1 | grep -o 'flaps_10m=[0-9]*' | cut -d= -f2
}

start_n2() {
    (cd "$TMP/n2" && exec "$BIN" --port 17974 --peer-file "$TMP/peers" --control-port 18974 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17973-17974 (gossip) and 18973-18974 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17973
127.0.0.1:17974
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17973 --peer-file "$TMP/peers" --control-port 18973 --flap-rate-threshold 3 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
start_n2

# 1. Both nodes converge from the peer file, and the START line
# shows the flap-rate threshold the node is watching with.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

if grep -q "START 127.0.0.1:17973 .* flap-rate=3" "$TMP/n1.log"; then
    note ok "START line shows flap-rate=3"
else
    note fail "START line shows flap-rate=3"
    grep '^.*START' "$TMP/n1.log" | head -2
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test so every event is captured live.
(timeout 120 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18973" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")
if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# Before any crash the peer is not flap-rate: flap_count 0,
# flap_rate false, no FLAPRATE line anywhere.
if [ "$(json_member 18973 127.0.0.1:17974 flap_rate)" = "False" ] \
   && [ "$(json_member 18973 127.0.0.1:17974 flap_count)" = "0" ] \
   && [ "$(json_metrics 18973 flap_rate_peers)" = "0" ] \
   && [ -z "$(metrics_flaprate 18973 127.0.0.1:17974)" ] \
   && [ "$(prom_gauge 18973 pulse_flap_rate_peers)" = "0" ]; then
    note ok "a healthy peer reads flap_rate false with no FLAPRATE line anywhere"
else
    note fail "a healthy peer reads flap_rate false with no FLAPRATE line anywhere"
    ctl 18973 JSON | sed -n '1p'
    ctl 18973 METRICS | grep -E 'FLAP|flap_rate' || true
fi

# 2. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through probing — then restart it: one completed
# cycle, still below the threshold of 3.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17974" 10; then
    note ok "node 1 marks the crashed peer DEAD"
else
    note fail "node 1 marks the crashed peer DEAD"
    tail -5 "$TMP/n1.log"
fi
start_n2
if wait_for_json_state 18973 127.0.0.1:17974 ALIVE 10; then
    note ok "restarted peer comes back ALIVE after the first crash"
else
    note fail "restarted peer comes back ALIVE after the first crash"
    tail -5 "$TMP/n1.log"
fi
# One kill is one flap, and a slow restart can add a genuine
# refuted-suspicion flap of its own — so the count is 1 or 2,
# still below the threshold of 3 either way, and no FLAPRATE
# has fired.
COUNT1="$(json_member 18973 127.0.0.1:17974 flap_count)"
if python3 -c "exit(0 if 1 <= int('$COUNT1') <= 2 else 1)" 2>/dev/null \
   && [ "$(json_member 18973 127.0.0.1:17974 flap_rate)" = "False" ] \
   && ! grep -q "FLAPRATE 127.0.0.1:17974 " "$TMP/n1.log"; then
    note ok "after one crash, flap_count $COUNT1 is below the threshold and no FLAPRATE fired"
else
    note fail "after one crash, flap_count is below the threshold and no FLAPRATE fired (got $COUNT1)"
    grep 'FLAPRATE' "$TMP/n1.log" || true
fi

# 3. Kill and restart node 2 again, up to two more times, until
# the FLAPRATE fires: each kill is one more completed cycle, so
# three kills guarantee at least 3 recent flaps, and a restart
# flap can only bring the firing earlier. The loop breaks the
# moment the log shows the FLAPRATE line.
FIRED=0
CYCLE=2
while [ "$CYCLE" -le 4 ]; do
    kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
    if ! wait_for_count "$TMP/n1.log" "DEAD 127.0.0.1:17974" "$CYCLE" 10; then
        note fail "node 1 marks the peer DEAD on cycle $CYCLE"
        tail -5 "$TMP/n1.log"
        break
    fi
    start_n2
    if ! wait_for_json_state 18973 127.0.0.1:17974 ALIVE 10; then
        note fail "restarted peer comes back ALIVE on cycle $CYCLE"
        tail -5 "$TMP/n1.log"
        break
    fi
    if wait_for "$TMP/n1.log" "FLAPRATE 127.0.0.1:17974 flaps=" 10; then
        FIRED=1
        break
    fi
    CYCLE=$((CYCLE + 1))
done
if [ "$FIRED" = "1" ]; then
    note ok "FLAPRATE fired after $CYCLE kill/restart cycles"
else
    note fail "FLAPRATE fired within 4 kill/restart cycles"
    tail -5 "$TMP/n1.log"
fi

# Exactly one FLAPRATE line: once per episode, not once per probe
# cycle — give it a second to fire a duplicate if it would.
sleep 1
if [ "$(grep -c 'FLAPRATE 127.0.0.1:17974 flaps=' "$TMP/n1.log")" = "1" ]; then
    note ok "node 1 logs exactly one FLAPRATE line"
else
    note fail "node 1 logs exactly one FLAPRATE line"
    grep 'FLAPRATE' "$TMP/n1.log" || true
fi

# The log line carries the count and the threshold.
if grep -Eq "FLAPRATE 127.0.0.1:17974 flaps=([3-9]|1[0-6]) threshold=3" "$TMP/n1.log"; then
    note ok "FLAPRATE log line carries flaps at least 3 and threshold=3"
else
    note fail "FLAPRATE log line carries flaps at least 3 and threshold=3"
    grep 'FLAPRATE' "$TMP/n1.log" || true
fi

# While the peer is ALIVE its JSON says flap_rate true, with a
# recent count at least 3.
FLAPS="$(json_member 18973 127.0.0.1:17974 flaps_10m)"
if [ "$(json_member 18973 127.0.0.1:17974 flap_rate)" = "True" ] \
   && python3 -c "exit(0 if int('$FLAPS') >= 3 else 1)" 2>/dev/null; then
    note ok "while ALIVE, the peer reads flap_rate true with flaps_10m $FLAPS"
else
    note fail "while ALIVE, the peer reads flap_rate true with flaps_10m at least 3 (got $FLAPS)"
    ctl 18973 JSON | sed -n '1p'
fi

# METRICS agrees: flap_rate_peers=1 and a FLAPRATE line whose
# count agrees with JSON.
METRICS_FLAP="$(metrics_flaprate 18973 127.0.0.1:17974)"
if ctl 18973 METRICS | grep -q "flap_rate_peers=1" \
   && [ "$METRICS_FLAP" = "FLAPRATE 127.0.0.1:17974 $FLAPS" ]; then
    note ok "METRICS carries flap_rate_peers=1 and FLAPRATE 127.0.0.1:17974 $FLAPS"
else
    note fail "METRICS carries flap_rate_peers=1 and FLAPRATE 127.0.0.1:17974 $FLAPS (got '$METRICS_FLAP')"
    ctl 18973 METRICS | grep -E 'FLAP|flap_rate' || true
fi

# PROMETHEUS agrees: the flap-rate gauge is 1, and the
# per-member flaps_10m gauge agrees with JSON too.
PROM_FLAPS="$(prom_member_gauge 18973 127.0.0.1:17974 pulse_member_flaps_10m)"
if [ "$(prom_gauge 18973 pulse_flap_rate_peers)" = "1" ] \
   && [ "$PROM_FLAPS" = "$FLAPS" ]; then
    note ok "PROMETHEUS carries pulse_flap_rate_peers 1 and flaps_10m $FLAPS"
else
    note fail "PROMETHEUS carries pulse_flap_rate_peers 1 and flaps_10m $FLAPS (got $PROM_FLAPS)"
    ctl 18973 PROMETHEUS | grep -E 'flap_rate|flaps_10m' || true
fi

# The WATCH stream carries exactly one FLAPRATE event with the
# count frozen as flaps_10m, at least 3.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ FLAPRATE 127\.0\.0\.1:17974 ' 10; then
    note ok "WATCH streams a FLAPRATE event"
else
    note fail "WATCH streams a FLAPRATE event"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi
EVENT_FLAPS="$(flaprate_event_flaps "$TMP/watch1.out")"
if [ "$(grep -c '^EVENT [0-9]* FLAPRATE 127\.0\.0\.1:17974 ' "$TMP/watch1.out")" = "1" ] \
   && python3 -c "exit(0 if int('$EVENT_FLAPS') >= 3 else 1)" 2>/dev/null; then
    note ok "WATCH carries exactly one FLAPRATE event with flaps_10m $EVENT_FLAPS"
else
    note fail "WATCH carries exactly one FLAPRATE event with flaps_10m at least 3 (got $EVENT_FLAPS)"
    grep '^EVENT.*FLAPRATE' "$TMP/watch1.out" 2>/dev/null
fi

# 4. The persisted <data-dir>/events file carries the same
# FLAPRATE line as the WATCH stream did, and a WATCH FROM 0
# replay shows it too.
if grep -Eq " FLAPRATE 127.0.0.1:17974 [0-9]+ [0-9]+ .* flaps_10m=$EVENT_FLAPS\$" "$TMP/n1/events"; then
    note ok "persisted events file carries the FLAPRATE line with flaps_10m $EVENT_FLAPS"
else
    note fail "persisted events file carries the FLAPRATE line with flaps_10m $EVENT_FLAPS"
    grep 'FLAPRATE' "$TMP/n1/events" 2>/dev/null || echo "(no events file)"
fi
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18973" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ FLAPRATE 127\\.0\\.0\\.1:17974 [0-9]+ [0-9]+ .* flaps_10m=$EVENT_FLAPS\$" 10; then
    note ok "replayed WATCH shows the FLAPRATE event with the same flaps_10m"
else
    note fail "replayed WATCH shows the FLAPRATE event with the same flaps_10m"
    grep '^EVENT.*FLAPRATE' "$TMP/watch2.out" 2>/dev/null
fi

echo "test_flaprate: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
