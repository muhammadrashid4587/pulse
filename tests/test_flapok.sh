#!/usr/bin/env bash
# Integration test for the flap-rate recovery (0.78.0): the FLAPOK
# end of the flap-rate alert from 0.76.0. When an ALIVE peer's
# completed flap cycles in the last 10 minutes fall back below
# --flap-rate-threshold as the flaps age out of the window, the
# node logs one "FLAPOK <id> flaps=<n> threshold=<t>" line,
# records one FLAPOK event on the WATCH stream (and in the
# persisted events file) carrying the fallen count frozen as
# flaps_10m, and clears the flag: "flap_rate":false in JSON with
# flap_rate_peers=0 in METRICS and JSON, no FLAPRATE line in
# METRICS, and pulse_flap_rate_peers 0 in PROMETHEUS — while the
# peer is still ALIVE. The warning is once per episode: one
# FLAPOK line, not one per probe cycle.
#  1. Two nodes start from one shared --peer-file with
#     --flap-rate-threshold 3 and converge on alive=2, with a
#     WATCH open on node 1. Node 2 is killed with SIGKILL and
#     restarted, up to four times, until the FLAPRATE fires —
#     the same drive as tests/test_flaprate.sh, which covers the
#     FLAPRATE side in full; here it is only the setup, so the
#     checks are light: exactly one FLAPRATE line and one
#     FLAPRATE event, and JSON "flap_rate":true while ALIVE.
#  2. Then the test waits — really waits — for the flaps to age
#     out of the 10-minute window. The first flap completed at
#     the first restart, so the recovery fires about 10 minutes
#     after that, when the recent count drops from 3 to 2. The
#     wait polls the log for up to 12 minutes; a run of this
#     test takes about 11 minutes end to end. There is no
#     shortcut: a shorter window would test a different
#     configuration, not the documented 10-minute one, and the
#     predicate's ageing is already covered at the unit level
#     in test_member.c — this test is the live proof that the
#     recovery fires on a real cluster with the real window.
#  3. When the FLAPOK fires: node 1 logs exactly one FLAPOK
#     line with flaps=1 or 2 (the count drops from 3 to 2 as the
#     oldest flap ages out — or to 1, when two flaps age out in
#     the same probe cycle, which happens when their completions
#     landed within ~100 ms of each other during a restart storm
#     — so the fallen count is 1 or 2 with a threshold of 3; the
#     test reads the fallen count off the log line and checks the
#     event, the persisted file and the replay carry that same
#     value), the WATCH stream carries exactly one FLAPOK event
#     with that same frozen flaps_10m, and while the peer is ALIVE
#     its JSON says "flap_rate":false with flaps_10m below 3,
#     METRICS says flap_rate_peers=0 with no FLAPRATE line and a
#     FLAP line whose recent count is below 3 too, and PROMETHEUS
#     says pulse_flap_rate_peers 0 with pulse_member_flaps_10m
#     below 3 too. The live counts are ranges, not exact
#     values: the count keeps falling as the remaining flaps
#     age out seconds after the FLAPOK, so a live query may
#     already read 1 or 0 — the frozen log line and event are
#     the exact proof, and no exact agreement between two
#     live queries a moment apart is asserted.
#  4. The persisted <data-dir>/events file carries the same
#     FLAPOK line as the WATCH stream did, and a WATCH FROM 0
#     replay shows it too — the frozen flaps_10m survives a
#     restart and a replay.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-flapok-test.XXXXXX)"
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

# metrics_flap <control-port> <member-id>: the FLAP line for the
# member from the live METRICS reply, or empty when there is none.
metrics_flap() {
    ctl "$1" METRICS | grep "^FLAP $2 " || true
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

# flapok_event_flaps <file>: the flaps_10m of the first FLAPOK
# EVENT line in the file, or empty when there is none.
flapok_event_flaps() {
    grep -E '^EVENT [0-9]+ FLAPOK ' "$1" | head -n 1 | grep -o 'flaps_10m=[0-9]*' | cut -d= -f2
}

start_n2() {
    (cd "$TMP/n2" && exec "$BIN" --port 17976 --peer-file "$TMP/peers" --control-port 18976 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17975-17976 (gossip) and 18975-18976 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17975
127.0.0.1:17976
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17975 --peer-file "$TMP/peers" --control-port 18975 --flap-rate-threshold 3 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
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

if grep -q "START 127.0.0.1:17975 .* flap-rate=3" "$TMP/n1.log"; then
    note ok "START line shows flap-rate=3"
else
    note fail "START line shows flap-rate=3"
    grep '^.*START' "$TMP/n1.log" | head -2
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test so every event is captured live. The 900 s
# timeout covers the ~10-minute wait for the flaps to age out
# plus the setup and the checks after it.
(timeout 900 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18975" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")
if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# Drive node 2 through kill/restart cycles until the FLAPRATE
# fires: each kill is one more completed cycle, so three kills
# guarantee at least 3 recent flaps, and a restart flap can only
# bring the firing earlier. The loop breaks the moment the log
# shows the FLAPRATE line. This is the setup for the recovery,
# covered in full by tests/test_flaprate.sh — the checks here
# are only that the episode started.
FIRED=0
CYCLE=1
while [ "$CYCLE" -le 4 ]; do
    kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
    if ! wait_for_count "$TMP/n1.log" "DEAD 127.0.0.1:17976" "$CYCLE" 10; then
        note fail "node 1 marks the peer DEAD on cycle $CYCLE"
        tail -5 "$TMP/n1.log"
        break
    fi
    start_n2
    if ! wait_for_json_state 18975 127.0.0.1:17976 ALIVE 10; then
        note fail "restarted peer comes back ALIVE on cycle $CYCLE"
        tail -5 "$TMP/n1.log"
        break
    fi
    if wait_for "$TMP/n1.log" "FLAPRATE 127.0.0.1:17976 flaps=" 10; then
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
# cycle — give it a second to fire a duplicate if it would — and
# the peer reads flap_rate true while ALIVE, so the episode the
# recovery has to end is really open.
sleep 1
if [ "$(grep -c 'FLAPRATE 127.0.0.1:17976 flaps=' "$TMP/n1.log")" = "1" ] \
   && [ "$(json_member 18975 127.0.0.1:17976 flap_rate)" = "True" ]; then
    note ok "exactly one FLAPRATE line and the peer reads flap_rate true"
else
    note fail "exactly one FLAPRATE line and the peer reads flap_rate true"
    grep 'FLAPRATE' "$TMP/n1.log" || true
    ctl 18975 JSON | sed -n '1p'
fi

# 2. Wait for the recovery: the first flap completed at the first
# restart, so the recent count drops from 3 to 2 about 10 minutes
# after that — the FLAPOK fires on the first probe cycle that
# sees it below the threshold. The wait is 12 minutes from here,
# which leaves about two minutes of margin over the 10-minute
# window plus the setup cycles; the whole test runs about 11
# minutes.
echo "waiting up to 12 minutes for the flaps to age out of the 10-minute window..."
if wait_for "$TMP/n1.log" "FLAPOK 127.0.0.1:17976 flaps=" 720; then
    note ok "FLAPOK fired within 12 minutes"
else
    note fail "FLAPOK fired within 12 minutes"
    tail -5 "$TMP/n1.log"
    grep 'FLAPOK\|FLAPRATE' "$TMP/n1.log" || true
fi

# The fallen count the FLAPOK carries: 1 or 2 with a threshold of
# 3 — 2 when the oldest flap ages out alone, 1 when two flaps age
# out in the same probe cycle. Read it off the log line; the
# event, the persisted file and the replay must carry this same
# value, whatever it is.
FLAPOK_FLAPS="$(grep -o 'FLAPOK 127.0.0.1:17976 flaps=[0-9]*' "$TMP/n1.log" | head -n 1 | grep -o '[0-9]*$')"
if [ -n "$FLAPOK_FLAPS" ] \
   && python3 -c "exit(0 if 1 <= int('$FLAPOK_FLAPS') <= 2 else 1)" 2>/dev/null; then
    note ok "FLAPOK fired with flaps=$FLAPOK_FLAPS threshold=3"
else
    note fail "FLAPOK fired with flaps 1 or 2 (got '$FLAPOK_FLAPS')"
    grep 'FLAPOK' "$TMP/n1.log" || true
fi

# Exactly one FLAPOK line: once per episode, not once per probe
# cycle — give it a second to fire a duplicate if it would.
sleep 1
if [ "$(grep -c 'FLAPOK 127.0.0.1:17976 flaps=' "$TMP/n1.log")" = "1" ]; then
    note ok "node 1 logs exactly one FLAPOK line"
else
    note fail "node 1 logs exactly one FLAPOK line"
    grep 'FLAPOK' "$TMP/n1.log" || true
fi

# While the peer is ALIVE its JSON says flap_rate false, with a
# recent count below the threshold. The count is 1 or 2 the moment
# the recovery fires — and then keeps falling: the flaps that are
# still in the window completed within a second or two of the ones
# that just aged out (a kill flap and the restart's refuted
# flaps land together), so by the time this query lands the count
# may already be 1 or 0. The range is the honest check for a
# live, falling count — the frozen log line and event above are
# the exact proof, with the fallen count read off the log line.
FLAPS="$(json_member 18975 127.0.0.1:17976 flaps_10m)"
if [ "$(json_member 18975 127.0.0.1:17976 flap_rate)" = "False" ] \
   && python3 -c "exit(0 if 0 <= int('$FLAPS') <= 2 else 1)" 2>/dev/null; then
    note ok "while ALIVE, the peer reads flap_rate false with flaps_10m $FLAPS"
else
    note fail "while ALIVE, the peer reads flap_rate false with flaps_10m 0, 1 or 2 (got $FLAPS)"
    ctl 18975 JSON | sed -n '1p'
fi

# METRICS agrees: flap_rate_peers=0, no FLAPRATE line any more,
# and a FLAP line whose recent count is below the threshold too.
# No exact agreement with JSON is asserted here: the count is
# still falling as flaps age out, so a flap can age out between
# the JSON query and this one — the frozen log line and event
# are the exact-2 proof, this check is that every live surface
# agrees the peer is below the threshold.
METRICS_FLAP="$(metrics_flap 18975 127.0.0.1:17976)"
METRICS_RECENT="$(echo "$METRICS_FLAP" | awk '{print $4}')"
if ctl 18975 METRICS | grep -q "flap_rate_peers=0" \
   && [ -z "$(metrics_flaprate 18975 127.0.0.1:17976)" ] \
   && python3 -c "exit(0 if 0 <= int('$METRICS_RECENT') <= 2 else 1)" 2>/dev/null; then
    note ok "METRICS carries flap_rate_peers=0, no FLAPRATE line, and FLAP recent $METRICS_RECENT"
else
    note fail "METRICS carries flap_rate_peers=0, no FLAPRATE line, and FLAP recent 0, 1 or 2 (got '$METRICS_FLAP')"
    ctl 18975 METRICS | grep -E 'FLAP|flap_rate' || true
fi

# PROMETHEUS agrees: the flap-rate gauge is 0, and the
# per-member flaps_10m gauge is below the threshold too — same
# falling-count caveat as METRICS, so a range, not an equality
# with the JSON value read a moment earlier.
PROM_FLAPS="$(prom_member_gauge 18975 127.0.0.1:17976 pulse_member_flaps_10m)"
if [ "$(prom_gauge 18975 pulse_flap_rate_peers)" = "0" ] \
   && python3 -c "exit(0 if 0 <= int('$PROM_FLAPS') <= 2 else 1)" 2>/dev/null; then
    note ok "PROMETHEUS carries pulse_flap_rate_peers 0 and flaps_10m $PROM_FLAPS"
else
    note fail "PROMETHEUS carries pulse_flap_rate_peers 0 and flaps_10m 0, 1 or 2 (got $PROM_FLAPS)"
    ctl 18975 PROMETHEUS | grep -E 'flap_rate|flaps_10m' || true
fi

# The WATCH stream carries exactly one FLAPOK event with the
# count frozen as flaps_10m, the same value as the log line.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ FLAPOK 127\.0\.0\.1:17976 ' 10; then
    note ok "WATCH streams a FLAPOK event"
else
    note fail "WATCH streams a FLAPOK event"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi
EVENT_FLAPS="$(flapok_event_flaps "$TMP/watch1.out")"
if [ "$(grep -c '^EVENT [0-9]* FLAPOK 127\.0\.0\.1:17976 ' "$TMP/watch1.out")" = "1" ] \
   && [ "$EVENT_FLAPS" = "$FLAPOK_FLAPS" ]; then
    note ok "WATCH carries exactly one FLAPOK event with flaps_10m $EVENT_FLAPS"
else
    note fail "WATCH carries exactly one FLAPOK event with flaps_10m $FLAPOK_FLAPS (got $EVENT_FLAPS)"
    grep '^EVENT.*FLAPOK' "$TMP/watch1.out" 2>/dev/null
fi

# 3. The persisted <data-dir>/events file carries the same
# FLAPOK line as the WATCH stream did, and a WATCH FROM 0
# replay shows it too.
if grep -Eq " FLAPOK 127.0.0.1:17976 [0-9]+ [0-9]+ .* flaps_10m=$FLAPOK_FLAPS\$" "$TMP/n1/events"; then
    note ok "persisted events file carries the FLAPOK line with flaps_10m $FLAPOK_FLAPS"
else
    note fail "persisted events file carries the FLAPOK line with flaps_10m $FLAPOK_FLAPS"
    grep 'FLAPOK' "$TMP/n1/events" 2>/dev/null || echo "(no events file)"
fi
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18975" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ FLAPOK 127\\.0\\.0\\.1:17976 [0-9]+ [0-9]+ .* flaps_10m=$FLAPOK_FLAPS\$" 10; then
    note ok "replayed WATCH shows the FLAPOK event with the same flaps_10m"
else
    note fail "replayed WATCH shows the FLAPOK event with the same flaps_10m"
    grep '^EVENT.*FLAPOK' "$TMP/watch2.out" 2>/dev/null
fi

echo "test_flapok: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
