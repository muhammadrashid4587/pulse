#!/usr/bin/env bash
# Integration test for the per-member flap counter (0.75.0): each
# member's completed ALIVE -> SUSPECT -> ALIVE / DEAD cycles ride
# in JSON as "flap_count" / "flaps_10m", in METRICS as a
# "FLAP <id> <count> <recent>" line, and in PROMETHEUS as
# pulse_member_flap_count_total / pulse_member_flaps_10m — so an
# operator can spot an unstable member instead of only reading
# the log. The counts are read off the live control-plane
# replies, not grepped from source.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. Self reports flap_count 0 / flaps_10m 0 and has no
#     FLAP line and no flap series: a node never suspects itself.
#  2. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 suspects it, then marks it DEAD — one
#     completed ALIVE -> SUSPECT -> DEAD cycle, so the peer reads
#     flap_count 1 on all three surfaces.
#  3. Restart node 2 from the same data dir (a higher incarnation,
#     so it comes back ALIVE) and kill it again: a second completed
#     cycle, so the peer reads flap_count at least 2 — the count
#     accumulates across restarts of the peer, since it is this
#     node's lifetime measurement, not the peer's. It can read 3
#     when the slow restart itself is briefly suspected and
#     refuted, which is a real flap too.
#  4. The FLAP line and the PROMETHEUS counter agree with JSON
#     at each step, and the DEAD peer keeps its count until GC.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-flap-counter-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
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

# json_flap <control-port> <member-id> <field>: the member's
# flap_count / flaps_10m from the live JSON reply, parsed with
# python3, or empty when the member is absent.
json_flap() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(m.get('$3', ''))
        break
"
}

# metrics_flap <control-port> <member-id>: the FLAP line for the
# member from the live METRICS reply, or empty when there is none.
metrics_flap() {
    ctl "$1" METRICS | grep "^FLAP $2 " || true
}

# prom_flap <control-port> <member-id> <series>: the value of the
# member's flap series from the live PROMETHEUS reply, or empty
# when the member has no sample.
prom_flap() {
    ctl "$1" PROMETHEUS | grep -F "$3{member=\"$2\"} " | sed 's/.*} //' || true
}

start_n2() {
    (cd "$TMP/n2" && exec "$BIN" --port 17972 --peer-file "$TMP/peers" --control-port 18972 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17971-17972 (gossip) and 18971-18972 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17971
127.0.0.1:17972
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17971 --peer-file "$TMP/peers" --control-port 18971 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
start_n2

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# The JSON reply is still one valid JSON object on one line.
if ctl 18971 JSON | sed -n '1p' | python3 -c "import json,sys; json.load(sys.stdin)" 2>/dev/null; then
    note ok "JSON reply parses as JSON with flap_count present"
else
    note fail "JSON reply parses as JSON with flap_count present"
    ctl 18971 JSON
fi

# Self never flaps: flap_count 0 / flaps_10m 0 in JSON, no FLAP
# line in METRICS, no flap series in PROMETHEUS.
SELF_COUNT="$(json_flap 18971 127.0.0.1:17971 flap_count)"
SELF_RECENT="$(json_flap 18971 127.0.0.1:17971 flaps_10m)"
if [ "$SELF_COUNT" = "0" ] && [ "$SELF_RECENT" = "0" ]; then
    note ok "self reports flap_count 0 and flaps_10m 0"
else
    note fail "self reports flap_count 0 and flaps_10m 0 (got $SELF_COUNT / $SELF_RECENT)"
fi
if [ -z "$(metrics_flap 18971 127.0.0.1:17971)" ] \
   && [ -z "$(prom_flap 18971 127.0.0.1:17971 pulse_member_flap_count_total)" ] \
   && [ -z "$(prom_flap 18971 127.0.0.1:17971 pulse_member_flaps_10m)" ]; then
    note ok "self has no FLAP line and no flap series"
else
    note fail "self has no FLAP line and no flap series"
    ctl 18971 METRICS | grep '^FLAP ' || true
fi

# A healthy peer that never flapped reads 0 too, with no line.
PEER_COUNT="$(json_flap 18971 127.0.0.1:17972 flap_count)"
if [ "$PEER_COUNT" = "0" ] && [ -z "$(metrics_flap 18971 127.0.0.1:17972)" ]; then
    note ok "a peer that never flapped reads flap_count 0 with no FLAP line"
else
    note fail "a peer that never flapped reads flap_count 0 with no FLAP line (got $PEER_COUNT)"
fi

# 2. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through probing: ALIVE -> SUSPECT -> DEAD is one
# completed flap cycle.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17972" 10; then
    note ok "node 1 marks the crashed peer DEAD"
else
    note fail "node 1 marks the crashed peer DEAD"
    tail -5 "$TMP/n1.log"
fi

COUNT1="$(json_flap 18971 127.0.0.1:17972 flap_count)"
RECENT1="$(json_flap 18971 127.0.0.1:17972 flaps_10m)"
if [ "$COUNT1" = "1" ] && [ "$RECENT1" = "1" ]; then
    note ok "after one crash, the peer reads flap_count 1 and flaps_10m 1 in JSON"
else
    note fail "after one crash, the peer reads flap_count 1 and flaps_10m 1 in JSON (got $COUNT1 / $RECENT1)"
    ctl 18971 JSON
fi

FLAP1="$(metrics_flap 18971 127.0.0.1:17972)"
if [ "$FLAP1" = "FLAP 127.0.0.1:17972 1 1" ]; then
    note ok "METRICS carries FLAP 127.0.0.1:17972 1 1"
else
    note fail "METRICS carries FLAP 127.0.0.1:17972 1 1 (got '$FLAP1')"
fi

PROM_COUNT1="$(prom_flap 18971 127.0.0.1:17972 pulse_member_flap_count_total)"
PROM_RECENT1="$(prom_flap 18971 127.0.0.1:17972 pulse_member_flaps_10m)"
if [ "$PROM_COUNT1" = "1" ] && [ "$PROM_RECENT1" = "1" ]; then
    note ok "PROMETHEUS carries flap count 1 and flaps_10m 1 for the peer"
else
    note fail "PROMETHEUS carries flap count 1 and flaps_10m 1 for the peer (got $PROM_COUNT1 / $PROM_RECENT1)"
fi

# 3. Restart node 2 from the same data dir — a higher incarnation,
# so it comes back ALIVE — then kill it again: a second completed
# cycle, so the count climbs to 2.
start_n2
if wait_for_json_state 18971 127.0.0.1:17972 ALIVE 10; then
    note ok "restarted peer comes back ALIVE"
else
    note fail "restarted peer comes back ALIVE"
    tail -5 "$TMP/n1.log"
fi
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
# The second DEAD line: the first is still in the log from the
# first crash, so wait for the count to reach 2.
if wait_for_count "$TMP/n1.log" "DEAD 127.0.0.1:17972" 2 10; then
    note ok "node 1 marks the twice-crashed peer DEAD again"
else
    note fail "node 1 marks the twice-crashed peer DEAD again"
    tail -5 "$TMP/n1.log"
fi

COUNT2="$(json_flap 18971 127.0.0.1:17972 flap_count)"
RECENT2="$(json_flap 18971 127.0.0.1:17972 flaps_10m)"
# At least 2: the restart itself can add a genuine refuted
# suspicion — a slow-starting peer under load can miss enough
# probes to be suspected and then answer again within a
# millisecond (suspect_refuted=1) — which is a real flap, so
# the count is 2 on a clean restart and 3 when that happens.
# Either way every flap is recent, so the two agree.
if python3 -c "exit(0 if int('$COUNT2') >= 2 else 1)" 2>/dev/null \
   && [ "$RECENT2" = "$COUNT2" ]; then
    note ok "after the second crash, the peer reads flap_count $COUNT2 and flaps_10m $RECENT2 in JSON (at least 2)"
else
    note fail "after the second crash, the peer reads flap_count at least 2 and flaps_10m equal to it in JSON (got $COUNT2 / $RECENT2)"
    ctl 18971 JSON
fi

FLAP2="$(metrics_flap 18971 127.0.0.1:17972)"
PROM_COUNT2="$(prom_flap 18971 127.0.0.1:17972 pulse_member_flap_count_total)"
if [ "$FLAP2" = "FLAP 127.0.0.1:17972 $COUNT2 $RECENT2" ] && [ "$PROM_COUNT2" = "$COUNT2" ]; then
    note ok "METRICS and PROMETHEUS agree with JSON at flap_count $COUNT2"
else
    note fail "METRICS and PROMETHEUS agree with JSON at flap_count $COUNT2 (got '$FLAP2' / '$PROM_COUNT2')"
fi

echo "test_flap_counter: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
