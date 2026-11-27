#!/usr/bin/env bash
# Integration test for per-member probe RTT in METRICS (0.63.0):
# the last and mean direct-probe RTT JSON reports per member as
# rtt_ms / rtt_mean_ms and PROMETHEUS as pulse_member_rtt_ms /
# pulse_member_rtt_mean_ms now also ride in the text protocol —
# one "RTT <id> <last_ms> <mean_ms>" line per member that has
# answered a probe, after the LASTSEEN lines — so an operator
# polling the text protocol can watch a peer's RTT climb without
# parsing JSON or scraping Prometheus. Worst probe RTT in METRICS
# (0.65.0): the line now carries a fourth field, the worst single
# probe round trip JSON reports as rtt_max_ms — "RTT <id> <last>
# <mean> <max>" — so the text protocol shows the slow tail per
# peer too, not just the mean. The values are read off the
# live control-plane replies and parsed, not grepped from source.
#  1. Two nodes start from one shared --peer-file (which also lists
#     a third peer that never starts) and converge on alive=2, with
#     a 10 s suspect timeout so the peer stays SUSPECT long enough
#     to observe.
#  2. While both are ALIVE the live peer's METRICS RTT line carries
#     the same last and mean as its JSON rtt_ms / rtt_mean_ms —
#     both numbers >= 0 — while self reports rtt_ms null in JSON
#     and has no RTT line, and the never-started peer reports null
#     and has none either.
#  3. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 marks it SUSPECT, and its RTT line is
#     still present with the same values a second apart — the last
#     measured RTT, frozen at the death, not a live one — until GC,
#     with its mean agreeing with JSON within 1 (an ACK already in
#     flight when the peer died can move the last RTT by a
#     millisecond between two reads).
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-rtt-metrics-test.XXXXXX)"
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

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

# json_rtt <control-port> <member-id>: print the member's
# "rtt_ms,rtt_mean_ms,rtt_max_ms" as JSON ("null,null,null" when
# self or never heard from), parsed from the live JSON reply
# with python3.
json_rtt() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print('%s,%s,%s' % (json.dumps(m.get('rtt_ms')), json.dumps(m.get('rtt_mean_ms')), json.dumps(m.get('rtt_max_ms'))))
        break
"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17951-17952 (gossip) and 18951-18952 (control) so this test
# never clashes with the other suites. 17959 is listed but never
# started: the never-heard-from peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17951
127.0.0.1:17952
127.0.0.1:17959
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17951 --peer-file "$TMP/peers" --control-port 18951 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17952 --peer-file "$TMP/peers" --control-port 18952 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# 2. While both are ALIVE, the live peer's METRICS RTT line carries
# three non-negative integers, and its JSON rtt_ms / rtt_mean_ms /
# rtt_max_ms are numbers too — the last RTT flips between 0 and 1
# ms on loopback as probes land, so the two surfaces are read a
# moment apart and are checked for range, not byte equality (the
# frozen SUSPECT case below checks exact equality). The mean is
# checked against a JSON reading taken both before and after the
# METRICS read: a slow probe landing between two reads can move
# the mean by more than 1 while the sample count is still small,
# so agreement with either bracketing reading counts — while the
# max is sandwiched the same way, since it only ever grows: the
# METRICS max is at least the earlier JSON max and at most the
# later one, and at least the last and mean on the same line; self
# and the never-started peer report null in JSON and have no RTT
# line.
PEER_RTT="$(json_rtt 18951 127.0.0.1:17952)"
SELF_RTT="$(json_rtt 18951 127.0.0.1:17951)"
NEVER_RTT="$(json_rtt 18951 127.0.0.1:17959)"
METRICS_PEER="$(ctl 18951 METRICS | grep '^RTT 127.0.0.1:17952 ' || true)"
PEER_RTT_AFTER="$(json_rtt 18951 127.0.0.1:17952)"
METRICS_SELF="$(ctl 18951 METRICS | grep '^RTT 127.0.0.1:17951 ' || true)"
METRICS_NEVER="$(ctl 18951 METRICS | grep '^RTT 127.0.0.1:17959 ' || true)"
PEER_LAST="$(echo "$PEER_RTT" | cut -d, -f1)"
PEER_MEAN="$(echo "$PEER_RTT" | cut -d, -f2)"
PEER_MAX="$(echo "$PEER_RTT" | cut -d, -f3)"
PEER_MEAN_AFTER="$(echo "$PEER_RTT_AFTER" | cut -d, -f2)"
PEER_MAX_AFTER="$(echo "$PEER_RTT_AFTER" | cut -d, -f3)"
M_LAST="$(echo "$METRICS_PEER" | awk '{print $3}')"
M_MEAN="$(echo "$METRICS_PEER" | awk '{print $4}')"
M_MAX="$(echo "$METRICS_PEER" | awk '{print $5}')"
if [ -n "$METRICS_PEER" ] && [ -z "$METRICS_SELF" ] && [ -z "$METRICS_NEVER" ] \
   && [ "$SELF_RTT" = "null,null,null" ] && [ "$NEVER_RTT" = "null,null,null" ] \
   && python3 -c "exit(0 if 0 <= int('$PEER_LAST') and 0 <= int('$PEER_MEAN') and 0 <= int('$PEER_MAX') and 0 <= int('$M_LAST') < 10000 and 0 <= int('$M_MEAN') < 10000 and 0 <= int('$M_MAX') < 10000 and (abs(int('$M_MEAN') - int('$PEER_MEAN')) <= 1 or abs(int('$M_MEAN') - int('$PEER_MEAN_AFTER')) <= 1) and int('$PEER_MAX') <= int('$M_MAX') <= int('$PEER_MAX_AFTER') and int('$M_MAX') >= int('$M_LAST') and int('$M_MAX') >= int('$M_MEAN') else 1)" 2>/dev/null; then
    note ok "METRICS RTT line for the live peer is three non-negative numbers ($METRICS_PEER), JSON agrees ($PEER_RTT / $PEER_RTT_AFTER), none for self or the never-started peer"
else
    note fail "METRICS RTT line for the live peer is three non-negative numbers (metrics '$METRICS_PEER', json '$PEER_RTT' then '$PEER_RTT_AFTER', self '$METRICS_SELF', never-started '$METRICS_NEVER', json self '$SELF_RTT', never-started '$NEVER_RTT')"
    ctl 18951 METRICS
fi

# The RTT line rides after the LASTSEEN lines, before END — the
# established per-member order is PHI, SUSPECT, LASTSEEN, RTT.
METRICS_ORDER="$(ctl 18951 METRICS | grep -E '^(LASTSEEN|RTT|END)' | awk '{print $1}' | tr '\n' ' ')"
if [ "$METRICS_ORDER" = "LASTSEEN RTT END " ]; then
    note ok "RTT line rides after the LASTSEEN lines, before END"
else
    note fail "RTT line rides after the LASTSEEN lines, before END (got '$METRICS_ORDER')"
    ctl 18951 METRICS
fi

# 3. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through probing.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17952" 10; then
    note ok "node 1 marks the crashed peer SUSPECT"
else
    note fail "node 1 marks the crashed peer SUSPECT"
    tail -5 "$TMP/n1.log"
fi

# The RTT line is still present while the peer sits SUSPECT, with
# the same values as JSON — the last measured RTT, frozen at the
# death, not a live one — read twice a second apart to prove it
# does not move. The mean is compared within 1, not byte-equal:
# a probe ACK that was already in flight when the peer died can
# land between two reads and move the last RTT by a millisecond;
# the max is compared exactly, since a frozen peer takes no new
# probes and the worst can only grow on a new sample.
PEER_RTT2="$(json_rtt 18951 127.0.0.1:17952)"
METRICS_SUSPECT="$(ctl 18951 METRICS | grep '^RTT 127.0.0.1:17952 ' || true)"
M2_LAST="$(echo "$METRICS_SUSPECT" | awk '{print $3}')"
M2_MEAN="$(echo "$METRICS_SUSPECT" | awk '{print $4}')"
M2_MAX="$(echo "$METRICS_SUSPECT" | awk '{print $5}')"
J2_MEAN="$(echo "$PEER_RTT2" | cut -d, -f2)"
J2_MAX="$(echo "$PEER_RTT2" | cut -d, -f3)"
sleep 1
METRICS_SUSPECT2="$(ctl 18951 METRICS | grep '^RTT 127.0.0.1:17952 ' || true)"
if [ -n "$METRICS_SUSPECT" ] && [ "$METRICS_SUSPECT" = "$METRICS_SUSPECT2" ] \
   && python3 -c "exit(0 if 0 <= int('$M2_LAST') < 10000 and 0 <= int('$M2_MEAN') < 10000 and 0 <= int('$M2_MAX') < 10000 and abs(int('$M2_MEAN') - int('$J2_MEAN')) <= 1 and int('$M2_MAX') == int('$J2_MAX') else 1)" 2>/dev/null; then
    note ok "METRICS still carries the crashed peer's RTT line while SUSPECT, frozen at the death ($METRICS_SUSPECT)"
else
    note fail "METRICS still carries the crashed peer's RTT line while SUSPECT, frozen at the death (metrics '$METRICS_SUSPECT', again '$METRICS_SUSPECT2', json '$PEER_RTT2')"
    ctl 18951 METRICS
fi

echo "test_rtt_metrics: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
