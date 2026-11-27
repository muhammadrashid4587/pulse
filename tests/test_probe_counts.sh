#!/usr/bin/env bash
# Integration test for per-member probe counts (0.66.0): how many
# direct probes this node sent to each peer and how many were
# answered — the probe success rate — exposed as
# "probes_sent" / "probes_answered" per member in JSON, one
# "PROBES <id> <sent> <answered>" line per probed member after
# the RTT lines in METRICS, and
# pulse_member_probes_sent_total /
# pulse_member_probes_answered_total counters in PROMETHEUS.
# The values are read off the live control-plane replies and
# parsed, not grepped from source.
#  1. Two nodes start from one shared --peer-file (which also lists
#     a third peer that never starts) and converge on alive=2, with
#     a 10 s suspect timeout so the peer stays SUSPECT long enough
#     to observe.
#  2. While both are ALIVE the live peer's PROBES line carries sent
#     >= answered >= 1, the JSON counts agree with METRICS (the
#     counts only grow, so a METRICS read sits between two JSON
#     reads), and the PROMETHEUS counters exist for the peer with
#     values at least the METRICS ones (read later, so only
#     larger); self reports 0 / 0 in JSON and has no PROBES line
#     and no probe counters, and the never-started peer has a
#     PROBES line with sent >= 1 and answered 0 — probed, never
#     answered — with the same 0 in JSON and a 0 answered counter.
#  3. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 marks it SUSPECT, and its PROBES line
#     is still present — answered frozen at the death (no new ACK
#     can arrive), sent still climbing while the peer is probed —
#     with answered agreeing with JSON.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-probe-counts-test.XXXXXX)"
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

# json_probes <control-port> <member-id>: print the member's
# "probes_sent,probes_answered" from the live JSON reply with
# python3.
json_probes() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print('%s,%s' % (m.get('probes_sent'), m.get('probes_answered')))
        break
"
}

# prom_probe <control-port> <metric> <member-id>: print the
# PROMETHEUS counter value for a member, or "absent" when the
# series does not exist.
prom_probe() {
    local v
    v="$(ctl "$1" PROMETHEUS | grep "^$2{member=\"$3\"} " | awk '{print $2}' || true)"
    [ -n "$v" ] && echo "$v" || echo "absent"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17961-17962 (gossip) and 18961-18962 (control) so this test
# never clashes with the other suites. 17969 is listed but never
# started: the probed-but-never-answering peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17961
127.0.0.1:17962
127.0.0.1:17969
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17961 --peer-file "$TMP/peers" --control-port 18961 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17962 --peer-file "$TMP/peers" --control-port 18962 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# 2. While both are ALIVE, the live peer's PROBES line carries sent
# >= answered >= 1, and the counts only grow, so a METRICS read is
# sandwiched by two JSON reads and the PROMETHEUS counters read
# after are at least the METRICS values; self reports 0 / 0 in
# JSON and has no PROBES line and no probe counters, and the
# never-started peer has a PROBES line with sent >= 1 and
# answered 0 — probed, never answered — with the same 0 in JSON
# and a 0 answered counter.
PEER_JSON1="$(json_probes 18961 127.0.0.1:17962)"
SELF_JSON="$(json_probes 18961 127.0.0.1:17961)"
NEVER_JSON="$(json_probes 18961 127.0.0.1:17969)"
METRICS_PEER="$(ctl 18961 METRICS | grep '^PROBES 127.0.0.1:17962 ' || true)"
METRICS_SELF="$(ctl 18961 METRICS | grep '^PROBES 127.0.0.1:17961 ' || true)"
METRICS_NEVER="$(ctl 18961 METRICS | grep '^PROBES 127.0.0.1:17969 ' || true)"
PEER_JSON2="$(json_probes 18961 127.0.0.1:17962)"
NEVER_JSON2="$(json_probes 18961 127.0.0.1:17969)"
PROM_SENT="$(prom_probe 18961 pulse_member_probes_sent_total 127.0.0.1:17962)"
PROM_ANS="$(prom_probe 18961 pulse_member_probes_answered_total 127.0.0.1:17962)"
PROM_SELF_SENT="$(prom_probe 18961 pulse_member_probes_sent_total 127.0.0.1:17961)"
PROM_NEVER_SENT="$(prom_probe 18961 pulse_member_probes_sent_total 127.0.0.1:17969)"
PROM_NEVER_ANS="$(prom_probe 18961 pulse_member_probes_answered_total 127.0.0.1:17969)"
M_SENT="$(echo "$METRICS_PEER" | awk '{print $3}')"
M_ANS="$(echo "$METRICS_PEER" | awk '{print $4}')"
J1_SENT="$(echo "$PEER_JSON1" | cut -d, -f1)"
J1_ANS="$(echo "$PEER_JSON1" | cut -d, -f2)"
J2_SENT="$(echo "$PEER_JSON2" | cut -d, -f1)"
J2_ANS="$(echo "$PEER_JSON2" | cut -d, -f2)"
N_SENT="$(echo "$METRICS_NEVER" | awk '{print $3}')"
N_ANS="$(echo "$METRICS_NEVER" | awk '{print $4}')"
NJ_SENT="$(echo "$NEVER_JSON" | cut -d, -f1)"
NJ_ANS="$(echo "$NEVER_JSON" | cut -d, -f2)"
NJ2_SENT="$(echo "$NEVER_JSON2" | cut -d, -f1)"
if [ -n "$METRICS_PEER" ] && [ -z "$METRICS_SELF" ] \
   && [ "$SELF_JSON" = "0,0" ] \
   && [ -n "$METRICS_NEVER" ] && [ "$N_ANS" = "0" ] \
   && [ "$NJ_ANS" = "0" ] && [ "$PROM_NEVER_ANS" = "0" ] \
   && [ "$PROM_SELF_SENT" = "absent" ] \
   && python3 -c "exit(0 if int('$M_SENT') >= int('$M_ANS') >= 1 and int('$J1_SENT') <= int('$M_SENT') <= int('$J2_SENT') and int('$J1_ANS') <= int('$M_ANS') <= int('$J2_ANS') and int('$PROM_SENT') >= int('$M_SENT') and int('$PROM_ANS') >= int('$M_ANS') and int('$N_SENT') >= 1 and int('$NJ_SENT') <= int('$N_SENT') <= int('$NJ2_SENT') and int('$PROM_NEVER_SENT') >= int('$N_SENT') else 1)" 2>/dev/null; then
    note ok "live peer PROBES line sent >= answered >= 1 ($METRICS_PEER), JSON agrees ($PEER_JSON1 / $PEER_JSON2), PROMETHEUS counters agree ($PROM_SENT / $PROM_ANS); self has none (JSON $SELF_JSON); never-started peer probed but never answered ($METRICS_NEVER, JSON $NEVER_JSON)"
else
    note fail "live peer PROBES line sent >= answered >= 1 (metrics peer '$METRICS_PEER', self '$METRICS_SELF', never-started '$METRICS_NEVER', json peer '$PEER_JSON1' then '$PEER_JSON2', self '$SELF_JSON', never-started '$NEVER_JSON' then '$NEVER_JSON2', prom sent/ans '$PROM_SENT' / '$PROM_ANS', prom self '$PROM_SELF_SENT', prom never-started '$PROM_NEVER_SENT' / '$PROM_NEVER_ANS')"
    ctl 18961 METRICS
fi

# The PROBES lines ride after the RTT lines, before END — the
# established per-member order is PHI, SUSPECT, LASTSEEN, RTT,
# PROBES.
METRICS_ORDER="$(ctl 18961 METRICS | grep -E '^(LASTSEEN|RTT|PROBES|END)' | awk '{print $1}' | tr '\n' ' ')"
if [ "$METRICS_ORDER" = "LASTSEEN RTT PROBES PROBES END " ]; then
    note ok "PROBES lines ride after the RTT lines, before END"
else
    note fail "PROBES lines ride after the RTT lines, before END (got '$METRICS_ORDER')"
    ctl 18961 METRICS
fi

# 3. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through probing.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17962" 10; then
    note ok "node 1 marks the crashed peer SUSPECT"
else
    note fail "node 1 marks the crashed peer SUSPECT"
    tail -5 "$TMP/n1.log"
fi

# The PROBES line is still present while the peer sits SUSPECT: no
# new ACK can arrive, so answered is frozen, while sent still
# climbs as the prober keeps probing the SUSPECT peer — read twice
# a second apart and check answered is identical and sent does not
# fall, and that answered still agrees with JSON.
METRICS_SUSPECT="$(ctl 18961 METRICS | grep '^PROBES 127.0.0.1:17962 ' || true)"
S_SENT="$(echo "$METRICS_SUSPECT" | awk '{print $3}')"
S_ANS="$(echo "$METRICS_SUSPECT" | awk '{print $4}')"
J_SUSPECT="$(json_probes 18961 127.0.0.1:17962)"
sleep 1
METRICS_SUSPECT2="$(ctl 18961 METRICS | grep '^PROBES 127.0.0.1:17962 ' || true)"
S2_SENT="$(echo "$METRICS_SUSPECT2" | awk '{print $3}')"
S2_ANS="$(echo "$METRICS_SUSPECT2" | awk '{print $4}')"
J_ANS="$(echo "$J_SUSPECT" | cut -d, -f2)"
if [ -n "$METRICS_SUSPECT" ] && [ "$S_ANS" = "$S2_ANS" ] \
   && python3 -c "exit(0 if int('$S_SENT') >= int('$S_ANS') >= 1 and int('$S2_SENT') >= int('$S_SENT') and int('$S_ANS') == int('$J_ANS') else 1)" 2>/dev/null; then
    note ok "METRICS still carries the crashed peer's PROBES line while SUSPECT, answered frozen at the death ($METRICS_SUSPECT -> $METRICS_SUSPECT2)"
else
    note fail "METRICS still carries the crashed peer's PROBES line while SUSPECT, answered frozen at the death (metrics '$METRICS_SUSPECT', again '$METRICS_SUSPECT2', json '$J_SUSPECT')"
    ctl 18961 METRICS
fi

echo "test_probe_counts: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
