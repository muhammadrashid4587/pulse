#!/usr/bin/env bash
# Integration test for gossip traffic counters (0.80.0): how many
# messages of each kind (ping, ack, ping_req, join, gossip, sync)
# this node sent to each peer and received from it — exposed as a
# "traffic" object per member in JSON, one "TRAFFIC <id>
# sent=... recv=..." line per member with any traffic in METRICS,
# and pulse_member_traffic_sent_total /
# pulse_member_traffic_recv_total counters labelled by member and
# kind in PROMETHEUS. The values are read off the live
# control-plane replies and parsed, not grepped from source.
#  1. Two nodes start from one shared --peer-file (which also lists
#     a third peer that never starts) and converge on alive=2, with
#     a 10 s suspect timeout so the peer stays ALIVE long enough
#     to observe.
#  2. While both are ALIVE the live peer's TRAFFIC line carries
#     ping/ack/gossip sent and received each >= 1, the JSON
#     traffic object agrees with METRICS (the counts only grow, so
#     a METRICS read sits between two JSON reads), and the
#     PROMETHEUS counters exist for the peer with values at least
#     the METRICS ones (read later, so only larger); self reports
#     all zeros in JSON for ping/ack/gossip and has no ping/ack/
#     gossip traffic series — it never probes or gossips with
#     itself, though it can carry join/sync when the shared peer
#     file lists its own id — and the never-started peer has a
#     TRAFFIC line with ping sent >= 1 and every received count
#     0 — probed, never heard from — with the same zeros in JSON.
#  3. The TRAFFIC lines ride after the PROBES lines, before END.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-traffic-counters-test.XXXXXX)"
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

# traffic_field <line> <direction> <kind>: print the count for one
# kind out of a TRAFFIC line's sent=/recv= field.
traffic_field() {
    local line="$1" dir="$2" kind="$3" field
    field="$(echo "$line" | sed -n "s/.* $dir=\\([^ ]*\\).*/\\1/p")"
    echo "$field" | tr ',' '\n' | sed -n "s/^$kind://p"
}

# tf0 <line> <direction> <kind>: traffic_field, or 0 when the line
# is absent — a member with no TRAFFIC line exchanged nothing.
tf0() {
    local v
    v="$(traffic_field "$1" "$2" "$3")"
    [ -n "$v" ] && echo "$v" || echo 0
}

# json_traffic <control-port> <member-id>: print the member's
# traffic object as "sent_ping,sent_ack,sent_gossip,recv_ping,
# recv_ack,recv_gossip" from the live JSON reply with python3.
json_traffic() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        t = m.get('traffic', {})
        s, r = t.get('sent', {}), t.get('recv', {})
        print('%s,%s,%s,%s,%s,%s' % (s.get('ping'), s.get('ack'),
              s.get('gossip'), r.get('ping'), r.get('ack'),
              r.get('gossip')))
        break
"
}

# prom_traffic <control-port> <sent|recv> <kind> <member-id>: print
# the PROMETHEUS counter value, or "absent" when the series does
# not exist.
prom_traffic() {
    local v
    v="$(ctl "$1" PROMETHEUS | grep "^pulse_member_traffic_$2_total{member=\"$4\",kind=\"$3\"} " | awk '{print $2}' || true)"
    [ -n "$v" ] && echo "$v" || echo "absent"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17981-17982 (gossip) and 18981-18982 (control) so this test
# never clashes with the other suites. 17989 is listed but never
# started: the probed-but-never-heard-from peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17981
127.0.0.1:17982
127.0.0.1:17989
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17981 --peer-file "$TMP/peers" --control-port 18981 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17982 --peer-file "$TMP/peers" --control-port 18982 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# 2. Poll the live peer's TRAFFIC line until ping/ack/gossip have
# moved in both directions — the probe cycle fires every 100 ms,
# but the gossip digest only goes out while the table is still
# changing, so the first read may catch it before the first
# digest lands. The poll only waits: the values the assertions
# check come from the reads below, in the same order as the
# probe-counts test — JSON, then METRICS, then JSON again — so
# the METRICS read sits between the two JSON reads and the
# counts, which only grow, sandwich it.
TRAFFIC_POLL=""
for _ in $(seq 1 50); do
    TRAFFIC_POLL="$(ctl 18981 METRICS | grep '^TRAFFIC 127.0.0.1:17982 ' || true)"
    [ -n "$TRAFFIC_POLL" ] \
        && [ "$(traffic_field "$TRAFFIC_POLL" sent ping)" -ge 1 ] 2>/dev/null \
        && [ "$(traffic_field "$TRAFFIC_POLL" recv ping)" -ge 1 ] 2>/dev/null \
        && [ "$(traffic_field "$TRAFFIC_POLL" sent ack)" -ge 1 ] 2>/dev/null \
        && [ "$(traffic_field "$TRAFFIC_POLL" recv ack)" -ge 1 ] 2>/dev/null \
        && [ "$(traffic_field "$TRAFFIC_POLL" sent gossip)" -ge 1 ] 2>/dev/null \
        && [ "$(traffic_field "$TRAFFIC_POLL" recv gossip)" -ge 1 ] 2>/dev/null \
        && break
    sleep 0.2
done

PEER_JSON1="$(json_traffic 18981 127.0.0.1:17982)"
TRAFFIC_PEER="$(ctl 18981 METRICS | grep '^TRAFFIC 127.0.0.1:17982 ' || true)"
SELF_JSON="$(json_traffic 18981 127.0.0.1:17981)"
NEVER_JSON="$(json_traffic 18981 127.0.0.1:17989)"
TRAFFIC_SELF="$(ctl 18981 METRICS | grep '^TRAFFIC 127.0.0.1:17981 ' || true)"
TRAFFIC_NEVER="$(ctl 18981 METRICS | grep '^TRAFFIC 127.0.0.1:17989 ' || true)"
PEER_JSON2="$(json_traffic 18981 127.0.0.1:17982)"
NEVER_JSON2="$(json_traffic 18981 127.0.0.1:17989)"
PROM_SENT_PING="$(prom_traffic 18981 sent ping 127.0.0.1:17982)"
PROM_RECV_PING="$(prom_traffic 18981 recv ping 127.0.0.1:17982)"
PROM_SENT_GOSSIP="$(prom_traffic 18981 sent gossip 127.0.0.1:17982)"
PROM_SELF_PING="$(prom_traffic 18981 sent ping 127.0.0.1:17981)"
PROM_NEVER_SENT="$(prom_traffic 18981 sent ping 127.0.0.1:17989)"
PROM_NEVER_RECV="$(prom_traffic 18981 recv ping 127.0.0.1:17989)"
M_SP="$(traffic_field "$TRAFFIC_PEER" sent ping)"
M_SA="$(traffic_field "$TRAFFIC_PEER" sent ack)"
M_SG="$(traffic_field "$TRAFFIC_PEER" sent gossip)"
M_RP="$(traffic_field "$TRAFFIC_PEER" recv ping)"
M_RA="$(traffic_field "$TRAFFIC_PEER" recv ack)"
M_RG="$(traffic_field "$TRAFFIC_PEER" recv gossip)"
J1_SP="$(echo "$PEER_JSON1" | cut -d, -f1)"
J1_SA="$(echo "$PEER_JSON1" | cut -d, -f2)"
J1_SG="$(echo "$PEER_JSON1" | cut -d, -f3)"
J1_RP="$(echo "$PEER_JSON1" | cut -d, -f4)"
J1_RA="$(echo "$PEER_JSON1" | cut -d, -f5)"
J1_RG="$(echo "$PEER_JSON1" | cut -d, -f6)"
J2_SP="$(echo "$PEER_JSON2" | cut -d, -f1)"
J2_SA="$(echo "$PEER_JSON2" | cut -d, -f2)"
J2_SG="$(echo "$PEER_JSON2" | cut -d, -f3)"
J2_RP="$(echo "$PEER_JSON2" | cut -d, -f4)"
J2_RA="$(echo "$PEER_JSON2" | cut -d, -f5)"
J2_RG="$(echo "$PEER_JSON2" | cut -d, -f6)"
N_SP="$(traffic_field "$TRAFFIC_NEVER" sent ping)"
N_RP="$(traffic_field "$TRAFFIC_NEVER" recv ping)"
N_RA="$(traffic_field "$TRAFFIC_NEVER" recv ack)"
N_RG="$(traffic_field "$TRAFFIC_NEVER" recv gossip)"
N_RJ="$(traffic_field "$TRAFFIC_NEVER" recv join)"
N_RS="$(traffic_field "$TRAFFIC_NEVER" recv sync)"
NJ_RP="$(echo "$NEVER_JSON" | cut -d, -f4)"
NJ_RA="$(echo "$NEVER_JSON" | cut -d, -f5)"
NJ_RG="$(echo "$NEVER_JSON" | cut -d, -f6)"
if [ -n "$TRAFFIC_PEER" ] \
   && [ "$(tf0 "$TRAFFIC_SELF" sent ping)" = "0" ] \
   && [ "$(tf0 "$TRAFFIC_SELF" recv ping)" = "0" ] \
   && [ "$(tf0 "$TRAFFIC_SELF" sent ack)" = "0" ] \
   && [ "$(tf0 "$TRAFFIC_SELF" recv ack)" = "0" ] \
   && [ "$(tf0 "$TRAFFIC_SELF" sent gossip)" = "0" ] \
   && [ "$(tf0 "$TRAFFIC_SELF" recv gossip)" = "0" ] \
   && [ "$SELF_JSON" = "0,0,0,0,0,0" ] \
   && [ -n "$TRAFFIC_NEVER" ] && [ "$N_RP" = "0" ] && [ "$N_RA" = "0" ] \
   && [ "$N_RG" = "0" ] && [ "$N_RJ" = "0" ] && [ "$N_RS" = "0" ] \
   && [ "$NJ_RP" = "0" ] && [ "$NJ_RA" = "0" ] && [ "$NJ_RG" = "0" ] \
   && [ "$PROM_SELF_PING" = "absent" ] \
   && [ "$PROM_NEVER_RECV" = "absent" ] \
   && python3 -c "exit(0 if int('$M_SP') >= 1 and int('$M_SA') >= 1 and int('$M_SG') >= 1 and int('$M_RP') >= 1 and int('$M_RA') >= 1 and int('$M_RG') >= 1 and int('$J1_SP') <= int('$M_SP') <= int('$J2_SP') and int('$J1_SA') <= int('$M_SA') <= int('$J2_SA') and int('$J1_SG') <= int('$M_SG') <= int('$J2_SG') and int('$J1_RP') <= int('$M_RP') <= int('$J2_RP') and int('$J1_RA') <= int('$M_RA') <= int('$J2_RA') and int('$J1_RG') <= int('$M_RG') <= int('$J2_RG') and int('$PROM_SENT_PING') >= int('$M_SP') and int('$PROM_RECV_PING') >= int('$M_RP') and int('$PROM_SENT_GOSSIP') >= int('$M_SG') and int('$N_SP') >= 1 and int('$PROM_NEVER_SENT') >= int('$N_SP') else 1)" 2>/dev/null; then
    note ok "live peer TRAFFIC line ping/ack/gossip sent and received >= 1 ($TRAFFIC_PEER), JSON agrees ($PEER_JSON1 / $PEER_JSON2), PROMETHEUS counters agree ($PROM_SENT_PING / $PROM_RECV_PING / $PROM_SENT_GOSSIP); self has no ping/ack/gossip traffic (JSON $SELF_JSON, own line '$TRAFFIC_SELF'); never-started peer probed but never heard from ($TRAFFIC_NEVER, JSON $NEVER_JSON)"
else
    note fail "live peer TRAFFIC line ping/ack/gossip sent and received >= 1 (metrics peer '$TRAFFIC_PEER', self '$TRAFFIC_SELF', never-started '$TRAFFIC_NEVER', json peer '$PEER_JSON1' then '$PEER_JSON2', self '$SELF_JSON', never-started '$NEVER_JSON' then '$NEVER_JSON2', prom sent/recv ping '$PROM_SENT_PING' / '$PROM_RECV_PING', prom sent gossip '$PROM_SENT_GOSSIP', prom self '$PROM_SELF_PING', prom never-started '$PROM_NEVER_SENT' / '$PROM_NEVER_RECV')"
    ctl 18981 METRICS
fi

# 3. The TRAFFIC lines ride after the PROBES lines, before END —
# the established per-member order is PHI, SUSPECT, LASTSEEN,
# RTT, PROBES, TRAFFIC.
LAST_PROBES="$(ctl 18981 METRICS | grep -n '^PROBES ' | tail -1 | cut -d: -f1)"
FIRST_TRAFFIC="$(ctl 18981 METRICS | grep -n '^TRAFFIC ' | head -1 | cut -d: -f1)"
END_LINE="$(ctl 18981 METRICS | grep -n '^END' | cut -d: -f1)"
if [ -n "$LAST_PROBES" ] && [ -n "$FIRST_TRAFFIC" ] && [ -n "$END_LINE" ] \
   && [ "$LAST_PROBES" -lt "$FIRST_TRAFFIC" ] \
   && [ "$FIRST_TRAFFIC" -lt "$END_LINE" ]; then
    note ok "TRAFFIC lines ride after the PROBES lines, before END"
else
    note fail "TRAFFIC lines ride after the PROBES lines, before END (last PROBES line $LAST_PROBES, first TRAFFIC line $FIRST_TRAFFIC, END line $END_LINE)"
    ctl 18981 METRICS
fi

echo "test_traffic_counters: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
