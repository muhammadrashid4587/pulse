#!/usr/bin/env bash
# Integration test for probe RTT in phi-accrual mode (0.49.0): before
# this version the prober sent the PING in --phi-accrual mode but
# created no pending entry, so ACKs matched nothing and both the
# per-member rtt_ms and the probe-RTT histogram stayed empty there.
# Since 0.49.0 the probe creates a pending entry marked rtt_only —
# it exists only to match the ACK and measure the RTT, never to
# escalate or suspect — so the same RTT sample path feeds METRICS,
# JSON and PROMETHEUS in phi mode too, while suspicion still comes
# from phi alone. The values are read off the live control-plane
# replies and parsed, not grepped from source.
#  1. A solo phi node has answered no probe: METRICS reports
#     probe_rtt_ms samples=0, JSON reports mean null and an
#     all-zero hist.
#  2. Two phi nodes converge from one shared --peer-file and probe
#     each other: the peer's JSON rtt_ms / rtt_mean_ms are non-null,
#     METRICS samples grow past 0 and the hist buckets sum to the
#     samples, and PROMETHEUS carries the member RTT gauges and a
#     non-empty probe-RTT histogram.
#  3. Kill one node; the survivor still SUSPECTs it with a phi value
#     logged — the rtt_only entries never escalate, and suspicion
#     still comes from phi, not from a probe timeout.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-phi-rtt-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17951-17952 (gossip) and 18951-18952 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17951
127.0.0.1:17952
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17951 --peer-file "$TMP/peers" --control-port 18951 --phi-accrual --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")

if ! wait_for "$TMP/n1.log" "START .* phi=on threshold=8.0" 10; then
    note fail "node 1 starts in phi mode"
    tail -5 "$TMP/n1.log"
fi

# 1. A solo phi node has answered no probe yet.
METRICS1="$(ctl 18951 METRICS | sed -n '1p')"
if echo "$METRICS1" | grep -q 'probe_rtt_ms mean=- max=- samples=0 hist=\[0,0,0,0,0\]'; then
    note ok "solo phi node METRICS reports probe_rtt_ms mean=- max=- samples=0 hist=[0,0,0,0,0]"
else
    note fail "solo phi node METRICS reports probe_rtt_ms mean=- max=- samples=0 hist=[0,0,0,0,0] (got '$METRICS1')"
fi

RTT1="$(ctl 18951 JSON | sed -n '1p' | python3 -c "
import json, sys
print(json.dumps(json.load(sys.stdin)['metrics']['probe_rtt_ms']))
")"
CHECK1="$(echo "$RTT1" | python3 -c "
import json, sys
r = json.load(sys.stdin)
ok = r['mean'] is None and r['max'] == 0 and r['samples'] == 0
ok = ok and r['hist'] == [0, 0, 0, 0, 0]
print('ok' if ok else 'bad')
")"
if [ "$CHECK1" = "ok" ]; then
    note ok "solo phi node JSON reports probe_rtt_ms mean null, max 0, samples 0, all-zero hist"
else
    note fail "solo phi node JSON reports probe_rtt_ms mean null, max 0, samples 0, all-zero hist (got '$RTT1')"
fi

# 2. A second phi node joins; the two probe each other.
(cd "$TMP/n2" && exec "$BIN" --port 17952 --peer-file "$TMP/peers" --control-port 18952 --phi-accrual --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 phi nodes converge from the peer file"
else
    note fail "2 phi nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Give the prober a few cycles so the histogram has samples: wait
# until METRICS reports at least 10, so the checks below cannot
# flake on a single slow sample.
SAMPLES=0
for i in $(seq 1 25); do
    SAMPLES="$(ctl 18951 METRICS | sed -n '1p' | python3 -c "import re,sys; m = re.search(r'probe_rtt_ms mean=\S+ max=\S+ samples=(\d+)', sys.stdin.read()); print(m.group(1) if m else 0)")"
    [ "$SAMPLES" -ge 10 ] && break
    sleep 0.2
done
if [ "$SAMPLES" -lt 10 ]; then
    note fail "phi METRICS reaches 10 probe-RTT samples (got $SAMPLES)"
else
    note ok "phi METRICS reaches 10 probe-RTT samples (got $SAMPLES)"
fi

# The peer's per-member RTT is populated in phi mode — the exact
# gap this version closes — and its phi is still computed, so the
# failure-detection path is intact.
JSON2="$(ctl 18951 JSON | sed -n '1p')"
CHECK2="$(echo "$JSON2" | python3 -c "
import json, sys
d = json.load(sys.stdin)
peer = [m for m in d['members'] if m['id'] == '127.0.0.1:17952']
if len(peer) != 1:
    print('bad'); sys.exit()
peer = peer[0]
ok = isinstance(peer['rtt_ms'], int) and peer['rtt_ms'] >= 0
ok = ok and isinstance(peer['rtt_mean_ms'], int) and peer['rtt_mean_ms'] >= 0
ok = ok and isinstance(peer['phi'], (int, float)) and peer['phi'] >= 0
r = d['metrics']['probe_rtt_ms']
ok = ok and r['samples'] > 0 and sum(r['hist']) == r['samples']
ok = ok and isinstance(r['mean'], int) and r['mean'] >= 0 and r['max'] >= r['mean']
print('ok' if ok else 'bad')
")"
if [ "$CHECK2" = "ok" ]; then
    note ok "in phi mode the peer's JSON rtt_ms / rtt_mean_ms are populated, phi is present, and probe_rtt_ms hist sums to samples"
else
    note fail "in phi mode the peer's JSON rtt_ms / rtt_mean_ms are populated, phi is present, and probe_rtt_ms hist sums to samples"
    echo "$JSON2" | python3 -m json.tool
fi

# PROMETHEUS: the probe-RTT histogram is non-empty and the member
# RTT gauges ride for the peer, so a scraper sees phi-mode RTT too.
PROM="$(ctl 18951 PROMETHEUS)"
CHECK3="$(echo "$PROM" | python3 -c "
import re, sys
vals = {}
member_rtt = False
for line in sys.stdin:
    m = re.match(r'pulse_probe_rtt_ms_(count|samples) (\d+)', line)
    if m: vals[m.group(1)] = int(m.group(2))
    if line.startswith('pulse_member_rtt_ms{member=\"127.0.0.1:17952\"}'):
        member_rtt = True
ok = vals.get('count', 0) > 0 and vals.get('samples', 0) == vals.get('count')
ok = ok and member_rtt
print('ok' if ok else 'bad')
")"
if [ "$CHECK3" = "ok" ]; then
    note ok "in phi mode PROMETHEUS probe_rtt_ms_count > 0 and the peer's pulse_member_rtt_ms gauge is present"
else
    note fail "in phi mode PROMETHEUS probe_rtt_ms_count > 0 and the peer's pulse_member_rtt_ms gauge is present"
    echo "$PROM" | grep -E "probe_rtt|member_rtt"
fi

# 3. Kill node 2 with SIGKILL — a real crash, not a clean shutdown:
# SIGTERM would trigger a graceful leave, which marks the peer DEAD
# directly with no SUSPECT. The survivor must still suspect it on
# phi — the rtt_only entries never escalate, and suspicion still
# comes from phi, not from a probe timeout.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17952 phi=" 15; then
    note ok "survivor SUSPECTs the killed peer with a phi value logged"
else
    note fail "survivor SUSPECTs the killed peer with a phi value logged"
    tail -5 "$TMP/n1.log"
fi

echo "test_phi_rtt: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
