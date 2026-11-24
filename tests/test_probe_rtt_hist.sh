#!/usr/bin/env bash
# Integration test for the probe-RTT histogram (0.48.0): every
# answered direct probe adds one sample to a global RTT histogram —
# mean/max/samples plus five buckets (<1, 1-4, 5-9, 10-49, >=50 ms)
# — exposed as "probe_rtt_ms" in the METRICS line, as
# metrics.probe_rtt_ms in JSON, and as a real histogram
# (pulse_probe_rtt_ms_bucket with _sum/_count) in PROMETHEUS, so
# the mean RTT cannot hide a slow tail. The values are read off
# the live control-plane replies and parsed, not grepped from
# source.
#  1. A solo node has answered no probe: METRICS reports
#     probe_rtt_ms mean=- max=- samples=0 hist=[0,0,0,0,0], JSON
#     reports mean null and an all-zero hist, PROMETHEUS reports
#     pulse_probe_rtt_ms_count 0.
#  2. Two nodes converge from one shared --peer-file and probe each
#     other: METRICS samples grow past 0 and the hist buckets sum
#     to the samples; JSON parses, its hist sums to its samples,
#     and its mean is the integer mean of the samples.
#  3. PROMETHEUS buckets are cumulative (each le >= the one
#     before), the +Inf bucket equals _count equals the METRICS
#     samples, and on loopback the <=5 ms bucket holds samples —
#     the healthy case is a few milliseconds at most.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-probe-rtt-hist-test.XXXXXX)"
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

# json_rtt <control-port>: the metrics.probe_rtt_ms object as JSON.
json_rtt() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
print(json.dumps(json.load(sys.stdin)['metrics']['probe_rtt_ms']))
"
}

# prom_value <control-port> <metric>: the value of one Prometheus
# line (no labels), or empty when the metric is absent.
prom_value() {
    ctl "$1" PROMETHEUS | grep -E "^$2 " | awk '{print $2}'
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17941-17942 (gossip) and 18941-18942 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17941
127.0.0.1:17942
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17941 --peer-file "$TMP/peers" --control-port 18941 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")

# Wait for node 1's control plane to answer before querying it:
# the STATUS line proves the node is up and its prober is running.
if ! wait_for "$TMP/n1.log" "STATUS alive=1" 10; then
    note fail "node 1 starts and reports STATUS alive=1"
    tail -5 "$TMP/n1.log"
fi

# 1. A solo node has answered no probe yet.
METRICS1="$(ctl 18941 METRICS | sed -n '1p')"
if echo "$METRICS1" | grep -q 'probe_rtt_ms mean=- max=- samples=0 hist=\[0,0,0,0,0\]'; then
    note ok "solo node METRICS reports probe_rtt_ms mean=- max=- samples=0 hist=[0,0,0,0,0]"
else
    note fail "solo node METRICS reports probe_rtt_ms mean=- max=- samples=0 hist=[0,0,0,0,0] (got '$METRICS1')"
fi

RTT1="$(json_rtt 18941)"
CHECK1="$(echo "$RTT1" | python3 -c "
import json, sys
r = json.load(sys.stdin)
ok = r['mean'] is None and r['max'] == 0 and r['samples'] == 0
ok = ok and r['hist'] == [0, 0, 0, 0, 0]
print('ok' if ok else 'bad')
")"
if [ "$CHECK1" = "ok" ]; then
    note ok "solo node JSON reports probe_rtt_ms mean null, max 0, samples 0, all-zero hist"
else
    note fail "solo node JSON reports probe_rtt_ms mean null, max 0, samples 0, all-zero hist (got '$RTT1')"
fi

if [ "$(prom_value 18941 pulse_probe_rtt_ms_count)" = "0" ] \
   && [ "$(prom_value 18941 pulse_probe_rtt_ms_samples)" = "0" ]; then
    note ok "solo node PROMETHEUS reports pulse_probe_rtt_ms_count 0 and samples 0"
else
    note fail "solo node PROMETHEUS reports pulse_probe_rtt_ms_count 0 and samples 0"
    ctl 18941 PROMETHEUS | grep probe_rtt
fi

# 2. A second node joins; the two probe each other.
(cd "$TMP/n2" && exec "$BIN" --port 17942 --peer-file "$TMP/peers" --control-port 18942 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Give the prober a few cycles so the histogram has samples: wait
# until METRICS reports at least 10, so the fast-bucket check
# below cannot flake on a single slow sample.
SAMPLES=0
for i in $(seq 1 25); do
    SAMPLES="$(ctl 18941 METRICS | sed -n '1p' | python3 -c "import re,sys; m = re.search(r'probe_rtt_ms mean=\S+ max=\S+ samples=(\d+)', sys.stdin.read()); print(m.group(1) if m else 0)")"
    [ "$SAMPLES" -ge 10 ] && break
    sleep 0.2
done
if [ "$SAMPLES" -lt 10 ]; then
    note fail "METRICS reaches 10 probe-RTT samples (got $SAMPLES)"
else
    note ok "METRICS reaches 10 probe-RTT samples (got $SAMPLES)"
fi

METRICS2="$(ctl 18941 METRICS | sed -n '1p')"
CHECK2="$(echo "$METRICS2" | python3 -c "
import re, sys
m = re.search(r'probe_rtt_ms mean=(\S+) max=(\S+) samples=(\d+) hist=\[([0-9,]+)\]', sys.stdin.read())
if not m:
    print('bad'); sys.exit()
mean, mx, samples, hist = m.group(1), int(m.group(2)), int(m.group(3)), [int(x) for x in m.group(4).split(',')]
ok = samples > 0 and sum(hist) == samples and mx >= 0
ok = ok and (mean == '-' if samples == 0 else int(mean) >= 0)
print('ok' if ok else 'bad')
")"
if [ "$CHECK2" = "ok" ]; then
    note ok "after probing, METRICS samples > 0 and hist buckets sum to samples ($METRICS2)"
else
    note fail "after probing, METRICS samples > 0 and hist buckets sum to samples (got '$METRICS2')"
fi

RTT2="$(json_rtt 18941)"
CHECK3="$(echo "$RTT2" | python3 -c "
import json, sys
r = json.load(sys.stdin)
ok = r['samples'] > 0 and sum(r['hist']) == r['samples']
ok = ok and isinstance(r['mean'], int) and r['mean'] >= 0
ok = ok and r['max'] >= r['mean']
print('ok' if ok else 'bad')
")"
if [ "$CHECK3" = "ok" ]; then
    note ok "after probing, JSON probe_rtt_ms hist sums to samples with a non-negative integer mean ($RTT2)"
else
    note fail "after probing, JSON probe_rtt_ms hist sums to samples with a non-negative integer mean (got '$RTT2')"
fi

# 3. PROMETHEUS: cumulative buckets, +Inf == _count == samples,
# and on loopback the fast buckets hold samples — the healthy
# case is a few milliseconds at most, so the <=5 ms cumulative
# bucket is non-empty (a single slow sample cannot empty it,
# since the bucket is cumulative over every fast sample so far).
PROM="$(ctl 18941 PROMETHEUS)"
CHECK4="$(echo "$PROM" | python3 -c "
import re, sys
vals = {}
for line in sys.stdin:
    m = re.match(r'pulse_probe_rtt_ms_bucket\{le=\"([^\"]+)\"\} (\d+)', line)
    if m: vals[m.group(1)] = int(m.group(2))
    m = re.match(r'pulse_probe_rtt_ms_(sum|count|mean|max|samples) (\d+)', line)
    if m: vals[m.group(1)] = int(m.group(2))
order = ['1', '5', '10', '50', '+Inf']
ok = all(k in vals for k in order + ['count', 'samples'])
ok = ok and all(vals[order[i]] <= vals[order[i+1]] for i in range(len(order)-1))
ok = ok and vals['+Inf'] == vals['count'] == vals['samples'] > 0
ok = ok and vals['5'] > 0
print('ok' if ok else 'bad')
")"
if [ "$CHECK4" = "ok" ]; then
    note ok "PROMETHEUS buckets are cumulative, +Inf == count == samples, and the <=5 ms bucket holds samples"
else
    note fail "PROMETHEUS buckets are cumulative, +Inf == count == samples, and the <=5 ms bucket holds samples"
    echo "$PROM" | grep probe_rtt
fi

# The JSON samples and the METRICS samples agree: same counter,
# read twice in a row on a live node, so allow the later read to
# be equal or larger (probes keep landing between the two reads).
S_M="$(echo "$METRICS2" | python3 -c "import re,sys; print(re.search(r'probe_rtt_ms mean=\S+ max=\S+ samples=(\d+)', sys.stdin.read()).group(1))")"
S_J="$(echo "$RTT2" | python3 -c "import json,sys; print(json.load(sys.stdin)['samples'])")"
if [ "$S_J" -ge "$S_M" ]; then
    note ok "JSON samples ($S_J) >= METRICS samples ($S_M): same counter, read later"
else
    note fail "JSON samples ($S_J) >= METRICS samples ($S_M): same counter, read later"
fi

echo "test_probe_rtt_hist: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
