#!/usr/bin/env bash
# Integration test for the per-member probe-RTT histogram (0.83.0):
# the same five buckets as the global probe-RTT histogram (<1,
# 1-4, 5-9, 10-49, >=50 ms), but per peer, as a real histogram
# (pulse_member_probe_rtt_ms_bucket with _sum/_count) in
# PROMETHEUS, so a scraper can compute quantiles of one peer's
# RTTs instead of trusting its mean and max alone. The values
# are read off the live control-plane replies and parsed, not
# grepped from source.
#  1. A solo node has answered no probe: PROMETHEUS carries no
#     pulse_member_probe_rtt_ms line at all — self never probes
#     itself, and a never-started peer has no answered probe.
#  2. Two nodes converge from one shared --peer-file and probe
#     each other: the probed peer's histogram buckets are
#     cumulative, the +Inf bucket equals _count, _count is at
#     least the probes_answered JSON reported a moment earlier
#     (probes keep landing between the reads), and on loopback
#     the <=1 ms bucket holds samples.
#  3. The histogram is absent for self and for the never-started
#     peer, which is probed but never answers.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-rtt-hist-member-test.XXXXXX)"
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

# Ports 17943-17945 (gossip) and 18943-18944 (control) so this test
# never clashes with the other suites. 17945 is listed but never
# started: the probed-but-never-answered peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17943
127.0.0.1:17944
127.0.0.1:17945
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17943 --peer-file "$TMP/peers" --control-port 18943 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")

if ! wait_for "$TMP/n1.log" "STATUS alive=1" 10; then
    note fail "node 1 starts and reports STATUS alive=1"
    tail -5 "$TMP/n1.log"
fi

# 1. A solo node has answered no probe: no per-member RTT
# histogram line at all — self never probes itself, and the
# never-started peer has no answered probe to bucket. The
# HELP/TYPE pair is always emitted, so match a sample line
# (with a member label), not the family name.
if ctl 18943 PROMETHEUS | grep -q 'pulse_member_probe_rtt_ms_bucket{'; then
    note fail "solo node PROMETHEUS carries no pulse_member_probe_rtt_ms line"
    ctl 18943 PROMETHEUS | grep pulse_member_probe_rtt_ms
else
    note ok "solo node PROMETHEUS carries no pulse_member_probe_rtt_ms line"
fi

# 2. A second node joins; the two probe each other.
(cd "$TMP/n2" && exec "$BIN" --port 17944 --peer-file "$TMP/peers" --control-port 18944 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Give the prober a few cycles so the histogram has samples: wait
# until JSON reports at least 10 answered probes for the peer, so
# the fast-bucket check below cannot flake on a single slow sample.
ANSWERED=0
for i in $(seq 1 25); do
    ANSWERED="$(ctl 18943 JSON | sed -n '1p' | python3 -c "
import json, sys
ms = json.load(sys.stdin)['members']
for m in ms:
    if m['id'] == '127.0.0.1:17944':
        print(m['probes_answered'])
        break
else:
    print(0)
")"
    [ "$ANSWERED" -ge 10 ] && break
    sleep 0.2
done
if [ "$ANSWERED" -lt 10 ]; then
    note fail "JSON reaches 10 answered probes for the peer (got $ANSWERED)"
else
    note ok "JSON reaches 10 answered probes for the peer (got $ANSWERED)"
fi

# 3. PROMETHEUS: the peer's histogram buckets are cumulative, the
# +Inf bucket equals _count, _count is at least the answered
# count JSON reported a moment earlier (probes keep landing
# between the two reads), and on loopback the <=1 ms bucket holds
# samples — the healthy case is sub-millisecond. Self and the
# never-started peer have no histogram at all.
PROM="$(ctl 18943 PROMETHEUS)"
CHECK="$(echo "$PROM" | python3 -c "
import re, sys
vals = {}
present = set()
for line in sys.stdin:
    m = re.match(r'pulse_member_probe_rtt_ms_bucket\{member=\"([^\"]+)\",le=\"([^\"]+)\"\} (\d+)', line)
    if m:
        vals[(m.group(1), m.group(2))] = int(m.group(3))
        present.add(m.group(1))
    m = re.match(r'pulse_member_probe_rtt_ms_(sum|count)\{member=\"([^\"]+)\"\} (\d+)', line)
    if m:
        vals[(m.group(2), m.group(1))] = int(m.group(3))
        present.add(m.group(2))
peer = '127.0.0.1:17944'
order = ['1', '5', '10', '50', '+Inf']
ok = peer in present
ok = ok and all((peer, k) in vals for k in order + ['count', 'sum'])
ok = ok and all(vals[(peer, order[i])] <= vals[(peer, order[i+1])] for i in range(len(order)-1))
ok = ok and vals[(peer, '+Inf')] == vals[(peer, 'count')] > 0
ok = ok and vals[(peer, 'count')] >= $ANSWERED
ok = ok and vals[(peer, '1')] > 0
ok = ok and '127.0.0.1:17943' not in present
ok = ok and '127.0.0.1:17945' not in present
print('ok' if ok else 'bad')
")"
if [ "$CHECK" = "ok" ]; then
    note ok "PROMETHEUS peer histogram is cumulative, +Inf == count >= $ANSWERED answered, <=1 ms bucket holds samples, self and the never-started peer have none"
else
    note fail "PROMETHEUS peer histogram is cumulative, +Inf == count >= $ANSWERED answered, <=1 ms bucket holds samples, self and the never-started peer have none"
    echo "$PROM" | grep pulse_member_probe_rtt_ms
fi

echo "test_rtt_hist_member: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
