#!/usr/bin/env bash
# Integration test for per-member detection latency (0.81.0): how
# long detection took for each peer — the last, mean and worst
# last-direct-contact -> SUSPECT latency and SUSPECT -> DEAD
# duration, with how many samples each mean is over — exposed as
# "detect_ms" / "detect_mean_ms" / "detect_max_ms" /
# "detect_samples" and "suspect_dead_ms" / "suspect_dead_mean_ms" /
# "suspect_dead_max_ms" / "suspect_dead_samples" per member in
# JSON, one "DETECT <id> <last> <mean> <max> <samples>" line per
# member with at least one locally measured suspicion and one
# "SUSPECTDEAD <id> <last> <mean> <max> <samples>" line per member
# with at least one locally swept death in METRICS, and
# pulse_member_detect_ms / _mean_ms / _max_ms / _samples and
# pulse_member_suspect_dead_ms / _mean_ms / _max_ms / _samples
# gauges in PROMETHEUS. The values are read off the live
# control-plane replies and parsed, not grepped from source.
#  1. Node 1 starts first from a --peer-file listing itself and a
#     peer that never starts. Node 1 is the only node that ever
#     probes the never-started peer, so it detects it locally: the
#     probe timeout suspects it and the sweep declares it DEAD,
#     and the SUSPECTDEAD line appears — while no DETECT line ever
#     does, since the peer was never heard from and has no
#     heartbeat gap to measure. Node 2 then joins through node 1
#     as its seed and the two converge on alive=2, with the
#     default 300 ms suspect timeout and a 120 s dead retention
#     so the peer stays DEAD long enough to observe.
#  2. Node 2 is killed with SIGKILL (a crash, so no graceful-leave
#     announcement and the probe timeout suspects it). While it is
#     SUSPECT the DETECT line carries a last/mean/max between 50 and
#     5000 ms with 1 sample — the fixed path suspects about two ack
#     timeouts after the last contact — and JSON and PROMETHEUS
#     agree with it exactly; self has no DETECT line and nulls with
#     0 samples in JSON; the never-started peer still has no DETECT
#     line, but its SUSPECTDEAD line from step 1 is still there
#     with its JSON and PROMETHEUS agreeing exactly.
#  3. Once node 2 is DEAD the SUSPECTDEAD line carries a last/mean/
#     max between 250 and 5000 ms with 1 sample — the peer sat out
#     the 300 ms suspect timeout — and JSON and PROMETHEUS agree
#     with it exactly, while the DETECT values stay frozen at the
#     suspicion's values.
#  4. The DETECT and SUSPECTDEAD lines ride after the TRAFFIC lines,
#     before END.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-detect-latency-test.XXXXXX)"
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

# wait_metrics <control-port> <pattern> <timeout-seconds>: poll the
# METRICS reply until a line matches, or the timeout elapses.
wait_metrics() {
    local port="$1" pat="$2" timeout="$3" i
    for i in $(seq 1 $((timeout * 5))); do
        if ctl "$port" METRICS 2>/dev/null | grep -Eq "$pat"; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

# detect_field <line> <position>: print the Nth numeric field of a
# DETECT / SUSPECTDEAD line (1 = last, 2 = mean, 3 = max, 4 = samples).
detect_field() {
    echo "$1" | awk -v f="$2" '{print $(f + 2)}'
}

# json_detect <control-port> <member-id>: print the member's detect
# fields as "detect_ms,detect_mean_ms,detect_max_ms,detect_samples,
# suspect_dead_ms,suspect_dead_mean_ms,suspect_dead_max_ms,
# suspect_dead_samples" from the live JSON reply with python3 —
# nulls print as None.
json_detect() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print('%s,%s,%s,%s,%s,%s,%s,%s' % (
            m.get('detect_ms'), m.get('detect_mean_ms'),
            m.get('detect_max_ms'), m.get('detect_samples'),
            m.get('suspect_dead_ms'), m.get('suspect_dead_mean_ms'),
            m.get('suspect_dead_max_ms'),
            m.get('suspect_dead_samples')))
        break
"
}

# prom_detect <control-port> <family> <member-id>: print the
# PROMETHEUS gauge value, or "absent" when the series does not
# exist.
prom_detect() {
    local v
    v="$(ctl "$1" PROMETHEUS | grep "^$2{member=\"$3\"} " | awk '{print $2}' || true)"
    [ -n "$v" ] && echo "$v" || echo "absent"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17991-17992 (gossip) and 18991-18992 (control) so this test
# never clashes with the other suites. 17999 is listed in node 1's
# peer file but never started: the probed-but-never-heard-from peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17991
127.0.0.1:17999
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17991 --peer-file "$TMP/peers" --control-port 18991 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")

# 1. Node 1 detects the never-started peer locally: the probe
# timeout suspects it (no DETECT line — it was never heard from,
# so there is no heartbeat gap to measure) and the sweep declares
# it DEAD, which records the SUSPECTDEAD sample. Node 1 starts
# alone, so no other node can beat it to the detection and the
# values are local, not gossip-learned.
if wait_metrics 18991 '^SUSPECTDEAD 127.0.0.1:17999 ' 10; then
    note ok "node 1 locally declares the never-started peer DEAD"
else
    note fail "node 1 locally declares the never-started peer DEAD"
    ctl 18991 METRICS
fi
if [ -z "$(ctl 18991 METRICS | grep '^DETECT 127.0.0.1:17999 ' || true)" ] \
   && [ -n "$(ctl 18991 METRICS | grep '^SUSPECTDEAD 127.0.0.1:17999 ' || true)" ]; then
    note ok "never-started peer has a SUSPECTDEAD line and no DETECT line"
else
    note fail "never-started peer has a SUSPECTDEAD line and no DETECT line"
    ctl 18991 METRICS
fi

# Node 2 joins through node 1 as its seed, then the two converge.
(cd "$TMP/n2" && exec "$BIN" --port 17992 --seed 127.0.0.1:17991 --control-port 18992 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge (node 2 joined through node 1)"
else
    note fail "2 nodes converge (node 2 joined through node 1)"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# 2. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# the probe timeout suspects it instead of a graceful LEAVE
# announcing it DEAD at once.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null

if ! wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17992" 10; then
    note fail "node 1 suspects the killed node 2"
    tail -5 "$TMP/n1.log"
else
    note ok "node 1 suspects the killed node 2"
fi

# While node 2 is SUSPECT (or just past it — the values freeze at
# the suspicion, so the reads below hold either way), the DETECT
# line carries the locally measured latency and JSON and
# PROMETHEUS agree with it exactly. Self has no DETECT line and
# nulls with 0 samples in JSON; the never-started peer still has
# no DETECT line, but its SUSPECTDEAD line from step 1 is still
# there with JSON and PROMETHEUS agreeing exactly.
DETECT_PEER="$(ctl 18991 METRICS | grep '^DETECT 127.0.0.1:17992 ' || true)"
DETECT_SELF="$(ctl 18991 METRICS | grep '^DETECT 127.0.0.1:17991 ' || true)"
DETECT_NEVER="$(ctl 18991 METRICS | grep '^DETECT 127.0.0.1:17999 ' || true)"
SDEAD_NEVER="$(ctl 18991 METRICS | grep '^SUSPECTDEAD 127.0.0.1:17999 ' || true)"
PEER_JSON="$(json_detect 18991 127.0.0.1:17992)"
SELF_JSON="$(json_detect 18991 127.0.0.1:17991)"
NEVER_JSON="$(json_detect 18991 127.0.0.1:17999)"
PROM_LAST="$(prom_detect 18991 pulse_member_detect_ms 127.0.0.1:17992)"
PROM_MEAN="$(prom_detect 18991 pulse_member_detect_mean_ms 127.0.0.1:17992)"
PROM_MAX="$(prom_detect 18991 pulse_member_detect_max_ms 127.0.0.1:17992)"
PROM_SAMPLES="$(prom_detect 18991 pulse_member_detect_samples 127.0.0.1:17992)"
PROM_SELF="$(prom_detect 18991 pulse_member_detect_ms 127.0.0.1:17991)"
PROM_NEVER="$(prom_detect 18991 pulse_member_detect_ms 127.0.0.1:17999)"
PROM_N_SD_LAST="$(prom_detect 18991 pulse_member_suspect_dead_ms 127.0.0.1:17999)"
M_LAST="$(detect_field "$DETECT_PEER" 1)"
M_MEAN="$(detect_field "$DETECT_PEER" 2)"
M_MAX="$(detect_field "$DETECT_PEER" 3)"
M_SAMPLES="$(detect_field "$DETECT_PEER" 4)"
J_LAST="$(echo "$PEER_JSON" | cut -d, -f1)"
J_MEAN="$(echo "$PEER_JSON" | cut -d, -f2)"
J_MAX="$(echo "$PEER_JSON" | cut -d, -f3)"
J_SAMPLES="$(echo "$PEER_JSON" | cut -d, -f4)"
N_LAST="$(echo "$NEVER_JSON" | cut -d, -f1)"
N_MEAN="$(echo "$NEVER_JSON" | cut -d, -f2)"
N_MAX="$(echo "$NEVER_JSON" | cut -d, -f3)"
N_SAMPLES="$(echo "$NEVER_JSON" | cut -d, -f4)"
N_SD_LAST="$(echo "$NEVER_JSON" | cut -d, -f5)"
N_SD_MEAN="$(echo "$NEVER_JSON" | cut -d, -f6)"
N_SD_MAX="$(echo "$NEVER_JSON" | cut -d, -f7)"
N_SD_SAMPLES="$(echo "$NEVER_JSON" | cut -d, -f8)"
NSD_LAST="$(detect_field "$SDEAD_NEVER" 1)"
NSD_MEAN="$(detect_field "$SDEAD_NEVER" 2)"
NSD_MAX="$(detect_field "$SDEAD_NEVER" 3)"
NSD_SAMPLES="$(detect_field "$SDEAD_NEVER" 4)"
if [ -n "$DETECT_PEER" ] && [ -z "$DETECT_SELF" ] && [ -z "$DETECT_NEVER" ] \
   && [ -n "$SDEAD_NEVER" ] \
   && [ "$SELF_JSON" = "None,None,None,0,None,None,None,0" ] \
   && [ "$N_LAST" = "None" ] && [ "$N_MEAN" = "None" ] \
   && [ "$N_MAX" = "None" ] && [ "$N_SAMPLES" = "0" ] \
   && [ "$PROM_SELF" = "absent" ] && [ "$PROM_NEVER" = "absent" ] \
   && python3 -c "exit(0 if 50 <= int('$M_LAST') <= 5000 and 50 <= int('$M_MEAN') <= 5000 and 50 <= int('$M_MAX') <= 5000 and int('$M_SAMPLES') == 1 and int('$M_LAST') == int('$J_LAST') and int('$M_MEAN') == int('$J_MEAN') and int('$M_MAX') == int('$J_MAX') and int('$M_SAMPLES') == int('$J_SAMPLES') and int('$PROM_LAST') == int('$M_LAST') and int('$PROM_MEAN') == int('$M_MEAN') and int('$PROM_MAX') == int('$M_MAX') and int('$PROM_SAMPLES') == int('$M_SAMPLES') and 250 <= int('$NSD_LAST') <= 5000 and 250 <= int('$NSD_MEAN') <= 5000 and 250 <= int('$NSD_MAX') <= 5000 and int('$NSD_SAMPLES') >= 1 and int('$NSD_LAST') == int('$N_SD_LAST') and int('$NSD_MEAN') == int('$N_SD_MEAN') and int('$NSD_MAX') == int('$N_SD_MAX') and int('$NSD_SAMPLES') == int('$N_SD_SAMPLES') and int('$PROM_N_SD_LAST') == int('$NSD_LAST') else 1)" 2>/dev/null; then
    note ok "DETECT line for the killed peer ($DETECT_PEER), JSON agrees ($PEER_JSON), PROMETHEUS agrees ($PROM_LAST / $PROM_MEAN / $PROM_MAX / $PROM_SAMPLES); self has none (JSON $SELF_JSON); never-started peer has no DETECT line but a SUSPECTDEAD line ($SDEAD_NEVER, JSON $NEVER_JSON)"
else
    note fail "DETECT line for the killed peer (metrics '$DETECT_PEER', self '$DETECT_SELF', never-started '$DETECT_NEVER', never-started SUSPECTDEAD '$SDEAD_NEVER', json peer '$PEER_JSON', self '$SELF_JSON', never-started '$NEVER_JSON', prom last/mean/max/samples '$PROM_LAST' / '$PROM_MEAN' / '$PROM_MAX' / '$PROM_SAMPLES', prom self '$PROM_SELF', prom never-started '$PROM_NEVER', prom never-started suspect-dead '$PROM_N_SD_LAST')"
    ctl 18991 METRICS
fi

# 3. Once node 2 is DEAD the SUSPECTDEAD line carries the duration
# it sat SUSPECT, and JSON and PROMETHEUS agree with it exactly,
# while the DETECT values stay frozen at the suspicion's values.
if ! wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17992" 10; then
    note fail "node 1 declares the killed node 2 DEAD"
    tail -5 "$TMP/n1.log"
else
    note ok "node 1 declares the killed node 2 DEAD"
fi

SDEAD_PEER="$(ctl 18991 METRICS | grep '^SUSPECTDEAD 127.0.0.1:17992 ' || true)"
DETECT_PEER2="$(ctl 18991 METRICS | grep '^DETECT 127.0.0.1:17992 ' || true)"
PEER_JSON2="$(json_detect 18991 127.0.0.1:17992)"
PROM_SD_LAST="$(prom_detect 18991 pulse_member_suspect_dead_ms 127.0.0.1:17992)"
PROM_SD_MEAN="$(prom_detect 18991 pulse_member_suspect_dead_mean_ms 127.0.0.1:17992)"
PROM_SD_MAX="$(prom_detect 18991 pulse_member_suspect_dead_max_ms 127.0.0.1:17992)"
PROM_SD_SAMPLES="$(prom_detect 18991 pulse_member_suspect_dead_samples 127.0.0.1:17992)"
SD_LAST="$(detect_field "$SDEAD_PEER" 1)"
SD_MEAN="$(detect_field "$SDEAD_PEER" 2)"
SD_MAX="$(detect_field "$SDEAD_PEER" 3)"
SD_SAMPLES="$(detect_field "$SDEAD_PEER" 4)"
J2_SD_LAST="$(echo "$PEER_JSON2" | cut -d, -f5)"
J2_SD_MEAN="$(echo "$PEER_JSON2" | cut -d, -f6)"
J2_SD_MAX="$(echo "$PEER_JSON2" | cut -d, -f7)"
J2_SD_SAMPLES="$(echo "$PEER_JSON2" | cut -d, -f8)"
if [ -n "$SDEAD_PEER" ] && [ "$DETECT_PEER2" = "$DETECT_PEER" ] \
   && python3 -c "exit(0 if 250 <= int('$SD_LAST') <= 5000 and 250 <= int('$SD_MEAN') <= 5000 and 250 <= int('$SD_MAX') <= 5000 and int('$SD_SAMPLES') == 1 and int('$SD_LAST') == int('$J2_SD_LAST') and int('$SD_MEAN') == int('$J2_SD_MEAN') and int('$SD_MAX') == int('$J2_SD_MAX') and int('$SD_SAMPLES') == int('$J2_SD_SAMPLES') and int('$PROM_SD_LAST') == int('$SD_LAST') and int('$PROM_SD_MEAN') == int('$SD_MEAN') and int('$PROM_SD_MAX') == int('$SD_MAX') and int('$PROM_SD_SAMPLES') == int('$SD_SAMPLES') else 1)" 2>/dev/null; then
    note ok "SUSPECTDEAD line for the killed peer ($SDEAD_PEER), JSON agrees ($PEER_JSON2), PROMETHEUS agrees ($PROM_SD_LAST / $PROM_SD_MEAN / $PROM_SD_MAX / $PROM_SD_SAMPLES); DETECT values frozen ($DETECT_PEER2)"
else
    note fail "SUSPECTDEAD line for the killed peer (metrics '$SDEAD_PEER', DETECT now '$DETECT_PEER2' was '$DETECT_PEER', json '$PEER_JSON2', prom last/mean/max/samples '$PROM_SD_LAST' / '$PROM_SD_MEAN' / '$PROM_SD_MAX' / '$PROM_SD_SAMPLES')"
    ctl 18991 METRICS
fi

# 4. The DETECT and SUSPECTDEAD lines ride after the TRAFFIC lines,
# before END — the established per-member order is PHI, SUSPECT,
# LASTSEEN, RTT, PROBES, TRAFFIC, DETECT, SUSPECTDEAD.
LAST_TRAFFIC="$(ctl 18991 METRICS | grep -n '^TRAFFIC ' | tail -1 | cut -d: -f1)"
FIRST_DETECT="$(ctl 18991 METRICS | grep -n '^DETECT ' | head -1 | cut -d: -f1)"
FIRST_SDEAD="$(ctl 18991 METRICS | grep -n '^SUSPECTDEAD ' | head -1 | cut -d: -f1)"
END_LINE="$(ctl 18991 METRICS | grep -n '^END' | cut -d: -f1)"
if [ -n "$LAST_TRAFFIC" ] && [ -n "$FIRST_DETECT" ] && [ -n "$FIRST_SDEAD" ] \
   && [ -n "$END_LINE" ] \
   && [ "$LAST_TRAFFIC" -lt "$FIRST_DETECT" ] \
   && [ "$FIRST_DETECT" -lt "$FIRST_SDEAD" ] \
   && [ "$FIRST_SDEAD" -lt "$END_LINE" ]; then
    note ok "DETECT and SUSPECTDEAD lines ride after the TRAFFIC lines, before END"
else
    note fail "DETECT and SUSPECTDEAD lines ride after the TRAFFIC lines, before END (last TRAFFIC line $LAST_TRAFFIC, first DETECT line $FIRST_DETECT, first SUSPECTDEAD line $FIRST_SDEAD, END line $END_LINE)"
    ctl 18991 METRICS
fi

echo "test_detect_latency: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
