#!/usr/bin/env bash
# Integration test for the low-probe-rate alert (0.68.0): when an
# ALIVE peer's recent probe success rate — answered / sent over
# the last 20 direct probes — falls below --low-probe-rate-frac,
# the node logs one "LOWRATE <id> rate=<r> threshold=<t>" line,
# records one LOWRATE event on the WATCH stream (and in the
# persisted events file), and flags the member
# "low_probe_rate":true in JSON with a "probe_rate" number, a
# low_probe_rate_peers count in METRICS and JSON, a LOWRATE line
# in METRICS, and a pulse_low_probe_rate_peers gauge plus a
# pulse_member_probe_rate gauge in PROMETHEUS — while the peer is
# still ALIVE, before the suspicion fires. The warning is once
# per crossing: a persistently low peer warns once, not once per
# probe cycle, and the flag clears the moment the peer is
# suspected.
#  1. Two nodes start from one shared --peer-file with
#     --low-probe-rate-frac 0.5, a long ack timeout (2000 ms, so
#     suspicion fires ~4 s after the last contact, long after the
#     ~1.1 s it takes the window to slide below 0.5) and a 10 s
#     suspect timeout, and converge on alive=2, with a WATCH open
#     on node 1. Before the kill the peer reads "low_probe_rate":
#     false with "probe_rate":1.00, self reads "probe_rate":null,
#     and no LOWRATE line has fired.
#  2. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 logs exactly one LOWRATE line for it,
#     the WATCH stream carries one LOWRATE event with a
#     probe_rate below 0.5, and while the peer is still ALIVE its
#     JSON says "low_probe_rate":true with a "probe_rate" below
#     0.5, METRICS says low_probe_rate_peers=1 with a LOWRATE
#     line, and PROMETHEUS says pulse_low_probe_rate_peers 1
#     with a pulse_member_probe_rate below 0.5.
#  3. Once node 1 suspects the peer, the flag clears: its JSON
#     says "low_probe_rate":false and METRICS says
#     low_probe_rate_peers=0 — a SUSPECT peer is not low-rate,
#     the suspicion already says it is silent — and no second
#     LOWRATE line ever appears.
#  4. The persisted <data-dir>/events file carries the same
#     LOWRATE line as the WATCH stream did, and a WATCH FROM 0
#     replay shows it too.
#  5. Recovery (0.69.0): restart node 2 (same data dir, so a
#     higher incarnation): node 1 revives it, its windowed rate
#     climbs back to the threshold as probes are answered again,
#     and node 1 logs exactly one "RATEOK <id> rate=<r>
#     threshold=<t>" line and streams one RATEOK event with a
#     probe_rate at or above 0.5 — once per episode, re-armed by
#     the next LOWRATE — while the JSON flag stays false, METRICS
#     stays at low_probe_rate_peers=0 with no LOWRATE line, and
#     the persisted file and a replay carry the same RATEOK line.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-low-probe-rate-test.XXXXXX)"
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

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

# json_member <control-port> <member-id> <field>: print the
# member's field as JSON, parsed from the live JSON reply.
json_member() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(json.dumps(m.get('$3')))
        break
"
}

# wait_json_healthy <timeout-seconds>: poll node 1's JSON until
# the live peer reads "low_probe_rate":false with a probe_rate of
# 1.0 — every probe in its window answered — so the pre-kill
# state is a clean baseline, not a window still sliding out
# early missed probes from before the peer was listening.
wait_json_healthy() {
    local timeout="$1" i
    for i in $(seq 1 $((timeout * 10))); do
        if [ "$(json_member 18351 127.0.0.1:17352 low_probe_rate)" = "false" ] \
           && [ "$(json_member 18351 127.0.0.1:17352 probe_rate)" = "1.0" ]; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# wait_json_lowrate <timeout-seconds>: poll node 1's JSON until
# the killed peer reads "low_probe_rate":true while still ALIVE
# with a probe_rate below the 0.5 threshold — the window between
# the rate crossing (~1.1 s after the kill) and the suspicion
# (~4 s) is over two seconds wide, and the probe cycle runs
# every 100 ms, so a 100 ms poll catches it.
wait_json_lowrate() {
    local timeout="$1" i
    for i in $(seq 1 $((timeout * 10))); do
        if [ "$(json_member 18351 127.0.0.1:17352 low_probe_rate)" = "true" ] \
           && [ "$(json_member 18351 127.0.0.1:17352 state)" = '"ALIVE"' ]; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# wait_metrics_lowrate <timeout-seconds>: poll node 1's METRICS
# until it reports low_probe_rate_peers=1.
wait_metrics_lowrate() {
    local timeout="$1" i
    for i in $(seq 1 $((timeout * 10))); do
        if ctl 18351 METRICS | grep -q "low_probe_rate_peers=1"; then return 0; fi
        sleep 0.1
    done
    return 1
}

# wait_prom_lowrate <timeout-seconds>: poll node 1's PROMETHEUS
# until it reports pulse_low_probe_rate_peers 1.
wait_prom_lowrate() {
    local timeout="$1" i
    for i in $(seq 1 $((timeout * 10))); do
        if ctl 18351 PROMETHEUS | grep -q "^pulse_low_probe_rate_peers 1$"; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# event_rate <file> <pattern>: print the probe_rate of the first
# EVENT line in the file matching the pattern.
event_rate() {
    grep -E "$2" "$1" | head -n 1 | grep -o 'probe_rate=[0-9.]*' | cut -d= -f2
}

# rate_below <rate> <threshold>: 1 when the rate is a number in
# [0, threshold).
rate_below() {
    python3 -c "import sys; r=float('$1'); exit(0 if 0 <= r < float('$2') else 1)" 2>/dev/null
}

# rate_at_least <rate> <threshold>: 1 when the rate is a number
# at or above the threshold.
rate_at_least() {
    python3 -c "import sys; r=float('$1'); exit(0 if r >= float('$2') else 1)" 2>/dev/null
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17351-17352 (gossip) and 18351-18352 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17351
127.0.0.1:17352
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17351 --peer-file "$TMP/peers" --control-port 18351 --low-probe-rate-frac 0.5 --ack-timeout-ms 2000 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17352 --peer-file "$TMP/peers" --control-port 18352 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file, and the START line
# shows the low-probe-rate threshold the node is watching with.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

if grep -q "START .* low-rate=0.50" "$TMP/n1.log"; then
    note ok "START line shows the low-probe-rate threshold (low-rate=0.50)"
else
    note fail "START line shows the low-probe-rate threshold (low-rate=0.50)"
    grep "START" "$TMP/n1.log" | head -2
fi

# A healthy peer reads low_probe_rate false with a probe_rate of
# 1.0, self reads probe_rate null (it never probes itself), and
# no LOWRATE line has fired before the kill.
if wait_json_healthy 10; then
    note ok "JSON reads the live peer healthy (low_probe_rate false, probe_rate 1.0)"
else
    note fail "JSON reads the live peer healthy (low_probe_rate false, probe_rate 1.0)"
    ctl 18351 JSON | sed -n '1p'
fi

if [ "$(json_member 18351 127.0.0.1:17351 probe_rate)" = "null" ] \
   && [ "$(json_member 18351 127.0.0.1:17351 low_probe_rate)" = "false" ]; then
    note ok "JSON reads self with probe_rate null and low_probe_rate false"
else
    note fail "JSON reads self with probe_rate null and low_probe_rate false"
    ctl 18351 JSON | sed -n '1p'
fi

if ctl 18351 METRICS | grep -q "low_probe_rate_peers=0" \
   && ! ctl 18351 METRICS | grep -q "^LOWRATE "; then
    note ok "METRICS reports low_probe_rate_peers=0 with no LOWRATE line while healthy"
else
    note fail "METRICS reports low_probe_rate_peers=0 with no LOWRATE line while healthy"
    ctl 18351 METRICS
fi

if [ "$(grep -c ' LOWRATE ' "$TMP/n1.log")" = "0" ]; then
    note ok "no LOWRATE line fired before the kill"
else
    note fail "no LOWRATE line fired before the kill"
    grep ' LOWRATE ' "$TMP/n1.log"
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 120 s bounds it if the test hangs — the
# recovery step restarts a node, so the run takes longer than the
# warning-only test did).
(timeout 120 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18351" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 2. Kill node 2 — with SIGKILL, an unclean crash: a clean SIGTERM
# would announce its leave and node 1 would mark it DEAD straight
# away, never suspecting it, so no silence would ever accumulate.
kill -9 "$(cat "$TMP/n2.pid")"

# The LOWRATE log line fires once the windowed rate slides below
# the threshold, while the peer is still ALIVE — well before the
# ~4 s suspicion delay.
if wait_for "$TMP/n1.log" 'LOWRATE 127\.0\.0\.1:17352 rate=0\.[0-9]+ threshold=0\.50' 10; then
    note ok "node 1 logs one LOWRATE line for the silent peer"
else
    note fail "node 1 logs one LOWRATE line for the silent peer"
    tail -5 "$TMP/n1.log"
fi

# The WATCH stream carries the LOWRATE event with the rate frozen
# on it: below the 0.5 threshold — the window had slid that far
# when the threshold was crossed — and the probe counts show how
# many probes went unanswered.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ LOWRATE 127\.0\.0\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ probe_rate=0\.[0-9]+$' 10; then
    note ok "WATCH streams a LOWRATE event with a probe_rate"
else
    note fail "WATCH streams a LOWRATE event with a probe_rate"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

LOWRATE_RATE="$(event_rate "$TMP/watch1.out" '^EVENT [0-9]+ LOWRATE 127\.0\.0\.1:17352 ')"
if [ -n "$LOWRATE_RATE" ] && rate_below "$LOWRATE_RATE" 0.5; then
    note ok "LOWRATE event probe_rate is below the threshold ($LOWRATE_RATE)"
else
    note fail "LOWRATE event probe_rate is below the threshold (got '$LOWRATE_RATE')"
fi

# While the peer is still ALIVE its JSON flag reads true with a
# rate below the threshold, METRICS counts one low-rate peer with
# a LOWRATE line, and PROMETHEUS gauges one with a per-member
# rate below the threshold.
if wait_json_lowrate 5; then
    note ok "JSON flags the peer low-probe-rate while it is still ALIVE"
else
    note fail "JSON flags the peer low-probe-rate while it is still ALIVE"
    ctl 18351 JSON | sed -n '1p'
fi

JSON_RATE="$(json_member 18351 127.0.0.1:17352 probe_rate)"
if [ -n "$JSON_RATE" ] && rate_below "$JSON_RATE" 0.5; then
    note ok "JSON probe_rate is below the threshold while the peer is low ($JSON_RATE)"
else
    note fail "JSON probe_rate is below the threshold while the peer is low (got '$JSON_RATE')"
fi

if wait_metrics_lowrate 5; then
    note ok "METRICS reports low_probe_rate_peers=1 while the peer is low"
else
    note fail "METRICS reports low_probe_rate_peers=1 while the peer is low"
    ctl 18351 METRICS
fi

METRICS_RATE="$(ctl 18351 METRICS | grep '^LOWRATE 127\.0\.0\.1:17352 ' | awk '{print $3}')"
if [ -n "$METRICS_RATE" ] && rate_below "$METRICS_RATE" 0.5; then
    note ok "METRICS LOWRATE line carries a rate below the threshold ($METRICS_RATE)"
else
    note fail "METRICS LOWRATE line carries a rate below the threshold (got '$METRICS_RATE')"
    ctl 18351 METRICS | grep LOWRATE
fi

if wait_prom_lowrate 5; then
    note ok "PROMETHEUS reports pulse_low_probe_rate_peers 1 while the peer is low"
else
    note fail "PROMETHEUS reports pulse_low_probe_rate_peers 1 while the peer is low"
    ctl 18351 PROMETHEUS | grep -E "low_probe|probe_rate"
fi

PROM_RATE="$(ctl 18351 PROMETHEUS | grep '^pulse_member_probe_rate{member="127.0.0.1:17352"}' | awk '{print $2}')"
if [ -n "$PROM_RATE" ] && rate_below "$PROM_RATE" 0.5; then
    note ok "PROMETHEUS pulse_member_probe_rate is below the threshold ($PROM_RATE)"
else
    note fail "PROMETHEUS pulse_member_probe_rate is below the threshold (got '$PROM_RATE')"
    ctl 18351 PROMETHEUS | grep -E "probe_rate"
fi

# 3. Once the peer is suspected the flag clears: a SUSPECT peer
# is not low-rate, the suspicion already says it is silent — and
# the LOWRATE line never fires a second time for this episode.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17352 ' 10; then
    note ok "WATCH streams SUSPECTED for the peer after the LOWRATE"
else
    note fail "WATCH streams SUSPECTED for the peer after the LOWRATE"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

if [ "$(json_member 18351 127.0.0.1:17352 low_probe_rate)" = "false" ]; then
    note ok "JSON clears the low_probe_rate flag once the peer is SUSPECT"
else
    note fail "JSON clears the low_probe_rate flag once the peer is SUSPECT"
    ctl 18351 JSON | sed -n '1p'
fi

if ctl 18351 METRICS | grep -q "low_probe_rate_peers=0"; then
    note ok "METRICS reports low_probe_rate_peers=0 once the peer is SUSPECT"
else
    note fail "METRICS reports low_probe_rate_peers=0 once the peer is SUSPECT"
    ctl 18351 METRICS
fi

if [ "$(grep -c ' LOWRATE 127\.0\.0\.1:17352 ' "$TMP/n1.log")" = "1" ]; then
    note ok "the LOWRATE line fired exactly once for the episode"
else
    note fail "the LOWRATE line fired exactly once for the episode"
    grep ' LOWRATE ' "$TMP/n1.log"
fi

if [ "$(grep -c '^EVENT [0-9]* LOWRATE 127\.0\.0\.1:17352 ' "$TMP/watch1.out")" = "1" ]; then
    note ok "the LOWRATE event fired exactly once for the episode"
else
    note fail "the LOWRATE event fired exactly once for the episode"
    grep '^EVENT.*LOWRATE' "$TMP/watch1.out" 2>/dev/null
fi

# 4. The persisted event log carries the same LOWRATE line as the
# WATCH stream did: the warning survives a restart and a replay.
if wait_for "$TMP/n1/events" "LOWRATE 127\\.0\\.0\\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ probe_rate=$LOWRATE_RATE\$" 10; then
    note ok "persisted events file carries the same LOWRATE line ($LOWRATE_RATE)"
else
    note fail "persisted events file carries the same LOWRATE line ($LOWRATE_RATE)"
    cat "$TMP/n1/events" 2>/dev/null
fi

# A replay from the start shows the LOWRATE event too.
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18351" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ LOWRATE 127\\.0\\.0\\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ probe_rate=$LOWRATE_RATE\$" 10; then
    note ok "replayed WATCH shows the LOWRATE event with the same rate"
else
    note fail "replayed WATCH shows the LOWRATE event with the same rate"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi

# 5. Restart node 2 — same data dir, so it rejoins at a higher
# incarnation — and node 1 revives it. Its windowed rate then
# climbs back as probes are answered again: ten answered probes
# in the 20-probe window bring it to 0.50, about a second of
# probing, and the recovery fires once.
(cd "$TMP/n2" && exec "$BIN" --port 17352 --peer-file "$TMP/peers" --control-port 18352 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17352 ' 15; then
    note ok "WATCH streams REVIVED for the restarted peer"
else
    note fail "WATCH streams REVIVED for the restarted peer"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

if wait_for "$TMP/n1.log" 'RATEOK 127\.0\.0\.1:17352 rate=[0-9.]+ threshold=0\.50' 15; then
    note ok "node 1 logs a RATEOK line for the recovered peer"
else
    note fail "node 1 logs a RATEOK line for the recovered peer"
    tail -5 "$TMP/n1.log"
fi

# The WATCH stream carries the RATEOK event with the recovered
# rate frozen on it: at or above the 0.5 threshold — the window
# had climbed that far when the episode ended — and the probe
# counts show how many probes were answered again.
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ RATEOK 127\.0\.0\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ probe_rate=[0-9.]+$' 15; then
    note ok "WATCH streams a RATEOK event with a probe_rate"
else
    note fail "WATCH streams a RATEOK event with a probe_rate"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

RATEOK_RATE="$(event_rate "$TMP/watch1.out" '^EVENT [0-9]+ RATEOK 127\.0\.0\.1:17352 ')"
if [ -n "$RATEOK_RATE" ] && rate_at_least "$RATEOK_RATE" 0.5; then
    note ok "RATEOK event probe_rate is at or above the threshold ($RATEOK_RATE)"
else
    note fail "RATEOK event probe_rate is at or above the threshold (got '$RATEOK_RATE')"
fi

# The recovered peer reads healthy again: JSON flag false, METRICS
# at zero with no LOWRATE line, PROMETHEUS gauge at zero — the
# flag cleared because the peer is answering again, and the
# RATEOK event is the record of it.
if [ "$(json_member 18351 127.0.0.1:17352 low_probe_rate)" = "false" ]; then
    note ok "JSON reads the recovered peer low_probe_rate false"
else
    note fail "JSON reads the recovered peer low_probe_rate false"
    ctl 18351 JSON | sed -n '1p'
fi

if ctl 18351 METRICS | grep -q "low_probe_rate_peers=0" \
   && ! ctl 18351 METRICS | grep -q "^LOWRATE "; then
    note ok "METRICS reports low_probe_rate_peers=0 with no LOWRATE line after recovery"
else
    note fail "METRICS reports low_probe_rate_peers=0 with no LOWRATE line after recovery"
    ctl 18351 METRICS
fi

if ctl 18351 PROMETHEUS | grep -q "^pulse_low_probe_rate_peers 0$"; then
    note ok "PROMETHEUS reports pulse_low_probe_rate_peers 0 after recovery"
else
    note fail "PROMETHEUS reports pulse_low_probe_rate_peers 0 after recovery"
    ctl 18351 PROMETHEUS | grep -E "low_probe|probe_rate"
fi

if [ "$(grep -c ' RATEOK 127\.0\.0\.1:17352 ' "$TMP/n1.log")" = "1" ]; then
    note ok "the RATEOK line fired exactly once for the episode"
else
    note fail "the RATEOK line fired exactly once for the episode"
    grep ' RATEOK ' "$TMP/n1.log"
fi

if [ "$(grep -c '^EVENT [0-9]* RATEOK 127\.0\.0\.1:17352 ' "$TMP/watch1.out")" = "1" ]; then
    note ok "the RATEOK event fired exactly once for the episode"
else
    note fail "the RATEOK event fired exactly once for the episode"
    grep '^EVENT.*RATEOK' "$TMP/watch1.out" 2>/dev/null
fi

# The persisted event log carries the same RATEOK line as the
# WATCH stream did: the recovery survives a restart and a replay.
if wait_for "$TMP/n1/events" "RATEOK 127\\.0\\.0\\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ probe_rate=$RATEOK_RATE\$" 10; then
    note ok "persisted events file carries the same RATEOK line ($RATEOK_RATE)"
else
    note fail "persisted events file carries the same RATEOK line ($RATEOK_RATE)"
    cat "$TMP/n1/events" 2>/dev/null
fi

# A replay from the start shows the RATEOK event too.
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18351" WATCH FROM 0 > "$TMP/watch3.out" 2>&1 & echo $! > "$TMP/watch3.pid")
if wait_for "$TMP/watch3.out" "^EVENT [0-9]+ RATEOK 127\\.0\\.0\\.1:17352 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ probe_rate=$RATEOK_RATE\$" 10; then
    note ok "replayed WATCH shows the RATEOK event with the same rate"
else
    note fail "replayed WATCH shows the RATEOK event with the same rate"
    grep '^EVENT' "$TMP/watch3.out" 2>/dev/null
fi

echo "test_low_probe_rate: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
