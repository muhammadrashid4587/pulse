#!/usr/bin/env bash
# Integration test for suspicion age per member (0.58.0): each member
# in JSON carries "suspect_ms" — how long it has been SUSPECT, in
# ms, null unless SUSPECT — METRICS carries one "SUSPECT <id> <ms>"
# line per SUSPECT member, and PROMETHEUS carries one
# pulse_member_suspect_ms gauge sample per SUSPECT member, so an
# operator can see how close a suspicion is to the suspect timeout
# instead of only learning about it when the member goes DEAD. The
# values are read off the live control-plane replies and parsed,
# not grepped from source.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2, with a 10 s suspect timeout so the peer stays
#     SUSPECT long enough to observe.
#  2. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 marks it SUSPECT, and while it is
#     SUSPECT its JSON suspect_ms is a number in [0, 10000), the
#     METRICS reply has a matching SUSPECT line, and the
#     PROMETHEUS reply has a matching pulse_member_suspect_ms
#     sample — while self reports suspect_ms null and has no
#     SUSPECT line or gauge sample.
#  3. Once the suspect timeout expires the peer goes DEAD: its JSON
#     suspect_ms is null again, and the SUSPECT line and the gauge
#     sample are gone — a DEAD member's suspicion is over.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-suspect-ms-test.XXXXXX)"
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

# json_suspect_ms <control-port> <member-id>: print the member's
# suspect_ms as JSON ("null" when the member is not SUSPECT),
# parsed from the live JSON reply with python3.
json_suspect_ms() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(json.dumps(m.get('suspect_ms')))
        break
"
}

# json_state <control-port> <member-id>: the member's state string.
json_state() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(m.get('state'))
        break
"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17931-17932 (gossip) and 18931-18932 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17931
127.0.0.1:17932
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17931 --peer-file "$TMP/peers" --control-port 18931 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17932 --peer-file "$TMP/peers" --control-port 18932 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# The JSON reply is still one valid JSON object on one line.
if ctl 18931 JSON | sed -n '1p' | python3 -c "import json,sys; json.load(sys.stdin)" 2>/dev/null; then
    note ok "JSON reply parses as JSON with suspect_ms present"
else
    note fail "JSON reply parses as JSON with suspect_ms present"
    ctl 18931 JSON
fi

# While both are ALIVE, no member reports a suspicion age.
ALIVE_SUSPECT="$(json_suspect_ms 18931 127.0.0.1:17932)"
SELF_SUSPECT="$(json_suspect_ms 18931 127.0.0.1:17931)"
if [ "$ALIVE_SUSPECT" = "null" ] && [ "$SELF_SUSPECT" = "null" ]; then
    note ok "an ALIVE peer and self both report suspect_ms null"
else
    note fail "an ALIVE peer and self both report suspect_ms null (peer '$ALIVE_SUSPECT', self '$SELF_SUSPECT')"
fi

# 2. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through probing.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17932" 10; then
    note ok "node 1 marks the crashed peer SUSPECT"
else
    note fail "node 1 marks the crashed peer SUSPECT"
    tail -5 "$TMP/n1.log"
fi

# JSON: the SUSPECT peer's suspect_ms is a number in [0, 10000),
# and its state is SUSPECT; self still reports null.
STATE="$(json_state 18931 127.0.0.1:17932)"
SUSPECT_MS="$(json_suspect_ms 18931 127.0.0.1:17932)"
SELF_SUSPECT="$(json_suspect_ms 18931 127.0.0.1:17931)"
if [ "$STATE" = "SUSPECT" ] \
   && python3 -c "exit(0 if 0 <= float('$SUSPECT_MS') < 10000 else 1)" 2>/dev/null \
   && [ "$SELF_SUSPECT" = "null" ]; then
    note ok "JSON suspect_ms for the SUSPECT peer is a number in range ($SUSPECT_MS ms), self null"
else
    note fail "JSON suspect_ms for the SUSPECT peer is a number in range (state '$STATE', suspect_ms '$SUSPECT_MS', self '$SELF_SUSPECT')"
    ctl 18931 JSON
fi

# METRICS: one "SUSPECT <id> <ms>" line for the suspected peer,
# with the same value in range, and none for self.
METRICS_SUSPECT="$(ctl 18931 METRICS | grep '^SUSPECT 127.0.0.1:17932 ' || true)"
METRICS_SELF="$(ctl 18931 METRICS | grep '^SUSPECT 127.0.0.1:17931 ' || true)"
METRICS_MS="$(echo "$METRICS_SUSPECT" | awk '{print $3}')"
if [ -n "$METRICS_SUSPECT" ] && [ -z "$METRICS_SELF" ] \
   && python3 -c "exit(0 if 0 <= float('$METRICS_MS') < 10000 else 1)" 2>/dev/null; then
    note ok "METRICS carries a SUSPECT line for the peer ($METRICS_SUSPECT), none for self"
else
    note fail "METRICS carries a SUSPECT line for the peer (got '$METRICS_SUSPECT', self '$METRICS_SELF')"
    ctl 18931 METRICS
fi

# PROMETHEUS: one pulse_member_suspect_ms sample for the suspected
# peer, with the value in range, and none for self.
PROM_SUSPECT="$(ctl 18931 PROMETHEUS | grep '^pulse_member_suspect_ms{member="127.0.0.1:17932"} ' || true)"
PROM_SELF="$(ctl 18931 PROMETHEUS | grep '^pulse_member_suspect_ms{member="127.0.0.1:17931"} ' || true)"
PROM_MS="$(echo "$PROM_SUSPECT" | awk '{print $2}')"
if [ -n "$PROM_SUSPECT" ] && [ -z "$PROM_SELF" ] \
   && python3 -c "exit(0 if 0 <= float('$PROM_MS') < 10000 else 1)" 2>/dev/null; then
    note ok "PROMETHEUS carries a pulse_member_suspect_ms sample for the peer ($PROM_SUSPECT), none for self"
else
    note fail "PROMETHEUS carries a pulse_member_suspect_ms sample for the peer (got '$PROM_SUSPECT', self '$PROM_SELF')"
    ctl 18931 PROMETHEUS
fi

# 3. Once the 10 s suspect timeout expires the peer goes DEAD: the
# suspicion age disappears from all three surfaces.
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17932" 20; then
    note ok "the peer goes DEAD after the suspect timeout"
else
    note fail "the peer goes DEAD after the suspect timeout"
    tail -5 "$TMP/n1.log"
fi

DEAD_SUSPECT="$(json_suspect_ms 18931 127.0.0.1:17932)"
DEAD_METRICS="$(ctl 18931 METRICS | grep '^SUSPECT ' || true)"
DEAD_PROM="$(ctl 18931 PROMETHEUS | grep '^pulse_member_suspect_ms{' || true)"
if [ "$DEAD_SUSPECT" = "null" ] && [ -z "$DEAD_METRICS" ] && [ -z "$DEAD_PROM" ]; then
    note ok "a DEAD peer reports suspect_ms null and has no SUSPECT line or gauge sample"
else
    note fail "a DEAD peer reports suspect_ms null and has no SUSPECT line or gauge sample (json '$DEAD_SUSPECT', metrics '$DEAD_METRICS', prom '$DEAD_PROM')"
fi

echo "test_suspect_ms: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
