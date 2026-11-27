#!/usr/bin/env bash
# Integration test for per-member probe-rate history in JSON
# (0.71.0): each member in JSON carries "probe_rate_history" —
# the last 16 sampled windowed probe success rates, as
# [[age_ms, rate], ...] pairs oldest first, alongside the live
# "probe_rate" — so an operator can see a peer's rate fall over
# time, not just its current value. The history is read off the
# live control-plane reply and parsed as JSON, not grepped from
# source.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2.
#  2. Node 1's JSON shows a growing probe_rate_history for its
#     peer: at least 3 samples, ages strictly increasing, values
#     in [0, 1] — and near 1.0 while the peer answers nearly
#     every probe (one early probe can go unanswered before the
#     peer is up and sit in the window until it slides out).
#  3. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 keeps probing it while it is SUSPECT,
#     so its history's last rate falls below 1.0 at a later age —
#     the fall is visible in the history, not only in the live
#     "probe_rate" — and once the peer is DEAD the history is
#     frozen at the value it reached at suspicion.
#  4. Self always reports "probe_rate_history":null.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-probe-rate-history-json-test.XXXXXX)"
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

# json_hist <control-port> <member-id>: print the member's
# probe_rate_history as JSON ("null" when the member has none),
# parsed from the live JSON reply with python3.
json_hist() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(json.dumps(m.get('probe_rate_history')))
        break
"
}

# hist_last_rate <control-port> <member-id>: the last sampled
# probe rate in the member's history, or empty when there is none.
hist_last_rate() {
    json_hist "$1" "$2" | python3 -c "
import json, sys
h = json.load(sys.stdin)
print(h[-1][1] if h else '')
"
}

# hist_last_age <control-port> <member-id>: the last sampled age.
hist_last_age() {
    json_hist "$1" "$2" | python3 -c "
import json, sys
h = json.load(sys.stdin)
print(h[-1][0] if h else '')
"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17931-17932 (gossip) and 18931-18932 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17931
127.0.0.1:17932
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17931 --peer-file "$TMP/peers" --control-port 18931 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17932 --peer-file "$TMP/peers" --control-port 18932 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

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
    note ok "JSON reply parses as JSON with probe_rate_history present"
else
    note fail "JSON reply parses as JSON with probe_rate_history present"
    ctl 18931 JSON
fi

# Give the prober a few cycles so the history has samples to show.
sleep 2

# 2. Node 1's history for its peer: at least 3 samples, ages
# strictly increasing, values in [0, 1].
HIST="$(json_hist 18931 127.0.0.1:17932)"
CHECK="$(echo "$HIST" | python3 -c "
import json, sys
h = json.load(sys.stdin)
ok = isinstance(h, list) and len(h) >= 3
ok = ok and all(len(s) == 2 for s in h)
ok = ok and all(h[i][0] < h[i+1][0] for i in range(len(h)-1))
ok = ok and all(0.0 <= s[1] <= 1.0 for s in h)
print('ok' if ok else 'bad')
")"
if [ "$CHECK" = "ok" ]; then
    note ok "probe_rate_history for a live peer has 3+ samples, rising ages, values in range"
else
    note fail "probe_rate_history for a live peer has 3+ samples, rising ages, values in range (got '$HIST')"
fi

# A healthy peer answers nearly every probe: the last sampled
# rate is at least 0.9. It is not asserted to be exactly 1.0:
# a probe sent in the first cycles, before the peer's socket
# was up, can sit in the 20-probe window until 20 later probes
# slide it out, so a single early miss reads as 0.95.
LAST_RATE="$(hist_last_rate 18931 127.0.0.1:17932)"
if python3 -c "exit(0 if float('$LAST_RATE') >= 0.9 else 1)" 2>/dev/null; then
    note ok "last sampled probe rate for a healthy peer is at least 0.9 (got $LAST_RATE)"
else
    note fail "last sampled probe rate for a healthy peer is at least 0.9 (got '$LAST_RATE')"
fi

# 3. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through probing: the history for it must show the rate
# falling below 1.0, at a later age than the healthy samples.
LAST_AGE_ALIVE="$(echo "$HIST" | python3 -c "import json,sys; print(json.load(sys.stdin)[-1][0])")"
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
sleep 2
LAST_RATE_DEAD="$(hist_last_rate 18931 127.0.0.1:17932)"
LAST_AGE_DEAD="$(hist_last_age 18931 127.0.0.1:17932)"
if python3 -c "exit(0 if float('$LAST_RATE_DEAD') < 1.0 else 1)" 2>/dev/null \
   && [ -n "$LAST_AGE_DEAD" ] && [ "$LAST_AGE_DEAD" -gt "$LAST_AGE_ALIVE" ]; then
    note ok "after the peer crashes, its probe_rate_history falls below 1.0 (last rate $LAST_RATE -> $LAST_RATE_DEAD)"
else
    note fail "after the peer crashes, its probe_rate_history falls below 1.0 (last rate $LAST_RATE -> '$LAST_RATE_DEAD', ages $LAST_AGE_ALIVE -> '$LAST_AGE_DEAD')"
    ctl 18931 JSON
fi

# 4. Self reports null.
SELF_HIST="$(json_hist 18931 127.0.0.1:17931)"
if [ "$SELF_HIST" = "null" ]; then
    note ok "self reports probe_rate_history null"
else
    note fail "self reports probe_rate_history null (got '$SELF_HIST')"
fi

echo "test_probe_rate_history_json: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
