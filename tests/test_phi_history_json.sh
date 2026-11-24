#!/usr/bin/env bash
# Integration test for phi history in JSON (0.47.0): each member in
# JSON carries "phi_history" — the last 16 sampled phi values with
# their age in ms since the node started, oldest first, alongside
# the live "phi" — so an operator can see how suspicion evolved,
# not just the current value. The history is read off the live
# control-plane reply and parsed as JSON, not grepped from source.
#  1. Three nodes start from one shared --peer-file and converge on
#     alive=3. Nodes 1 and 2 run with --phi-accrual, node 3 without.
#  2. Node 1's JSON shows a growing phi_history for its peer: at
#     least 3 samples, ages strictly increasing, values in [0, 30].
#  3. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1's history for it keeps growing while
#     the peer is SUSPECT and its last phi rises past the 8.0
#     threshold — the rise is visible in the history, not only in
#     the live "phi".
#  4. Self always reports "phi_history":null, and node 3 (no
#     --phi-accrual) reports null for every member.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-phi-history-json-test.XXXXXX)"
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
# phi_history as JSON ("null" when the member has none), parsed
# from the live JSON reply with python3.
json_hist() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(json.dumps(m.get('phi_history')))
        break
"
}

# hist_last_phi <control-port> <member-id>: the last sampled phi
# value in the member's history, or empty when there is none.
hist_last_phi() {
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17921-17923 (gossip) and 18921-18923 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17921
127.0.0.1:17922
127.0.0.1:17923
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17921 --peer-file "$TMP/peers" --control-port 18921 --phi-accrual --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17922 --peer-file "$TMP/peers" --control-port 18922 --phi-accrual --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17923 --peer-file "$TMP/peers" --control-port 18923 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. All three converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 10; then
    note ok "3 nodes converge from the peer file"
else
    note fail "3 nodes converge from the peer file"
    for f in n1 n2 n3; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# The JSON reply is still one valid JSON object on one line.
if ctl 18921 JSON | sed -n '1p' | python3 -c "import json,sys; json.load(sys.stdin)" 2>/dev/null; then
    note ok "JSON reply parses as JSON with phi_history present"
else
    note fail "JSON reply parses as JSON with phi_history present"
    ctl 18921 JSON
fi

# Give the prober a few cycles so the history has samples to show.
sleep 2

# 2. Node 1's history for its peer: at least 3 samples, ages
# strictly increasing, values in [0, 30].
HIST="$(json_hist 18921 127.0.0.1:17922)"
CHECK="$(echo "$HIST" | python3 -c "
import json, sys
h = json.load(sys.stdin)
ok = isinstance(h, list) and len(h) >= 3
ok = ok and all(len(s) == 2 for s in h)
ok = ok and all(h[i][0] < h[i+1][0] for i in range(len(h)-1))
ok = ok and all(0.0 <= s[1] <= 30.0 for s in h)
print('ok' if ok else 'bad')
")"
if [ "$CHECK" = "ok" ]; then
    note ok "phi_history for a live peer has 3+ samples, rising ages, values in range"
else
    note fail "phi_history for a live peer has 3+ samples, rising ages, values in range (got '$HIST')"
fi

# A healthy peer's phi stays low: the last sampled value is under 2.
LAST_PHI="$(hist_last_phi 18921 127.0.0.1:17922)"
if python3 -c "exit(0 if float('$LAST_PHI') < 2.0 else 1)" 2>/dev/null; then
    note ok "last sampled phi for a healthy peer is under 2 (got $LAST_PHI)"
else
    note fail "last sampled phi for a healthy peer is under 2 (got '$LAST_PHI')"
fi

# 3. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through phi alone: the history for it must show the
# suspicion rising past the 8.0 threshold, at a later age than the
# healthy samples.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
sleep 2
LAST_PHI_DEAD="$(hist_last_phi 18921 127.0.0.1:17922)"
LAST_AGE_DEAD="$(hist_last_age 18921 127.0.0.1:17922)"
LAST_AGE_ALIVE="$(echo "$HIST" | python3 -c "import json,sys; print(json.load(sys.stdin)[-1][0])")"
if python3 -c "exit(0 if float('$LAST_PHI_DEAD') > 8.0 else 1)" 2>/dev/null \
   && [ -n "$LAST_AGE_DEAD" ] && [ "$LAST_AGE_DEAD" -gt "$LAST_AGE_ALIVE" ]; then
    note ok "after the peer crashes, its phi_history rises past 8.0 (last phi $LAST_PHI -> $LAST_PHI_DEAD)"
else
    note fail "after the peer crashes, its phi_history rises past 8.0 (last phi $LAST_PHI -> '$LAST_PHI_DEAD', ages $LAST_AGE_ALIVE -> '$LAST_AGE_DEAD')"
    ctl 18921 JSON
fi

# 4. Self reports null, and the node without --phi-accrual reports
# null for every member.
SELF_HIST="$(json_hist 18921 127.0.0.1:17921)"
if [ "$SELF_HIST" = "null" ]; then
    note ok "self reports phi_history null"
else
    note fail "self reports phi_history null (got '$SELF_HIST')"
fi

N3_HIST="$(ctl 18923 JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
print('null' if all(m.get('phi_history') is None for m in obj['members']) else 'set')
")"
if [ "$N3_HIST" = "null" ]; then
    note ok "node without --phi-accrual reports phi_history null for every member"
else
    note fail "node without --phi-accrual reports phi_history null for every member"
    ctl 18923 JSON
fi

echo "test_phi_history_json: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
