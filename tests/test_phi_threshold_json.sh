#!/usr/bin/env bash
# Integration test for the phi suspicion threshold in JSON
# (0.46.0): a node started with --phi-threshold N reports
# "phi_threshold":N.NN as a top-level field in its JSON reply —
# the --phi-threshold the node started with, 8.00 by default —
# and the field is emitted always, whether or not --phi-accrual
# is on. The value is read off the live control-plane reply: the
# JSON line is parsed for its phi_threshold field, not grepped
# from source.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. Node 1 runs with --phi-threshold 3.25, node 2 with
#     the default and neither with --phi-accrual. The JSON parsed
#     from node 1 reports phi_threshold 3.25 and from node 2
#     phi_threshold 8.00.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-phi-threshold-json-test.XXXXXX)"
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

# json_phi_threshold <control-port>: parse the JSON reply and
# print its top-level phi_threshold value. Prints nothing when
# the field is absent.
json_phi_threshold() {
    ctl "$1" JSON | sed -n '1p' | grep -o '"phi_threshold":[0-9.]*' | sed 's/"phi_threshold"://'
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17911-17912 (gossip) and 18911-18912 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17911
127.0.0.1:17912
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17911 --peer-file "$TMP/peers" --control-port 18911 --phi-threshold 3.25 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17912 --peer-file "$TMP/peers" --control-port 18912 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# The JSON parsed from the --phi-threshold 3.25 node reports
# phi_threshold 3.25 — the value it started with, read off the
# live reply, not from source.
T1="$(json_phi_threshold 18911)"
if [ "$T1" = "3.25" ]; then
    note ok "JSON on the --phi-threshold 3.25 node reports phi_threshold 3.25"
else
    note fail "JSON on the --phi-threshold 3.25 node reports phi_threshold 3.25 (got '$T1')"
    ctl 18911 JSON
fi

# The default node reports 8.00, and it does so without
# --phi-accrual: the field is emitted always.
T2="$(json_phi_threshold 18912)"
if [ "$T2" = "8.00" ]; then
    note ok "JSON on the default node reports phi_threshold 8.00 without --phi-accrual"
else
    note fail "JSON on the default node reports phi_threshold 8.00 without --phi-accrual (got '$T2')"
    ctl 18912 JSON
fi

# The field rides top-level in the JSON object — after
# gossip_digest_size, the last field before the closing brace —
# not inside "metrics" or a member object.
if ctl 18911 JSON | sed -n '1p' | grep -Eq '"gossip_digest_size":16,"phi_threshold":3.25}'; then
    note ok "phi_threshold rides top-level in JSON, after gossip_digest_size"
else
    note fail "phi_threshold rides top-level in JSON, after gossip_digest_size"
    ctl 18911 JSON
fi

echo "test_phi_threshold_json: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
