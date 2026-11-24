#!/usr/bin/env bash
# Integration test for the phi suspicion threshold in METRICS
# (0.45.0): a node started with --phi-threshold N reports
# phi_threshold=N.NN at the end of its METRICS line — the
# --phi-threshold the node started with, 8.00 by default — and
# the field is emitted always, whether or not --phi-accrual is
# on. The value is read off the live control-plane reply: the
# METRICS line is parsed for its phi_threshold field, not
# grepped from source.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. Node 1 runs with --phi-threshold 3.25, node 2 with
#     the default and neither with --phi-accrual. The METRICS
#     line parsed from node 1 reports phi_threshold=3.25 and
#     from node 2 phi_threshold=8.00.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-phi-threshold-metrics-test.XXXXXX)"
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

# metrics_phi_threshold <control-port>: parse the METRICS line
# from a live control-plane reply and print its phi_threshold
# value (the first line is the METRICS line; PHI lines, if any,
# ride after it). Prints nothing when the field is absent.
metrics_phi_threshold() {
    ctl "$1" METRICS | sed -n '1p' | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^phi_threshold=/) { sub(/^phi_threshold=/, "", $i); print $i; exit } }'
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17901-17902 (gossip) and 18901-18902 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17901
127.0.0.1:17902
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17901 --peer-file "$TMP/peers" --control-port 18901 --phi-threshold 3.25 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17902 --peer-file "$TMP/peers" --control-port 18902 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# The METRICS line parsed from the --phi-threshold 3.25 node
# reports phi_threshold=3.25 — the value it started with, read
# off the live reply, not from source.
T1="$(metrics_phi_threshold 18901)"
if [ "$T1" = "3.25" ]; then
    note ok "METRICS on the --phi-threshold 3.25 node reports phi_threshold=3.25"
else
    note fail "METRICS on the --phi-threshold 3.25 node reports phi_threshold=3.25 (got '$T1')"
    ctl 18901 METRICS
fi

# The default node reports 8.00, and it does so without
# --phi-accrual: the field is emitted always.
T2="$(metrics_phi_threshold 18902)"
if [ "$T2" = "8.00" ]; then
    note ok "METRICS on the default node reports phi_threshold=8.00 without --phi-accrual"
else
    note fail "METRICS on the default node reports phi_threshold=8.00 without --phi-accrual (got '$T2')"
    ctl 18902 METRICS
fi

# The field rides on the METRICS line itself — the first line of
# the reply — not on a later PHI line or the END trailer.
if ctl 18901 METRICS | sed -n '1p' | grep -Eq '^METRICS .* phi_threshold=3.25$'; then
    note ok "phi_threshold rides at the end of the METRICS line itself"
else
    note fail "phi_threshold rides at the end of the METRICS line itself"
    ctl 18901 METRICS
fi

echo "test_phi_threshold_metrics: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
