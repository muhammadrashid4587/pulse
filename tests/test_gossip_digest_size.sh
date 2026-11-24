#!/usr/bin/env bash
# Integration test for the configurable gossip digest size (0.41.0):
# a node started with --gossip-digest-size N covers that many
# members per gossip round's digest, and METRICS / JSON /
# PROMETHEUS report the size.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. Node 1 runs with --gossip-digest-size 4, node 2 with
#     the default. METRICS on node 1 reports gossip_digest_size=4
#     and on node 2 gossip_digest_size=16; JSON carries
#     "gossip_digest_size":4 / 16 top-level, and PROMETHEUS
#     carries "pulse_gossip_digest_size 4" on node 1. The small
#     digest still converges the cluster: a digest of 4 is a
#     working digest, not a broken one.
#  2. A bad size is a clean startup error: --gossip-digest-size 0
#     and --gossip-digest-size 65 both exit 2 with an error message.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-gossip-digest-size-test.XXXXXX)"
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

# Ports 17891-17892 (gossip) and 18891-18892 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17891
127.0.0.1:17892
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17891 --peer-file "$TMP/peers" --control-port 18891 --gossip-digest-size 4 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17892 --peer-file "$TMP/peers" --control-port 18892 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file, even on the 4-entry digest.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file on a 4-entry digest"
else
    note fail "2 nodes converge from the peer file on a 4-entry digest"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi
if ctl 18891 METRICS | grep -Fq "gossip_digest_size=4"; then
    note ok "METRICS on the --gossip-digest-size 4 node reports gossip_digest_size=4"
else
    note fail "METRICS on the --gossip-digest-size 4 node reports gossip_digest_size=4"
    ctl 18891 METRICS
fi
if ctl 18892 METRICS | grep -Fq "gossip_digest_size=16"; then
    note ok "METRICS on the default node reports gossip_digest_size=16"
else
    note fail "METRICS on the default node reports gossip_digest_size=16"
    ctl 18892 METRICS
fi
if ctl 18891 JSON | grep -Fq '"gossip_digest_size":4'; then
    note ok "JSON on the --gossip-digest-size 4 node reports gossip_digest_size 4"
else
    note fail "JSON on the --gossip-digest-size 4 node reports gossip_digest_size 4"
    ctl 18891 JSON
fi
if ctl 18892 JSON | grep -Fq '"gossip_digest_size":16'; then
    note ok "JSON on the default node reports gossip_digest_size 16"
else
    note fail "JSON on the default node reports gossip_digest_size 16"
    ctl 18892 JSON
fi
if ctl 18891 PROMETHEUS | grep -Fq "pulse_gossip_digest_size 4"; then
    note ok "PROMETHEUS on the --gossip-digest-size 4 node reports pulse_gossip_digest_size 4"
else
    note fail "PROMETHEUS on the --gossip-digest-size 4 node reports pulse_gossip_digest_size 4"
    ctl 18891 PROMETHEUS
fi

# 2. A bad size is a clean startup error, not a silent default.
if "$BIN" --port 17893 --gossip-digest-size 0 --data-dir "$TMP/bad" 2> "$TMP/bad0.err"; then
    note fail "--gossip-digest-size 0 exits nonzero"
else
    if grep -Fq -- "--gossip-digest-size must be between 1 and 64" "$TMP/bad0.err"; then
        note ok "--gossip-digest-size 0 exits 2 with an error message"
    else
        note fail "--gossip-digest-size 0 exits 2 with an error message"
        cat "$TMP/bad0.err"
    fi
fi
if "$BIN" --port 17893 --gossip-digest-size 65 --data-dir "$TMP/bad" 2> "$TMP/bad1.err"; then
    note fail "--gossip-digest-size 65 exits nonzero"
else
    if grep -Fq -- "--gossip-digest-size must be between 1 and 64" "$TMP/bad1.err"; then
        note ok "--gossip-digest-size 65 exits 2 with an error message"
    else
        note fail "--gossip-digest-size 65 exits 2 with an error message"
        cat "$TMP/bad1.err"
    fi
fi

echo "test_gossip_digest_size: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
