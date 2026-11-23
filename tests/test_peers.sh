#!/usr/bin/env bash
# Integration test for the PEERS control-plane command (0.27.0):
# one line per member — id, incarnation, state, ms since last direct
# contact, and where the peer came from (seed / peer-file /
# discovered / self) — then END.
#  1. Three nodes start from one shared --peer-file and converge on
#     alive=3. PEERS on node 1 lists its file peers as peer-file and
#     itself as self, with a numeric last-seen for the live peers
#     and '-' for self.
#  2. Node 4 starts with --seed 127.0.0.1:17811 only and no peer
#     file. It learns nodes 2 and 3 through the seed's SYNC, so
#     PEERS on node 4 lists the seed as seed and the other two as
#     discovered.
#  3. Node 3 is killed; once node 1 marks it DEAD, PEERS still lists
#     it — now DEAD, still peer-file — until the retention GCs it.
#  4. PEERS ends with an END line.
# The '-' vs digits last-seen distinction is pinned by the unit test
# (tests/test_control.c); here the seen field is matched loosely so
# the test is not timing-sensitive.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-peers-test.XXXXXX)"
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
        if grep -q "$pat" "$log" 2>/dev/null; then return 0; fi
        sleep 0.2
    done
    return 1
}

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

# peers <control-port>: the PEERS reply for a node.
peers() {
    ctl "$1" PEERS
}

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3" "$TMP/n4"

# Ports 17811-17814 (gossip) and 18811-18814 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17811
127.0.0.1:17812
127.0.0.1:17813
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17811 --peer-file "$TMP/peers" --control-port 18811 --dead-retention 30 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17812 --peer-file "$TMP/peers" --control-port 18812 --dead-retention 30 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17813 --peer-file "$TMP/peers" --control-port 18813 --dead-retention 30 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. All three converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 10; then
    note ok "3 nodes converge from the peer file"
else
    note fail "3 nodes converge from the peer file"
    for f in n1 n2 n3; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# 1a. PEERS on node 1: file peers are peer-file, self is self with
# a '-' last-seen, live peers carry a numeric last-seen.
p1="$(peers 18811)"
if printf '%s\n' "$p1" | grep -Eq '^PEER 127\.0\.0\.1:17811 1 ALIVE - self$' \
   && printf '%s\n' "$p1" | grep -Eq '^PEER 127\.0\.0\.1:17812 1 ALIVE [0-9]+ peer-file$' \
   && printf '%s\n' "$p1" | grep -Eq '^PEER 127\.0\.0\.1:17813 1 ALIVE [0-9]+ peer-file$'; then
    note ok "PEERS on a peer-file node: file peers peer-file, self self"
else
    note fail "PEERS on a peer-file node: file peers peer-file, self self"
    printf '%s\n' "$p1"
fi

# 2. Node 4 joins through --seed only: the seed is seed, the peers
# it learned from the seed's SYNC are discovered.
(cd "$TMP/n4" && exec "$BIN" --port 17814 --seed 127.0.0.1:17811 --control-port 18814 --dead-retention 30 > "$TMP/n4.log" 2>&1 & echo $! > "$TMP/n4.pid")
if wait_for "$TMP/n4.log" "STATUS alive=4" 10 \
   && wait_for "$TMP/n1.log" "STATUS alive=4" 10; then
    note ok "node 4 joins via --seed and the cluster is alive=4"
else
    note fail "node 4 joins via --seed and the cluster is alive=4"
    echo "--- n4.log"; tail -5 "$TMP/n4.log"
fi
p4="$(peers 18814)"
if printf '%s\n' "$p4" | grep -Eq '^PEER 127\.0\.0\.1:17814 1 ALIVE - self$' \
   && printf '%s\n' "$p4" | grep -Eq '^PEER 127\.0\.0\.1:17811 1 ALIVE [0-9]+ seed$' \
   && printf '%s\n' "$p4" | grep -Eq '^PEER 127\.0\.0\.1:17812 1 ALIVE .+ discovered$' \
   && printf '%s\n' "$p4" | grep -Eq '^PEER 127\.0\.0\.1:17813 1 ALIVE .+ discovered$'; then
    note ok "PEERS on a --seed node: seed is seed, gossip peers discovered"
else
    note fail "PEERS on a --seed node: seed is seed, gossip peers discovered"
    printf '%s\n' "$p4"
fi

# 3. Kill node 3; once node 1 marks it DEAD, PEERS shows it DEAD
# and still peer-file — the roster is where it came from, even
# though it is no longer answering.
kill -9 "$(cat "$TMP/n3.pid")"
if ! wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17813" 10; then
    note fail "node 1 marks the killed node 3 DEAD"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
else
    p1="$(peers 18811)"
    if printf '%s\n' "$p1" | grep -Eq '^PEER 127\.0\.0\.1:17813 1 DEAD .+ peer-file$'; then
        note ok "PEERS shows a dead peer as DEAD, still peer-file"
    else
        note fail "PEERS shows a dead peer as DEAD, still peer-file"
        printf '%s\n' "$p1"
    fi
fi

# 4. PEERS ends with an END line.
if [ "$(peers 18811 | tail -n 1)" = "END" ]; then
    note ok "PEERS ends with END"
else
    note fail "PEERS ends with END"
    peers 18811
fi

echo "test_peers: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
