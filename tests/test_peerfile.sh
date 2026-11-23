#!/usr/bin/env bash
# Integration test for seedless bootstrap via a peer list file.
#  1. A missing peer file is a clean startup error: the node exits
#     nonzero at once instead of starting alone and isolated.
#  2. Three nodes start with --peer-file (no --seed at all) from one
#     shared file listing all three members, plus a comment, a blank
#     line and a bad line. All three converge on alive=3: the bad
#     line is logged and skipped, the file's peers are probed and
#     JOINed like --seed flags.
#  3. A node whose peer file lists a peer that never answers still
#     starts and converges with the live peers; the dead entry goes
#     SUSPECT, DEAD, then GC like any unanswering seed.
# Without the peer file, step 2 never happens: nodes started with no
# --seed and no restored membership have no reason to contact anyone,
# so each stays a 1-node cluster forever.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-peerfile-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3" "$TMP/n4"

# 1. Missing peer file: clean error, nonzero exit, no START line.
"$BIN" --port 17601 --peer-file "$TMP/no-such-file" --data-dir "$TMP/n1" \
    > "$TMP/missing.log" 2>&1
code=$?
if [ "$code" -ne 0 ] && grep -q "cannot read peer file" "$TMP/missing.log" \
   && ! grep -q "START" "$TMP/missing.log"; then
    note ok "missing peer file is a clean startup error (exit $code)"
else
    note fail "missing peer file is a clean startup error (exit $code)"
    echo "--- missing.log"; cat "$TMP/missing.log"
fi

# Shared peer file for the whole cluster, with a comment, a blank
# line, a bad line, and every member (including self for each node).
cat > "$TMP/peers" <<'EOF'
# ward cluster — one file, every node
127.0.0.1:17601

127.0.0.1:17602
not-a-peer
127.0.0.1:17603
EOF

# Ports 17601-17603 so this test never clashes with the other tests.
(cd "$TMP/n1" && exec "$BIN" --port 17601 --peer-file "$TMP/peers" --dead-retention 2 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17602 --peer-file "$TMP/peers" --dead-retention 2 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17603 --peer-file "$TMP/peers" --dead-retention 2 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 2. All three converge with no --seed anywhere.
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 10; then
    note ok "3 nodes converge from the peer file with no --seed"
else
    note fail "3 nodes converge from the peer file with no --seed"
    for f in n1 n2 n3; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# The bad line was logged and skipped, not fatal.
if grep -q "PEERS 3 from $TMP/peers (1 bad lines skipped)" "$TMP/n1.log" \
   && grep -q "skipping bad peer 'not-a-peer'" "$TMP/n1.log"; then
    note ok "bad peer line logged and skipped"
else
    note fail "bad peer line logged and skipped"
    echo "--- n1.log"; cat "$TMP/n1.log"
fi

# 3. A peer file with a dead peer: node 4 still starts, converges
# with the live cluster, and GCs the entry that never answers.
cat > "$TMP/peers4" <<'EOF'
127.0.0.1:17601
127.0.0.1:17699
EOF
(cd "$TMP/n4" && exec "$BIN" --port 17604 --peer-file "$TMP/peers4" --dead-retention 2 > "$TMP/n4.log" 2>&1 & echo $! > "$TMP/n4.pid")
if wait_for "$TMP/n1.log" "STATUS alive=4" 10 \
   && wait_for "$TMP/n4.log" "GC 127.0.0.1:17699" 10; then
    note ok "node with a dead peer in its file still joins and GCs it"
else
    note fail "node with a dead peer in its file still joins and GCs it"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi

echo "test_peerfile: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
