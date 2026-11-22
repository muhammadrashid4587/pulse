#!/usr/bin/env bash
# Integration test for the optional shared secret: real nodes over
# localhost UDP, queried over TCP with pulse-ctl.
#  1. Same secret: two nodes converge on alive=2, MEMBERS lists both.
#  2. Different secrets: neither node ever lists the other; the node
#     that seeded through the other suspects, deads, and GCs it.
#  3. No secret: two nodes converge, as before (backwards compatible).
#  4. Mixed (one secret, one plain): the secret node never lists the
#     plain node, and the plain node ends up alone after GC — the
#     clusters do not merge.
# START lines report secret=on only for nodes started with a secret.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-secret-test.XXXXXX)"
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

# start_node <name> <port> <control-port> [extra args...]
start_node() {
    local name="$1" port="$2" cport="$3"; shift 3
    mkdir -p "$TMP/$name"
    "$BIN" --port "$port" --control-port "$cport" --data-dir "$TMP/$name" "$@" \
        > "$TMP/$name.log" 2>&1 &
    echo $! > "$TMP/$name.pid"
}

# members_total <control-port>: the total= N from STATUS.
members_total() {
    ctl "$1" STATUS | grep -o "total=[0-9]*" | head -1
}

# Ports 17501-17508 (gossip) and 18501-18508 (control) so this test
# never clashes with the other suites.

# 1. Same secret: converge.
start_node a 17501 18501 --secret ward-alpha
start_node b 17502 18502 --secret ward-alpha --seed 127.0.0.1:17501
if wait_for "$TMP/a.log" "STATUS alive=2" 6 \
   && wait_for "$TMP/b.log" "STATUS alive=2" 6; then
    note ok "same secret: 2 nodes converge on alive=2"
else
    note fail "same secret: 2 nodes converge on alive=2"
fi
members="$(ctl 18501 MEMBERS)"
if echo "$members" | grep -q "127.0.0.1:17501.*ALIVE" \
   && echo "$members" | grep -q "127.0.0.1:17502.*ALIVE"; then
    note ok "same secret: MEMBERS lists both nodes"
else
    note fail "same secret: MEMBERS lists both nodes"
    echo "--- ctl output:"; echo "$members"
fi
if grep -q "secret=on" "$TMP/a.log" && grep -q "secret=on" "$TMP/b.log"; then
    note ok "START lines report secret=on for secret nodes"
else
    note fail "START lines report secret=on for secret nodes"
fi
# The secret itself must never appear in a log.
if grep -q "ward-alpha" "$TMP/a.log" || grep -q "ward-alpha" "$TMP/b.log"; then
    note fail "secret value never appears in logs"
else
    note ok "secret value never appears in logs"
fi

# 2. Different secrets: neither lists the other. The seeding node
# (d, secret beta) tries its seed (c, secret alpha), gets no reply,
# and GCs it with a 2 s retention.
start_node c 17503 18503 --secret ward-alpha
start_node d 17504 18504 --secret ward-beta --seed 127.0.0.1:17503 --dead-retention 2
sleep 6
if [ "$(members_total 18503)" = "total=1" ]; then
    note ok "different secrets: alpha node stays alone (total=1)"
else
    note fail "different secrets: alpha node stays alone (total=1)"
    echo "--- status:"; ctl 18503 STATUS
fi
if [ "$(members_total 18504)" = "total=1" ]; then
    note ok "different secrets: beta node ends up alone after GC (total=1)"
else
    note fail "different secrets: beta node ends up alone after GC (total=1)"
    echo "--- status:"; ctl 18504 STATUS
fi
if ! grep -q "JOIN 127.0.0.1:17504" "$TMP/c.log"; then
    note ok "different secrets: alpha node never logs a JOIN from beta"
else
    note fail "different secrets: alpha node never logs a JOIN from beta"
fi
if grep -q "SUSPECT 127.0.0.1:17503" "$TMP/d.log" \
   && grep -q "DEAD 127.0.0.1:17503" "$TMP/d.log"; then
    note ok "different secrets: beta node suspects then deads the unanswering seed"
else
    note fail "different secrets: beta node suspects then deads the unanswering seed"
fi

# 3. No secret: converge, as before.
start_node e 17505 18505
start_node f 17506 18506 --seed 127.0.0.1:17505
if wait_for "$TMP/e.log" "STATUS alive=2" 6 \
   && wait_for "$TMP/f.log" "STATUS alive=2" 6; then
    note ok "no secret: 2 nodes converge on alive=2"
else
    note fail "no secret: 2 nodes converge on alive=2"
fi
if ! grep -q "secret=on" "$TMP/e.log" && ! grep -q "secret=on" "$TMP/f.log"; then
    note ok "START lines do not report secret=on for plain nodes"
else
    note fail "START lines do not report secret=on for plain nodes"
fi

# 4. Mixed: secret node (s) and plain node (n, seeded through s).
# The plain node sends untagged traffic, which the secret node drops,
# so s never lists n; n gets no reply, suspects, deads, and GCs s.
start_node s 17507 18507 --secret ward-alpha
start_node n 17508 18508 --seed 127.0.0.1:17507 --dead-retention 2
sleep 6
if [ "$(members_total 18507)" = "total=1" ] \
   && ! grep -q "JOIN 127.0.0.1:17508" "$TMP/s.log"; then
    note ok "mixed: secret node never lists the plain node"
else
    note fail "mixed: secret node never lists the plain node"
    echo "--- status:"; ctl 18507 STATUS
fi
if [ "$(members_total 18508)" = "total=1" ]; then
    note ok "mixed: plain node ends up alone after GC (total=1)"
else
    note fail "mixed: plain node ends up alone after GC (total=1)"
    echo "--- status:"; ctl 18508 STATUS
fi

echo "test_secret: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
