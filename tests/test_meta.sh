#!/usr/bin/env bash
# Integration test for member metadata/annotations: real nodes over
# localhost UDP, queried over TCP with pulse-ctl.
#  1. Node 1 starts with --meta flags: its MEMBERS line carries them,
#     JSON carries them as a "meta" object, and it logs a META line.
#  2. Node 2 joins with no annotations: each node's MEMBERS shows the
#     other's annotations (node 1's gossiped to node 2, "-" for node 2).
#  3. The META control command sets an annotation on node 2 at runtime:
#     node 1 sees it in MEMBERS within a few seconds (gossiped), and
#     bad META usage gets clean ERR lines. The 8-annotation cap is
#     enforced: the ninth distinct key is refused.
#  4. Node 2 stops cleanly and restarts with the same data dir and no
#     --meta flags: its annotations come back from the saved meta file
#     (META log line), and node 1 sees them again after the rejoin.
#     Node 2 also restores node 1's annotations from its members file.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-meta-test.XXXXXX)"
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

# members_contain <control-port> <pattern>: poll MEMBERS until a line
# matches the pattern (up to ~6 s). Gossip takes a round or two.
members_contain() {
    local port="$1" pat="$2" i
    for i in $(seq 1 30); do
        if ctl "$port" MEMBERS | grep -q "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17501-17502 (gossip) and 18501-18502 (control) so this test
# never clashes with the other suites.
(cd "$TMP/n1" && exec "$BIN" --port 17501 --control-port 18501 \
    --meta role=worker --meta zone=us-east-1 > "$TMP/n1.log" 2>&1 \
    & echo $! > "$TMP/n1.pid")

# 1. The node logs its annotations and MEMBERS carries them.
sleep 0.5
if wait_for "$TMP/n1.log" "META role=worker,zone=us-east-1" 3; then
    note ok "node logs its --meta annotations at startup"
else
    note fail "node logs its --meta annotations at startup"
fi
members="$(ctl 18501 MEMBERS)"
if echo "$members" | grep -q "MEMBER 127.0.0.1:17501 .* ALIVE .* role=worker,zone=us-east-1"; then
    note ok "MEMBERS carries the node's own annotations"
else
    note fail "MEMBERS carries the node's own annotations"
    echo "--- ctl output:"; echo "$members"
fi
json="$(ctl 18501 JSON)"
if echo "$json" | head -n 1 | python3 -c "import json,sys; json.load(sys.stdin)" \
   && echo "$json" | grep -q '"members":\[{"id":"127.0.0.1:17501","incarnation":1,"state":"ALIVE","phi":null,"meta":{"role":"worker","zone":"us-east-1"}}\]' \
   && echo "$json" | grep -q "^END$"; then
    note ok "JSON carries annotations as a meta object"
else
    note fail "JSON carries annotations as a meta object"
    echo "--- ctl output:"; echo "$json"
fi

# 2. Node 2 joins with no annotations: each side sees the other.
(cd "$TMP/n2" && exec "$BIN" --port 17502 --seed 127.0.0.1:17501 --control-port 18502 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
if wait_for "$TMP/n1.log" "STATUS alive=2" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 6; then
    note ok "2 nodes converge on alive=2"
else
    note fail "2 nodes converge on alive=2"
fi
if members_contain 18502 "MEMBER 127.0.0.1:17501 .* ALIVE .* role=worker,zone=us-east-1"; then
    note ok "joiner learns the seed's annotations via gossip"
else
    note fail "joiner learns the seed's annotations via gossip"
    echo "--- ctl output:"; ctl 18502 MEMBERS
fi
members="$(ctl 18501 MEMBERS)"
if echo "$members" | grep -q "MEMBER 127.0.0.1:17502 .* ALIVE .* -$"; then
    note ok "a member with no annotations shows '-' in MEMBERS"
else
    note fail "a member with no annotations shows '-' in MEMBERS"
    echo "--- ctl output:"; echo "$members"
fi
json="$(ctl 18501 JSON)"
if echo "$json" | grep -q '"id":"127.0.0.1:17502","incarnation":1,"state":"ALIVE","phi":null,"meta":{}}'; then
    note ok "JSON shows an empty meta object for a member with none"
else
    note fail "JSON shows an empty meta object for a member with none"
    echo "--- ctl output:"; echo "$json"
fi

# 3. META at runtime: node 2 gains an annotation, node 1 sees it.
if [ "$(ctl 18502 META version=1.2.3)" = "OK version=1.2.3" ]; then
    note ok "META sets an annotation at runtime"
else
    note fail "META sets an annotation at runtime"
fi
if members_contain 18501 "MEMBER 127.0.0.1:17502 .* ALIVE .* version=1.2.3"; then
    note ok "a runtime META change is gossiped to peers"
else
    note fail "a runtime META change is gossiped to peers"
    echo "--- ctl output:"; ctl 18501 MEMBERS
fi
if [ "$(ctl 18502 META badpair)" = "ERR invalid meta badpair" ]; then
    note ok "META with no '=' returns ERR invalid meta"
else
    note fail "META with no '=' returns ERR invalid meta"
fi
if [ "$(ctl 18502 META)" = "ERR usage: META <key=value>" ]; then
    note ok "META with no argument returns ERR usage"
else
    note fail "META with no argument returns ERR usage"
fi
# The cap is 8: node 2 has 1, so 7 more fit and the 9th is refused.
cap_ok=1
for k in k1 k2 k3 k4 k5 k6 k7; do
    [ "$(ctl 18502 META "$k=v")" = "OK $k=v" ] || cap_ok=0
done
if [ "$cap_ok" -eq 1 ]; then
    note ok "META fills annotations up to the 8 cap"
else
    note fail "META fills annotations up to the 8 cap"
fi
if [ "$(ctl 18502 META k8=v)" = "ERR meta full (max 8)" ]; then
    note ok "a ninth annotation is refused with ERR meta full"
else
    note fail "a ninth annotation is refused with ERR meta full"
fi

# 4. Clean restart of node 2 with the same data dir and no --meta
# flags: its annotations come back from the saved meta file, and node
# 1 sees them again after the rejoin. Node 2 also restores node 1's
# annotations from its members file.
kill "$(cat "$TMP/n2.pid")"
sleep 0.5
(cd "$TMP/n2" && exec "$BIN" --port 17502 --control-port 18502 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
if wait_for "$TMP/n2.log" "META version=1.2.3,k1=v" 6; then
    note ok "restarted node restores its annotations from the meta file"
else
    note fail "restarted node restores its annotations from the meta file"
    echo "--- n2 log:"; cat "$TMP/n2.log"
fi
if wait_for "$TMP/n2.log" "RESTORE 1 members" 6; then
    note ok "restarted node restores its peers from the members file"
else
    note fail "restarted node restores its peers from the members file"
fi
if members_contain 18502 "MEMBER 127.0.0.1:17501 .* ALIVE .* role=worker,zone=us-east-1"; then
    note ok "restored peer annotations survive the restart"
else
    note fail "restored peer annotations survive the restart"
    echo "--- ctl output:"; ctl 18502 MEMBERS
fi
if members_contain 18501 "MEMBER 127.0.0.1:17502 .* ALIVE .* version=1.2.3"; then
    note ok "peers see the restarted node's annotations again"
else
    note fail "peers see the restarted node's annotations again"
    echo "--- ctl output:"; ctl 18501 MEMBERS
fi

echo "test_meta: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
