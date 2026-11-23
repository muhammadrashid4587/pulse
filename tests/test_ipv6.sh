#!/usr/bin/env bash
# Integration test for IPv6: real nodes over ::1 UDP.
#  1. A 3-node --ipv6 cluster converges on alive=3.
#  2. Kill one node; the survivors mark it DEAD.
#  3. The survivors agree (alive=2, dead=1).
#  4. Restart the killed node; it comes back ALIVE.
#  5. The TCP control plane answers over IPv6 (pulse-ctl '[::1]:port').
#  6. --bind ::1 (v6) and --bind 127.0.0.1 (v4) nodes join normally.
#  7. Mixed families do not merge: a v4 node seeded with a v6 id (and
#     a v6 node seeded with a v4 id) never hears the other side; the
#     unanswering seed is suspected, dead, and GC'd like any dead seed.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-ipv6-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    # Kill the nodes by pid file (the pid file holds the node pid because
    # the start commands below use exec). jobs -p is not enough: the nodes
    # are children of subshells, not of this script.
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

# wait_for <logfile> <fixed-string> <timeout-seconds>
# Fixed strings, not regex: ids contain [::1] and grep -q would read
# the brackets as a character class.
wait_for() {
    local log="$1" pat="$2" timeout="$3" i
    for i in $(seq 1 $((timeout * 5))); do
        if grep -Fq "$pat" "$log" 2>/dev/null; then return 0; fi
        sleep 0.2
    done
    return 1
}

# ctl <control-address> <command...>: run pulse-ctl against a node.
ctl() {
    local addr="$1"; shift
    "$CTL" "$addr" "$@"
}

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# exec: the subshell replaces itself with the node, so $! is the node pid.
# Ports 17501-17503 (gossip) and 18501 (control) so this test never
# clashes with the other suites.
(cd "$TMP/n1" && exec "$BIN" --ipv6 --port 17501 --control-port 18501 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --ipv6 --port 17502 --seed '[::1]:17501' > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --ipv6 --port 17503 --seed '[::1]:17501' > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. Convergence: every node reports alive=3, with bracketed v6 ids.
if wait_for "$TMP/n1.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 6 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 6; then
    note ok "3 IPv6 nodes converge on alive=3"
else
    note fail "3 IPv6 nodes converge on alive=3"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
    echo "--- n3.log"; tail -20 "$TMP/n3.log"
fi

# The START line reports the family, and ids are bracketed.
if grep -Fq "START [::1]:17501" "$TMP/n1.log" \
   && grep -Fq "ipv6=on" "$TMP/n1.log"; then
    note ok "START line shows the bracketed id and ipv6=on"
else
    note fail "START line shows the bracketed id and ipv6=on"
    head -3 "$TMP/n1.log"
fi

# 2. Kill node 3 with SIGKILL — an unclean crash, so the survivors
# must detect it by probing, not by a leave announcement (a clean
# SIGTERM announces its leave; see test_leave.sh).
kill -9 "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD [::1]:17503" 6 \
   && wait_for "$TMP/n2.log" "DEAD [::1]:17503" 6; then
    note ok "killed IPv6 node is marked DEAD by both survivors"
else
    note fail "killed IPv6 node is marked DEAD by both survivors"
fi

# 3. Survivors agree: alive=2, dead=1.
if wait_for "$TMP/n1.log" "STATUS alive=2 suspect=0 dead=1" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=2 suspect=0 dead=1" 6; then
    note ok "survivors agree on membership (alive=2, dead=1)"
else
    note fail "survivors agree on membership (alive=2, dead=1)"
fi

# 4. Flap: restart node 3 in the same data dir (incarnation bumps).
(cd "$TMP/n3" && exec "$BIN" --ipv6 --port 17503 --seed '[::1]:17501' >> "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "ALIVE [::1]:17503" 6 \
   && wait_for "$TMP/n2.log" "ALIVE [::1]:17503" 6; then
    note ok "restarted IPv6 node is cleared back to ALIVE"
else
    note fail "restarted IPv6 node is cleared back to ALIVE"
fi

# 5. Control plane over IPv6: pulse-ctl '[::1]:18501' MEMBERS/STATUS.
members="$(ctl '[::1]:18501' MEMBERS)"
if echo "$members" | grep -Fq "MEMBER [::1]:17501" \
   && echo "$members" | grep -Fq "MEMBER [::1]:17502" \
   && echo "$members" | grep -Fq "MEMBER [::1]:17503" \
   && echo "$members" | grep -Fxq "END"; then
    note ok "control plane over IPv6 lists all three members"
else
    note fail "control plane over IPv6 lists all three members"
    echo "--- ctl output:"; echo "$members"
fi
status="$(ctl '[::1]:18501' STATUS)"
if echo "$status" | grep -Fq "STATUS id=[::1]:17501" \
   && echo "$status" | grep -Fq "total=3"; then
    note ok "STATUS over IPv6 reports the bracketed id and total=3"
else
    note fail "STATUS over IPv6 reports the bracketed id and total=3"
    echo "--- ctl output:"; echo "$status"
fi

# 6. --bind: a v6 node bound to ::1 and a v4 node bound to 127.0.0.1
# both join a same-family peer normally.
mkdir -p "$TMP/b1" "$TMP/b2" "$TMP/b3" "$TMP/b4"
(cd "$TMP/b1" && exec "$BIN" --ipv6 --bind ::1 --port 17521 > "$TMP/b1.log" 2>&1 & echo $! > "$TMP/b1.pid")
(cd "$TMP/b2" && exec "$BIN" --ipv6 --port 17522 --seed '[::1]:17521' > "$TMP/b2.log" 2>&1 & echo $! > "$TMP/b2.pid")
(cd "$TMP/b3" && exec "$BIN" --bind 127.0.0.1 --port 17523 > "$TMP/b3.log" 2>&1 & echo $! > "$TMP/b3.pid")
(cd "$TMP/b4" && exec "$BIN" --port 17524 --seed 127.0.0.1:17523 > "$TMP/b4.log" 2>&1 & echo $! > "$TMP/b4.pid")
if wait_for "$TMP/b1.log" "STATUS alive=2" 6 \
   && wait_for "$TMP/b2.log" "STATUS alive=2" 6; then
    note ok "--bind ::1 node forms a v6 cluster"
else
    note fail "--bind ::1 node forms a v6 cluster"
    echo "--- b1.log"; tail -10 "$TMP/b1.log"
    echo "--- b2.log"; tail -10 "$TMP/b2.log"
fi
if wait_for "$TMP/b3.log" "STATUS alive=2" 6 \
   && wait_for "$TMP/b4.log" "STATUS alive=2" 6; then
    note ok "--bind 127.0.0.1 node forms a v4 cluster"
else
    note fail "--bind 127.0.0.1 node forms a v4 cluster"
    echo "--- b3.log"; tail -10 "$TMP/b3.log"
    echo "--- b4.log"; tail -10 "$TMP/b4.log"
fi

# 7. Mixed families do not merge. A v4 node seeded with a v6 id and a
# v6 node seeded with a v4 id: neither can send to the other family,
# so the seed is suspected, dead, and GC'd, and neither side ever
# learns the other's id.
mkdir -p "$TMP/m1" "$TMP/m2"
(cd "$TMP/m1" && exec "$BIN" --port 17511 --seed '[::1]:17501' --dead-retention 2 > "$TMP/m1.log" 2>&1 & echo $! > "$TMP/m1.pid")
(cd "$TMP/m2" && exec "$BIN" --ipv6 --port 17512 --seed 127.0.0.1:17511 --dead-retention 2 > "$TMP/m2.log" 2>&1 & echo $! > "$TMP/m2.pid")
if wait_for "$TMP/m1.log" "GC [::1]:17501" 8 \
   && wait_for "$TMP/m2.log" "GC 127.0.0.1:17511" 8; then
    note ok "mixed-family seeds are GC'd as unanswering, on both sides"
else
    note fail "mixed-family seeds are GC'd as unanswering, on both sides"
    echo "--- m1.log"; tail -15 "$TMP/m1.log"
    echo "--- m2.log"; tail -15 "$TMP/m2.log"
fi
sleep 1
if ! grep -Fq "127.0.0.1:17511" "$TMP/n1.log" \
   && ! grep -Fq "[::1]:17512" "$TMP/m1.log" \
   && ! grep -Fq "127.0.0.1:17511" "$TMP/n2.log"; then
    note ok "neither side ever learns the other's id (no merge)"
else
    note fail "neither side ever learns the other's id (no merge)"
    echo "--- n1.log (v6 cluster) mentions v4 id:"; grep -F "127.0.0.1:17511" "$TMP/n1.log" | head -3
    echo "--- m1.log (v4 node) mentions v6 id:"; grep -F "[::1]:17512" "$TMP/m1.log" | head -3
fi
if wait_for "$TMP/m1.log" "STATUS alive=1" 6 \
   && wait_for "$TMP/m2.log" "STATUS alive=1" 6; then
    note ok "both mixed nodes end up alone (alive=1) after GC"
else
    note fail "both mixed nodes end up alone (alive=1) after GC"
fi

echo "test_ipv6: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
