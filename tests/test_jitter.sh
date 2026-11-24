#!/usr/bin/env bash
# Integration test for probe-interval jitter (0.50.0): real nodes over
# localhost UDP.
#  1. A 2-node cluster converges with jitter on (node 1 runs with
#     --probe-jitter-frac 0.2, node 2 with the 0.1 default): probes
#     still succeed and both members stay ALIVE.
#  2. With jitter disabled (--probe-jitter-frac 0) the same pair still
#     converges, so the flag itself is accepted and harmless.
#  3. The members stay ALIVE for a few seconds after convergence:
#     jitter must not break failure detection or make a healthy peer
#     look dead.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-jitter-test.XXXXXX)"
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

# alive2 <control-port>: the node reports alive=2 right now.
alive2() {
    ctl "$1" STATUS | grep -q "alive=2"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17601-17602 (gossip) and 18601-18602 (control) so this test
# never clashes with the other suites.
(cd "$TMP/n1" && exec "$BIN" --port 17601 --control-port 18601 --probe-jitter-frac 0.2 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17602 --seed 127.0.0.1:17601 --control-port 18602 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. The jittered pair converges.
if wait_for "$TMP/n1.log" "STATUS alive=2" 8 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 8; then
    note ok "2-node cluster converges with jitter on (alive=2)"
else
    note fail "2-node cluster converges with jitter on (alive=2)"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# Probes still succeed: MEMBERS lists the peer ALIVE on both nodes.
if ctl 18601 MEMBERS | grep -q "127.0.0.1:17602 1 ALIVE" \
   && ctl 18602 MEMBERS | grep -q "127.0.0.1:17601 1 ALIVE"; then
    note ok "both members list the peer ALIVE with jitter on"
else
    note fail "both members list the peer ALIVE with jitter on"
    echo "--- n1 MEMBERS:"; ctl 18601 MEMBERS
    echo "--- n2 MEMBERS:"; ctl 18602 MEMBERS
fi

# 3. They stay ALIVE: jitter must not make a healthy peer look dead.
sleep 3
if alive2 18601 && alive2 18602; then
    note ok "members stay ALIVE after 3 s with jitter on"
else
    note fail "members stay ALIVE after 3 s with jitter on"
    echo "--- n1 STATUS:"; ctl 18601 STATUS
    echo "--- n2 STATUS:"; ctl 18602 STATUS
fi

# Stop the jittered pair before the no-jitter pair, so the ports are
# free to reuse.
kill "$(cat "$TMP/n1.pid")" "$(cat "$TMP/n2.pid")" 2>/dev/null
sleep 0.5

# 2. Jitter disabled: the same pair still converges.
mkdir -p "$TMP/m1" "$TMP/m2"
(cd "$TMP/m1" && exec "$BIN" --port 17601 --control-port 18601 --probe-jitter-frac 0 > "$TMP/m1.log" 2>&1 & echo $! > "$TMP/m1.pid")
(cd "$TMP/m2" && exec "$BIN" --port 17602 --seed 127.0.0.1:17601 --control-port 18602 --probe-jitter-frac 0 > "$TMP/m2.log" 2>&1 & echo $! > "$TMP/m2.pid")
if wait_for "$TMP/m1.log" "STATUS alive=2" 8 \
   && wait_for "$TMP/m2.log" "STATUS alive=2" 8; then
    note ok "2-node cluster converges with jitter off (--probe-jitter-frac 0)"
else
    note fail "2-node cluster converges with jitter off (--probe-jitter-frac 0)"
    echo "--- m1.log"; tail -20 "$TMP/m1.log"
    echo "--- m2.log"; tail -20 "$TMP/m2.log"
fi

echo "jitter: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
