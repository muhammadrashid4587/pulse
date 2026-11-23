#!/usr/bin/env bash
# Integration test for the RELOAD control-plane command (0.25.0):
# the same live peer-file reload SIGHUP triggers, driven over TCP.
#  1. Three nodes start from one shared --peer-file and converge on
#     alive=3.
#  2. Node 3 is removed from the file while it is still ALIVE, and
#     node 1 gets RELOAD: it replies OK peers=2 added=0 dropped=0
#     kept=1, logs PEERFILE KEEP, and node 3 stays a member — a file
#     edit alone never drops a live peer.
#  3. Node 3 is then killed; once node 1 suspects it, RELOAD drops
#     it at once (OK peers=2 added=0 dropped=1 kept=0, alive=2
#     within seconds instead of waiting out the 30 s dead retention).
#  4. Node 4 is added to the file and started; RELOAD on node 1 logs
#     PEERFILE ADD and the cluster is back to alive=3.
#  5. RELOAD with an unchanged file is a clean no-op (added=0
#     dropped=0 kept=0), and a bad line added to the file is logged
#     and skipped (bad=1) without disturbing the cluster.
#  6. RELOAD on a node with no --peer-file is refused cleanly
#     (ERR no peer file) and the node keeps running.
#  7. RELOAD when the file has been deleted is refused cleanly
#     (ERR cannot read peer file ...) and the old list is kept — the
#     node still reports its members.
# RELOAD must never restart a node: START appears exactly once in
# every log.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-reload-test.XXXXXX)"
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

# members_contain <control-port> <id>: 1 when the node's JSON lists id.
members_contain() {
    ctl "$1" JSON | head -n 1 | grep -q "\"id\":\"$2\""
}

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3" "$TMP/n4" "$TMP/n5" "$TMP/n6"

# Ports 17801-17806 (gossip) and 18801-18806 (control) so this test
# never clashes with the other suites. Dead retention is 30 s, so a
# DEAD entry would linger for half a minute without the reload drop.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17801
127.0.0.1:17802
127.0.0.1:17803
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17801 --peer-file "$TMP/peers" --control-port 18801 --dead-retention 30 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17802 --peer-file "$TMP/peers" --control-port 18802 --dead-retention 30 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17803 --peer-file "$TMP/peers" --control-port 18803 --dead-retention 30 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. All three converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 10; then
    note ok "3 nodes converge from the peer file"
else
    note fail "3 nodes converge from the peer file"
    for f in n1 n2 n3; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# 2. Remove node 3 from the file while it is ALIVE; RELOAD node 1.
# It must KEEP node 3: still a member, still ALIVE.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17801
127.0.0.1:17802
EOF
reply="$(ctl 18801 RELOAD)"
if [ "$reply" = "OK peers=2 added=0 dropped=0 kept=1 bad=0" ] \
   && wait_for "$TMP/n1.log" "PEERFILE KEEP 127.0.0.1:17803 (still ALIVE)" 5; then
    note ok "RELOAD keeps a live peer removed from the file (KEEP)"
else
    note fail "RELOAD keeps a live peer removed from the file (KEEP)"
    echo "reply: $reply"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
fi
sleep 1.5
if members_contain 18801 "127.0.0.1:17803" \
   && grep -q "STATUS alive=3" "$TMP/n1.log"; then
    note ok "kept peer is still ALIVE in the membership after RELOAD"
else
    note fail "kept peer is still ALIVE in the membership after RELOAD"
    ctl 18801 JSON | head -n 1
fi

# 3. Kill node 3; once node 1 suspects it, RELOAD drops it at once.
kill -9 "$(cat "$TMP/n3.pid")"
if ! wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17803" 10; then
    note fail "node 1 suspects the killed node 3"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
else
    reply="$(ctl 18801 RELOAD)"
    if [ "$reply" = "OK peers=2 added=0 dropped=1 kept=0 bad=0" ] \
       && wait_for "$TMP/n1.log" "PEERFILE DROP 127.0.0.1:17803" 5 \
       && wait_for "$TMP/n1.log" "STATUS alive=2" 10; then
        note ok "RELOAD drops an unreachable peer at once (DROP, alive=2)"
    else
        note fail "RELOAD drops an unreachable peer at once (DROP, alive=2)"
        echo "reply: $reply"
        echo "--- n1.log"; tail -10 "$TMP/n1.log"
    fi
    # The JSON must actually answer (with self in it) for the absence
    # below to mean anything: an empty reply from a dead node would
    # pass vacuously.
    json1="$(ctl 18801 JSON | head -n 1)"
    if [ -n "$json1" ] \
       && printf '%s' "$json1" | grep -q '"id":"127.0.0.1:17801"' \
       && ! printf '%s' "$json1" | grep -q '"id":"127.0.0.1:17803"'; then
        note ok "dropped peer is gone from MEMBERS/JSON"
    else
        note fail "dropped peer is gone from MEMBERS/JSON"
        printf '%s\n' "$json1"
    fi
fi

# 4. Add node 4 to the file, start it, RELOAD node 1: ADD + alive=3.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17801
127.0.0.1:17802
127.0.0.1:17804
EOF
(cd "$TMP/n4" && exec "$BIN" --port 17804 --peer-file "$TMP/peers" --control-port 18804 --dead-retention 30 > "$TMP/n4.log" 2>&1 & echo $! > "$TMP/n4.pid")
reply="$(ctl 18801 RELOAD)"
if [ "$reply" = "OK peers=3 added=1 dropped=0 kept=0 bad=0" ] \
   && wait_for "$TMP/n1.log" "PEERFILE ADD 127.0.0.1:17804" 5 \
   && wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n4.log" "STATUS alive=3" 10; then
    note ok "RELOAD adds a newly listed peer (ADD, alive=3)"
else
    note fail "RELOAD adds a newly listed peer (ADD, alive=3)"
    echo "reply: $reply"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi

# 5. RELOAD with an unchanged file is a clean no-op; then a bad line
# in the file is logged and skipped, cluster undisturbed.
reply="$(ctl 18801 RELOAD)"
if [ "$reply" = "OK peers=3 added=0 dropped=0 kept=0 bad=0" ]; then
    note ok "RELOAD with an unchanged file is a clean no-op"
else
    note fail "RELOAD with an unchanged file is a clean no-op"
    echo "reply: $reply"
fi
printf 'garbage-line\n' >> "$TMP/peers"
reply="$(ctl 18801 RELOAD)"
if [ "$reply" = "OK peers=3 added=0 dropped=0 kept=0 bad=1" ] \
   && wait_for "$TMP/n1.log" "skipping bad peer 'garbage-line'" 5 \
   && wait_for "$TMP/n1.log" "STATUS alive=3" 5; then
    note ok "bad line on RELOAD is logged and skipped, cluster undisturbed"
else
    note fail "bad line on RELOAD is logged and skipped, cluster undisturbed"
    echo "reply: $reply"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
fi

# 6. RELOAD on a node with no --peer-file: clean refusal, node alive.
(cd "$TMP/n5" && exec "$BIN" --port 17805 --control-port 18805 --dead-retention 30 > "$TMP/n5.log" 2>&1 & echo $! > "$TMP/n5.pid")
if ! wait_for "$TMP/n5.log" "STATUS alive=1" 5; then
    note fail "solo node without a peer file starts (setup)"
else
    reply="$(ctl 18805 RELOAD)"
    sleep 1
    if [ "$reply" = "ERR no peer file" ] \
       && grep -q "STATUS alive=1" "$TMP/n5.log" \
       && [ "$(grep -c "START " "$TMP/n5.log")" -eq 1 ] \
       && kill -0 "$(cat "$TMP/n5.pid")" 2>/dev/null; then
        note ok "RELOAD without --peer-file is refused cleanly (node still up)"
    else
        note fail "RELOAD without --peer-file is refused cleanly (node still up)"
        echo "reply: $reply"
        echo "--- n5.log"; cat "$TMP/n5.log"
    fi
fi

# 7. RELOAD when the peer file has been deleted: clean refusal, and
# the old list is kept — the node still reports its members.
cat > "$TMP/peers6" <<'EOF'
127.0.0.1:17801
127.0.0.1:17806
EOF
(cd "$TMP/n6" && exec "$BIN" --port 17806 --peer-file "$TMP/peers6" --control-port 18806 --dead-retention 30 > "$TMP/n6.log" 2>&1 & echo $! > "$TMP/n6.pid")
# Node 6 joins through node 1 and learns the whole cluster from it
# (alive=4 here: nodes 1, 2, 4 and itself — node 3 is a DEAD
# tombstone), so the setup check is that node 1 is in its table,
# not an exact alive count.
joined=0
for i in $(seq 1 50); do
    if members_contain 18806 "127.0.0.1:17801"; then joined=1; break; fi
    sleep 0.2
done
if [ "$joined" -eq 0 ]; then
    note fail "node 6 joins node 1 from its own peer file (setup)"
    echo "--- n6.log"; tail -5 "$TMP/n6.log"
else
    rm "$TMP/peers6"
    reply="$(ctl 18806 RELOAD)"
    sleep 1
    json6="$(ctl 18806 JSON | head -n 1)"
    if printf '%s' "$reply" | grep -q "^ERR cannot read peer file $TMP/peers6: " \
       && wait_for "$TMP/n6.log" "PEERFILE reload $TMP/peers6: cannot read:" 5 \
       && [ -n "$json6" ] \
       && printf '%s' "$json6" | grep -q '"id":"127.0.0.1:17801"' \
       && printf '%s' "$json6" | grep -q '"id":"127.0.0.1:17806"'; then
        note ok "RELOAD with a deleted file is refused cleanly, old list kept"
    else
        note fail "RELOAD with a deleted file is refused cleanly, old list kept"
        echo "reply: $reply"
        printf '%s\n' "$json6"
        echo "--- n6.log"; tail -5 "$TMP/n6.log"
    fi
fi

# RELOAD never restarted any node: exactly one START line each.
starts_ok=1
for f in n1 n2 n4 n5 n6; do
    [ "$(grep -c "START " "$TMP/$f.log")" -eq 1 ] || starts_ok=0
done
if [ "$starts_ok" -eq 1 ]; then
    note ok "no node restarted on RELOAD (one START line each)"
else
    note fail "no node restarted on RELOAD (one START line each)"
    for f in n1 n2 n4 n5 n6; do echo "--- $f.log START count: $(grep -c "START " "$TMP/$f.log")"; done
fi

echo "test_reload: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
