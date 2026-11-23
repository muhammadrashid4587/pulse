#!/usr/bin/env bash
# Integration test for live peer-file reload on SIGHUP (0.24.0).
#  1. Three nodes start from one shared --peer-file and converge on
#     alive=3.
#  2. Node 3 is removed from the file while it is still ALIVE, and
#     node 1 gets SIGHUP: it logs PEERFILE KEEP and node 3 stays a
#     member — a file edit alone never drops a live peer.
#  3. Node 3 is then killed; once node 1 suspects it, a second SIGHUP
#     drops it at once (PEERFILE DROP) instead of waiting out the
#     30 s dead retention — node 1 reports alive=2 within seconds.
#  4. Node 4 is added to the file and started; a SIGHUP on node 1
#     logs PEERFILE ADD and the cluster is back to alive=3.
#  5. SIGHUP with an unchanged file reloads cleanly (added=0
#     dropped=0 kept=0), and a bad line added to the file is logged
#     and skipped without disturbing the cluster.
#  6. SIGHUP on a node with no --peer-file is a no-op: the node
#     keeps running, logs no PEERFILE line, and never restarts.
# SIGHUP must never restart a node: START appears exactly once in
# every log.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-peerfile-reload-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3" "$TMP/n4" "$TMP/n5"

# Ports 17701-17705 (gossip) and 18701-18705 (control) so this test
# never clashes with the other suites. Dead retention is 30 s, so a
# DEAD entry would linger for half a minute without the reload drop.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17701
127.0.0.1:17702
127.0.0.1:17703
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17701 --peer-file "$TMP/peers" --control-port 18701 --dead-retention 30 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17702 --peer-file "$TMP/peers" --control-port 18702 --dead-retention 30 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17703 --peer-file "$TMP/peers" --control-port 18703 --dead-retention 30 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

# 1. All three converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n3.log" "STATUS alive=3" 10; then
    note ok "3 nodes converge from the peer file"
else
    note fail "3 nodes converge from the peer file"
    for f in n1 n2 n3; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# 2. Remove node 3 from the file while it is ALIVE; SIGHUP node 1.
# It must KEEP node 3: still a member, still ALIVE.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17701
127.0.0.1:17702
EOF
kill -HUP "$(cat "$TMP/n1.pid")"
if wait_for "$TMP/n1.log" "PEERFILE KEEP 127.0.0.1:17703 (still ALIVE)" 5 \
   && wait_for "$TMP/n1.log" "PEERFILE RELOAD $TMP/peers peers=2 added=0 dropped=0 kept=1" 5; then
    note ok "SIGHUP keeps a live peer removed from the file (KEEP)"
else
    note fail "SIGHUP keeps a live peer removed from the file (KEEP)"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
fi
sleep 1.5
if members_contain 18701 "127.0.0.1:17703" \
   && grep -q "STATUS alive=3" "$TMP/n1.log"; then
    note ok "kept peer is still ALIVE in the membership after reload"
else
    note fail "kept peer is still ALIVE in the membership after reload"
    ctl 18701 JSON | head -n 1
fi

# 3. Kill node 3; once node 1 suspects it, SIGHUP drops it at once.
kill -9 "$(cat "$TMP/n3.pid")"
if ! wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17703" 10; then
    note fail "node 1 suspects the killed node 3"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
else
    kill -HUP "$(cat "$TMP/n1.pid")"
    if wait_for "$TMP/n1.log" "PEERFILE DROP 127.0.0.1:17703" 5 \
       && wait_for "$TMP/n1.log" "STATUS alive=2" 10; then
        note ok "SIGHUP drops an unreachable peer at once (DROP, alive=2)"
    else
        note fail "SIGHUP drops an unreachable peer at once (DROP, alive=2)"
        echo "--- n1.log"; tail -10 "$TMP/n1.log"
    fi
    # The JSON must actually answer (with self in it) for the absence
    # below to mean anything: an empty reply from a dead node would
    # pass vacuously.
    json1="$(ctl 18701 JSON | head -n 1)"
    if [ -n "$json1" ] \
       && printf '%s' "$json1" | grep -q '"id":"127.0.0.1:17701"' \
       && ! printf '%s' "$json1" | grep -q '"id":"127.0.0.1:17703"'; then
        note ok "dropped peer is gone from MEMBERS/JSON"
    else
        note fail "dropped peer is gone from MEMBERS/JSON"
        printf '%s\n' "$json1"
    fi
fi

# 4. Add node 4 to the file, start it, SIGHUP node 1: ADD + alive=3.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17701
127.0.0.1:17702
127.0.0.1:17704
EOF
(cd "$TMP/n4" && exec "$BIN" --port 17704 --peer-file "$TMP/peers" --control-port 18704 --dead-retention 30 > "$TMP/n4.log" 2>&1 & echo $! > "$TMP/n4.pid")
kill -HUP "$(cat "$TMP/n1.pid")"
if wait_for "$TMP/n1.log" "PEERFILE ADD 127.0.0.1:17704" 5 \
   && wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n4.log" "STATUS alive=3" 10; then
    note ok "SIGHUP adds a newly listed peer (ADD, alive=3)"
else
    note fail "SIGHUP adds a newly listed peer (ADD, alive=3)"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi

# 5. SIGHUP with an unchanged file is a clean no-op reload; then a
# bad line in the file is logged and skipped, cluster undisturbed.
# (Node 1 reloaded in step 4, so its list already matches the file;
# node 2 never reloaded, so its list still names the dead node 3.)
kill -HUP "$(cat "$TMP/n1.pid")"
if wait_for "$TMP/n1.log" "PEERFILE RELOAD $TMP/peers peers=3 added=0 dropped=0 kept=0" 5; then
    note ok "SIGHUP with an unchanged file reloads cleanly"
else
    note fail "SIGHUP with an unchanged file reloads cleanly"
    echo "--- n1.log"; tail -5 "$TMP/n1.log"
fi
printf 'garbage-line\n' >> "$TMP/peers"
kill -HUP "$(cat "$TMP/n1.pid")"
if wait_for "$TMP/n1.log" "skipping bad peer 'garbage-line'" 5 \
   && wait_for "$TMP/n1.log" "PEERFILE RELOAD $TMP/peers peers=3 added=0 dropped=0 kept=0 (1 bad lines skipped)" 5 \
   && wait_for "$TMP/n1.log" "STATUS alive=3" 5; then
    note ok "bad line on reload is logged and skipped, cluster undisturbed"
else
    note fail "bad line on reload is logged and skipped, cluster undisturbed"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
fi

# 6. SIGHUP on a node with no --peer-file: no-op, no restart, no
# PEERFILE line.
(cd "$TMP/n5" && exec "$BIN" --port 17705 --control-port 18705 --dead-retention 30 > "$TMP/n5.log" 2>&1 & echo $! > "$TMP/n5.pid")
if ! wait_for "$TMP/n5.log" "STATUS alive=1" 5; then
    note fail "solo node without a peer file starts (setup)"
else
    kill -HUP "$(cat "$TMP/n5.pid")"
    sleep 1.5
    if grep -q "STATUS alive=1" "$TMP/n5.log" \
       && ! grep -q "PEERFILE" "$TMP/n5.log" \
       && [ "$(grep -c "START " "$TMP/n5.log")" -eq 1 ] \
       && kill -0 "$(cat "$TMP/n5.pid")" 2>/dev/null; then
        note ok "SIGHUP without --peer-file is a no-op (node still up)"
    else
        note fail "SIGHUP without --peer-file is a no-op (node still up)"
        echo "--- n5.log"; cat "$TMP/n5.log"
    fi
fi

# SIGHUP never restarted any node: exactly one START line each.
starts_ok=1
for f in n1 n2 n4 n5; do
    [ "$(grep -c "START " "$TMP/$f.log")" -eq 1 ] || starts_ok=0
done
if [ "$starts_ok" -eq 1 ]; then
    note ok "no node restarted on SIGHUP (one START line each)"
else
    note fail "no node restarted on SIGHUP (one START line each)"
    for f in n1 n2 n4 n5; do echo "--- $f.log START count: $(grep -c "START " "$TMP/$f.log")"; done
fi

echo "test_peerfile_reload: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
