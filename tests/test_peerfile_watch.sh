#!/usr/bin/env bash
# Integration test for auto-reload on peer-file mtime change (0.26.0).
# No SIGHUP and no RELOAD command is ever sent in this test: the node
# itself must notice the file changed.
#  1. Three nodes start from one shared --peer-file and converge on
#     alive=3.
#  2. Node 3 is removed from the file while it is still ALIVE; within
#     a few seconds node 1 logs PEERFILE KEEP on its own — the watch
#     saw the mtime move and re-ran the merge — and node 3 stays a
#     member.
#  3. Node 3 is then killed; once node 1 suspects it, a comment-only
#     edit to the file (no peer change, but a new mtime) makes the
#     watch re-run the merge and drop the now-unreachable kept peer
#     at once (PEERFILE DROP) instead of waiting out the 30 s dead
#     retention — node 1 reports alive=2 within seconds.
#  4. Node 4 is added to the file and started; the watch logs
#     PEERFILE ADD and the cluster is back to alive=3, still with no
#     signal sent.
#  5. With the file untouched, no further reload happens: the PEERFILE
#     RELOAD count for node 1 is stable across a 3 s idle window, so
#     the watch does not spam reloads on an unchanged file.
# A node must never restart: START appears exactly once in every log.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-peerfile-watch-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3" "$TMP/n4"

# Ports 17801-17804 (gossip) and 18801-18804 (control) so this test
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

# 2. Remove node 3 from the file while it is ALIVE; the watch must
# KEEP it on its own — no SIGHUP, no RELOAD, just the file edit.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17801
127.0.0.1:17802
EOF
if wait_for "$TMP/n1.log" "PEERFILE $TMP/peers changed on disk, auto-reloading" 5 \
   && wait_for "$TMP/n1.log" "PEERFILE KEEP 127.0.0.1:17803 (still ALIVE)" 5 \
   && wait_for "$TMP/n1.log" "PEERFILE RELOAD $TMP/peers peers=2 added=0 dropped=0 kept=1" 5; then
    note ok "watch auto-reloads on an edit and keeps a live peer (KEEP)"
else
    note fail "watch auto-reloads on an edit and keeps a live peer (KEEP)"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
fi
sleep 1.5
if members_contain 18801 "127.0.0.1:17803" \
   && grep -q "STATUS alive=3" "$TMP/n1.log"; then
    note ok "kept peer is still ALIVE in the membership after auto-reload"
else
    note fail "kept peer is still ALIVE in the membership after auto-reload"
    ctl 18801 JSON | head -n 1
fi

# 3. Kill node 3; once node 1 suspects it, a comment-only edit (no
# peer change, new mtime) must make the watch drop the kept peer.
kill -9 "$(cat "$TMP/n3.pid")"
if ! wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17803" 10; then
    note fail "node 1 suspects the killed node 3"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
else
    printf '# roster note, no peer change\n' >> "$TMP/peers"
    if wait_for "$TMP/n1.log" "PEERFILE DROP 127.0.0.1:17803" 5 \
       && wait_for "$TMP/n1.log" "STATUS alive=2" 10; then
        note ok "watch drops an unreachable kept peer on the next edit (DROP, alive=2)"
    else
        note fail "watch drops an unreachable kept peer on the next edit (DROP, alive=2)"
        echo "--- n1.log"; tail -10 "$TMP/n1.log"
    fi
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

# 4. Add node 4 to the file and start it; the watch adds it with no
# signal sent.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17801
127.0.0.1:17802
127.0.0.1:17804
EOF
(cd "$TMP/n4" && exec "$BIN" --port 17804 --peer-file "$TMP/peers" --control-port 18804 --dead-retention 30 > "$TMP/n4.log" 2>&1 & echo $! > "$TMP/n4.pid")
if wait_for "$TMP/n1.log" "PEERFILE ADD 127.0.0.1:17804" 5 \
   && wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/n4.log" "STATUS alive=3" 10; then
    note ok "watch adds a newly listed peer (ADD, alive=3)"
else
    note fail "watch adds a newly listed peer (ADD, alive=3)"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
    echo "--- n4.log"; tail -10 "$TMP/n4.log"
fi

# 5. With the file untouched, the watch must not reload: the
# PEERFILE RELOAD count for node 1 is stable across a 3 s window.
before="$(grep -c "PEERFILE RELOAD" "$TMP/n1.log")"
sleep 3
after="$(grep -c "PEERFILE RELOAD" "$TMP/n1.log")"
if [ "$before" -eq "$after" ] && [ "$before" -ge 3 ]; then
    note ok "no spurious reload on an unchanged file ($before reloads, stable)"
else
    note fail "no spurious reload on an unchanged file (before=$before after=$after)"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
fi

# No node restarted: exactly one START line each.
starts_ok=1
for f in n1 n2 n4; do
    [ "$(grep -c "START " "$TMP/$f.log")" -eq 1 ] || starts_ok=0
done
if [ "$starts_ok" -eq 1 ]; then
    note ok "no node restarted (one START line each)"
else
    note fail "no node restarted (one START line each)"
    for f in n1 n2 n4; do echo "--- $f.log START count: $(grep -c "START " "$TMP/$f.log")"; done
fi

echo "test_peerfile_watch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
