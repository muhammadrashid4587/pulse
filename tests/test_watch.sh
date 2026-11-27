#!/usr/bin/env bash
# Integration test for the WATCH control-plane command (0.29.0):
# a long-lived stream that starts with a snapshot of the table and
# then pushes one EVENT line per membership event as it happens.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. A WATCH on node 1 opens with "OK watching", a
#     SNAPSHOT line per member in the PEERS shape, and END — and
#     MEMBERS still answers on another connection while it is open.
#  2. Node 3 joins: the stream carries JOINED for it.
#  3. Node 3 is stopped (SIGSTOP, not killed): node 1 suspects it
#     and the stream carries SUSPECTED; resumed (SIGCONT) it comes
#     back and the stream carries REVIVED — with a trailing
#     suspect_ms (0.70.0), how long it sat SUSPECT before coming
#     back. Node 1 runs with a 2 s
#     suspect timeout so the stop/resume window is comfortable.
#  4. Node 3 is killed: the stream carries DEAD for it.
#  5. LEAVE over the control plane drops it at once: the stream
#     carries DROPPED for it.
#  6. The EVENT lines arrive in lifecycle order with strictly
#     increasing sequence numbers.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch.pid" ] && kill "$(cat "$TMP/watch.pid")" 2>/dev/null
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

# first_line <file> <pattern>: the line number of the first line in
# the file matching the pattern, or 0 when there is none.
first_line() {
    grep -En "$2" "$1" 2>/dev/null | head -n 1 | cut -d: -f1
}

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17821-17823 (gossip) and 18821-18823 (control) so this test
# never clashes with the other suites. The peer file lists only
# nodes 1 and 2: node 3 joins later through --seed, so it is
# genuinely new to node 1's table (see step 2).
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17821
127.0.0.1:17822
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17821 --peer-file "$TMP/peers" --control-port 18821 --suspect-timeout-ms 2000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17822 --peer-file "$TMP/peers" --control-port 18822 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 60 s bounds it if the test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18821" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

# 1a. The stream opens with OK, a SNAPSHOT per member, and END.
if wait_for "$TMP/watch1.out" "^END$" 10; then
    w1="$(cat "$TMP/watch1.out")"
    if [ "$(printf '%s\n' "$w1" | head -n 1)" = "OK watching" ] \
       && printf '%s\n' "$w1" | grep -Eq '^SNAPSHOT 127\.0\.0\.1:17821 1 ALIVE - self$' \
       && printf '%s\n' "$w1" | grep -Eq '^SNAPSHOT 127\.0\.0\.1:17822 1 ALIVE [0-9]+ peer-file$'; then
        note ok "WATCH opens with OK, a SNAPSHOT per member, and END"
    else
        note fail "WATCH opens with OK, a SNAPSHOT per member, and END"
        printf '%s\n' "$w1"
    fi
else
    note fail "WATCH opens with OK, a SNAPSHOT per member, and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 1b. MEMBERS still answers while the WATCH is open: the stream
# does not starve the control plane.
if ctl 18821 MEMBERS | grep -q '^MEMBER 127.0.0.1:17821 1 ALIVE'; then
    note ok "MEMBERS answers while a WATCH is open"
else
    note fail "MEMBERS answers while a WATCH is open"
    ctl 18821 MEMBERS
fi

# 2. Node 3 joins through --seed only, not the peer file: it is
# genuinely new to node 1's table, so the stream carries JOINED
# for it. (A peer-file peer would already sit in the table — added
# at startup with incarnation 0, suspected and dead before it ever
# answered — and coming back would be a REVIVED, not a JOINED;
# the snapshot already covers that pre-existing state.)
(cd "$TMP/n3" && exec "$BIN" --port 17823 --seed 127.0.0.1:17821 --control-port 18823 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17823 1 [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams JOINED when node 3 joins"
else
    note fail "WATCH streams JOINED when node 3 joins"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi

# 3. Stop node 3 (SIGSTOP): node 1 suspects it, then resume it
# (SIGCONT) and it comes back — SUSPECTED then REVIVED. The
# incarnation is matched loosely: if the suspect window runs out
# before the resume, node 3 hears itself DEAD and refutes, bumping
# its incarnation to 2 — still a SUSPECTED/REVIVED pair.
kill -STOP "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17823 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams SUSPECTED when node 3 stops answering"
else
    note fail "WATCH streams SUSPECTED when node 3 stops answering"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi
kill -CONT "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17823 [0-9]+ [0-9]+ last_seen_ms=[0-9]+ probes_sent=[0-9]+ probes_answered=[0-9]+ suspect_ms=[0-9]+$' 10; then
    note ok "WATCH streams REVIVED when node 3 answers again"
else
    note fail "WATCH streams REVIVED when node 3 answers again"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi

# 4. Kill node 3: the stream carries DEAD for it.
kill -9 "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17823" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17823 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "WATCH streams DEAD when node 3 is killed"
else
    note fail "WATCH streams DEAD when node 3 is killed"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi

# 5. LEAVE over the control plane drops it at once: DROPPED.
if [ "$(ctl 18821 LEAVE 127.0.0.1:17823)" = "OK 127.0.0.1:17823" ] \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DROPPED 127\.0\.0\.1:17823 [0-9]+ [0-9]+$' 10; then
    note ok "WATCH streams DROPPED on LEAVE"
else
    note fail "WATCH streams DROPPED on LEAVE"
    ctl 18821 LEAVE 127.0.0.1:17823
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi

# 6. The EVENT lines arrive in lifecycle order with strictly
# increasing sequence numbers.
l_join="$(first_line "$TMP/watch1.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17823 ')"
l_suspect="$(first_line "$TMP/watch1.out" '^EVENT [0-9]+ SUSPECTED 127\.0\.0\.1:17823 ')"
l_revive="$(first_line "$TMP/watch1.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17823 ')"
l_dead="$(first_line "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17823 ')"
l_drop="$(first_line "$TMP/watch1.out" '^EVENT [0-9]+ DROPPED 127\.0\.0\.1:17823 ')"
l_join="${l_join:-0}"; l_suspect="${l_suspect:-0}"; l_revive="${l_revive:-0}"
l_dead="${l_dead:-0}"; l_drop="${l_drop:-0}"
if [ "$l_join" -gt 0 ] && [ "$l_suspect" -gt "$l_join" ] \
   && [ "$l_revive" -gt "$l_suspect" ] && [ "$l_dead" -gt "$l_revive" ] \
   && [ "$l_drop" -gt "$l_dead" ]; then
    note ok "events arrive in lifecycle order (JOINED < SUSPECTED < REVIVED < DEAD < DROPPED)"
else
    note fail "events arrive in lifecycle order (JOINED < SUSPECTED < REVIVED < DEAD < DROPPED)"
    echo "lines: join=$l_join suspect=$l_suspect revive=$l_revive dead=$l_dead drop=$l_drop"
    grep '^EVENT' "$TMP/watch1.out"
fi
if awk '/^EVENT / { s=$2; if (s <= prev) exit 1; prev=s } END { exit (NR > 0 && prev > 0) ? 0 : 1 }' "$TMP/watch1.out"; then
    note ok "event sequence numbers strictly increase"
else
    note fail "event sequence numbers strictly increase"
    grep '^EVENT' "$TMP/watch1.out"
fi

# The watcher is still alive at the end: the stream stayed open
# for the whole test instead of dropping.
if kill -0 "$(cat "$TMP/watch.pid")" 2>/dev/null; then
    note ok "WATCH connection stayed open for the whole test"
else
    note fail "WATCH connection stayed open for the whole test"
fi

echo "test_watch: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
