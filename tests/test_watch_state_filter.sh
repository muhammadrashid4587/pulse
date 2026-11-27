#!/usr/bin/env bash
# Integration test for WATCH state filtering (0.39.0): a WATCH
# client can subscribe to events by the member state they leave
# their member in — "WATCH STATE SUSPECT,DEAD" carries only
# events that land on SUSPECT or DEAD — and the snapshot is
# narrowed by the member's current state too.
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. A STATE DEAD WATCH on node 1 opens with "OK
#     watching" and END but no SNAPSHOT lines at all (both members
#     are ALIVE), and a STATE ALIVE WATCH snapshots both members.
#  2. Node 3 joins: the ALIVE stream carries JOINED for it, the
#     DEAD stream carries nothing.
#  3. Node 3 is stopped (SIGSTOP): node 1 suspects it, but the
#     DEAD stream carries nothing — SUSPECTED lands on SUSPECT,
#     not DEAD — and the ALIVE stream carries nothing either.
#     Resumed (SIGCONT) it comes back: the ALIVE stream carries
#     REVIVED for it, the DEAD stream still carries nothing.
#  4. Node 3 is killed: the DEAD stream carries DEAD for it, the
#     ALIVE stream carries nothing.
#  5. After the kill, a fresh STATE DEAD WATCH snapshots only the
#     dead node 3, and a fresh STATE ALIVE WATCH snapshots only
#     nodes 1 and 2.
#  6. Bad STATE filters are refused up front: WATCH STATE BOGUS,
#     WATCH STATE with no list, and a duplicate STATE.
#  7. The DEAD stream's EVENT lines are exactly one DEAD, and the
#     ALIVE stream's are exactly JOINED then REVIVED, in order,
#     with strictly increasing sequence numbers.
# Node 1 runs with a 3 s suspect timeout so the stop/resume window
# stays well inside the suspect-to-dead window: node 3 is suspected
# but never declared DEAD while stopped. Node 2 gets the same
# timeout: with its default sub-second timeout it would declare
# node 3 DEAD while stopped and gossip that DEAD to node 1, whose
# stream would then — correctly, DEAD is in the filter — carry a
# DEAD before the kill step.
# The watchers run under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-watch-state-filter-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch1.pid" ] && kill "$(cat "$TMP/watch1.pid")" 2>/dev/null
    [ -f "$TMP/watch2.pid" ] && kill "$(cat "$TMP/watch2.pid")" 2>/dev/null
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

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17841-17843 (gossip) and 18841-18843 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17841
127.0.0.1:17842
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17841 --peer-file "$TMP/peers" --control-port 18841 --suspect-timeout-ms 3000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17842 --peer-file "$TMP/peers" --control-port 18842 --suspect-timeout-ms 3000 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# Open the two state-filtered WATCHes on node 1 in the background:
# they stay open for the whole test (timeout 60 s bounds them if
# the test hangs).
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18841" WATCH STATE DEAD > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch1.pid")
(timeout 60 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18841" WATCH STATE ALIVE > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")

# 1a. The STATE DEAD stream opens with OK and END but no SNAPSHOT:
# both members are ALIVE, so the state filter narrows the
# snapshot to nothing.
if wait_for "$TMP/watch1.out" "^END$" 10; then
    if [ "$(head -n 1 "$TMP/watch1.out")" = "OK watching" ] \
       && [ "$(grep -c '^SNAPSHOT' "$TMP/watch1.out")" -eq 0 ]; then
        note ok "STATE DEAD WATCH opens with OK and END, no SNAPSHOT on an all-ALIVE cluster"
    else
        note fail "STATE DEAD WATCH opens with OK and END, no SNAPSHOT on an all-ALIVE cluster"
        cat "$TMP/watch1.out"
    fi
else
    note fail "STATE DEAD WATCH opens with OK and END, no SNAPSHOT on an all-ALIVE cluster"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 1b. The STATE ALIVE stream snapshots both members.
if wait_for "$TMP/watch2.out" "^END$" 10; then
    if [ "$(head -n 1 "$TMP/watch2.out")" = "OK watching" ] \
       && grep -Eq '^SNAPSHOT 127\.0\.0\.1:17841 1 ALIVE - self$' "$TMP/watch2.out" \
       && grep -Eq '^SNAPSHOT 127\.0\.0\.1:17842 1 ALIVE [0-9]+ peer-file$' "$TMP/watch2.out" \
       && [ "$(grep -c '^SNAPSHOT' "$TMP/watch2.out")" -eq 2 ]; then
        note ok "STATE ALIVE WATCH snapshots both ALIVE members"
    else
        note fail "STATE ALIVE WATCH snapshots both ALIVE members"
        cat "$TMP/watch2.out"
    fi
else
    note fail "STATE ALIVE WATCH snapshots both ALIVE members"
    cat "$TMP/watch2.out" 2>/dev/null
fi

# 2. Node 3 joins through --seed only: the ALIVE stream carries
# JOINED for it (JOINED lands on ALIVE), the DEAD stream carries
# nothing.
(cd "$TMP/n3" && exec "$BIN" --port 17843 --seed 127.0.0.1:17841 --control-port 18843 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch2.out" '^EVENT [0-9]+ JOINED 127\.0\.0\.1:17843 1 [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "STATE ALIVE WATCH streams JOINED when a peer joins"
else
    note fail "STATE ALIVE WATCH streams JOINED when a peer joins"
    echo "--- watch2.out"; cat "$TMP/watch2.out"
fi
sleep 1
if [ "$(grep -c '^EVENT' "$TMP/watch1.out")" -eq 0 ]; then
    note ok "STATE DEAD WATCH stays silent for a JOINED"
else
    note fail "STATE DEAD WATCH stays silent for a JOINED"
    cat "$TMP/watch1.out"
fi

# 3. Stop node 3 (SIGSTOP): node 1 suspects it, but SUSPECTED
# lands on SUSPECT, so neither stream carries it. The filtered
# events are consumed server-side at once, so a 1 s settle is
# plenty before asserting absence.
kill -STOP "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17843" 10; then
    sleep 1
    if [ "$(grep -c 'SUSPECTED 127\.0\.0\.1:17843' "$TMP/watch1.out")" -eq 0 ] \
       && [ "$(grep -c 'SUSPECTED 127\.0\.0\.1:17843' "$TMP/watch2.out")" -eq 0 ]; then
        note ok "state-filtered WATCHes drop SUSPECTED (lands on SUSPECT, in neither filter)"
    else
        note fail "state-filtered WATCHes drop SUSPECTED (lands on SUSPECT, in neither filter)"
        echo "--- watch1.out"; grep '^EVENT' "$TMP/watch1.out"
        echo "--- watch2.out"; grep '^EVENT' "$TMP/watch2.out"
    fi
else
    note fail "state-filtered WATCHes drop SUSPECTED (lands on SUSPECT, in neither filter)"
    tail -5 "$TMP/n1.log"
fi

# Resume node 3: it comes back, and REVIVED lands on ALIVE, so
# the ALIVE stream carries it and the DEAD stream stays silent.
kill -CONT "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/watch2.out" '^EVENT [0-9]+ REVIVED 127\.0\.0\.1:17843 1 [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "STATE ALIVE WATCH streams REVIVED when the peer comes back"
else
    note fail "STATE ALIVE WATCH streams REVIVED when the peer comes back"
    echo "--- watch2.out"; cat "$TMP/watch2.out"
fi
sleep 1
if [ "$(grep -c '^EVENT' "$TMP/watch1.out")" -eq 0 ]; then
    note ok "STATE DEAD WATCH stays silent for a REVIVED"
else
    note fail "STATE DEAD WATCH stays silent for a REVIVED"
    grep '^EVENT' "$TMP/watch1.out"
fi

# 4. Kill node 3: the DEAD stream carries DEAD for it, the ALIVE
# stream carries nothing.
kill -9 "$(cat "$TMP/n3.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17843" 10 \
   && wait_for "$TMP/watch1.out" '^EVENT [0-9]+ DEAD 127\.0\.0\.1:17843 [0-9]+ [0-9]+ last_seen_ms=[0-9]+( probes_sent=[0-9]+ probes_answered=[0-9]+)?$' 10; then
    note ok "STATE DEAD WATCH streams DEAD when the peer is killed"
else
    note fail "STATE DEAD WATCH streams DEAD when the peer is killed"
    echo "--- watch1.out"; cat "$TMP/watch1.out"
fi
sleep 1
if [ "$(grep -c 'DEAD 127\.0\.0\.1:17843' "$TMP/watch2.out")" -eq 0 ]; then
    note ok "STATE ALIVE WATCH stays silent for a DEAD"
else
    note fail "STATE ALIVE WATCH stays silent for a DEAD"
    grep '^EVENT' "$TMP/watch2.out"
fi

# 5. Fresh state-filtered WATCHes snapshot by current state: the
# DEAD one shows only node 3, the ALIVE one shows only nodes 1
# and 2. A successful WATCH never closes on its own, so these run
# under timeout and the snapshot is read from the file. Node 3 is
# at incarnation 2 here, not 1: when it resumed after the SIGSTOP
# it heard gossip calling itself SUSPECT and refuted by bumping
# its incarnation, before it was killed.
timeout 3 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18841" WATCH STATE DEAD > "$TMP/snap_dead.out" 2>&1
if grep -Eq '^SNAPSHOT 127\.0\.0\.1:17843 2 DEAD [0-9]+ discovered$' "$TMP/snap_dead.out" \
   && [ "$(grep -c '^SNAPSHOT' "$TMP/snap_dead.out")" -eq 1 ]; then
    note ok "fresh STATE DEAD WATCH snapshots only the dead member"
else
    note fail "fresh STATE DEAD WATCH snapshots only the dead member"
    cat "$TMP/snap_dead.out"
fi
timeout 3 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18841" WATCH STATE ALIVE > "$TMP/snap_alive.out" 2>&1
if grep -Eq '^SNAPSHOT 127\.0\.0\.1:17841 1 ALIVE - self$' "$TMP/snap_alive.out" \
   && grep -Eq '^SNAPSHOT 127\.0\.0\.1:17842 1 ALIVE [0-9]+ peer-file$' "$TMP/snap_alive.out" \
   && [ "$(grep -c '^SNAPSHOT' "$TMP/snap_alive.out")" -eq 2 ]; then
    note ok "fresh STATE ALIVE WATCH snapshots only the alive members"
else
    note fail "fresh STATE ALIVE WATCH snapshots only the alive members"
    cat "$TMP/snap_alive.out"
fi

# 6. Bad STATE filters are refused up front.
if [ "$(ctl 18841 WATCH STATE BOGUS)" = "ERR invalid watch filter unknown member state BOGUS" ]; then
    note ok "unknown STATE is refused with ERR invalid watch filter"
else
    note fail "unknown STATE is refused with ERR invalid watch filter"
    ctl 18841 WATCH STATE BOGUS
fi
if [ "$(ctl 18841 WATCH STATE)" = "ERR invalid watch filter STATE requires a state list" ]; then
    note ok "missing STATE list is refused with ERR invalid watch filter"
else
    note fail "missing STATE list is refused with ERR invalid watch filter"
    ctl 18841 WATCH STATE
fi
if [ "$(ctl 18841 WATCH STATE ALIVE STATE DEAD)" = "ERR invalid watch filter duplicate STATE" ]; then
    note ok "duplicate STATE is refused with ERR invalid watch filter"
else
    note fail "duplicate STATE is refused with ERR invalid watch filter"
    ctl 18841 WATCH STATE ALIVE STATE DEAD
fi

# 7. The DEAD stream carried exactly one DEAD, and the ALIVE
# stream exactly JOINED then REVIVED, in order, with strictly
# increasing sequence numbers.
if [ "$(grep -c '^EVENT' "$TMP/watch1.out")" -eq 1 ] \
   && [ "$(grep '^EVENT' "$TMP/watch1.out" | awk '{print $3}')" = "DEAD" ] \
   && awk '/^EVENT / { s=$2; if (s <= prev) exit 1; prev=s } END { exit (NR > 0 && prev > 0) ? 0 : 1 }' "$TMP/watch1.out"; then
    note ok "STATE DEAD stream carries exactly one DEAD"
else
    note fail "STATE DEAD stream carries exactly one DEAD"
    grep '^EVENT' "$TMP/watch1.out"
fi
if [ "$(grep -c '^EVENT' "$TMP/watch2.out")" -eq 2 ] \
   && [ "$(grep '^EVENT' "$TMP/watch2.out" | head -n 1 | awk '{print $3}')" = "JOINED" ] \
   && [ "$(grep '^EVENT' "$TMP/watch2.out" | tail -n 1 | awk '{print $3}')" = "REVIVED" ] \
   && awk '/^EVENT / { s=$2; if (s <= prev) exit 1; prev=s } END { exit (NR > 0 && prev > 0) ? 0 : 1 }' "$TMP/watch2.out"; then
    note ok "STATE ALIVE stream carries exactly JOINED then REVIVED, sequence numbers strictly increase"
else
    note fail "STATE ALIVE stream carries exactly JOINED then REVIVED, sequence numbers strictly increase"
    grep '^EVENT' "$TMP/watch2.out"
fi

# Both watchers are still alive at the end: the streams stayed open
# for the whole test instead of dropping.
if kill -0 "$(cat "$TMP/watch1.pid")" 2>/dev/null \
   && kill -0 "$(cat "$TMP/watch2.pid")" 2>/dev/null; then
    note ok "state-filtered WATCH connections stayed open for the whole test"
else
    note fail "state-filtered WATCH connections stayed open for the whole test"
fi

echo "test_watch_state_filter: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
