#!/usr/bin/env bash
# Integration test for the Prometheus exposition format (0.35.0).
#  1. Two nodes start from one shared --peer-file and converge on
#     alive=2. PROMETHEUS answers in Prometheus text format — a
#     "# HELP" / "# TYPE" pair per family, then "name value" lines,
#     then END — with the membership gauges (alive=2, total=2),
#     zeroed counters (nothing has been suspected yet), a
#     pulse_member_info gauge per member labelled by member, state
#     and source, and the peer-file gauges from the initial load
#     (peers=2, added=2, bad=0). joins stays 0: peer-file peers are
#     pre-added to the table at startup, so their first contact is
#     not a join.
#  2. Node 2 is killed with SIGKILL — an unclean death, so no
#     graceful-leave announcement: node 1 must detect it by
#     probing. Node 1 suspects then deads it, and PROMETHEUS moves
#     with it — pulse_suspected_total and pulse_dead_total rise
#     above 0, the dead member's info gauge flips to
#     state="DEAD", the detection-latency histogram gains a sample
#     (count 1, sum above 0), and the alive gauge falls to 1 while
#     the dead gauge rises to 1. (A plain kill sends SIGTERM, and
#     the node would leave gracefully — gossiping itself DEAD at
#     once — so the suspicion and the latency sample would never
#     happen.)
#  3. A node with no --peer-file exposes no pulse_peer_file_* metric
#     at all — an absent metric is how Prometheus says "not
#     applicable" — while the rest of the exposition is unchanged.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-prometheus-test.XXXXXX)"
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

# prom <control-port>: the PROMETHEUS reply.
prom() {
    ctl "$1" PROMETHEUS
}

# wait_prom <control-port> <grep-pattern> <timeout-seconds>: poll
# PROMETHEUS until a line matches the pattern.
wait_prom() {
    local port="$1" pat="$2" timeout="$3" i
    for i in $(seq 1 $((timeout * 5))); do
        if prom "$port" | grep -Eq "$pat"; then return 0; fi
        sleep 0.2
    done
    return 1
}

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17881-17882 (gossip) and 18881-18882 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17881
127.0.0.1:17882
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17881 --peer-file "$TMP/peers" --control-port 18881 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17882 --peer-file "$TMP/peers" --control-port 18882 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

P1="$(prom 18881)"
if [ -n "$P1" ] \
   && printf '%s' "$P1" | grep -q '^# HELP pulse_members_alive ' \
   && printf '%s' "$P1" | grep -q '^# TYPE pulse_members_alive gauge$' \
   && printf '%s' "$P1" | grep -q '^pulse_members_alive 2$' \
   && printf '%s' "$P1" | grep -q '^pulse_members_suspect 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_members_dead 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_members_total 2$' \
   && printf '%s' "$P1" | grep -q '^# TYPE pulse_suspected_total counter$' \
   && printf '%s' "$P1" | grep -q '^pulse_suspected_total 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_dead_total 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_joins_total 0$' \
   && printf '%s' "$P1" | grep -q '^# TYPE pulse_detect_latency_ms histogram$' \
   && printf '%s' "$P1" | grep -q '^pulse_detect_latency_ms_count 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_member_info{member="127.0.0.1:17881",state="ALIVE",source="self"} 1$' \
   && printf '%s' "$P1" | grep -q '^pulse_member_info{member="127.0.0.1:17882",state="ALIVE",source="peer-file"} 1$' \
   && printf '%s' "$P1" | grep -q '^pulse_peer_file_peers 2$' \
   && printf '%s' "$P1" | grep -q '^pulse_peer_file_added 2$' \
   && printf '%s' "$P1" | grep -q '^pulse_peer_file_dropped 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_peer_file_kept 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_peer_file_bad 0$' \
   && printf '%s' "$P1" | grep -q '^pulse_peer_file_last_reload_seconds [0-9][0-9]*$' \
   && printf '%s' "$P1" | grep -q '^pulse_peer_file_last_mtime_seconds [0-9][0-9]*$' \
   && printf '%s' "$P1" | tail -n 1 | grep -q '^END$'; then
    note ok "PROMETHEUS exposes the converged cluster (gauges, counters, member info, peer-file gauges, END)"
else
    note fail "PROMETHEUS exposes the converged cluster (gauges, counters, member info, peer-file gauges, END)"
    printf '%s\n' "$P1"
fi

# 2. Kill node 2: node 1 suspects then deads it, and the exposition
# moves with it.
kill -9 "$(cat "$TMP/n2.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17882" 10; then
    note ok "node 1 marks the killed node DEAD"
else
    note fail "node 1 marks the killed node DEAD"
    tail -5 "$TMP/n1.log"
fi
if wait_prom 18881 '^pulse_suspected_total [1-9][0-9]*$' 5 \
   && wait_prom 18881 '^pulse_dead_total [1-9][0-9]*$' 5 \
   && wait_prom 18881 '^pulse_members_alive 1$' 5 \
   && wait_prom 18881 '^pulse_members_dead 1$' 5 \
   && wait_prom 18881 '^pulse_member_info\{member="127.0.0.1:17882",state="DEAD",source="peer-file"\} 1$' 5 \
   && wait_prom 18881 '^pulse_detect_latency_ms_count 1$' 5 \
   && wait_prom 18881 '^pulse_detect_latency_ms_sum [1-9][0-9]*$' 5 \
   && wait_prom 18881 '^pulse_detect_latency_ms_bucket\{le="\+Inf"\} 1$' 5; then
    note ok "PROMETHEUS follows the death (counters rise, dead gauge 1, histogram gains a sample)"
else
    note fail "PROMETHEUS follows the death (counters rise, dead gauge 1, histogram gains a sample)"
    prom 18881
fi

# 3. A node with no --peer-file: no pulse_peer_file_* metric at all.
# It joins through the seed, whose SYNC also teaches it the dead
# node 2 — so its table holds 3 members (2 alive, 1 dead).
(cd "$TMP/n3" && exec "$BIN" --port 17883 --seed 127.0.0.1:17881 --control-port 18883 --dead-retention 120 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
if wait_for "$TMP/n3.log" "STATUS alive=2" 10; then
    P3="$(prom 18883)"
    if [ -n "$P3" ] \
       && ! printf '%s' "$P3" | grep -q 'pulse_peer_file_' \
       && printf '%s' "$P3" | grep -q '^pulse_members_total 3$' \
       && printf '%s' "$P3" | grep -q '^pulse_members_alive 2$' \
       && printf '%s' "$P3" | grep -q '^pulse_member_info{member="127.0.0.1:17881",state="ALIVE",source="seed"} 1$'; then
        note ok "PROMETHEUS without --peer-file omits the peer-file gauges"
    else
        note fail "PROMETHEUS without --peer-file omits the peer-file gauges"
        printf '%s\n' "$P3"
    fi
else
    note fail "seed-only node joins the cluster (setup)"
    tail -5 "$TMP/n3.log"
fi

echo "test_prometheus: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
