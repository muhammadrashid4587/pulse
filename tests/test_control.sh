#!/usr/bin/env bash
# Integration test for the TCP control plane: real nodes over localhost
# UDP, queried over TCP with pulse-ctl.
#  1. One node: MEMBERS lists itself, STATUS reports id and total=1,
#     METRICS reports zero counts and no latency samples yet, JSON
#     returns one line of valid JSON with the node itself as the only
#     member and zeroed metrics, unknown commands and bad LEAVE usage
#     get clean ERR lines.
#  1b. Slow-peer warning (0.22.0): node 1 runs with --slow-peer-rtt-ms
#     1; loopback RTTs are 0-1 ms, so the peer is not slow — JSON and
#     METRICS report slow_peers=0 and "slow":false for the peer.
#  2. A second node joins; MEMBERS on the first lists both.
#  3. LEAVE the second node on the first: OK, and it is gone from the
#     first node's listing immediately. (It is alive, so direct contact
#     brings it back within a probe interval — that is correct SWIM
#     behaviour, and step 4b waits for the comeback before step 5 kills
#     it; otherwise there would be no peer for the first node to mark
#     DEAD.)
#  4. LEAVE of an unknown id and of self are rejected cleanly.
#  5. Kill the second node; once the first marks it DEAD, METRICS
#     reports the detection with measured latencies, and LEAVE removes
#     it for good — it stays gone, because a dead member cannot refute.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-control-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17401-17402 (gossip) and 18401-18402 (control) so this test
# never clashes with the other suites. Node 1 runs with a 1 ms
# slow-peer threshold (0.22.0): loopback probe RTTs are 0-1 ms, so no
# peer is ever slow here — the test checks the wiring (the fields
# exist, the threshold is honoured), while the threshold logic itself
# is covered by the unit tests.
(cd "$TMP/n1" && exec "$BIN" --port 17401 --control-port 18401 --slow-peer-rtt-ms 1 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")

# 1. MEMBERS on a lone node lists itself; STATUS reports id and total=1.
sleep 0.5
members="$(ctl 18401 MEMBERS)"
if echo "$members" | grep -q "MEMBER 127.0.0.1:17401 .* ALIVE" \
   && echo "$members" | grep -q "^END$"; then
    note ok "MEMBERS on a lone node lists itself ALIVE"
else
    note fail "MEMBERS on a lone node lists itself ALIVE"
    echo "--- ctl output:"; echo "$members"
fi

status="$(ctl 18401 STATUS)"
if echo "$status" | grep -q "STATUS id=127.0.0.1:17401 .* total=1"; then
    note ok "STATUS reports id and total=1"
else
    note fail "STATUS reports id and total=1"
    echo "--- ctl output:"; echo "$status"
fi

# METRICS on a fresh node: no transitions yet, so the latency groups
# have no samples. Slow-peer warning (0.22.0): no peers, so
# slow_peers=0.
metrics="$(ctl 18401 METRICS)"
if echo "$metrics" | grep -q "METRICS suspected=0 dead=0" \
   && echo "$metrics" | grep -q "detect_latency_ms mean=- max=- samples=0" \
   && echo "$metrics" | grep -q "slow_peers=0" \
   && echo "$metrics" | grep -q "^END$"; then
    note ok "METRICS on a fresh node reports zero counts and no samples"
else
    note fail "METRICS on a fresh node reports zero counts and no samples"
    echo "--- ctl output:"; echo "$metrics"
fi

# JSON on a fresh node: one line of valid JSON, then END, with the
# node itself as the only member and zeroed metrics.
json="$(ctl 18401 JSON)"
if echo "$json" | head -n 1 | python3 -c "import json,sys; json.load(sys.stdin)" \
   && echo "$json" | grep -q '"id":"127.0.0.1:17401"' \
   && echo "$json" | grep -q '"total":1' \
   && echo "$json" | grep -q '"members":\[{"id":"127.0.0.1:17401","incarnation":1,"state":"ALIVE","phi":null,"phi_history":null,"meta":{},"rtt_ms":null,"rtt_mean_ms":null,"rtt_max_ms":null,"slow":false,"quiet":false,"suspect_ms":null,"last_seen_ms":null,"source":"self","probes_sent":0,"probes_answered":0,"low_probe_rate":false,"probe_rate":null,"probe_rate_history":null,"flap_count":0,"flaps_10m":0}\]' \
   && echo "$json" | grep -q '"metrics":{"suspected":0,"dead":0,"refutes":0,"joins":0' \
   && echo "$json" | grep -q '"slow_peers":0' \
   && echo "$json" | grep -q '"low_probe_rate_peers":0' \
   && echo "$json" | grep -q '"peer_file":null' \
   && echo "$json" | grep -q '"detect_latency_ms":{"mean":null,"max":0,"samples":0,"hist":\[0,0,0,0,0\]}' \
   && echo "$json" | grep -q "^END$"; then
    note ok "JSON on a fresh node is valid JSON with self member and zeroed metrics"
else
    note fail "JSON on a fresh node is valid JSON with self member and zeroed metrics"
    echo "--- ctl output:"; echo "$json"
fi

# Unknown command and bad LEAVE usage get clean ERR lines.
if [ "$(ctl 18401 BOGUS)" = "ERR unknown command" ]; then
    note ok "unknown command returns ERR unknown command"
else
    note fail "unknown command returns ERR unknown command"
fi
if [ "$(ctl 18401 LEAVE)" = "ERR usage: LEAVE <member-id>" ]; then
    note ok "LEAVE with no id returns ERR usage"
else
    note fail "LEAVE with no id returns ERR usage"
fi

# 2. A second node joins; MEMBERS on the first lists both.
(cd "$TMP/n2" && exec "$BIN" --port 17402 --seed 127.0.0.1:17401 --control-port 18402 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
if wait_for "$TMP/n1.log" "STATUS alive=2" 6 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 6; then
    note ok "2 nodes converge on alive=2"
else
    note fail "2 nodes converge on alive=2"
fi

members="$(ctl 18401 MEMBERS)"
if echo "$members" | grep -q "127.0.0.1:17401.*ALIVE" \
   && echo "$members" | grep -q "127.0.0.1:17402.*ALIVE"; then
    note ok "MEMBERS lists both nodes after join"
else
    note fail "MEMBERS lists both nodes after join"
    echo "--- ctl output:"; echo "$members"
fi

# 2b. Probe RTT (0.21.0): once the nodes have probed each other, JSON
# reports a numeric rtt_ms / rtt_mean_ms / rtt_max_ms for the peer (self stays
# null — a node never probes itself). Poll: the first probe round
# takes a cycle or two.
rtt_seen=0
for i in $(seq 1 25); do
    if ctl 18401 JSON | head -n 1 | grep -q '"id":"127.0.0.1:17402".*"rtt_ms":[0-9][0-9]*,"rtt_mean_ms":[0-9][0-9]*,"rtt_max_ms":[0-9][0-9]*'; then
        rtt_seen=1; break
    fi
    sleep 0.2
done
if [ "$rtt_seen" -eq 1 ] \
   && ctl 18401 JSON | head -n 1 | grep -q '"id":"127.0.0.1:17401".*"rtt_ms":null,"rtt_mean_ms":null,"rtt_max_ms":null'; then
    note ok "JSON reports a numeric probe RTT for the peer and null for self"
else
    note fail "JSON reports a numeric probe RTT for the peer and null for self"
    echo "--- ctl output:"; ctl 18401 JSON
fi

# 2c. Slow-peer warning (0.22.0): node 1 runs with a 1 ms threshold
# and loopback RTTs are 0-1 ms, so the peer is not slow — JSON reports
# "slow":false for it and slow_peers 0 in METRICS and JSON. (A
# positive crossing is covered by the unit tests; loopback cannot be
# made deterministically slow.)
json="$(ctl 18401 JSON)"
metrics="$(ctl 18401 METRICS)"
if echo "$json" | grep -q '"id":"127.0.0.1:17402".*"slow":false' \
   && echo "$json" | grep -q '"slow_peers":0' \
   && echo "$metrics" | grep -q "slow_peers=0"; then
    note ok "JSON/METRICS report a healthy loopback peer as not slow (slow_peers=0)"
else
    note fail "JSON/METRICS report a healthy loopback peer as not slow (slow_peers=0)"
    echo "--- json:"; echo "$json"
    echo "--- metrics:"; echo "$metrics"
fi

# 3. LEAVE the second node on the first: OK, and it is gone from the
# listing immediately (checked right away — the live node refutes and
# comes back within a gossip round or two, which is correct).
if [ "$(ctl 18401 LEAVE 127.0.0.1:17402)" = "OK 127.0.0.1:17402" ]; then
    note ok "LEAVE returns OK for a known member"
else
    note fail "LEAVE returns OK for a known member"
fi
members="$(ctl 18401 MEMBERS)"
if ! echo "$members" | grep -q "127.0.0.1:17402"; then
    note ok "LEAVE removes the member from the listing immediately"
else
    note fail "LEAVE removes the member from the listing immediately"
    echo "--- ctl output:"; echo "$members"
fi
if wait_for "$TMP/n1.log" "LEAVE 127.0.0.1:17402" 3; then
    note ok "node logs the LEAVE"
else
    note fail "node logs the LEAVE"
fi

# 4. LEAVE of an unknown id and of self are rejected cleanly.
if [ "$(ctl 18401 LEAVE 127.0.0.1:17999)" = "ERR unknown member 127.0.0.1:17999" ]; then
    note ok "LEAVE of an unknown member returns ERR unknown member"
else
    note fail "LEAVE of an unknown member returns ERR unknown member"
fi
if [ "$(ctl 18401 LEAVE 127.0.0.1:17401)" = "ERR cannot leave self" ]; then
    note ok "LEAVE of self returns ERR cannot leave self"
else
    note fail "LEAVE of self returns ERR cannot leave self"
fi

# 4b. The member is alive, so direct contact brings it back within a
# probe interval: poll MEMBERS until it reappears. This also
# synchronizes the test — the kill in step 5 only makes sense once the
# member is back in the table (otherwise there is no peer for the
# first node to mark DEAD). Poll until the peer also has a numeric
# probe RTT: the reappearance itself comes from direct contact, not
# from a probe, so without this the kill in step 5 could land before
# any probe round-trip was recorded and the dead member would carry
# null RTTs.
reappeared=0
for i in $(seq 1 25); do
    if ctl 18401 MEMBERS | grep -q "127.0.0.1:17402" \
       && ctl 18401 JSON | head -n 1 | grep -q '"id":"127.0.0.1:17402".*"rtt_ms":[0-9][0-9]*,"rtt_mean_ms":[0-9][0-9]*,"rtt_max_ms":[0-9][0-9]*'; then
        reappeared=1; break
    fi
    sleep 0.2
done
if [ "$reappeared" -eq 1 ]; then
    note ok "live member comes back after LEAVE (direct contact) with a measured probe RTT"
else
    note fail "live member comes back after LEAVE (direct contact) with a measured probe RTT"
fi

# 5. Kill the second node — with SIGKILL, an unclean crash: a clean
# SIGTERM would announce its leave and the survivor would learn the
# DEAD from gossip, recording no detection latency. Once the first
# marks it DEAD, LEAVE removes it for good — a dead member cannot
# refute, so it stays gone.
kill -9 "$(cat "$TMP/n2.pid")"
if wait_for "$TMP/n1.log" "DEAD 127.0.0.1:17402" 6; then
    note ok "killed node is marked DEAD"
else
    note fail "killed node is marked DEAD"
fi
# METRICS on the survivor: it detected the kill itself, so it has one
# suspected and one dead transition, each with a measured latency, and
# it saw the second node join.
metrics="$(ctl 18401 METRICS)"
if echo "$metrics" | grep -qE "suspected=[1-9][0-9]* dead=[1-9][0-9]*" \
   && echo "$metrics" | grep -qE "detect_latency_ms mean=[0-9]+ max=[0-9]+ samples=[1-9][0-9]*" \
   && echo "$metrics" | grep -qE "hist=\[[0-9,]+\]" \
   && echo "$metrics" | grep -qE "suspect_to_dead_ms mean=[0-9]+ max=[0-9]+ samples=[1-9][0-9]*" \
   && echo "$metrics" | grep -qE "joins=[1-9][0-9]*"; then
    note ok "METRICS reports the detected kill with measured latencies"
else
    note fail "METRICS reports the detected kill with measured latencies"
    echo "--- ctl output:"; echo "$metrics"
fi
# JSON after the kill (before the LEAVE below removes the dead member):
# still valid JSON, the dead member listed with state DEAD, counts
# reflect the detected kill, and the latency means are numbers now
# (not null).
json="$(ctl 18401 JSON)"
if echo "$json" | head -n 1 | python3 -c "import json,sys; json.load(sys.stdin)" \
   && echo "$json" | grep -q '"id":"127.0.0.1:17402","incarnation":1,"state":"DEAD","phi":null,"phi_history":null,"meta":{},"rtt_ms":[0-9][0-9]*,"rtt_mean_ms":[0-9][0-9]*,"rtt_max_ms":[0-9][0-9]*,"slow":false,"quiet":false,"suspect_ms":null,"last_seen_ms":[0-9][0-9]*,"source":"discovered","probes_sent":[0-9][0-9]*,"probes_answered":[0-9][0-9]*,"low_probe_rate":false,"probe_rate":[0-9.][0-9.]*,"probe_rate_history":\[\[[0-9][0-9]*,[0-9.][0-9.]*\]' \
   && echo "$json" | grep -q '"dead":1' \
   && echo "$json" | grep -qE '"suspected":[1-9][0-9]*' \
   && echo "$json" | grep -qE '"detect_latency_ms":\{"mean":[0-9]+,"max":[0-9]+,"samples":[1-9][0-9]*,"hist":\[[0-9,]+\]\}' \
   && echo "$json" | grep -qE '"suspect_to_dead_ms":\{"mean":[0-9]+,"max":[0-9]+,"samples":[1-9][0-9]*\}' \
   && echo "$json" | grep -q "^END$"; then
    note ok "JSON after the kill lists the dead member with measured latencies"
else
    note fail "JSON after the kill lists the dead member with measured latencies"
    echo "--- ctl output:"; echo "$json"
fi
if [ "$(ctl 18401 LEAVE 127.0.0.1:17402)" = "OK 127.0.0.1:17402" ]; then
    note ok "LEAVE returns OK for the dead member"
else
    note fail "LEAVE returns OK for the dead member"
fi
members="$(ctl 18401 MEMBERS)"
if ! echo "$members" | grep -q "127.0.0.1:17402"; then
    note ok "dead member is gone from the listing after LEAVE"
else
    note fail "dead member is gone from the listing after LEAVE"
    echo "--- ctl output:"; echo "$members"
fi
sleep 2
members="$(ctl 18401 MEMBERS)"
status="$(ctl 18401 STATUS)"
if ! echo "$members" | grep -q "127.0.0.1:17402" \
   && echo "$status" | grep -q "total=1"; then
    note ok "dead member stays gone (no refutation) and STATUS total=1"
else
    note fail "dead member stays gone (no refutation) and STATUS total=1"
    echo "--- members:"; echo "$members"
    echo "--- status:"; echo "$status"
fi

echo "test_control: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
