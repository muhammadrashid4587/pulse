#!/usr/bin/env bash
# Integration test for the slow-peer warning on the WATCH stream
# (0.73.0): when an ALIVE peer's mean probe RTT crosses
# --slow-peer-rtt-ms the node logs one "SLOW <id> mean=<ms>
# threshold=<ms>" line and records one SLOW event on the WATCH
# stream (and in the persisted events file), carrying the mean at
# the crossing as a trailing " rtt_mean_ms=<ms>"; and when the
# mean recovers to the threshold or below it logs one "SLOWOK
# <id> mean=<ms> threshold=<ms>" line and records one SLOWOK
# event carrying the recovered mean — the mirror of LOWRATE/RATEOK
# and QUIET/QUIETOK, completing the slow-peer alert lifecycle.
# A real slow peer is hard to arrange with two pulse-nodes — a
# healthy localhost peer answers in under a millisecond — so the
# peer here is a small Python stand-in: it JOINs the node, answers
# each PING with an ACK after a delay read from a file (0 ms while
# fast, 20 ms while slow), and ignores everything else. The delay
# file is the test's only control over the peer, so the slow and
# fast phases are exact. The peer starts fast and only goes slow
# once the WATCH is open: a SLOW fires on the first slow ACKs,
# and a WATCH opened after it would never see it live (a plain
# WATCH starts live from the latest sequence, it does not replay).
#  1. Node 1 starts with --slow-peer-rtt-ms 5; the stand-in peer
#     JOINs it while fast and the two converge on alive=2, with a
#     WATCH open on node 1.
#  2. The test flips the peer slow (20 ms): node 1 logs one SLOW
#     line for it and the WATCH stream carries one SLOW event with
#     a rtt_mean_ms above the 5 ms threshold, while the peer is
#     still ALIVE — a 20 ms RTT is well inside the 100 ms ack
#     timeout, so no suspicion ever fires.
#  3. The test flips the peer fast again (0 ms delay): the mean
#     falls back to the threshold or below within a few seconds —
#     the slow phase only banked a couple of dozen 20 ms samples
#     before the SLOW fired, so a few dozen fast probes at 0 ms
#     drag it back — node 1 logs one SLOWOK line for it and the
#     WATCH stream carries one SLOWOK event with a rtt_mean_ms
#     at or below the threshold, and no SUSPECTED event for the
#     peer appears at all.
#  4. While the peer is fast its JSON says "slow":false and
#     METRICS says slow_peers=0 (PROMETHEUS pulse_slow_peers 0),
#     the SLOWOK line and event each fired exactly once, and the
#     persisted <data-dir>/events file and a WATCH FROM 0 replay
#     carry the same SLOW and SLOWOK lines.
#  5. Flipping the peer slow again and fast again logs and streams
#     a second SLOW and a second SLOWOK, so the recovery re-arms
#     with the next slow episode instead of firing once ever.
# The watcher runs under stdbuf -o0: pulse-ctl's stdout is
# block-buffered to a file, so without it the streamed lines would
# sit in the buffer and the test's polling would see nothing.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-slowok-test.XXXXXX)"
PASS=0
FAIL=0

cleanup() {
    [ -f "$TMP/watch.pid" ] && kill "$(cat "$TMP/watch.pid")" 2>/dev/null
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

# wait_count <logfile> <pattern> <count> <timeout-seconds>: wait
# until the pattern has matched exactly <count> times — for a
# second episode, where a plain wait_for would match the first
# episode's line at once.
wait_count() {
    local log="$1" pat="$2" want="$3" timeout="$4" i
    for i in $(seq 1 $((timeout * 5))); do
        if [ "$(grep -c "$pat" "$log" 2>/dev/null)" = "$want" ]; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

# ctl <control-port> <command...>: run pulse-ctl against a node.
ctl() {
    local port="$1"; shift
    "$CTL" "127.0.0.1:$port" "$@"
}

# json_member <control-port> <member-id> <field>: print the
# member's field as JSON, parsed from the live JSON reply.
json_member() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(json.dumps(m.get('$3')))
        break
"
}

# event_rtt_mean <file> <pattern>: print the rtt_mean_ms of the
# first EVENT line in the file matching the pattern.
event_rtt_mean() {
    grep -E "$2" "$1" | head -n 1 | grep -o 'rtt_mean_ms=[0-9]*' | cut -d= -f2
}

mkdir -p "$TMP/n1"

# The stand-in slow peer: a UDP speaker that JOINs the node, then
# answers each PING with an ACK after the delay (ms) currently in
# the delay file — 0 while fast, 20 while slow — and ignores
# everything else. It re-JOINs every 2 s so a lost first JOIN
# cannot strand it. Two sockets, like pulse-node itself: this
# sandbox refuses sendto() on a socket that was never connect()ed,
# and a connected socket only receives from its peer — but the
# node's PINGs arrive from its send socket's ephemeral port, not
# its listen port. So one connected socket only sends (JOIN/ACK)
# and one bound socket only receives (PING). The node identifies
# the sender by the id in the message, never by the datagram
# source port, so the ACK's ephemeral source port is irrelevant.
cat > "$TMP/slow_peer.py" <<'EOF'
import socket, sys, time

port = int(sys.argv[1])
node = ("127.0.0.1", int(sys.argv[2]))
delay_file = sys.argv[3]
peer_id = "127.0.0.1:%d" % port
send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
send_sock.connect(node)
recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
recv_sock.bind(("127.0.0.1", port))
recv_sock.settimeout(0.2)

def delay_ms():
    try:
        with open(delay_file) as f:
            return int(f.read().strip())
    except Exception:
        return 0

def send_join():
    send_sock.send(("JOIN %s 1" % peer_id).encode())

send_join()
last_join = time.time()
while True:
    try:
        data, _ = recv_sock.recvfrom(4096)
    except socket.timeout:
        if time.time() - last_join >= 2.0:
            send_join()
            last_join = time.time()
        continue
    parts = data.decode(errors="ignore").split()
    if len(parts) >= 4 and parts[0] == "PING":
        seq = parts[1]
        d = delay_ms()
        if d > 0:
            time.sleep(d / 1000.0)
        send_sock.send(("ACK %s %s 1" % (seq, peer_id)).encode())
EOF

# Ports 17345 (node) and 17346 (stand-in peer) for gossip, 18345
# for control, so this test never clashes with the other suites.
# The peer starts fast (0 ms delay): it only goes slow once the
# WATCH is open, so the SLOW event streams live.
echo 0 > "$TMP/delay_ms"

(cd "$TMP/n1" && exec "$BIN" --port 17345 --control-port 18345 --slow-peer-rtt-ms 5 --data-dir "$TMP/n1" > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
sleep 0.2
(python3 "$TMP/slow_peer.py" 17346 17345 "$TMP/delay_ms" > "$TMP/peer.log" 2>&1 & echo $! > "$TMP/peer.pid")

# 1. The node and the stand-in peer converge on alive=2 while the
# peer is still fast, so no SLOW can fire before the WATCH opens.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10; then
    note ok "node and stand-in peer converge on alive=2"
else
    note fail "node and stand-in peer converge on alive=2"
    tail -5 "$TMP/n1.log"
fi

# Open the WATCH on node 1 in the background: it stays open for
# the whole test (timeout 120 s bounds it if the test hangs).
(timeout 120 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18345" WATCH > "$TMP/watch1.out" 2>&1 & echo $! > "$TMP/watch.pid")

if wait_for "$TMP/watch1.out" "^END$" 10; then
    note ok "WATCH opens with snapshot and END"
else
    note fail "WATCH opens with snapshot and END"
    cat "$TMP/watch1.out" 2>/dev/null
fi

# 2. Flip the peer slow (20 ms a probe): the mean RTT climbs past
# the 5 ms threshold within a couple of seconds, while the peer is
# still ALIVE — 20 ms is well inside the 100 ms ack timeout, so
# no suspicion ever fires.
echo 20 > "$TMP/delay_ms"

if wait_for "$TMP/n1.log" 'SLOW 127\.0\.0\.1:17346 mean=[0-9]+ms threshold=5ms' 10; then
    note ok "node 1 logs a SLOW line for the slow peer"
else
    note fail "node 1 logs a SLOW line for the slow peer"
    tail -5 "$TMP/n1.log"
fi

if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SLOW 127\.0\.0\.1:17346 [0-9]+ [0-9]+ .*rtt_mean_ms=[0-9]+$' 10; then
    note ok "WATCH streams a SLOW event with a rtt_mean_ms"
else
    note fail "WATCH streams a SLOW event with a rtt_mean_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

# The SLOW mean is the mean that crossed the threshold: above 5.
SLOW_MEAN="$(event_rtt_mean "$TMP/watch1.out" '^EVENT [0-9]+ SLOW 127\.0\.0\.1:17346 ')"
if [ -n "$SLOW_MEAN" ] && python3 -c "exit(0 if int('$SLOW_MEAN') > 5 else 1)" 2>/dev/null; then
    note ok "SLOW event rtt_mean_ms is above the threshold ($SLOW_MEAN ms)"
else
    note fail "SLOW event rtt_mean_ms is above the threshold (got '$SLOW_MEAN')"
fi

# While the peer is slow its JSON flag reads true and METRICS
# counts one slow peer, and PROMETHEUS agrees.
if [ "$(json_member 18345 127.0.0.1:17346 slow)" = "true" ]; then
    note ok "JSON flags the peer slow while its mean RTT is above the threshold"
else
    note fail "JSON flags the peer slow while its mean RTT is above the threshold"
    ctl 18345 JSON | sed -n '1p'
fi

if ctl 18345 METRICS | grep -q "slow_peers=1"; then
    note ok "METRICS reports slow_peers=1 while the peer is slow"
else
    note fail "METRICS reports slow_peers=1 while the peer is slow"
    ctl 18345 METRICS
fi

if ctl 18345 PROMETHEUS | grep -q "^pulse_slow_peers 1$"; then
    note ok "PROMETHEUS reports pulse_slow_peers 1 while the peer is slow"
else
    note fail "PROMETHEUS reports pulse_slow_peers 1 while the peer is slow"
    ctl 18345 PROMETHEUS | grep slow
fi

# 3. Flip the peer fast (0 ms delay): the mean falls back to the
# threshold or below within a few seconds — the slow phase only
# banked a couple of dozen 20 ms samples, so a few dozen fast
# probes at 0 ms drag it back — and the recovery fires while the
# peer is still ALIVE.
echo 0 > "$TMP/delay_ms"

if wait_for "$TMP/n1.log" 'SLOWOK 127\.0\.0\.1:17346 mean=[0-9]+ms threshold=5ms' 20; then
    note ok "node 1 logs a SLOWOK line once the peer answers fast again"
else
    note fail "node 1 logs a SLOWOK line once the peer answers fast again"
    tail -5 "$TMP/n1.log"
fi

if wait_for "$TMP/watch1.out" '^EVENT [0-9]+ SLOWOK 127\.0\.0\.1:17346 [0-9]+ [0-9]+ .*rtt_mean_ms=[0-9]+$' 10; then
    note ok "WATCH streams a SLOWOK event with a rtt_mean_ms"
else
    note fail "WATCH streams a SLOWOK event with a rtt_mean_ms"
    grep '^EVENT' "$TMP/watch1.out" 2>/dev/null
fi

# The SLOWOK mean is the mean at the recovery: at or below 5.
SLOWOK_MEAN="$(event_rtt_mean "$TMP/watch1.out" '^EVENT [0-9]+ SLOWOK 127\.0\.0\.1:17346 ')"
if [ -n "$SLOWOK_MEAN" ] && python3 -c "exit(0 if 0 <= int('$SLOWOK_MEAN') <= 5 else 1)" 2>/dev/null; then
    note ok "SLOWOK event rtt_mean_ms is at or below the threshold ($SLOWOK_MEAN ms)"
else
    note fail "SLOWOK event rtt_mean_ms is at or below the threshold (got '$SLOWOK_MEAN')"
fi

# The peer was never suspected: the recovery landed while it was
# still ALIVE, not on the way back from SUSPECT.
if ! grep -q '^EVENT [0-9]* SUSPECTED 127\.0\.0\.1:17346 ' "$TMP/watch1.out"; then
    note ok "no SUSPECTED event for the peer before the SLOWOK"
else
    note fail "no SUSPECTED event for the peer before the SLOWOK"
    grep '^EVENT.*SUSPECTED' "$TMP/watch1.out" 2>/dev/null
fi

# 4. While the peer is fast its JSON flag reads false and METRICS
# counts zero slow peers, and the SLOWOK fired exactly once.
if [ "$(json_member 18345 127.0.0.1:17346 slow)" = "false" ]; then
    note ok "JSON clears the slow flag once the peer answers fast again"
else
    note fail "JSON clears the slow flag once the peer answers fast again"
    ctl 18345 JSON | sed -n '1p'
fi

if ctl 18345 METRICS | grep -q "slow_peers=0"; then
    note ok "METRICS reports slow_peers=0 once the peer answers fast again"
else
    note fail "METRICS reports slow_peers=0 once the peer answers fast again"
    ctl 18345 METRICS
fi

if [ "$(grep -c ' SLOWOK 127\.0\.0\.1:17346 ' "$TMP/n1.log")" = "1" ]; then
    note ok "the SLOWOK line fired exactly once for the episode"
else
    note fail "the SLOWOK line fired exactly once for the episode"
    grep ' SLOWOK ' "$TMP/n1.log"
fi

if [ "$(grep -c '^EVENT [0-9]* SLOWOK 127\.0\.0\.1:17346 ' "$TMP/watch1.out")" = "1" ]; then
    note ok "the SLOWOK event fired exactly once for the episode"
else
    note fail "the SLOWOK event fired exactly once for the episode"
    grep '^EVENT.*SLOWOK' "$TMP/watch1.out" 2>/dev/null
fi

# The persisted event log carries the same SLOW and SLOWOK lines
# as the WATCH stream did: the warning and the recovery survive a
# restart and a replay.
if wait_for "$TMP/n1/events" "^[0-9]+ SLOW 127\\.0\\.0\\.1:17346 [0-9]+ [0-9]+ .* rtt_mean_ms=$SLOW_MEAN\$" 10; then
    note ok "persisted events file carries the same SLOW line ($SLOW_MEAN ms)"
else
    note fail "persisted events file carries the same SLOW line ($SLOW_MEAN ms)"
    cat "$TMP/n1/events" 2>/dev/null
fi

if wait_for "$TMP/n1/events" "^[0-9]+ SLOWOK 127\\.0\\.0\\.1:17346 [0-9]+ [0-9]+ .* rtt_mean_ms=$SLOWOK_MEAN\$" 10; then
    note ok "persisted events file carries the same SLOWOK line ($SLOWOK_MEAN ms)"
else
    note fail "persisted events file carries the same SLOWOK line ($SLOWOK_MEAN ms)"
    cat "$TMP/n1/events" 2>/dev/null
fi

# A replay from the start shows the SLOW and SLOWOK events too.
(timeout 30 stdbuf -o0 -e0 "$CTL" "127.0.0.1:18345" WATCH FROM 0 > "$TMP/watch2.out" 2>&1 & echo $! > "$TMP/watch2.pid")
if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ SLOW 127\\.0\\.0\\.1:17346 [0-9]+ [0-9]+ .* rtt_mean_ms=$SLOW_MEAN\$" 10; then
    note ok "replayed WATCH shows the SLOW event with the same mean"
else
    note fail "replayed WATCH shows the SLOW event with the same mean"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi

if wait_for "$TMP/watch2.out" "^EVENT [0-9]+ SLOWOK 127\\.0\\.0\\.1:17346 [0-9]+ [0-9]+ .* rtt_mean_ms=$SLOWOK_MEAN\$" 10; then
    note ok "replayed WATCH shows the SLOWOK event with the same mean"
else
    note fail "replayed WATCH shows the SLOWOK event with the same mean"
    grep '^EVENT' "$TMP/watch2.out" 2>/dev/null
fi

# 5. A second slow episode re-arms the warning and the recovery:
# flip the peer slow again, wait for a second SLOW, flip it fast,
# and wait for a second SLOWOK — one per episode, not once ever.
echo 20 > "$TMP/delay_ms"

if wait_count "$TMP/n1.log" ' SLOW 127\.0\.0\.1:17346 ' 2 15; then
    note ok "a second slow episode logs a second SLOW line"
else
    note fail "a second slow episode logs a second SLOW line"
    grep ' SLOW ' "$TMP/n1.log"
fi

echo 0 > "$TMP/delay_ms"

if wait_count "$TMP/n1.log" ' SLOWOK 127\.0\.0\.1:17346 ' 2 20; then
    note ok "a second slow episode logs a second SLOWOK line"
else
    note fail "a second slow episode logs a second SLOWOK line"
    grep ' SLOWOK ' "$TMP/n1.log"
fi

if wait_count "$TMP/watch1.out" '^EVENT [0-9]* SLOWOK 127\.0\.0\.1:17346 ' 2 10; then
    note ok "a second slow episode streams a second SLOWOK event"
else
    note fail "a second slow episode streams a second SLOWOK event"
    grep '^EVENT.*SLOWOK' "$TMP/watch1.out" 2>/dev/null
fi

echo "test_slowok: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
