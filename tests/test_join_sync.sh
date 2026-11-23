#!/usr/bin/env bash
# Integration test for JOIN answered with a full-table SYNC: a node that
# joins a cluster gets the seed's whole membership table at once, instead
# of the capped 16-entry digest, so it converges immediately instead of
# waiting for gossip rounds to rotate the missing members through.
#  1. Wire level: a raw JOIN datagram sent to a lone node is answered with
#     a SYNC carrying the node's table — not a GOSSIP digest.
#  2. End to end: a 4th node joining a converged 3-node cluster reports
#     alive=4 within a few seconds, having learned the whole table from
#     the JOIN reply.
#  3. No SYNC reply storm: once converged, both nodes stay near idle.
#     A SYNC is answered only when the exchange taught the receiver
#     something new; answering every SYNC unconditionally ping-pongs
#     forever, each SYNC answered by a SYNC at network speed, and a
#     converged pair burns a CPU core exchanging identical tables.
#     A converged node runs its probe cycle and STATUS log on a few
#     percent of a core at most; a storming node sits at tens of
#     percent. The threshold is deliberately loose (15%) so a loaded
#     machine cannot fail a healthy node — CPU a process never ran
#     cannot be counted against it.
# Without the SYNC reply, step 1 sees a GOSSIP digest instead: a joiner
# in a cluster larger than the 16-entry digest would then wait for the
# random window to rotate the missing members through. Without the
# reply-on-change rule, step 3 sees both nodes burning CPU after the
# JOIN exchange starts the loop.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-join-sync-test.XXXXXX)"
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

# --- 1. Wire level: raw JOIN gets a SYNC reply -------------------------
# Ports 17421 (node) and 17429 (fake joiner) so this test never clashes
# with the other tests.
mkdir -p "$TMP/n1"
(cd "$TMP/n1" && exec "$BIN" --port 17421 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")

if ! wait_for "$TMP/n1.log" "STATUS alive=1" 10; then
    note fail "lone node starts up for the JOIN wire test"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
else
    # Send a JOIN as a fake joiner id and collect datagrams for 3 s,
    # looking for the SYNC reply. The node may also PING the joiner
    # once it is in the table, so any datagram counts — the reply is
    # the one starting with "SYNC 127.0.0.1:17421 ". Two sockets, as
    # in test_gossip.c: the sandbox refuses sendto() on a UDP socket
    # that has never been connect()ed, so the send socket is
    # connect()ed once, while the receive socket stays unconnected
    # and bound to the joiner's port — the node replies to the
    # address advertised in the JOIN, from an ephemeral send port.
    REPLY="$(python3 - "$TMP/reply.txt" <<'PYEOF'
import socket, sys
out_path = sys.argv[1]
recv_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
recv_s.bind(("127.0.0.1", 17429))
recv_s.settimeout(3.0)
send_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
send_s.connect(("127.0.0.1", 17421))
send_s.sendto(b"JOIN 127.0.0.1:17429 1", ("127.0.0.1", 17421))
seen = []
try:
    while True:
        data, _ = recv_s.recvfrom(1400)
        seen.append(data.decode("utf-8", "replace"))
except socket.timeout:
    pass
with open(out_path, "w") as f:
    f.write("\n".join(seen))
print("\n".join(seen))
PYEOF
)"
    if printf '%s\n' "$REPLY" | grep -q "^SYNC 127.0.0.1:17421 "; then
        note ok "raw JOIN is answered with a SYNC, not a GOSSIP digest"
    else
        note fail "raw JOIN is answered with a SYNC, not a GOSSIP digest"
        echo "--- datagrams received:"; printf '%s\n' "$REPLY"
        echo "--- n1.log"; tail -20 "$TMP/n1.log"
    fi
    if printf '%s\n' "$REPLY" | grep -q "127.0.0.1:17421:1:A"; then
        note ok "JOIN SYNC reply carries the seed's own table entry"
    else
        note fail "JOIN SYNC reply carries the seed's own table entry"
        echo "--- datagrams received:"; printf '%s\n' "$REPLY"
    fi
fi
kill "$(cat "$TMP/n1.pid")" 2>/dev/null
sleep 0.2

# --- 2. End to end: 4th node joins a 3-node cluster --------------------
# Ports 17431-17434.
mkdir -p "$TMP/c1" "$TMP/c2" "$TMP/c3" "$TMP/c4"
(cd "$TMP/c1" && exec "$BIN" --port 17431 > "$TMP/c1.log" 2>&1 & echo $! > "$TMP/c1.pid")
(cd "$TMP/c2" && exec "$BIN" --port 17432 --seed 127.0.0.1:17431 > "$TMP/c2.log" 2>&1 & echo $! > "$TMP/c2.pid")
(cd "$TMP/c3" && exec "$BIN" --port 17433 --seed 127.0.0.1:17431 > "$TMP/c3.log" 2>&1 & echo $! > "$TMP/c3.pid")

if wait_for "$TMP/c1.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/c2.log" "STATUS alive=3" 10 \
   && wait_for "$TMP/c3.log" "STATUS alive=3" 10; then
    note ok "3-node cluster converges before the 4th node joins"
else
    note fail "3-node cluster converges before the 4th node joins"
    echo "--- c1.log"; tail -10 "$TMP/c1.log"
    echo "--- c2.log"; tail -10 "$TMP/c2.log"
    echo "--- c3.log"; tail -10 "$TMP/c3.log"
fi

(cd "$TMP/c4" && exec "$BIN" --port 17434 --seed 127.0.0.1:17431 > "$TMP/c4.log" 2>&1 & echo $! > "$TMP/c4.pid")

# The joiner learns the whole table from the JOIN SYNC reply, so it
# should know all 4 members within a couple of seconds — no waiting
# for gossip rounds.
if wait_for "$TMP/c4.log" "STATUS alive=4" 5; then
    note ok "joining node knows all 4 members within seconds of its JOIN"
else
    note fail "joining node knows all 4 members within seconds of its JOIN"
    echo "--- c4.log"; tail -20 "$TMP/c4.log"
fi

if wait_for "$TMP/c1.log" "STATUS alive=4" 10; then
    note ok "seed converges on alive=4 after the join"
else
    note fail "seed converges on alive=4 after the join"
    echo "--- c1.log"; tail -20 "$TMP/c1.log"
fi

# --- 3. No SYNC reply storm: converged nodes stay near idle -----------
# cpu_ticks <pid> prints utime+stime from /proc in clock ticks (100/s).
cpu_ticks() {
    awk '{print $14 + $15}' "/proc/$1/stat" 2>/dev/null
}

# cpu_pct <pid> <seconds>: percent of one core the pid burned over the
# window. Empty (or non-numeric) output means the pid is gone.
cpu_pct() {
    local pid="$1" secs="$2" t0 t1
    t0="$(cpu_ticks "$pid")"
    [ -n "$t0" ] || return 1
    sleep "$secs"
    t1="$(cpu_ticks "$pid")"
    [ -n "$t1" ] || return 1
    echo $(( (t1 - t0) * 100 / (secs * 100) ))
}

p1="$(cpu_pct "$(cat "$TMP/c1.pid")" 3)"
p4="$(cpu_pct "$(cat "$TMP/c4.pid")" 3)"
if [ -n "$p1" ] && [ -n "$p4" ] && [ "$p1" -lt 15 ] && [ "$p4" -lt 15 ]; then
    note ok "converged nodes stay near idle after the JOIN SYNC exchange (seed ${p1}%, joiner ${p4}% over 3 s)"
else
    note fail "converged nodes stay near idle after the JOIN SYNC exchange (seed ${p1:-?}%, joiner ${p4:-?}% over 3 s)"
    echo "--- c1.log"; tail -5 "$TMP/c1.log"
    echo "--- c4.log"; tail -5 "$TMP/c4.log"
fi

echo "test_join_sync: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
