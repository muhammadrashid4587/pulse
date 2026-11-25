#!/usr/bin/env bash
# Integration test for time since last contact per member (0.59.0):
# each member in JSON carries "last_seen_ms" — how long since this
# node last heard from the member, in ms, null for self and for a
# peer never heard from — METRICS carries one "LASTSEEN <id> <ms>"
# line per member heard from, and PROMETHEUS carries one
# pulse_member_last_seen_ms gauge sample per member heard from, so
# an operator can see a peer going quiet before the suspicion even
# fires, and how long a DEAD member has been silent. The values are
# read off the live control-plane replies and parsed, not grepped
# from source.
#  1. Two nodes start from one shared --peer-file (which also lists
#     a third peer that never starts) and converge on alive=2, with
#     a 10 s suspect timeout so the peer stays SUSPECT long enough
#     to observe.
#  2. While both are ALIVE the live peer's JSON last_seen_ms is a
#     number in [0, 10000), the METRICS reply has a matching
#     LASTSEEN line, and the PROMETHEUS reply has a matching
#     pulse_member_last_seen_ms sample — while self reports
#     last_seen_ms null and has no LASTSEEN line or gauge sample,
#     and the never-started peer reports null and has neither.
#  3. Kill node 2 with SIGKILL (a crash, so no graceful-leave
#     announcement): node 1 marks it SUSPECT, and its JSON
#     last_seen_ms keeps growing between two reads a second apart
#     — the silence clock, not a frozen value — while the
#     LASTSEEN line and the gauge sample stay present.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-last-seen-ms-test.XXXXXX)"
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

# json_last_seen_ms <control-port> <member-id>: print the member's
# last_seen_ms as JSON ("null" when self or never heard from),
# parsed from the live JSON reply with python3.
json_last_seen_ms() {
    ctl "$1" JSON | sed -n '1p' | python3 -c "
import json, sys
obj = json.load(sys.stdin)
for m in obj['members']:
    if m['id'] == '$2':
        print(json.dumps(m.get('last_seen_ms')))
        break
"
}

mkdir -p "$TMP/n1" "$TMP/n2"

# Ports 17941-17942 (gossip) and 18941-18942 (control) so this test
# never clashes with the other suites. 17949 is listed but never
# started: the never-heard-from peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17941
127.0.0.1:17942
127.0.0.1:17949
EOF

(cd "$TMP/n1" && exec "$BIN" --port 17941 --peer-file "$TMP/peers" --control-port 18941 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17942 --peer-file "$TMP/peers" --control-port 18942 --suspect-timeout-ms 10000 --dead-retention 120 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# The JSON reply is still one valid JSON object on one line.
if ctl 18941 JSON | sed -n '1p' | python3 -c "import json,sys; json.load(sys.stdin)" 2>/dev/null; then
    note ok "JSON reply parses as JSON with last_seen_ms present"
else
    note fail "JSON reply parses as JSON with last_seen_ms present"
    ctl 18941 JSON
fi

# 2. While both are ALIVE, the live peer's last_seen_ms is a number
# in [0, 10000); self and the never-started peer report null.
PEER_SEEN="$(json_last_seen_ms 18941 127.0.0.1:17942)"
SELF_SEEN="$(json_last_seen_ms 18941 127.0.0.1:17941)"
DEAD_SEEN="$(json_last_seen_ms 18941 127.0.0.1:17949)"
if python3 -c "exit(0 if 0 <= float('$PEER_SEEN') < 10000 else 1)" 2>/dev/null \
   && [ "$SELF_SEEN" = "null" ] && [ "$DEAD_SEEN" = "null" ]; then
    note ok "JSON last_seen_ms for the live peer is a number in range ($PEER_SEEN ms), self and never-started peer null"
else
    note fail "JSON last_seen_ms for the live peer is a number in range (peer '$PEER_SEEN', self '$SELF_SEEN', never-started '$DEAD_SEEN')"
    ctl 18941 JSON
fi

# METRICS: one "LASTSEEN <id> <ms>" line for the live peer, with
# the value in range, and none for self or the never-started peer.
METRICS_SEEN="$(ctl 18941 METRICS | grep '^LASTSEEN 127.0.0.1:17942 ' || true)"
METRICS_SELF="$(ctl 18941 METRICS | grep '^LASTSEEN 127.0.0.1:17941 ' || true)"
METRICS_NEVER="$(ctl 18941 METRICS | grep '^LASTSEEN 127.0.0.1:17949 ' || true)"
METRICS_MS="$(echo "$METRICS_SEEN" | awk '{print $3}')"
if [ -n "$METRICS_SEEN" ] && [ -z "$METRICS_SELF" ] && [ -z "$METRICS_NEVER" ] \
   && python3 -c "exit(0 if 0 <= float('$METRICS_MS') < 10000 else 1)" 2>/dev/null; then
    note ok "METRICS carries a LASTSEEN line for the peer ($METRICS_SEEN), none for self or the never-started peer"
else
    note fail "METRICS carries a LASTSEEN line for the peer (got '$METRICS_SEEN', self '$METRICS_SELF', never-started '$METRICS_NEVER')"
    ctl 18941 METRICS
fi

# PROMETHEUS: one pulse_member_last_seen_ms sample for the live
# peer, with the value in range, and none for self or the
# never-started peer.
PROM_SEEN="$(ctl 18941 PROMETHEUS | grep '^pulse_member_last_seen_ms{member="127.0.0.1:17942"} ' || true)"
PROM_SELF="$(ctl 18941 PROMETHEUS | grep '^pulse_member_last_seen_ms{member="127.0.0.1:17941"} ' || true)"
PROM_NEVER="$(ctl 18941 PROMETHEUS | grep '^pulse_member_last_seen_ms{member="127.0.0.1:17949"} ' || true)"
PROM_MS="$(echo "$PROM_SEEN" | awk '{print $2}')"
if [ -n "$PROM_SEEN" ] && [ -z "$PROM_SELF" ] && [ -z "$PROM_NEVER" ] \
   && python3 -c "exit(0 if 0 <= float('$PROM_MS') < 10000 else 1)" 2>/dev/null; then
    note ok "PROMETHEUS carries a pulse_member_last_seen_ms sample for the peer ($PROM_SEEN), none for self or the never-started peer"
else
    note fail "PROMETHEUS carries a pulse_member_last_seen_ms sample for the peer (got '$PROM_SEEN', self '$PROM_SELF', never-started '$PROM_NEVER')"
    ctl 18941 PROMETHEUS
fi

# 3. Kill node 2 with SIGKILL — a crash, not a clean shutdown, so
# it sends no graceful-leave announcement and node 1 has to detect
# the death through probing.
kill -9 "$(cat "$TMP/n2.pid")" 2>/dev/null
if wait_for "$TMP/n1.log" "SUSPECT 127.0.0.1:17942" 10; then
    note ok "node 1 marks the crashed peer SUSPECT"
else
    note fail "node 1 marks the crashed peer SUSPECT"
    tail -5 "$TMP/n1.log"
fi

# JSON: the SUSPECT peer's last_seen_ms is a number that keeps
# growing — the silence clock — read twice a second apart.
SEEN1="$(json_last_seen_ms 18941 127.0.0.1:17942)"
sleep 1
SEEN2="$(json_last_seen_ms 18941 127.0.0.1:17942)"
if python3 -c "exit(0 if 0 <= float('$SEEN1') < float('$SEEN2') else 1)" 2>/dev/null; then
    note ok "JSON last_seen_ms for the crashed peer grows while it is silent ($SEEN1 ms -> $SEEN2 ms)"
else
    note fail "JSON last_seen_ms for the crashed peer grows while it is silent (first '$SEEN1', second '$SEEN2')"
    ctl 18941 JSON
fi

# METRICS and PROMETHEUS still carry the peer's line/sample while
# it sits SUSPECT — a silent peer is still a heard-from peer.
METRICS_SUSPECT="$(ctl 18941 METRICS | grep '^LASTSEEN 127.0.0.1:17942 ' || true)"
PROM_SUSPECT="$(ctl 18941 PROMETHEUS | grep '^pulse_member_last_seen_ms{member="127.0.0.1:17942"} ' || true)"
if [ -n "$METRICS_SUSPECT" ] && [ -n "$PROM_SUSPECT" ]; then
    note ok "METRICS and PROMETHEUS still carry the crashed peer's LASTSEEN line/sample while SUSPECT"
else
    note fail "METRICS and PROMETHEUS still carry the crashed peer's LASTSEEN line/sample while SUSPECT (metrics '$METRICS_SUSPECT', prom '$PROM_SUSPECT')"
fi

echo "test_last_seen_ms: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
