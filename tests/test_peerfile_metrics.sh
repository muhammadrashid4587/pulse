#!/usr/bin/env bash
# Integration test for METRICS/JSON exposure of peer-file state (0.28.0).
#  1. Two nodes start from one shared --peer-file (with a bad line) and
#     converge on alive=2. METRICS reports the file path, the last
#     reload time (a plausible unix time) and the last mtime seen
#     (equal to the file's mtime), and the counts from the initial
#     load (pf_peers=2 pf_added=2 pf_bad=1); JSON carries the same
#     state as a "peer_file" object, and each member's "source" —
#     "self" for this node, "peer-file" for the file peer.
#  2. Node 2 is removed from the file while ALIVE and node 3 is added;
#     a SIGHUP on node 1 reloads it: METRICS/JSON show the new counts
#     (pf_peers=2 pf_kept=1 pf_bad=0, last_mtime moved to the file's
#     new mtime) and node 2 is still listed with source "peer-file"
#     (kept, not dropped).
#  3. A SIGHUP with an unchanged file refreshes the exposed state to
#     the no-op reload (added=0 dropped=0 kept=1), and a RELOAD over
#     the control plane does the same while advancing last_reload.
#  4. A node with no --peer-file reports "-" for every peer-file field
#     in METRICS and "peer_file":null in JSON, with the seed peer's
#     source still "seed".
# The auto-reload watch (0.26.0) may beat a SIGHUP to a file edit, so
# step 2 asserts only fields that are stable whichever trigger runs
# first; steps 3-4 use unchanged files, where only the triggered
# reload can fire, so their counts are exact.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-peerfile-metrics-test.XXXXXX)"
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

# metrics_field <control-port> <field>: the value of one field in the
# METRICS line (e.g. pf_peers, last_reload, peer_file).
metrics_field() {
    ctl "$1" METRICS | head -n 1 \
        | grep -o "$2=[^ ]*" | head -n 1 | cut -d= -f2-
}

# json_line <control-port>: the JSON line.
json_line() {
    ctl "$1" JSON | head -n 1
}

# all_digits <value>: 1 when the value is a non-empty run of digits.
all_digits() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

START_EPOCH="$(date +%s)"
mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"

# Ports 17711-17713 (gossip) and 18711-18713 (control) so this test
# never clashes with the other suites.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17711
127.0.0.1:17712
garbage-line
EOF
MTIME1="$(stat -c %Y "$TMP/peers")"

(cd "$TMP/n1" && exec "$BIN" --port 17711 --peer-file "$TMP/peers" --control-port 18711 --dead-retention 30 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17712 --peer-file "$TMP/peers" --control-port 18712 --dead-retention 30 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Both nodes converge from the peer file.
if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "2 nodes converge from the peer file"
else
    note fail "2 nodes converge from the peer file"
    for f in n1 n2; do echo "--- $f.log"; tail -5 "$TMP/$f.log"; done
fi

# METRICS after the initial load: path, counts, plausible times.
MFILE="$(metrics_field 18711 peer_file)"
MPEERS="$(metrics_field 18711 pf_peers)"
MADD="$(metrics_field 18711 pf_added)"
MDROP="$(metrics_field 18711 pf_dropped)"
MKEPT="$(metrics_field 18711 pf_kept)"
MBAD="$(metrics_field 18711 pf_bad)"
MRELOAD="$(metrics_field 18711 last_reload)"
MMTIME="$(metrics_field 18711 last_mtime)"
NOW_EPOCH="$(date +%s)"
if [ "$MFILE" = "$TMP/peers" ] \
   && [ "$MPEERS" = "2" ] && [ "$MADD" = "2" ] && [ "$MDROP" = "0" ] \
   && [ "$MKEPT" = "0" ] && [ "$MBAD" = "1" ] \
   && all_digits "$MRELOAD" && [ "$MRELOAD" -ge "$START_EPOCH" ] \
   && [ "$MRELOAD" -le "$NOW_EPOCH" ] \
   && [ "$MMTIME" = "$MTIME1" ]; then
    note ok "METRICS exposes the initial peer-file load (path, counts, times)"
else
    note fail "METRICS exposes the initial peer-file load (path, counts, times)"
    echo "  peer_file=$MFILE pf_peers=$MPEERS pf_added=$MADD pf_dropped=$MDROP pf_kept=$MKEPT pf_bad=$MBAD last_reload=$MRELOAD last_mtime=$MMTIME (file mtime $MTIME1)"
    ctl 18711 METRICS
fi

# JSON after the initial load: the peer_file object and per-member source.
J1="$(json_line 18711)"
if [ -n "$J1" ] \
   && printf '%s' "$J1" | grep -q "\"peer_file\":{\"path\":\"$TMP/peers\",\"last_reload\":$MRELOAD,\"last_mtime\":$MTIME1,\"peers\":2,\"added\":2,\"dropped\":0,\"kept\":0,\"bad\":1}" \
   && printf '%s' "$J1" | grep -q '"id":"127.0.0.1:17711".*"source":"self"' \
   && printf '%s' "$J1" | grep -q '"id":"127.0.0.1:17712".*"source":"peer-file"'; then
    note ok "JSON exposes the peer-file object and per-member source"
else
    note fail "JSON exposes the peer-file object and per-member source"
    printf '%s\n' "$J1"
fi

# 2. Remove node 2 from the file while ALIVE, add node 3 (not
# started); SIGHUP node 1. The watch may reload first — either way
# the state settles on peers=2 kept=1 bad=0 with the new mtime, and
# node 2 stays listed as a peer-file peer.
cat > "$TMP/peers" <<'EOF'
127.0.0.1:17711
127.0.0.1:17713
EOF
MTIME2="$(stat -c %Y "$TMP/peers")"
kill -HUP "$(cat "$TMP/n1.pid")"
if wait_for "$TMP/n1.log" "PEERFILE RELOAD $TMP/peers peers=2 added=1 dropped=0 kept=1" 5; then
    note ok "SIGHUP reloads the edited file (ADD 17713, KEEP 17712)"
else
    note fail "SIGHUP reloads the edited file (ADD 17713, KEEP 17712)"
    echo "--- n1.log"; tail -10 "$TMP/n1.log"
fi
sleep 1.5 # let any second reload (watch vs SIGHUP) settle
if [ "$(metrics_field 18711 pf_peers)" = "2" ] \
   && [ "$(metrics_field 18711 pf_kept)" = "1" ] \
   && [ "$(metrics_field 18711 pf_bad)" = "0" ] \
   && [ "$(metrics_field 18711 last_mtime)" = "$MTIME2" ]; then
    note ok "METRICS reflects the reload (peers=2 kept=1 bad=0, new mtime)"
else
    note fail "METRICS reflects the reload (peers=2 kept=1 bad=0, new mtime)"
    ctl 18711 METRICS
fi
J2="$(json_line 18711)"
if [ -n "$J2" ] \
   && printf '%s' "$J2" | grep -q "\"peer_file\":{\"path\":\"$TMP/peers\",\"last_reload\":[0-9]*,\"last_mtime\":$MTIME2,\"peers\":2,\"added\":[01],\"dropped\":0,\"kept\":1,\"bad\":0}" \
   && printf '%s' "$J2" | grep -q '"id":"127.0.0.1:17712".*"source":"peer-file"'; then
    note ok "JSON reflects the reload and the kept peer stays peer-file"
else
    note fail "JSON reflects the reload and the kept peer stays peer-file"
    printf '%s\n' "$J2"
fi

# 3. SIGHUP with an unchanged file: the exposed state becomes the
# no-op reload (added=0 dropped=0 kept=1). The watch cannot fire —
# the mtime did not move — so the counts are exact.
kill -HUP "$(cat "$TMP/n1.pid")"
sleep 1.5
if [ "$(metrics_field 18711 pf_peers)" = "2" ] \
   && [ "$(metrics_field 18711 pf_added)" = "0" ] \
   && [ "$(metrics_field 18711 pf_dropped)" = "0" ] \
   && [ "$(metrics_field 18711 pf_kept)" = "1" ] \
   && [ "$(metrics_field 18711 pf_bad)" = "0" ]; then
    note ok "SIGHUP on an unchanged file refreshes the state (added=0 kept=1)"
else
    note fail "SIGHUP on an unchanged file refreshes the state (added=0 kept=1)"
    ctl 18711 METRICS
fi

# RELOAD over the control plane does the same and advances
# last_reload: the reply is the no-op outcome, and the unix time
# moves forward across the 2 s wait.
BEFORE="$(metrics_field 18711 last_reload)"
sleep 2
RELOAD_REPLY="$(ctl 18711 RELOAD)"
AFTER="$(metrics_field 18711 last_reload)"
if [ "$RELOAD_REPLY" = "OK peers=2 added=0 dropped=0 kept=1 bad=0" ] \
   && all_digits "$BEFORE" && all_digits "$AFTER" \
   && [ "$AFTER" -gt "$BEFORE" ]; then
    note ok "RELOAD refreshes the state and advances last_reload ($BEFORE -> $AFTER)"
else
    note fail "RELOAD refreshes the state and advances last_reload ($BEFORE -> $AFTER)"
    echo "  reply: $RELOAD_REPLY"
fi

# 4. A node with no --peer-file: "-" everywhere in METRICS,
# "peer_file":null in JSON, seed source still reported.
(cd "$TMP/n3" && exec "$BIN" --port 17713 --seed 127.0.0.1:17711 --control-port 18713 --dead-retention 30 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
# Node 3 joins a 3-node cluster (nodes 1 and 2 are still alive), so
# it converges on alive=3, learning node 2 from the seed's SYNC.
if ! wait_for "$TMP/n3.log" "STATUS alive=3" 10; then
    note fail "seed-only node joins the cluster (setup)"
else
    if [ "$(metrics_field 18713 peer_file)" = "-" ] \
       && [ "$(metrics_field 18713 last_reload)" = "-" ] \
       && [ "$(metrics_field 18713 last_mtime)" = "-" ] \
       && [ "$(metrics_field 18713 pf_peers)" = "-" ] \
       && [ "$(metrics_field 18713 pf_added)" = "-" ] \
       && [ "$(metrics_field 18713 pf_dropped)" = "-" ] \
       && [ "$(metrics_field 18713 pf_kept)" = "-" ] \
       && [ "$(metrics_field 18713 pf_bad)" = "-" ]; then
        note ok "METRICS without --peer-file reports '-' for every peer-file field"
    else
        note fail "METRICS without --peer-file reports '-' for every peer-file field"
        ctl 18713 METRICS
    fi
    J3="$(json_line 18713)"
    if [ -n "$J3" ] \
       && printf '%s' "$J3" | grep -q '"peer_file":null' \
       && printf '%s' "$J3" | grep -q '"id":"127.0.0.1:17711".*"source":"seed"'; then
        note ok "JSON without --peer-file reports null and the seed source"
    else
        note fail "JSON without --peer-file reports null and the seed source"
        printf '%s\n' "$J3"
    fi
fi

# No node restarted: exactly one START line each.
starts_ok=1
for f in n1 n2 n3; do
    [ "$(grep -c "START " "$TMP/$f.log")" -eq 1 ] || starts_ok=0
done
if [ "$starts_ok" -eq 1 ]; then
    note ok "no node restarted (one START line each)"
else
    note fail "no node restarted (one START line each)"
    for f in n1 n2 n3; do echo "--- $f.log START count: $(grep -c "START " "$TMP/$f.log")"; done
fi

echo "test_peerfile_metrics: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
