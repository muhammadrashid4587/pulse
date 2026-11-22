#!/usr/bin/env bash
# Demo: start 3 pulse nodes, show membership, kill one, watch the other
# two detect it, restart it, watch it come back.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
TMP="$(mktemp -d /tmp/pulse-demo.XXXXXX)"

cleanup() {
    for f in "$TMP"/*.pid; do
        [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null
    done
    sleep 0.2
    rm -rf "$TMP"
}
trap cleanup EXIT

mkdir -p "$TMP/n1" "$TMP/n2" "$TMP/n3"
(cd "$TMP/n1" && exec "$BIN" --port 17101 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
(cd "$TMP/n2" && exec "$BIN" --port 17102 --seed 127.0.0.1:17101 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")
(cd "$TMP/n3" && exec "$BIN" --port 17103 --seed 127.0.0.1:17101 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

sleep 3
echo "=== 3 nodes up, membership on node 1 ==="
grep "STATUS" "$TMP/n1.log" | tail -2

echo
echo "=== killing node 3 ==="
kill "$(cat "$TMP/n3.pid")"
sleep 2
echo "--- node 1 saw:"
grep -E "SUSPECT|DEAD" "$TMP/n1.log" | tail -4
echo "--- node 2 saw:"
grep -E "SUSPECT|DEAD" "$TMP/n2.log" | tail -4
grep "STATUS" "$TMP/n1.log" | tail -1

echo
echo "=== restarting node 3 (same data dir, incarnation bumps) ==="
(cd "$TMP/n3" && exec "$BIN" --port 17103 --seed 127.0.0.1:17101 >> "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")
sleep 2
grep -E "ALIVE 127.0.0.1:17103" "$TMP/n1.log" | tail -1
grep "STATUS" "$TMP/n1.log" | tail -1
echo
echo "demo done."
