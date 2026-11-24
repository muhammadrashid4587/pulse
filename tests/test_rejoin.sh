#!/usr/bin/env bash
# Integration test for seed rejoin: a node that starts while its seed is
# down must eventually join the cluster once the seed comes up.
#  1. Start node 2 with --seed 127.0.0.1:17401 while node 1 is down.
#     Node 2 suspects, deads and GCs the unanswering seed.
#  2. Start node 1. Node 2 re-sends JOIN to the seed every ~5 s
#     (REJOIN log line) until node 1 answers.
#  3. Both nodes converge on alive=2 — the cluster heals instead of
#     leaving node 2 isolated forever.
# Without the rejoin retry, step 3 never happens: node 1 has no reason
# to contact node 2 and node 2 never contacts node 1 again.
#  7. Backoff (0.56.0): a third node whose seed never comes up at all
#     retries on a doubling schedule — 10 s, then 20 s — instead of
#     every 5 s forever, and reports the scheduled delay per seed.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/pulse-node"
CTL="$ROOT/pulse-ctl"
TMP="$(mktemp -d /tmp/pulse-rejoin-test.XXXXXX)"
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

mkdir -p "$TMP/n1" "$TMP/n2"

# exec: the subshell replaces itself with the node, so $! is the node pid.
# Ports 17401-17402 so this test never clashes with the other tests.
# Node 2 starts first; its seed (node 1) is down. Its control port is
# 18402, so the test can read METRICS at the end.
(cd "$TMP/n2" && exec "$BIN" --port 17402 --seed 127.0.0.1:17401 --control-port 18402 --dead-retention 2 > "$TMP/n2.log" 2>&1 & echo $! > "$TMP/n2.pid")

# 1. Node 2 suspects, deads and GCs the unanswering seed.
if wait_for "$TMP/n2.log" "GC 127.0.0.1:17401" 10; then
    note ok "node 2 GCs its down seed"
else
    note fail "node 2 GCs its down seed"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 2. Node 1 comes up. Node 2 must re-JOIN it (REJOIN log line) — the
# retry fires every ~5 s, so allow 15 s.
(cd "$TMP/n1" && exec "$BIN" --port 17401 --dead-retention 2 > "$TMP/n1.log" 2>&1 & echo $! > "$TMP/n1.pid")
if wait_for "$TMP/n2.log" "REJOIN 127.0.0.1:17401" 15; then
    note ok "node 2 re-sends JOIN to the seed once it is GC'd"
else
    note fail "node 2 re-sends JOIN to the seed once it is GC'd"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 3. The cluster heals: node 1 learns about node 2 from the re-JOIN,
# and both nodes report alive=2.
if wait_for "$TMP/n1.log" "JOIN 127.0.0.1:17402" 10; then
    note ok "late seed learns about the waiting node via re-JOIN"
else
    note fail "late seed learns about the waiting node via re-JOIN"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
fi

if wait_for "$TMP/n1.log" "STATUS alive=2" 10 \
   && wait_for "$TMP/n2.log" "STATUS alive=2" 10; then
    note ok "both nodes converge on alive=2 after the seed comes up"
else
    note fail "both nodes converge on alive=2 after the seed comes up"
    echo "--- n1.log"; tail -20 "$TMP/n1.log"
    echo "--- n2.log"; tail -20 "$TMP/n2.log"
fi

# 4. The rejoin retry is visible (0.53.0): METRICS on node 2 counts
# the re-JOINs it sent while its seed was down — at least the one
# after the GC, and usually one while the seed was SUSPECT/DEAD in
# the table too. The count is stable now that the seed is ALIVE.
metrics="$("$CTL" 127.0.0.1:18402 METRICS 2>/dev/null)"
if echo "$metrics" | grep -qE "seed_rejoins=[1-9][0-9]*"; then
    note ok "METRICS counts the seed rejoins (seed_rejoins above 0)"
else
    note fail "METRICS counts the seed rejoins (seed_rejoins above 0)"
    echo "--- metrics"; echo "$metrics"
fi

# 5. Per-seed breakdown (0.54.0): the same count broken down by
# seed, so an operator can see which seed a node keeps rejoining.
# Node 2 has exactly one seed, so the breakdown names it and
# agrees with the total.
if echo "$metrics" | grep -qE "seed_rejoins_by_seed=127\.0\.0\.1:17401=[1-9][0-9]*"; then
    note ok "METRICS breaks the rejoins down per seed"
else
    note fail "METRICS breaks the rejoins down per seed"
    echo "--- metrics"; echo "$metrics"
fi
json="$("$CTL" 127.0.0.1:18402 JSON 2>/dev/null)"
if echo "$json" | grep -qE '"seed_rejoins_by_seed":\{"127\.0\.0\.1:17401":[1-9][0-9]*\}'; then
    note ok "JSON breaks the rejoins down per seed"
else
    note fail "JSON breaks the rejoins down per seed"
    echo "--- json"; echo "$json"
fi
prom="$("$CTL" 127.0.0.1:18402 PROMETHEUS 2>/dev/null)"
if echo "$prom" | grep -qE 'pulse_seed_rejoin_total\{seed="127\.0\.0\.1:17401"\} [1-9][0-9]*'; then
    note ok "PROMETHEUS labels the rejoin counter per seed"
else
    note fail "PROMETHEUS labels the rejoin counter per seed"
    echo "--- prom"; echo "$prom"
fi

# 6. Per-seed last rejoin (0.55.0): when each seed was last retried,
# as unix milliseconds — a 13-digit timestamp, recent (this run),
# in all three views, under the same seed key as the breakdown.
# A seed that was never re-joined has no entry anywhere; node 2
# re-joined exactly one seed, so the value names it and is no
# fabricated zero.
if echo "$metrics" | grep -qE "seed_last_rejoin_ms_by_seed=127\\.0\\.0\\.1:17401=[0-9]{13}"; then
    note ok "METRICS reports when the seed was last rejoined"
else
    note fail "METRICS reports when the seed was last rejoined"
    echo "--- metrics"; echo "$metrics"
fi
if echo "$json" | grep -qE '"seed_last_rejoin_ms":\{"127\.0\.0\.1:17401":[0-9]{13}\}'; then
    note ok "JSON reports when the seed was last rejoined"
else
    note fail "JSON reports when the seed was last rejoined"
    echo "--- json"; echo "$json"
fi
if echo "$prom" | grep -qE 'pulse_seed_last_rejoin_ms\{seed="127\.0\.0\.1:17401"\} [0-9]{13}'; then
    note ok "PROMETHEUS gauges when the seed was last rejoined"
else
    note fail "PROMETHEUS gauges when the seed was last rejoined"
    echo "--- prom"; echo "$prom"
fi

# 6b. Backoff reset (0.56.0): node 2's seed is ALIVE now, so its
# backoff reset — the delay reads 0 (no backoff pending), not the
# 10 s it was scheduled at while the seed was down. A 0 here is a
# healthy seed, not a missing one: the entry still exists, with
# its count and last-rejoin timestamp intact.
if echo "$metrics" | grep -q "seed_rejoin_backoff_ms_by_seed=127.0.0.1:17401=0"; then
    note ok "METRICS resets the backoff to 0 once the seed is ALIVE"
else
    note fail "METRICS resets the backoff to 0 once the seed is ALIVE"
    echo "--- metrics"; echo "$metrics"
fi
if echo "$json" | grep -q '"seed_rejoin_backoff_ms":{"127.0.0.1:17401":0}'; then
    note ok "JSON resets the backoff to 0 once the seed is ALIVE"
else
    note fail "JSON resets the backoff to 0 once the seed is ALIVE"
    echo "--- json"; echo "$json"
fi
if echo "$prom" | grep -q 'pulse_seed_rejoin_backoff_ms{seed="127.0.0.1:17401"} 0'; then
    note ok "PROMETHEUS resets the backoff to 0 once the seed is ALIVE"
else
    note fail "PROMETHEUS resets the backoff to 0 once the seed is ALIVE"
    echo "--- prom"; echo "$prom"
fi

# 7. Seed-rejoin backoff (0.56.0): node 3's seed (127.0.0.1:17409)
# never comes up at all — nothing ever listens there. Its first
# re-JOIN still fires on the ~5 s tick, but the next waits 10 s
# and the one after that 20 s, instead of every 5 s forever.
# Two observations prove the interval grows: after the first retry
# the count is 1 and the scheduled delay is ~10 s (9–11 s with
# jitter), and after the second the count is 2 and the delay is
# ~20 s (18–22 s). Under the old fixed cadence the count would
# already be 2 by the first observation and 5 by the second.
# wait_backoff <timeout-s> <want-count> <want-lo-ms> <want-hi-ms>:
# poll node 3 METRICS until seed_rejoins == want-count and the
# dead seed's backoff delay is inside [lo, hi]. The window each
# state holds is ~9 s (first retry to second) and ~18 s (second
# to third), so the timeouts have ample margin either way.
wait_backoff() {
    local timeout="$1" want_n="$2" lo="$3" hi="$4" i m n b
    for i in $(seq 1 $((timeout * 2))); do
        m="$("$CTL" 127.0.0.1:18403 METRICS 2>/dev/null)"
        n="$(echo "$m" | sed -n 's/.* seed_rejoins=\([0-9]*\) .*/\1/p')"
        b="$(echo "$m" | sed -n 's/.*seed_rejoin_backoff_ms_by_seed=127\.0\.0\.1:17409=\([0-9]*\).*/\1/p')"
        if [ "$n" = "$want_n" ] && [ -n "$b" ] && [ "$b" -ge "$lo" ] && [ "$b" -le "$hi" ]; then return 0; fi
        sleep 0.5
    done
    return 1
}

mkdir -p "$TMP/n3"
(cd "$TMP/n3" && exec "$BIN" --port 17403 --seed 127.0.0.1:17409 --control-port 18403 --dead-retention 2 > "$TMP/n3.log" 2>&1 & echo $! > "$TMP/n3.pid")

if wait_backoff 20 1 9000 11000; then
    note ok "dead seed: first retry schedules a ~10 s backoff (count 1)"
else
    note fail "dead seed: first retry schedules a ~10 s backoff (count 1)"
    echo "--- n3 metrics"; "$CTL" 127.0.0.1:18403 METRICS 2>/dev/null
fi

if wait_backoff 40 2 18000 22000; then
    note ok "dead seed: second retry schedules a ~20 s backoff (count 2)"
else
    note fail "dead seed: second retry schedules a ~20 s backoff (count 2)"
    echo "--- n3 metrics"; "$CTL" 127.0.0.1:18403 METRICS 2>/dev/null
fi

# The backoff rides in all three views, under the same seed key,
# at the ~20 s value the second observation just proved.
n3metrics="$("$CTL" 127.0.0.1:18403 METRICS 2>/dev/null)"
n3b="$(echo "$n3metrics" | sed -n 's/.*seed_rejoin_backoff_ms_by_seed=127\.0\.0\.1:17409=\([0-9]*\).*/\1/p')"
if [ -n "$n3b" ] && [ "$n3b" -ge 18000 ] && [ "$n3b" -le 22000 ]; then
    note ok "METRICS reports the dead seed's backoff delay"
else
    note fail "METRICS reports the dead seed's backoff delay"
    echo "--- n3 metrics"; echo "$n3metrics"
fi
n3json="$("$CTL" 127.0.0.1:18403 JSON 2>/dev/null)"
n3jb="$(echo "$n3json" | sed -n 's/.*"seed_rejoin_backoff_ms":{"127\.0\.0\.1:17409":\([0-9]*\)}.*/\1/p')"
if [ -n "$n3jb" ] && [ "$n3jb" -ge 18000 ] && [ "$n3jb" -le 22000 ]; then
    note ok "JSON reports the dead seed's backoff delay"
else
    note fail "JSON reports the dead seed's backoff delay"
    echo "--- n3 json"; echo "$n3json"
fi
n3prom="$("$CTL" 127.0.0.1:18403 PROMETHEUS 2>/dev/null)"
n3pb="$(echo "$n3prom" | sed -n 's/.*pulse_seed_rejoin_backoff_ms{seed="127\.0\.0\.1:17409"} \([0-9]*\).*/\1/p')"
if [ -n "$n3pb" ] && [ "$n3pb" -ge 18000 ] && [ "$n3pb" -le 22000 ]; then
    note ok "PROMETHEUS gauges the dead seed's backoff delay"
else
    note fail "PROMETHEUS gauges the dead seed's backoff delay"
    echo "--- n3 prom"; echo "$n3prom"
fi

echo "test_rejoin: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
