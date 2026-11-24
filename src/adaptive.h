#ifndef PULSE_ADAPTIVE_H
#define PULSE_ADAPTIVE_H

/* Adaptive probe interval (0.18.0): how fast the prober loop runs
 * depends on whether the cluster is currently unstable.
 *
 * A stable cluster has nothing to prove in a hurry: it probes at the
 * configured base interval (--probe-interval-ms, default 100 ms) and
 * keeps CPU and gossip traffic low. But while any peer is SUSPECT the
 * cluster is mid-incident — a node may be dying, or may be about to
 * refute — and every part of the probe cycle matters more: direct
 * probes, indirect PING_REQ, the suspect-to-dead sweep, and gossip
 * rounds all run once per cycle. So while suspects exist the cycle
 * shortens to the fast interval, and it goes back to base the moment
 * the table is suspect-free again.
 *
 * This module is pure arithmetic so the policy is unit-testable
 * without a node: node.c counts suspects and sleeps the returned
 * interval; nothing here touches the table or the network. */

/* Floor for any interval: below 25 ms the loop would spin fast enough
 * to matter on a ward device's CPU for no detection gain. */
#define PULSE_ADAPTIVE_MIN_MS 25

/* The fast interval when none was configured explicitly: half the
 * base interval, clamped to [PULSE_ADAPTIVE_MIN_MS, base_ms]. A base
 * of 100 ms gives a fast interval of 50 ms; a base of 30 ms gives
 * the 25 ms floor. A base already below the floor returns the base
 * itself: the fast interval is never slower than base, so there is
 * simply nothing to speed up. A base <= 0 is nonsense and returns
 * the floor. */
int pulse_adaptive_fast_default_ms(int base_ms);

/* The interval the prober should sleep this cycle.
 *   base_ms    configured probe interval (must be > 0; <= 0 -> floor)
 *   fast_ms    configured fast interval, or 0 for the default above
 *   n_suspect  peers currently SUSPECT in the membership table
 * Returns base_ms when nothing is suspect, otherwise the fast
 * interval clamped to [PULSE_ADAPTIVE_MIN_MS, base_ms] — the fast
 * interval is never slower than base and never below the floor, even
 * if the operator configures something odd. */
int pulse_adaptive_interval_ms(int base_ms, int fast_ms, int n_suspect);

/* Probe-interval jitter (0.50.0): every node in a cluster that starts
 * at the same time probes in lockstep — same base interval, same
 * phase — so their PINGs, gossip rounds and SYNCs land together and
 * the cluster's traffic comes in bursts. Jitter breaks the lockstep:
 * each cycle sleeps base * (1 + u) with u uniform in [-frac, +frac],
 * so two nodes with the same base drift apart within a few cycles
 * while each node's mean interval stays the base. The jitter is
 * applied after the adaptive choice above, so base and fast intervals
 * are both jittered and the ADAPTIVE switch still logs the
 * unjittered interval.
 *
 * The randomness is an explicit xorshift32 state, not rand(): the
 * node seeds it once from time/pid/port, and a test seeds it with a
 * fixed value and gets a reproducible draw sequence. A NULL state is
 * a programming error and returns base_ms unchanged.
 *
 * pulse_jitter_next advances *state by one xorshift32 step and
 * returns the raw 32-bit draw (a zero state is replaced with a fixed
 * nonzero constant, so a zero seed still draws).
 *
 * pulse_jittered_interval_ms(base_ms, jitter_frac, state):
 *   base_ms     the interval to jitter (must be > 0; <= 0 -> floor)
 *   jitter_frac the ±fraction, 0 = no jitter; clamped to [0, 0.9] —
 *               beyond 0.9 a "probe interval" stops meaning anything
 *   state       the PRNG state, advanced by one draw per call
 * Returns an interval in [base*(1-frac), base*(1+frac)], rounded to
 * the nearest ms, and never below PULSE_ADAPTIVE_MIN_MS when the base
 * is at or above it (the same floor as the adaptive interval: below
 * 25 ms the loop would spin for no detection gain) — a base already
 * below the floor is floored at 1 ms instead. Failure detection
 * semantics are untouched: the ack and suspect timeouts are still
 * wall-clock durations, and suspicion still comes from the fixed
 * timeout or from phi alone. */

/* Seed-rejoin backoff (0.56.0): the delay before the next re-JOIN to
 * a seed that keeps not answering. A permanently dead seed was
 * re-JOINed every ~5 s forever — one UDP datagram per retry interval
 * and nothing else, but a node waiting on three dead seeds still
 * burns a steady trickle of pointless traffic, and a seed that
 * comes back is noticed no faster for it. So the retry backs off:
 * the first re-JOIN still fires on the ~5 s tick, then each
 * consecutive unanswered attempt doubles the wait — 10 s, 20 s —
 * up to a 30 s cap, so a dead seed is probed less and less often
 * while a seed that comes back is still re-joined within 30 s at
 * worst, and within ~5 s when the outage is fresh (the backoff
 * resets the moment the seed is ALIVE again). The caller jitters
 * the returned delay with pulse_jittered_interval_ms, so two nodes
 * waiting on the same dead seed do not retry in lockstep either.
 *
 * pulse_rejoin_backoff_ms(failures):
 *   failures  consecutive unanswered re-JOINs to this seed
 *               (0 or negative = none yet: the base interval)
 * Returns PULSE_REJOIN_BASE_MS * 2^failures, capped at
 * PULSE_REJOIN_CAP_MS — 5 s, 10 s, 20 s, then 30 s forever. */
#define PULSE_REJOIN_BASE_MS 5000
#define PULSE_REJOIN_CAP_MS 30000
int pulse_rejoin_backoff_ms(int failures);
uint32_t pulse_jitter_next(uint32_t *state);
int pulse_jittered_interval_ms(int base_ms, double jitter_frac,
                               uint32_t *state);

#endif
