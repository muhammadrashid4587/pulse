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

#endif
