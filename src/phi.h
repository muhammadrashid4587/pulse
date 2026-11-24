#ifndef PULSE_PHI_H
#define PULSE_PHI_H

#include <stddef.h>
#include <stdint.h>

/* Phi-accrual failure detection (Hayashibara et al., "The phi Accrual
 * Failure Detector").
 *
 * Each peer keeps a sliding window of recent heartbeat inter-arrival
 * intervals. phi(t) = -log10(P(X > t)) for X ~ Normal(mean, stddev) of
 * those intervals, where t is the time since the last heartbeat. A
 * healthy peer's phi stays near 0; a silent peer's phi grows without
 * bound, and the peer is suspected once phi crosses the threshold.
 *
 * The normal model is an approximation: real arrival intervals on a
 * LAN are closer to exponential, so phi here is conservative — it
 * rises a little faster than the "true" probability would suggest.
 * The spread is also floored (see phi.c): with perfectly regular
 * arrivals the variance is ~0 and any tiny scheduling delay would
 * look infinitely suspicious, so at least 100 ms of spread is assumed.
 */

#define PULSE_PHI_WINDOW 100
#define PULSE_PHI_MIN_SAMPLES 2
#define PULSE_PHI_MAX 30.0 /* cap: erfc underflows to 0 for large z */
#define PULSE_PHI_THRESHOLD_DEFAULT 8.0 /* --phi-threshold default */
/* Phi history (0.47.0): how many recent phi samples each peer keeps,
 * so JSON can show how suspicion evolved instead of only the live
 * value. One sample per probe cycle (~100 ms by default), so 16
 * samples cover ~1.6 s — enough to watch phi climb from ~0 to the
 * 8.0 threshold while a peer goes quiet, and bounded so a 64-member
 * table costs at most 64 x 16 samples. */
#define PULSE_PHI_HISTORY 16

typedef struct {
    double intervals[PULSE_PHI_WINDOW];
    int count;  /* samples in the window (<= PULSE_PHI_WINDOW) */
    int head;   /* next slot to overwrite */
    double sum;
    double sumsq;
    int64_t last_ms; /* last heartbeat arrival, monotonic ms */
    int have_last;
    /* Recent phi samples: the phi value at each probe cycle, oldest
     * first in ring order. hist_ms is monotonic ms (same clock as
     * last_ms); JSON reports each sample's age in ms since the node
     * started, so the timestamps are comparable across members. */
    double hist_phi[PULSE_PHI_HISTORY];
    int64_t hist_ms[PULSE_PHI_HISTORY];
    int hist_count; /* samples in the history (<= PULSE_PHI_HISTORY) */
    int hist_head;  /* next slot to overwrite */
} phi_accrual_t;

void phi_init(phi_accrual_t *p);
/* Record a heartbeat arrival at now_ms. */
void phi_heartbeat(phi_accrual_t *p, int64_t now_ms);
/* phi for this peer at now_ms. 0.0 when there is not enough history
 * to judge (fewer than PULSE_PHI_MIN_SAMPLES intervals). */
double phi_value(const phi_accrual_t *p, int64_t now_ms);
/* Sample the live phi at now_ms into the bounded history: one entry
 * per probe cycle, oldest dropped when the ring is full. A second
 * sample in the same millisecond is skipped, so a burst of calls
 * cannot fill the ring with duplicates. */
void phi_sample(phi_accrual_t *p, int64_t now_ms);
/* Write the history as a JSON array of [age_ms, phi] pairs, oldest
 * first — age_ms is ms since start_ms (the node's start, monotonic
 * ms), phi to two decimals like the live value. "[]" when no sample
 * has been taken yet. Returns the length written, or -1 when the
 * buffer is too small. */
int phi_history_json(const phi_accrual_t *p, int64_t start_ms, char *buf,
                     size_t len);

#endif
