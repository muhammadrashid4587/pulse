#ifndef PULSE_PHI_H
#define PULSE_PHI_H

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

typedef struct {
    double intervals[PULSE_PHI_WINDOW];
    int count;  /* samples in the window (<= PULSE_PHI_WINDOW) */
    int head;   /* next slot to overwrite */
    double sum;
    double sumsq;
    int64_t last_ms; /* last heartbeat arrival, monotonic ms */
    int have_last;
} phi_accrual_t;

void phi_init(phi_accrual_t *p);
/* Record a heartbeat arrival at now_ms. */
void phi_heartbeat(phi_accrual_t *p, int64_t now_ms);
/* phi for this peer at now_ms. 0.0 when there is not enough history
 * to judge (fewer than PULSE_PHI_MIN_SAMPLES intervals). */
double phi_value(const phi_accrual_t *p, int64_t now_ms);

#endif
