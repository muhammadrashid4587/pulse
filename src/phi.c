#define _POSIX_C_SOURCE 200809L
#include "phi.h"

#include <math.h>

void phi_init(phi_accrual_t *p) {
    p->count = 0;
    p->head = 0;
    p->sum = 0.0;
    p->sumsq = 0.0;
    p->last_ms = 0;
    p->have_last = 0;
}

void phi_heartbeat(phi_accrual_t *p, int64_t now_ms) {
    if (p->have_last) {
        double iv = (double)(now_ms - p->last_ms);
        if (iv > 0) {
            if (p->count < PULSE_PHI_WINDOW) {
                p->intervals[p->head] = iv;
                p->count++;
            } else {
                /* Window full: drop the oldest sample first. */
                double old = p->intervals[p->head];
                p->sum -= old;
                p->sumsq -= old * old;
                p->intervals[p->head] = iv;
            }
            p->head = (p->head + 1) % PULSE_PHI_WINDOW;
            p->sum += iv;
            p->sumsq += iv * iv;
        }
    }
    p->last_ms = now_ms;
    p->have_last = 1;
}

double phi_value(const phi_accrual_t *p, int64_t now_ms) {
    if (!p->have_last || p->count < PULSE_PHI_MIN_SAMPLES) return 0.0;
    double mean = p->sum / p->count;
    double var = p->sumsq / p->count - mean * mean;
    if (var < 0) var = 0; /* rounding guard */
    double stddev = sqrt(var);
    /* Floor the spread. With perfectly regular arrivals the variance
     * is ~0 and any tiny scheduling delay would look infinitely
     * suspicious, so assume at least 100 ms of spread (or 5% of the
     * mean for slow heartbeats, whichever is larger). */
    double floor = mean * 0.05;
    if (floor < 100.0) floor = 100.0;
    if (stddev < floor) stddev = floor;
    double t = (double)(now_ms - p->last_ms);
    double z = (t - mean) / stddev;
    double prob = 0.5 * erfc(z / sqrt(2.0));
    if (prob <= 0.0) return PULSE_PHI_MAX;
    double phi = -log10(prob);
    if (phi > PULSE_PHI_MAX) phi = PULSE_PHI_MAX;
    return phi;
}
