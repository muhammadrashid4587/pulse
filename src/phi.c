#define _POSIX_C_SOURCE 200809L
#include "phi.h"

#include <math.h>
#include <stdio.h>

void phi_init(phi_accrual_t *p) {
    p->count = 0;
    p->head = 0;
    p->sum = 0.0;
    p->sumsq = 0.0;
    p->last_ms = 0;
    p->have_last = 0;
    p->hist_count = 0;
    p->hist_head = 0;
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

void phi_sample(phi_accrual_t *p, int64_t now_ms) {
    /* Same-millisecond repeat: the probe cycle is at least 25 ms, so
     * this only fires when two callers sample in one instant — the
     * ring must hold distinct times, not duplicates. */
    if (p->hist_count > 0) {
        int last = (p->hist_head - 1 + PULSE_PHI_HISTORY) %
                   PULSE_PHI_HISTORY;
        if (p->hist_ms[last] == now_ms) return;
    }
    p->hist_phi[p->hist_head] = phi_value(p, now_ms);
    p->hist_ms[p->hist_head] = now_ms;
    p->hist_head = (p->hist_head + 1) % PULSE_PHI_HISTORY;
    if (p->hist_count < PULSE_PHI_HISTORY) p->hist_count++;
}

int phi_history_json(const phi_accrual_t *p, int64_t start_ms, char *buf,
                     size_t len) {
    size_t used = 0;
    int w = snprintf(buf + used, len - used, "[");
    if (w < 0 || (size_t)w >= len - used) return -1;
    used += (size_t)w;
    /* Oldest first: the ring's oldest slot is hist_head - hist_count
     * (wrapped), then forward. */
    int start = p->hist_head - p->hist_count;
    if (start < 0) start += PULSE_PHI_HISTORY;
    for (int k = 0; k < p->hist_count; k++) {
        int idx = (start + k) % PULSE_PHI_HISTORY;
        w = snprintf(buf + used, len - used, "%s[%lld,%.2f]",
                     k == 0 ? "" : ",",
                     (long long)(p->hist_ms[idx] - start_ms),
                     p->hist_phi[idx]);
        if (w < 0 || (size_t)w >= len - used) return -1;
        used += (size_t)w;
    }
    w = snprintf(buf + used, len - used, "]");
    if (w < 0 || (size_t)w >= len - used) return -1;
    used += (size_t)w;
    return (int)used;
}

void phi_history_stats(const phi_accrual_t *p, double *latest, double *max,
                       int *count) {
    *latest = 0.0;
    *max = 0.0;
    *count = p->hist_count;
    if (p->hist_count == 0) return;
    /* Oldest first, the same ring walk as phi_history_json; the
     * latest is the last one walked. */
    int start = p->hist_head - p->hist_count;
    if (start < 0) start += PULSE_PHI_HISTORY;
    for (int k = 0; k < p->hist_count; k++) {
        int idx = (start + k) % PULSE_PHI_HISTORY;
        double v = p->hist_phi[idx];
        if (v > *max) *max = v;
        *latest = v;
    }
}

void phi_history_hist(const phi_accrual_t *p, const double *bounds,
                      int n_bounds, int *counts, double *sum, int *count) {
    for (int b = 0; b <= n_bounds; b++) counts[b] = 0;
    *sum = 0.0;
    *count = p->hist_count;
    if (p->hist_count == 0) return;
    /* Oldest first, the same ring walk as phi_history_json. */
    int start = p->hist_head - p->hist_count;
    if (start < 0) start += PULSE_PHI_HISTORY;
    for (int k = 0; k < p->hist_count; k++) {
        int idx = (start + k) % PULSE_PHI_HISTORY;
        double v = p->hist_phi[idx];
        *sum += v;
        for (int b = 0; b < n_bounds; b++) {
            if (v <= bounds[b]) counts[b]++;
        }
    }
    /* The +Inf bucket holds every sample. */
    counts[n_bounds] = p->hist_count;
}
