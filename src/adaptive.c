#include <math.h>
#include <stdint.h>

#include "adaptive.h"

int pulse_adaptive_fast_default_ms(int base_ms) {
    if (base_ms <= 0) return PULSE_ADAPTIVE_MIN_MS;
    int fast = base_ms / 2;
    if (fast < PULSE_ADAPTIVE_MIN_MS) fast = PULSE_ADAPTIVE_MIN_MS;
    if (fast > base_ms) fast = base_ms;
    return fast;
}

int pulse_adaptive_interval_ms(int base_ms, int fast_ms, int n_suspect) {
    if (base_ms <= 0) return PULSE_ADAPTIVE_MIN_MS;
    if (n_suspect <= 0) return base_ms;
    if (fast_ms <= 0) fast_ms = pulse_adaptive_fast_default_ms(base_ms);
    if (fast_ms < PULSE_ADAPTIVE_MIN_MS) fast_ms = PULSE_ADAPTIVE_MIN_MS;
    if (fast_ms > base_ms) fast_ms = base_ms;
    return fast_ms;
}

uint32_t pulse_jitter_next(uint32_t *state) {
    uint32_t x = *state;
    if (x == 0) x = 0x9E3779B9u; /* golden-ratio constant: a zero seed
                                   still draws instead of sticking at 0 */
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

int pulse_jittered_interval_ms(int base_ms, double jitter_frac,
                               uint32_t *state) {
    if (base_ms <= 0) return PULSE_ADAPTIVE_MIN_MS;
    if (!state) return base_ms;
    if (jitter_frac <= 0.0) return base_ms;
    if (jitter_frac > 0.9) jitter_frac = 0.9;
    /* Uniform in [-1, 1]: the 32-bit draw over 2^32, scaled and shifted. */
    double u = (double)pulse_jitter_next(state) / 4294967296.0 * 2.0 - 1.0;
    int jittered = base_ms + (int)llround((double)base_ms * jitter_frac * u);
    int floor_ms = base_ms >= PULSE_ADAPTIVE_MIN_MS
                       ? PULSE_ADAPTIVE_MIN_MS
                       : 1;
    if (jittered < floor_ms) jittered = floor_ms;
    return jittered;
}
