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
