/* Unit tests for the adaptive probe interval (no networking). */
#include <stdint.h>
#include <stdio.h>

#include "adaptive.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

int main(void) {
    /* Default fast interval is half the base, floored at 25 ms. */
    CHECK(pulse_adaptive_fast_default_ms(100) == 50);
    CHECK(pulse_adaptive_fast_default_ms(200) == 100);
    CHECK(pulse_adaptive_fast_default_ms(30) == 25);
    CHECK(pulse_adaptive_fast_default_ms(25) == 25);
    /* A base already below the floor has nothing to speed up: the
     * fast interval is never slower than base, so it stays at base. */
    CHECK(pulse_adaptive_fast_default_ms(10) == 10);
    CHECK(pulse_adaptive_fast_default_ms(0) == 25);
    CHECK(pulse_adaptive_fast_default_ms(-5) == 25);

    /* No suspects: the base interval, whatever the fast setting. */
    CHECK(pulse_adaptive_interval_ms(100, 50, 0) == 100);
    CHECK(pulse_adaptive_interval_ms(100, 0, 0) == 100);
    CHECK(pulse_adaptive_interval_ms(100, 50, -1) == 100);

    /* Suspects: the configured fast interval. */
    CHECK(pulse_adaptive_interval_ms(100, 50, 1) == 50);
    CHECK(pulse_adaptive_interval_ms(100, 50, 3) == 50);

    /* Fast 0 means the default: half the base. */
    CHECK(pulse_adaptive_interval_ms(100, 0, 1) == 50);
    CHECK(pulse_adaptive_interval_ms(200, 0, 2) == 100);

    /* Clamps: fast is never below the 25 ms floor and never slower
     * than the base, however the operator configures it. */
    CHECK(pulse_adaptive_interval_ms(100, 10, 1) == 25);
    CHECK(pulse_adaptive_interval_ms(100, 500, 1) == 100);
    CHECK(pulse_adaptive_interval_ms(30, 0, 1) == 25);
    CHECK(pulse_adaptive_interval_ms(20, 0, 1) == 20);

    /* A nonsense base falls back to the floor rather than spinning. */
    CHECK(pulse_adaptive_interval_ms(0, 50, 1) == 25);
    CHECK(pulse_adaptive_interval_ms(-10, 50, 0) == 25);

    /* Probe-interval jitter (0.50.0): each cycle sleeps
     * base * (1 + u) with u uniform in [-frac, +frac]. */
    {
        uint32_t st;

        /* Zero fraction means no jitter at all: the exact base,
         * whatever the state. */
        st = 12345;
        CHECK(pulse_jittered_interval_ms(100, 0.0, &st) == 100);
        st = 12345;
        CHECK(pulse_jittered_interval_ms(100, -0.5, &st) == 100);

        /* Bounds: 1000 draws at ±10% stay inside [90, 110]. */
        st = 42;
        int lo = 1000, hi = 0, seen_both_sides = 0;
        for (int i = 0; i < 1000; i++) {
            int v = pulse_jittered_interval_ms(100, 0.1, &st);
            if (v < lo) lo = v;
            if (v > hi) hi = v;
            if (v < 100) seen_both_sides |= 1;
            if (v > 100) seen_both_sides |= 2;
        }
        CHECK(lo >= 90);
        CHECK(hi <= 110);
        CHECK(seen_both_sides == 3); /* draws land on both sides */

        /* The mean stays at the base: 10000 draws at ±10% average
         * within 2 ms of 100, so jitter de-synchronises without
         * speeding up or slowing down the cluster on average. */
        st = 7;
        long sum = 0;
        for (int i = 0; i < 10000; i++)
            sum += pulse_jittered_interval_ms(100, 0.1, &st);
        double mean = (double)sum / 10000.0;
        CHECK(mean > 98.0 && mean < 102.0);

        /* Deterministic: the same seed replays the same sequence,
         * so a test can reproduce any draw exactly. */
        uint32_t a = 99, b = 99;
        int same = 1;
        for (int i = 0; i < 50; i++) {
            if (pulse_jittered_interval_ms(100, 0.1, &a) !=
                pulse_jittered_interval_ms(100, 0.1, &b))
                same = 0;
        }
        CHECK(same);
        /* And a different seed draws a different sequence: the raw
         * draws differ at once, and the jittered intervals differ
         * somewhere over 50 cycles. */
        uint32_t c = 1, d = 2;
        CHECK(pulse_jitter_next(&c) != pulse_jitter_next(&d));
        c = 1;
        d = 2;
        int differ = 0;
        for (int i = 0; i < 50; i++) {
            if (pulse_jittered_interval_ms(100, 0.1, &c) !=
                pulse_jittered_interval_ms(100, 0.1, &d))
                differ = 1;
        }
        CHECK(differ);

        /* The fraction clamps at 0.9: a caller asking for ±200%
         * gets ±90%, never a negative interval. */
        st = 42;
        lo = 1000;
        for (int i = 0; i < 1000; i++) {
            int v = pulse_jittered_interval_ms(100, 2.0, &st);
            if (v < lo) lo = v;
        }
        CHECK(lo >= 10);

        /* The 25 ms floor: a base at the floor with a wide fraction
         * never sleeps below 25 ms, so jitter cannot make the loop
         * spin. A base already below the floor floors at 1 ms. */
        st = 42;
        lo = 1000;
        for (int i = 0; i < 1000; i++) {
            int v = pulse_jittered_interval_ms(30, 0.5, &st);
            if (v < lo) lo = v;
        }
        CHECK(lo >= 25);
        st = 42;
        lo = 1000;
        for (int i = 0; i < 1000; i++) {
            int v = pulse_jittered_interval_ms(10, 0.5, &st);
            if (v < lo) lo = v;
        }
        CHECK(lo >= 1);

        /* A nonsense base falls back to the floor; a NULL state
         * returns the base unchanged instead of crashing. */
        CHECK(pulse_jittered_interval_ms(0, 0.1, &st) == 25);
        CHECK(pulse_jittered_interval_ms(100, 0.1, NULL) == 100);
    }

    /* Seed-rejoin backoff (0.56.0): the delay before the next
     * re-JOIN after N consecutive unanswered attempts — the base
     * 5 s, doubling each time, capped at 30 s. */
    CHECK(pulse_rejoin_backoff_ms(0) == 5000);
    CHECK(pulse_rejoin_backoff_ms(-1) == 5000);
    CHECK(pulse_rejoin_backoff_ms(-100) == 5000);
    CHECK(pulse_rejoin_backoff_ms(1) == 10000);
    CHECK(pulse_rejoin_backoff_ms(2) == 20000);
    CHECK(pulse_rejoin_backoff_ms(3) == 30000); /* 40 s -> cap */
    CHECK(pulse_rejoin_backoff_ms(4) == 30000);
    CHECK(pulse_rejoin_backoff_ms(100) == 30000);
    CHECK(pulse_rejoin_backoff_ms(1000000) == 30000);

    printf("%d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
