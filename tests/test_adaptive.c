/* Unit tests for the adaptive probe interval (no networking). */
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

    printf("%d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
