/* Unit tests for phi-accrual failure detection (no networking).
 * Intervals are in milliseconds; the stddev floor is 100 ms (see
 * phi.c), so with a 100 ms mean a phi of 8 needs a gap of roughly
 * mean + 5.6 * 100 ms. */
#include <stdio.h>
#include <string.h>

#include "member.h"
#include "phi.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

static void feed_regular(phi_accrual_t *p, int64_t start, int n, int64_t step) {
    for (int i = 0; i < n; i++)
        phi_heartbeat(p, start + (int64_t)i * step);
}

int main(void) {
    /* No history at all: phi is 0, never suspicious. */
    phi_accrual_t p;
    phi_init(&p);
    CHECK(phi_value(&p, 1000) == 0.0);
    phi_heartbeat(&p, 1000);
    CHECK(phi_value(&p, 10000) == 0.0); /* one arrival: no intervals yet */
    phi_heartbeat(&p, 1100);
    CHECK(phi_value(&p, 10000) == 0.0); /* one interval: still too few */

    /* Regular 100 ms heartbeats: phi stays low while the peer is on
     * time, rises as it goes quiet, and crosses 8 well after the gap
     * passes the mean by several spread units. */
    phi_accrual_t q;
    phi_init(&q);
    feed_regular(&q, 1000, 21, 100); /* 20 intervals of 100 ms, last at 3000 */
    double phi_50 = phi_value(&q, 3050);  /* 50 ms after last beat */
    double phi_100 = phi_value(&q, 3100); /* one mean after last beat */
    double phi_300 = phi_value(&q, 3300);
    double phi_1000 = phi_value(&q, 4000);
    CHECK(phi_50 < 1.0);
    CHECK(phi_100 < 1.0);
    CHECK(phi_300 < 2.0);
    CHECK(phi_100 < phi_300);
    CHECK(phi_300 < phi_1000);
    CHECK(phi_1000 > 8.0);
    /* phi is monotone in silence: later is never less suspicious. */
    double prev = -1.0;
    for (int64_t t = 3000; t <= 5000; t += 100) {
        double v = phi_value(&q, t);
        CHECK(v >= prev);
        prev = v;
    }

    /* Jittery arrivals (80/120 ms alternating, mean 100): the same
     * shape — low while on time, high after a long silence. */
    phi_accrual_t r;
    phi_init(&r);
    int64_t t = 1000;
    phi_heartbeat(&r, t);
    for (int i = 0; i < 20; i++) {
        t += (i % 2 == 0) ? 80 : 120;
        phi_heartbeat(&r, t);
    }
    CHECK(phi_value(&r, t + 100) < 2.0);
    CHECK(phi_value(&r, t + 1000) > 8.0);

    /* Zero-length gaps (same-ms arrivals) are skipped, not recorded
     * as 0 ms intervals: the window keeps its real samples. */
    phi_accrual_t s;
    phi_init(&s);
    feed_regular(&s, 1000, 11, 100);
    phi_heartbeat(&s, 2000); /* same instant as the last arrival */
    phi_heartbeat(&s, 2000);
    CHECK(phi_value(&s, 2100) < 1.0);

    /* Old samples age out of the window: after a long run of 200 ms
     * intervals the mean follows the new cadence, not the old one. */
    phi_accrual_t w;
    phi_init(&w);
    feed_regular(&w, 1000, 21, 100);  /* 20 x 100 ms */
    feed_regular(&w, 3100, 101, 200); /* 100 x 200 ms, window is 100 */
    /* Last arrival at 3100 + 100*200 = 23100; one mean later phi is low. */
    CHECK(phi_value(&w, 23300) < 1.0);
    CHECK(phi_value(&w, 25000) > 8.0);

    /* Wiring through the membership table: mark_alive is a heartbeat.
     * A peer contacted every 100 ms has low phi right after contact
     * and high phi after a long silence. */
    member_table_t tab;
    member_table_init(&tab);
    struct sockaddr_storage a;
    member_parse_addr("127.0.0.1:17101", &a);
    member_add(&tab, "127.0.0.1:17101", &a, 1);
    struct sockaddr_storage b;
    member_parse_addr("127.0.0.1:17102", &b);
    member_add(&tab, "127.0.0.1:17102", &b, 1);
    for (int i = 0; i < 11; i++)
        member_mark_alive(&tab, "127.0.0.1:17102", &b, 1, 1000 + i * 100);
    member_t *m = member_find(&tab, "127.0.0.1:17102");
    CHECK(m != NULL);
    CHECK(phi_value(&m->phi, 2050) < 1.0);
    CHECK(phi_value(&m->phi, 3000) > 8.0);
    /* A peer never contacted has no phi at all, whatever the time. */
    member_t *quiet = member_find(&tab, "127.0.0.1:17101");
    CHECK(phi_value(&quiet->phi, 100000) == 0.0);

    /* Phi history (0.47.0): phi_sample records the live phi with its
     * timestamp, oldest first, bounded at 16 samples. */
    phi_accrual_t h;
    phi_init(&h);
    char hj[1024];
    CHECK(phi_history_json(&h, 1000, hj, sizeof hj) == 2);
    CHECK(strcmp(hj, "[]") == 0); /* no sample yet */
    feed_regular(&h, 1000, 11, 100); /* 10 intervals, last at 2000 */
    phi_sample(&h, 2100); /* 100 ms after last beat: phi near 0 */
    phi_sample(&h, 2600); /* 600 ms after: phi climbing */
    phi_sample(&h, 3000); /* 1000 ms after: phi past 8 */
    CHECK(h.hist_count == 3);
    CHECK(phi_history_json(&h, 1000, hj, sizeof hj) > 0);
    /* Oldest first: ages 1100, 1600, 2000 ms since start, and the
     * values rise with the silence. */
    CHECK(strncmp(hj, "[[1100,0.", 9) == 0);
    CHECK(strstr(hj, ",[1600,") != NULL);
    CHECK(strstr(hj, ",[2000,") != NULL);
    /* Same-millisecond re-sample is skipped, not duplicated. */
    phi_sample(&h, 3000);
    CHECK(h.hist_count == 3);
    /* The ring is bounded: 20 more samples keep only the last 16,
     * still oldest first, and the newest is the latest sample. */
    for (int i = 0; i < 20; i++)
        phi_sample(&h, 3100 + i * 100);
    CHECK(h.hist_count == 16);
    CHECK(phi_history_json(&h, 1000, hj, sizeof hj) > 0);
    CHECK(strncmp(hj, "[[2500,", 7) == 0); /* 3100+400-1000, oldest kept */
    CHECK(strstr(hj, "[4000,") != NULL);   /* newest: 5000-1000 age */
    /* A buffer too small for the history reports -1. */
    char tiny[4];
    CHECK(phi_history_json(&h, 1000, tiny, sizeof tiny) == -1);

    printf("test_phi: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
