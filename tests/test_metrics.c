/* Unit tests for failure-detector metrics (no networking). */
#include <stdio.h>
#include <string.h>

#include "metrics.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

int main(void) {
    /* A fresh metrics struct is all zeros, and formats with "-" for
     * the latency groups that have no samples yet. */
    pulse_metrics_t m;
    metrics_init(&m);
    char buf[512];
    metrics_format(&m, buf, sizeof buf);
    CHECK(m.suspected == 0 && m.dead == 0 && m.refutes == 0 && m.joins == 0);
    CHECK(strstr(buf, "suspected=0 dead=0 refutes=0 joins=0") != NULL);
    CHECK(strstr(buf, "detect_latency_ms mean=- max=- samples=0") != NULL);
    CHECK(strstr(buf, "suspect_to_dead_ms mean=- max=- samples=0") != NULL);
    CHECK(m.detect_hist[0] == 0 && m.detect_hist[4] == 0);
    CHECK(strstr(buf, "hist=[0,0,0,0,0]") != NULL);
    char hbuf[64];
    metrics_detect_hist_json(&m, hbuf, sizeof hbuf);
    CHECK(strcmp(hbuf, "0,0,0,0,0") == 0);

    /* Locally detected suspicions feed the latency stats: mean is the
     * integer mean, max is the largest sample. */
    metrics_note_suspect(&m, 100);
    metrics_note_suspect(&m, 200);
    CHECK(m.suspected == 2);
    CHECK(m.detect_samples == 2);
    CHECK(m.detect_sum_ms == 300);
    CHECK(m.detect_max_ms == 200);
    metrics_format(&m, buf, sizeof buf);
    CHECK(strstr(buf, "detect_latency_ms mean=150 max=200 samples=2") != NULL);

    /* The histogram buckets each sample: 50 -> <100, 100 -> 100-249,
     * 250 -> 250-499, 500 -> 500-999, 1000 -> >=1000. Boundaries land
     * in the higher bucket, so a sample exactly at a bound is never
     * counted as faster than it was. */
    metrics_note_suspect(&m, 50);
    metrics_note_suspect(&m, 100);
    metrics_note_suspect(&m, 250);
    metrics_note_suspect(&m, 500);
    metrics_note_suspect(&m, 1000);
    metrics_note_suspect(&m, 5000);
    CHECK(m.detect_hist[0] == 1);
    CHECK(m.detect_hist[1] == 3); /* 100, 100, 200 */
    CHECK(m.detect_hist[2] == 1); /* 250 */
    CHECK(m.detect_hist[3] == 1); /* 500 */
    CHECK(m.detect_hist[4] == 2); /* 1000, 5000 */
    CHECK(m.detect_hist[0] + m.detect_hist[1] + m.detect_hist[2] +
          m.detect_hist[3] + m.detect_hist[4] == m.detect_samples);
    metrics_format(&m, buf, sizeof buf);
    CHECK(strstr(buf, "hist=[1,3,1,1,2]") != NULL);
    metrics_detect_hist_json(&m, hbuf, sizeof hbuf);
    CHECK(strcmp(hbuf, "1,3,1,1,2") == 0);

    /* A suspicion learned from gossip bumps the count but adds no
     * latency sample: there is no local heartbeat to measure. */
    metrics_note_suspect(&m, -1);
    CHECK(m.suspected == 9);
    CHECK(m.detect_samples == 8);
    CHECK(m.detect_hist[0] + m.detect_hist[1] + m.detect_hist[2] +
          m.detect_hist[3] + m.detect_hist[4] == m.detect_samples);
    metrics_format(&m, buf, sizeof buf);
    CHECK(strstr(buf, "suspected=9") != NULL);
    CHECK(strstr(buf, "hist=[1,3,1,1,2]") != NULL);

    /* Same split for SUSPECT -> DEAD: a locally swept dead member
     * records how long it sat suspect; a gossiped one does not. */
    metrics_note_dead(&m, 300);
    CHECK(m.dead == 1);
    CHECK(m.suspect_dead_samples == 1);
    CHECK(m.suspect_dead_max_ms == 300);
    metrics_note_dead(&m, -1);
    CHECK(m.dead == 2);
    CHECK(m.suspect_dead_samples == 1);
    metrics_format(&m, buf, sizeof buf);
    CHECK(strstr(buf, "suspect_to_dead_ms mean=300 max=300 samples=1") != NULL);

    /* Refutes and joins are plain counters. */
    metrics_note_refute(&m);
    metrics_note_refute(&m);
    metrics_note_join(&m);
    CHECK(m.refutes == 2);
    CHECK(m.joins == 1);
    metrics_format(&m, buf, sizeof buf);
    CHECK(strstr(buf, "refutes=2 joins=1") != NULL);

    printf("test_metrics: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
