#include "metrics.h"

#include <stdio.h>
#include <string.h>

void metrics_init(pulse_metrics_t *m) {
    memset(m, 0, sizeof *m);
}

void metrics_note_suspect(pulse_metrics_t *m, int64_t latency_ms) {
    m->suspected++;
    if (latency_ms >= 0) {
        m->detect_samples++;
        m->detect_sum_ms += latency_ms;
        if (latency_ms > m->detect_max_ms) m->detect_max_ms = latency_ms;
        /* Bucket 0 is < 100 ms, bucket 1 is 100-249, bucket 2 is
         * 250-499, bucket 3 is 500-999, bucket 4 is >= 1000. */
        static const int64_t bounds[] = PULSE_DETECT_HIST_BOUNDS;
        int bucket = 0;
        while (bucket < PULSE_DETECT_HIST_BUCKETS - 1 &&
               latency_ms >= bounds[bucket])
            bucket++;
        m->detect_hist[bucket]++;
    }
}

void metrics_note_dead(pulse_metrics_t *m, int64_t duration_ms) {
    m->dead++;
    if (duration_ms >= 0) {
        m->suspect_dead_samples++;
        m->suspect_dead_sum_ms += duration_ms;
        if (duration_ms > m->suspect_dead_max_ms)
            m->suspect_dead_max_ms = duration_ms;
    }
}

void metrics_note_refute(pulse_metrics_t *m) { m->refutes++; }

void metrics_note_join(pulse_metrics_t *m) { m->joins++; }

void metrics_note_adaptive_fast(pulse_metrics_t *m) {
    m->adaptive_fast_cycles++;
}

/* One latency group as "mean=X max=Y samples=N", with "-" for mean/max
 * when there are no samples yet. */
static void format_group(char *buf, size_t len, int64_t samples,
                         int64_t sum_ms, int64_t max_ms) {
    if (samples <= 0) {
        snprintf(buf, len, "mean=- max=- samples=0");
    } else {
        snprintf(buf, len, "mean=%lld max=%lld samples=%lld",
                 (long long)(sum_ms / samples), (long long)max_ms,
                 (long long)samples);
    }
}

void metrics_detect_hist_json(const pulse_metrics_t *m, char *buf,
                              size_t len) {
    snprintf(buf, len, "%lld,%lld,%lld,%lld,%lld",
             (long long)m->detect_hist[0], (long long)m->detect_hist[1],
             (long long)m->detect_hist[2], (long long)m->detect_hist[3],
             (long long)m->detect_hist[4]);
}

void metrics_format(const pulse_metrics_t *m, char *buf, size_t len) {
    char detect[96], suspect_dead[96], hist[64];
    format_group(detect, sizeof detect, m->detect_samples, m->detect_sum_ms,
                 m->detect_max_ms);
    format_group(suspect_dead, sizeof suspect_dead, m->suspect_dead_samples,
                 m->suspect_dead_sum_ms, m->suspect_dead_max_ms);
    metrics_detect_hist_json(m, hist, sizeof hist);
    snprintf(buf, len,
             "METRICS suspected=%lld dead=%lld refutes=%lld joins=%lld "
             "adaptive_fast_cycles=%lld "
             "detect_latency_ms %s hist=[%s] suspect_to_dead_ms %s",
             (long long)m->suspected, (long long)m->dead,
             (long long)m->refutes, (long long)m->joins,
             (long long)m->adaptive_fast_cycles, detect, hist,
             suspect_dead);
}
