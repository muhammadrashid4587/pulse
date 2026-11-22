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

void metrics_format(const pulse_metrics_t *m, char *buf, size_t len) {
    char detect[96], suspect_dead[96];
    format_group(detect, sizeof detect, m->detect_samples, m->detect_sum_ms,
                 m->detect_max_ms);
    format_group(suspect_dead, sizeof suspect_dead, m->suspect_dead_samples,
                 m->suspect_dead_sum_ms, m->suspect_dead_max_ms);
    snprintf(buf, len,
             "METRICS suspected=%lld dead=%lld refutes=%lld joins=%lld "
             "detect_latency_ms %s suspect_to_dead_ms %s",
             (long long)m->suspected, (long long)m->dead,
             (long long)m->refutes, (long long)m->joins, detect,
             suspect_dead);
}
