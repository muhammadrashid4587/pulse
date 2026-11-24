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

void metrics_note_suspect_refuted(pulse_metrics_t *m, int64_t duration_ms) {
    m->suspect_refuted++;
    if (duration_ms >= 0) {
        m->refute_samples++;
        m->refute_sum_ms += duration_ms;
        if (duration_ms > m->refute_max_ms) m->refute_max_ms = duration_ms;
    }
}

void metrics_note_join(pulse_metrics_t *m) { m->joins++; }

void metrics_note_seed_rejoin(pulse_metrics_t *m, const char *seed_id) {
    m->seed_rejoins++;
    if (seed_id == NULL) return;
    /* Per-seed breakdown (0.54.0): find the seed's entry, or append
     * one in first-rejoin order. The seed list holds at most 16
     * entries, so the table can never overflow; if it somehow did,
     * the total still counts and the breakdown just misses one. */
    for (int i = 0; i < m->seed_rejoin_seed_count; i++) {
        if (strcmp(m->seed_rejoins_by_seed[i].id, seed_id) == 0) {
            m->seed_rejoins_by_seed[i].count++;
            return;
        }
    }
    if (m->seed_rejoin_seed_count < PULSE_MAX_SEED_REJOINS) {
        pulse_seed_rejoin_entry_t *e =
            &m->seed_rejoins_by_seed[m->seed_rejoin_seed_count++];
        snprintf(e->id, sizeof e->id, "%s", seed_id);
        e->count = 1;
    }
}

void metrics_note_adaptive_fast(pulse_metrics_t *m) {
    m->adaptive_fast_cycles++;
}

void metrics_note_rtt(pulse_metrics_t *m, int64_t rtt_ms) {
    if (rtt_ms < 0) return;
    m->rtt_samples++;
    m->rtt_sum_ms += rtt_ms;
    if (rtt_ms > m->rtt_max_ms) m->rtt_max_ms = rtt_ms;
    /* Bucket 0 is < 1 ms, bucket 1 is 1-4, bucket 2 is 5-9,
     * bucket 3 is 10-49, bucket 4 is >= 50 — the same boundary
     * rule as the detection histogram: a sample exactly at a
     * bound lands in the higher bucket. */
    static const int64_t bounds[] = PULSE_RTT_HIST_BOUNDS;
    int bucket = 0;
    while (bucket < PULSE_RTT_HIST_BUCKETS - 1 &&
           rtt_ms >= bounds[bucket])
        bucket++;
    m->rtt_hist[bucket]++;
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

void metrics_rtt_hist_json(const pulse_metrics_t *m, char *buf,
                           size_t len) {
    snprintf(buf, len, "%lld,%lld,%lld,%lld,%lld",
             (long long)m->rtt_hist[0], (long long)m->rtt_hist[1],
             (long long)m->rtt_hist[2], (long long)m->rtt_hist[3],
             (long long)m->rtt_hist[4]);
}

void metrics_seed_rejoins_text(const pulse_metrics_t *m, char *buf,
                               size_t len) {
    if (m->seed_rejoin_seed_count <= 0) {
        snprintf(buf, len, "-");
        return;
    }
    size_t used = 0;
    for (int i = 0; i < m->seed_rejoin_seed_count; i++) {
        int w = snprintf(buf + used, len - used, "%s%s=%lld",
                         i == 0 ? "" : ",",
                         m->seed_rejoins_by_seed[i].id,
                         (long long)m->seed_rejoins_by_seed[i].count);
        if (w < 0 || (size_t)w >= len - used) return;
        used += (size_t)w;
    }
}

void metrics_seed_rejoins_json(const pulse_metrics_t *m, char *buf,
                              size_t len) {
    size_t used = 0;
    int w = snprintf(buf, len, "{");
    if (w < 0 || (size_t)w >= len) return;
    used += (size_t)w;
    for (int i = 0; i < m->seed_rejoin_seed_count; i++) {
        w = snprintf(buf + used, len - used, "%s\"%s\":%lld",
                     i == 0 ? "" : ",",
                     m->seed_rejoins_by_seed[i].id,
                     (long long)m->seed_rejoins_by_seed[i].count);
        if (w < 0 || (size_t)w >= len - used) return;
        used += (size_t)w;
    }
    snprintf(buf + used, len - used, "}");
}

void metrics_format(const pulse_metrics_t *m, char *buf, size_t len) {
    char detect[96], suspect_dead[96], refute[96], rtt[96];
    char hist[64], rtthist[64];
    /* Per-seed breakdown: 16 seeds at most, each "host:port=N" —
     * a 64-char id plus a count — fits in ~1.2 KB worst case. */
    char by_seed[1408];
    format_group(detect, sizeof detect, m->detect_samples, m->detect_sum_ms,
                 m->detect_max_ms);
    format_group(suspect_dead, sizeof suspect_dead, m->suspect_dead_samples,
                 m->suspect_dead_sum_ms, m->suspect_dead_max_ms);
    format_group(refute, sizeof refute, m->refute_samples, m->refute_sum_ms,
                 m->refute_max_ms);
    format_group(rtt, sizeof rtt, m->rtt_samples, m->rtt_sum_ms,
                 m->rtt_max_ms);
    metrics_detect_hist_json(m, hist, sizeof hist);
    metrics_rtt_hist_json(m, rtthist, sizeof rtthist);
    metrics_seed_rejoins_text(m, by_seed, sizeof by_seed);
    snprintf(buf, len,
             "METRICS suspected=%lld dead=%lld refutes=%lld joins=%lld "
             "seed_rejoins=%lld seed_rejoins_by_seed=%s "
             "adaptive_fast_cycles=%lld suspect_refuted=%lld "
             "detect_latency_ms %s hist=[%s] suspect_to_dead_ms %s "
             "refute_latency_ms %s probe_rtt_ms %s hist=[%s]",
             (long long)m->suspected, (long long)m->dead,
             (long long)m->refutes, (long long)m->joins,
             (long long)m->seed_rejoins, by_seed,
             (long long)m->adaptive_fast_cycles,
             (long long)m->suspect_refuted, detect, hist,
             suspect_dead, refute, rtt, rtthist);
}
