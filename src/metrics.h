#ifndef PULSE_METRICS_H
#define PULSE_METRICS_H

#include <stddef.h>
#include <stdint.h>

#include "member.h" /* PULSE_ID_LEN: seed ids are "host:port" */

/* Failure-detector metrics: counts of membership transitions this node
 * has seen, plus the two latencies that say how fast detection actually
 * was. Latencies are only measured for transitions this node detects
 * itself (its own probe cycle): a SUSPECT learned from gossip has no
 * local last-heartbeat to measure against, so it bumps the count but
 * adds no latency sample.
 *
 *   detect latency: last direct contact with the peer -> SUSPECT.
 *   suspect-to-dead: SUSPECT -> DEAD, i.e. how long a suspect sat
 *     before the sweep declared it dead (should track
 *     --suspect-timeout-ms).
 *
 * The struct is plain data: the node updates it while holding its
 * table lock, and the control plane reads a copy through the same
 * lock. No locking inside this module. */
/* Detection-latency histogram: 5 buckets over the locally measured
 * detect latencies, so the mean/max cannot hide a slow tail. A
 * detection that usually lands in ~200 ms but sometimes takes over a
 * second looks fine as a mean and is exactly what the ward-device
 * use case needs to see. Bucket upper bounds (ms): 100, 250, 500,
 * 1000; the last bucket holds everything >= 1000. Only locally
 * measured latencies are bucketed, like the mean/max: a suspicion
 * learned from gossip adds no sample. */
#define PULSE_DETECT_HIST_BUCKETS 5
#define PULSE_DETECT_HIST_BOUNDS { 100, 250, 500, 1000 }

/* Probe-RTT histogram (0.48.0): 5 buckets over the direct-probe
 * round-trip times, so the mean RTT cannot hide a slow tail — a
 * peer that usually answers in under a millisecond but sometimes
 * takes 60 ms looks fine as a mean and is exactly what the ward
 * use case needs to see before the peer is suspected. Bucket upper
 * bounds (ms): 1, 5, 10, 50; the last bucket holds everything
 * >= 50. RTTs are measured in whole milliseconds, so bucket 0 is
 * the sub-millisecond loopback case (rtt_ms == 0). Every answered
 * direct probe adds one sample, including one that arrived via an
 * indirect probe (its sample is an upper bound on the direct RTT,
 * like the per-peer mean it feeds). */
#define PULSE_RTT_HIST_BUCKETS 5
#define PULSE_RTT_HIST_BOUNDS { 1, 5, 10, 50 }

/* Per-seed seed-rejoin breakdown (0.54.0, last rejoin 0.55.0): the
 * seed_rejoins total above says how many re-JOINs the retry sent,
 * but not to which seed — a node with three seeds and a climbing
 * total cannot tell which one is still down. One entry per seed
 * that has been re-joined, in first-rejoin order; at most
 * PULSE_MAX_SEED_REJOINS entries, which is the seed list's own
 * cap, so the table can never overflow. A seed that was never
 * re-joined has no entry (its count is implicitly 0), and the
 * table is local measurement like the total: never gossiped,
 * never persisted, reset on restart. */
#define PULSE_MAX_SEED_REJOINS 16

typedef struct {
    char id[PULSE_ID_LEN]; /* seed id, "host:port", IPv6 bracketed */
    int64_t count;         /* re-JOINs sent to this seed */
    /* Per-seed last rejoin (0.55.0): wall-clock time of the most
     * recent re-JOIN sent to this seed, as unix milliseconds —
     * the same clock as the peer-file last_reload, but in ms, so
     * an operator can tell when each seed was last retried (and,
     * against the log, whether the retry is still firing). 0
     * while the entry exists is impossible: the entry is created
     * by the first re-JOIN, which sets this to that attempt's
     * time. */
    int64_t last_rejoin_unix_ms;
} pulse_seed_rejoin_entry_t;

typedef struct {
    int64_t suspected; /* ALIVE -> SUSPECT transitions seen */
    int64_t dead;      /* SUSPECT -> DEAD transitions seen */
    int64_t refutes;   /* times this node refuted gossip about itself */
    int64_t joins;     /* first sightings of a member */
    /* Seed-rejoin count (0.53.0): re-JOINs sent to a seed that was not
     * ALIVE — the ~5 s retry while the seed is down, SUSPECT/DEAD in
     * the table, or already GC'd. Zero on a node whose seeds were up
     * at startup; it is the only trace that the rejoin retry ever
     * fired, since a seed that never answers leaves no member behind
     * to count. Local measurement, never gossiped, never persisted. */
    int64_t seed_rejoins;
    /* Per-seed breakdown of seed_rejoins (0.54.0): one entry per
     * seed that has been re-joined, in first-rejoin order; the
     * counts sum to seed_rejoins. seed_rejoin_seed_count is how
     * many entries are in use. */
    pulse_seed_rejoin_entry_t seed_rejoins_by_seed[PULSE_MAX_SEED_REJOINS];
    int seed_rejoin_seed_count;
    int64_t detect_samples; /* suspicions with a measured latency */
    int64_t detect_sum_ms;
    int64_t detect_max_ms;
    int64_t detect_hist[PULSE_DETECT_HIST_BUCKETS];
    int64_t suspect_dead_samples;
    int64_t suspect_dead_sum_ms;
    int64_t suspect_dead_max_ms;
    /* Suspicion-refutation window (0.19.0): SUSPECT -> ALIVE
     * transitions, i.e. suspicions that turned out wrong — the peer
     * was alive after all (a slow peer, a dropped probe, a transient
     * partition). refute latency is how long the peer sat SUSPECT
     * before coming back: the window a false suspicion stays visible
     * to the cluster. A DEAD peer coming back is a restart, not a
     * refutation, and is not counted here. Like the other latencies,
     * only locally revived peers (direct contact) add a sample; a
     * refutation learned from gossip bumps the count only. */
    int64_t suspect_refuted;
    int64_t refute_samples;
    int64_t refute_sum_ms;
    int64_t refute_max_ms;
    /* Adaptive probe interval (0.18.0): probe cycles that ran at the
     * fast interval because at least one peer was SUSPECT. Zero on a
     * cluster that never had a suspect; it is the only trace that the
     * prober ever sped up, since the current interval itself is not
     * stored. */
    int64_t adaptive_fast_cycles;
    /* Probe-RTT histogram (0.48.0): the round-trip time of every
     * answered direct probe, bucketed alongside the mean/max — the
     * same shape as the detection-latency histogram, but over probe
     * RTTs instead of suspicion latencies. A sample is added for
     * every probe answer (rtt_ms >= 0); a probe never answered adds
     * nothing, so samples can be smaller than the number of probes
     * sent. Like the per-peer RTT, this is local measurement, never
     * gossiped and never persisted. */
    int64_t rtt_samples;
    int64_t rtt_sum_ms;
    int64_t rtt_max_ms;
    int64_t rtt_hist[PULSE_RTT_HIST_BUCKETS];
} pulse_metrics_t;

void metrics_init(pulse_metrics_t *m);
/* A peer was suspected. latency_ms is the time from its last direct
 * contact to the suspicion, or -1 when it was learned from gossip
 * and there is nothing local to measure. */
void metrics_note_suspect(pulse_metrics_t *m, int64_t latency_ms);
/* A peer was declared dead. duration_ms is how long it sat SUSPECT,
 * or -1 when learned from gossip. */
void metrics_note_dead(pulse_metrics_t *m, int64_t duration_ms);
void metrics_note_refute(pulse_metrics_t *m);
void metrics_note_join(pulse_metrics_t *m);
/* A re-JOIN was sent to the seed seed_id, which was not ALIVE (the
 * ~5 s seed rejoin retry). Counted once per re-JOIN sent, whether
 * the seed was SUSPECT/DEAD in the table or already GC'd — and in
 * the per-seed breakdown too, so the total and the breakdown always
 * agree. seed_id must be a seed id ("host:port"); a NULL id counts
 * only the total. now_unix_ms is the wall-clock time of this
 * attempt, as unix milliseconds; it becomes the seed's
 * last_rejoin_unix_ms (0.55.0), so the breakdown also says when
 * each seed was last retried. */
void metrics_note_seed_rejoin(pulse_metrics_t *m, const char *seed_id,
                              int64_t now_unix_ms);
/* A suspicion was refuted: a peer marked SUSPECT came back ALIVE.
 * duration_ms is how long it sat SUSPECT, or -1 when it was learned
 * from gossip and there is nothing local to measure. */
void metrics_note_suspect_refuted(pulse_metrics_t *m, int64_t duration_ms);
/* One probe cycle ran at the fast interval (a peer was SUSPECT). */
void metrics_note_adaptive_fast(pulse_metrics_t *m);
/* A direct probe was answered. rtt_ms is the PING-to-ACK round-trip
 * in ms (>= 0); every answer adds one sample to the mean/max and
 * one count to the RTT histogram bucket it falls in. */
void metrics_note_rtt(pulse_metrics_t *m, int64_t rtt_ms);

/* Format one METRICS line (no trailing newline) into buf:
 *   METRICS suspected=N dead=N refutes=N joins=N seed_rejoins=N
 *           seed_rejoins_by_seed=<id>=N,<id>=N|->
 *           seed_last_rejoin_ms_by_seed=<id>=N,<id>=N|->
 *           adaptive_fast_cycles=N suspect_refuted=N
 *           detect_latency_ms mean=X max=Y samples=N hist=[a,b,c,d,e]
 *           suspect_to_dead_ms mean=X max=Y samples=N
 *           refute_latency_ms mean=X max=Y samples=N
 *           probe_rtt_ms mean=X max=Y samples=N hist=[a,b,c,d,e]
 * mean/max are "-" when there are no samples. The histograms are
 * always present (all zeros before any sample). The per-seed
 * breakdown is "-" when no seed has been re-joined yet, and the
 * per-seed last-rejoin timestamps are "-" under the same rule —
 * a seed that was never re-joined has no entry in either. */
void metrics_format(const pulse_metrics_t *m, char *buf, size_t len);

/* Format the per-seed seed-rejoin breakdown as a bare comma list
 * "id=N,id=N" (no spaces) for the METRICS line, or "-" when no
 * seed has been re-joined yet. */
void metrics_seed_rejoins_text(const pulse_metrics_t *m, char *buf,
                               size_t len);

/* Format the per-seed last-rejoin timestamps as a bare comma list
 * "id=N,id=N" (no spaces) for the METRICS line, or "-" when no
 * seed has been re-joined yet. N is the wall-clock time of the
 * seed's most recent re-JOIN, as unix milliseconds. */
void metrics_seed_last_rejoin_text(const pulse_metrics_t *m, char *buf,
                                   size_t len);

/* Format the per-seed seed-rejoin breakdown as a JSON object
 * "{\"id\":N,...}" for embedding in the JSON metrics object, or
 * "{}" when no seed has been re-joined yet. Member ids are
 * host:port (IPv6 bracketed), so they carry no quote or backslash
 * and need no JSON escaping. */
void metrics_seed_rejoins_json(const pulse_metrics_t *m, char *buf,
                               size_t len);

/* Format the per-seed last-rejoin timestamps as a JSON object
 * "{\"id\":N,...}" for embedding in the JSON metrics object, or
 * "{}" when no seed has been re-joined yet. N is the wall-clock
 * time of the seed's most recent re-JOIN, as unix milliseconds. */
void metrics_seed_last_rejoin_json(const pulse_metrics_t *m, char *buf,
                                   size_t len);

/* Format the detection-latency histogram as a bare comma list
 * "a,b,c,d,e" (no brackets) for embedding in a JSON array. */
void metrics_detect_hist_json(const pulse_metrics_t *m, char *buf,
                              size_t len);

/* Format the probe-RTT histogram the same way, for the JSON
 * probe_rtt_ms.hist array. */
void metrics_rtt_hist_json(const pulse_metrics_t *m, char *buf,
                           size_t len);

#endif
