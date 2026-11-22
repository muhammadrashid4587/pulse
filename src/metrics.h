#ifndef PULSE_METRICS_H
#define PULSE_METRICS_H

#include <stddef.h>
#include <stdint.h>

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
typedef struct {
    int64_t suspected; /* ALIVE -> SUSPECT transitions seen */
    int64_t dead;      /* SUSPECT -> DEAD transitions seen */
    int64_t refutes;   /* times this node refuted gossip about itself */
    int64_t joins;     /* first sightings of a member */
    int64_t detect_samples; /* suspicions with a measured latency */
    int64_t detect_sum_ms;
    int64_t detect_max_ms;
    int64_t suspect_dead_samples;
    int64_t suspect_dead_sum_ms;
    int64_t suspect_dead_max_ms;
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

/* Format one METRICS line (no trailing newline) into buf:
 *   METRICS suspected=N dead=N refutes=N joins=N
 *           detect_latency_ms mean=X max=Y samples=N
 *           suspect_to_dead_ms mean=X max=Y samples=N
 * mean/max are "-" when there are no samples. */
void metrics_format(const pulse_metrics_t *m, char *buf, size_t len);

#endif
