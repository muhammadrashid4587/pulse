#define _POSIX_C_SOURCE 200809L
#include "events.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

void pulse_event_log_init(pulse_event_log_t *log, int cap) {
    memset(log, 0, sizeof *log);
    if (cap <= 0) cap = PULSE_EVENT_LOG_CAP;
    log->cap = cap;
    /* The ring is heap-allocated so the cap is a runtime choice
     * (--event-log-cap), not a compile-time one. A node never
     * frees its log, so an allocation failure here is fatal
     * rather than a degraded start with a 0-capacity ring. */
    log->events = calloc((size_t)cap, sizeof *log->events);
    if (!log->events) {
        fprintf(stderr, "pulse: cannot allocate event log (%d events)\n",
                cap);
        exit(1);
    }
    pthread_mutex_init(&log->lock, NULL);
    pthread_cond_init(&log->cond, NULL);
    log->next_seq = 1;
}

void pulse_event_log_free(pulse_event_log_t *log) {
    free(log->events);
    log->events = NULL;
    log->cap = 0;
    pthread_mutex_destroy(&log->lock);
    pthread_cond_destroy(&log->cond);
}

int pulse_event_log_cap(pulse_event_log_t *log) {
    pthread_mutex_lock(&log->lock);
    int cap = log->cap;
    pthread_mutex_unlock(&log->lock);
    return cap;
}

const char *pulse_event_type_name(pulse_event_type_t type) {
    switch (type) {
        case PULSE_EVENT_JOINED:
            return "JOINED";
        case PULSE_EVENT_SUSPECTED:
            return "SUSPECTED";
        case PULSE_EVENT_DEAD:
            return "DEAD";
        case PULSE_EVENT_REVIVED:
            return "REVIVED";
        case PULSE_EVENT_DROPPED:
            return "DROPPED";
        case PULSE_EVENT_REFUTED:
            return "REFUTED";
        case PULSE_EVENT_QUIET:
            return "QUIET";
        default:
            return "UNKNOWN";
    }
}

int pulse_event_type_parse(const char *name, pulse_event_type_t *out) {
    static const char *names[] = {"JOINED",   "SUSPECTED", "DEAD",
                                  "REVIVED",  "DROPPED",   "REFUTED",
                                  "QUIET"};
    for (int i = 0; i < 7; i++) {
        if (strcmp(name, names[i]) == 0) {
            *out = (pulse_event_type_t)i;
            return 0;
        }
    }
    return -1;
}

/* The member state an event leaves its member in (0.39.0): JOINED,
 * REVIVED and REFUTED all land on ALIVE — a join is a live peer
 * appearing, a revival is a SUSPECT/DEAD peer answering again, a
 * refutation is this node bumping its incarnation to answer a
 * verdict about itself — SUSPECTED lands on SUSPECT and DEAD on
 * DEAD. DROPPED has no state to land on: the member left the
 * table, so it never matches a state filter — a watcher that wants
 * removals subscribes to the DROPPED type instead. Returns 1 with
 * *out filled, or 0 for DROPPED. */
int pulse_event_result_state(pulse_event_type_t type, member_state_t *out) {
    switch (type) {
        case PULSE_EVENT_JOINED:
        case PULSE_EVENT_REVIVED:
        case PULSE_EVENT_REFUTED:
        case PULSE_EVENT_QUIET:
            *out = MEMBER_ALIVE;
            return 1;
        case PULSE_EVENT_SUSPECTED:
            *out = MEMBER_SUSPECT;
            return 1;
        case PULSE_EVENT_DEAD:
            *out = MEMBER_DEAD;
            return 1;
        case PULSE_EVENT_DROPPED:
            return 0;
        default:
            return 0;
    }
}

/* Insert one event into the ring at the given sequence number.
 * Caller holds the lock. The ring keeps the last <cap> events:
 * inserting past the capacity scrolls the oldest out, exactly as
 * a fresh record does. */
static void log_insert(pulse_event_log_t *log, uint64_t seq,
                       pulse_event_type_t type, const char *id,
                       uint32_t incarnation, int64_t unix_ts, double phi,
                       int64_t last_seen_ms) {
    pulse_event_t *slot = &log->events[log->head];
    slot->seq = seq;
    slot->type = type;
    snprintf(slot->id, sizeof slot->id, "%s", id);
    slot->incarnation = incarnation;
    slot->unix_ts = unix_ts;
    slot->phi = phi;
    slot->last_seen_ms = last_seen_ms;
    log->head = (log->head + 1) % log->cap;
    if (log->count < log->cap) log->count++;
}

uint64_t pulse_event_record(pulse_event_log_t *log, pulse_event_type_t type,
                            const char *id, uint32_t incarnation,
                            int64_t unix_ts) {
    return pulse_event_record_phi(log, type, id, incarnation, unix_ts,
                                  -1);
}

uint64_t pulse_event_record_phi(pulse_event_log_t *log,
                                pulse_event_type_t type, const char *id,
                                uint32_t incarnation, int64_t unix_ts,
                                double phi) {
    return pulse_event_record_full(log, type, id, incarnation, unix_ts,
                                   phi, -1);
}

uint64_t pulse_event_record_full(pulse_event_log_t *log,
                                 pulse_event_type_t type, const char *id,
                                 uint32_t incarnation, int64_t unix_ts,
                                 double phi, int64_t last_seen_ms) {
    pthread_mutex_lock(&log->lock);
    uint64_t seq = log->next_seq++;
    log_insert(log, seq, type, id, incarnation, unix_ts, phi,
               last_seen_ms);
    /* A waiter may be waiting for exactly this event; a broadcast
     * wakes it, and it re-checks under the lock. */
    pthread_cond_broadcast(&log->cond);
    pthread_mutex_unlock(&log->lock);
    return seq;
}

/* One line per event in the persisted log (0.38.0):
 * "<seq> <TYPE> <id> <incarnation> <unix_ts>". Ids carry no spaces
 * (host:port, bracketed for IPv6), so a plain %s round-trips. A
 * phi-mode SUSPECTED event (0.60.0) carries a trailing
 * " phi=<value>" — two decimals, the phi at the threshold
 * crossing — and an event whose member was heard from (0.61.0)
 * carries a trailing " last_seen_ms=<ms>" — the ms since this
 * node last heard from the member when the event was recorded —
 * after the phi field when both are present; the loader accepts
 * lines with either, both, or neither. An event with neither
 * writes the plain five fields, so existing consumers see
 * byte-identical lines. */

int pulse_event_log_save(pulse_event_log_t *log, const char *path) {
    /* Snapshot the held events in sequence order under the lock,
     * then write outside it: the ring must not stall recorders
     * while the file is being written. */
    pthread_mutex_lock(&log->lock);
    int count = log->count;
    pulse_event_t *held = NULL;
    if (count > 0) {
        held = malloc((size_t)count * sizeof *held);
        if (!held) {
            pthread_mutex_unlock(&log->lock);
            return -1;
        }
        for (int i = 0; i < count; i++) {
            int idx = (log->head - count + i) % log->cap;
            if (idx < 0) idx += log->cap;
            held[i] = log->events[idx];
        }
    }
    pthread_mutex_unlock(&log->lock);

    char tmp[1024];
    if (snprintf(tmp, sizeof tmp, "%s.tmp", path) >= (int)sizeof tmp) {
        free(held);
        return -1;
    }
    FILE *f = fopen(tmp, "w");
    if (!f) {
        free(held);
        return -1;
    }
    for (int i = 0; i < count; i++) {
        /* The base five fields, then each trailing field that is
         * present: phi first, then the last-contact age — the
         * same order the WATCH stream writes them. */
        fprintf(f, "%llu %s %s %u %lld",
                (unsigned long long)held[i].seq,
                pulse_event_type_name(held[i].type), held[i].id,
                held[i].incarnation, (long long)held[i].unix_ts);
        if (held[i].phi >= 0) fprintf(f, " phi=%.2f", held[i].phi);
        if (held[i].last_seen_ms >= 0)
            fprintf(f, " last_seen_ms=%lld",
                    (long long)held[i].last_seen_ms);
        fputc('\n', f);
    }
    free(held);
    if (fclose(f) != 0) return -1;
    if (rename(tmp, path) != 0) return -1;
    return count;
}

int pulse_event_log_load(pulse_event_log_t *log, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? 0 : -1;
    char line[512];
    int loaded = 0;
    while (fgets(line, sizeof line, f)) {
        unsigned long long seq;
        char type_name[16];
        char id[PULSE_ID_LEN + 16];
        unsigned inc;
        long long ts;
        int n = sscanf(line, "%llu %15s %79s %u %lld", &seq,
                       type_name, id, &inc, &ts);
        if (n != 5) continue;
        /* A phi-mode SUSPECTED event carries a trailing
         * " phi=<value>" (0.60.0) and an event whose member was
         * heard from carries a trailing " last_seen_ms=<ms>"
         * (0.61.0), after the phi when both are present. The two
         * fields are parsed independently — either may be
         * present without the other — so a line without one, or
         * without both (written before 0.60.0/0.61.0, or an event
         * with nothing measured), loads with -1 for the missing
         * one. Ids carry no spaces, so the strstr cannot match
         * inside an id. */
        double phi = -1;
        int64_t last_seen_ms = -1;
        const char *phi_at = strstr(line, " phi=");
        if (phi_at) sscanf(phi_at + 5, "%lf", &phi);
        const char *seen_at = strstr(line, " last_seen_ms=");
        if (seen_at) {
            long long v;
            if (sscanf(seen_at + 14, "%lld", &v) == 1)
                last_seen_ms = (int64_t)v;
        }
        if (seq == 0) continue;
        pulse_event_type_t type;
        if (pulse_event_type_parse(type_name, &type) != 0) continue;
        pthread_mutex_lock(&log->lock);
        /* The file is written in sequence order, so each line's
         * seq is past the last: only advance next_seq forward, in
         * case a hand-edited file ever repeats a sequence. */
        if (seq >= log->next_seq) {
            log_insert(log, seq, type, id, inc, (int64_t)ts, phi,
                       last_seen_ms);
            log->next_seq = seq + 1;
            loaded++;
        }
        pthread_mutex_unlock(&log->lock);
    }
    int err = ferror(f) ? -1 : loaded;
    fclose(f);
    return err;
}

uint64_t pulse_event_latest_seq(pulse_event_log_t *log) {
    pthread_mutex_lock(&log->lock);
    uint64_t seq = log->next_seq - 1;
    pthread_mutex_unlock(&log->lock);
    return seq;
}

uint64_t pulse_event_oldest_seq(pulse_event_log_t *log) {
    pthread_mutex_lock(&log->lock);
    uint64_t seq = log->count > 0 ? log->next_seq - (uint64_t)log->count : 0;
    pthread_mutex_unlock(&log->lock);
    return seq;
}

int pulse_event_wait(pulse_event_log_t *log, uint64_t after_seq,
                     pulse_event_t *out, int timeout_ms) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += timeout_ms / 1000;
    deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        deadline.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&log->lock);
    for (;;) {
        uint64_t wanted = after_seq + 1;
        uint64_t oldest = log->next_seq - (uint64_t)log->count;
        if (log->count > 0 && wanted < oldest) {
            /* The event the caller wants next has scrolled out of
             * the ring: it fell too far behind to catch up from
             * the buffer, so it must resync instead. */
            pthread_mutex_unlock(&log->lock);
            return -1;
        }
        if (wanted < log->next_seq) {
            /* Buffered: the wanted event sits (wanted - oldest)
             * slots past the oldest held event. The index math
             * stays in range: head - count + offset is at worst
             * -(CAP - 1), so one add of CAP fixes a negative. */
            int idx = (log->head - log->count +
                       (int)(wanted - oldest)) %
                      log->cap;
            if (idx < 0) idx += log->cap;
            *out = log->events[idx];
            pthread_mutex_unlock(&log->lock);
            return 1;
        }
        /* Caught up: wait for the next record, or time out so the
         * caller can check the client is still there. A spurious
         * wakeup just re-checks the same conditions. */
        int r = pthread_cond_timedwait(&log->cond, &log->lock,
                                       &deadline);
        if (r == ETIMEDOUT) {
            pthread_mutex_unlock(&log->lock);
            return 0;
        }
    }
}
