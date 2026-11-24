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
        default:
            return "UNKNOWN";
    }
}

int pulse_event_type_parse(const char *name, pulse_event_type_t *out) {
    static const char *names[] = {"JOINED",   "SUSPECTED", "DEAD",
                                  "REVIVED",  "DROPPED",   "REFUTED"};
    for (int i = 0; i < 6; i++) {
        if (strcmp(name, names[i]) == 0) {
            *out = (pulse_event_type_t)i;
            return 0;
        }
    }
    return -1;
}

void pulse_event_record(pulse_event_log_t *log, pulse_event_type_t type,
                        const char *id, uint32_t incarnation,
                        int64_t unix_ts) {
    pthread_mutex_lock(&log->lock);
    pulse_event_t *slot = &log->events[log->head];
    slot->seq = log->next_seq++;
    slot->type = type;
    snprintf(slot->id, sizeof slot->id, "%s", id);
    slot->incarnation = incarnation;
    slot->unix_ts = unix_ts;
    log->head = (log->head + 1) % log->cap;
    if (log->count < log->cap) log->count++;
    /* A waiter may be waiting for exactly this event; a broadcast
     * wakes it, and it re-checks under the lock. */
    pthread_cond_broadcast(&log->cond);
    pthread_mutex_unlock(&log->lock);
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
