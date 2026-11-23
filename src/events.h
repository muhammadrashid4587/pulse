#ifndef PULSE_EVENTS_H
#define PULSE_EVENTS_H

#include <pthread.h>
#include <stdint.h>

#include "member.h"

/* Membership event log (0.29.0): the control plane's WATCH command
 * streams these. The node records one event per membership
 * transition — a peer joining the table, being suspected, declared
 * dead, coming back alive, being dropped from the table, or the
 * node itself refuting a verdict about itself — and a WATCH client
 * reads them in sequence-number order as they happen, after a
 * snapshot of the current table (see control.h).
 *
 * The log is a bounded ring: the last PULSE_EVENT_LOG_CAP events
 * are kept, older ones scroll out. A consumer that falls more than
 * a ring behind is told so (pulse_event_wait returns -1) instead of
 * the log growing without bound: WATCH then drops the connection
 * with an ERR line and the client reconnects and re-reads the
 * snapshot. Sequence numbers are 1-based and monotonic for the
 * node's life, so a client that sees a jump in them knows it
 * missed events even when the gap is smaller than the ring. The
 * log is not persisted: a restarted node starts at sequence 1,
 * and the members it restores from disk are pre-existing state,
 * not events. */

#define PULSE_EVENT_LOG_CAP 128

typedef enum {
    PULSE_EVENT_JOINED = 0,    /* a peer appeared in the table */
    PULSE_EVENT_SUSPECTED,     /* a peer went ALIVE -> SUSPECT */
    PULSE_EVENT_DEAD,          /* a peer went SUSPECT -> DEAD */
    PULSE_EVENT_REVIVED,       /* a SUSPECT/DEAD peer came back ALIVE */
    PULSE_EVENT_DROPPED,       /* a peer left the table (GC, LEAVE,
                                  or a peer-file reload drop) */
    PULSE_EVENT_REFUTED        /* this node heard itself SUSPECT/DEAD
                                  and bumped its incarnation */
} pulse_event_type_t;

typedef struct {
    uint64_t seq; /* 1-based, monotonic across the node's life */
    pulse_event_type_t type;
    char id[PULSE_ID_LEN];
    uint32_t incarnation;
    int64_t unix_ts; /* wall-clock seconds when the event was recorded */
} pulse_event_t;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pulse_event_t events[PULSE_EVENT_LOG_CAP];
    int head;      /* index of the next slot to write */
    int count;     /* events held, at most PULSE_EVENT_LOG_CAP */
    uint64_t next_seq; /* seq the next recorded event gets */
} pulse_event_log_t;

void pulse_event_log_init(pulse_event_log_t *log);

/* "JOINED", "SUSPECTED", "DEAD", "REVIVED", "DROPPED", "REFUTED":
 * the word that rides in the EVENT line for a type. */
const char *pulse_event_type_name(pulse_event_type_t type);

/* The reverse: the type for a wire word ("JOINED" -> JOINED), or -1
 * when the word names no event type. Used by the WATCH filter
 * (0.30.0), which takes the same words on the command line. */
int pulse_event_type_parse(const char *name, pulse_event_type_t *out);

/* Append one event (assigning it the next sequence number) and wake
 * any waiter. Thread-safe; never blocks. */
void pulse_event_record(pulse_event_log_t *log, pulse_event_type_t type,
                        const char *id, uint32_t incarnation,
                        int64_t unix_ts);

/* The latest sequence number recorded so far (0 when the log is
 * empty): a WATCH with no argument starts live from here. */
uint64_t pulse_event_latest_seq(pulse_event_log_t *log);

/* The earliest event with seq > after_seq. Returns 1 with *out
 * filled, 0 when no such event arrived within timeout_ms, and -1
 * when the event after after_seq has already scrolled out of the
 * ring — the caller fell too far behind and must resync (re-read
 * the snapshot) instead of trusting the stream. A future after_seq
 * (nothing recorded that far yet) just waits for the next event. */
int pulse_event_wait(pulse_event_log_t *log, uint64_t after_seq,
                     pulse_event_t *out, int timeout_ms);

#endif
