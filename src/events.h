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
 * The log is a bounded ring: the last <cap> events are kept, older
 * ones scroll out. A consumer that falls more than a ring behind
 * is told so (pulse_event_wait returns -1) instead of the log
 * growing without bound: WATCH then drops the connection with an
 * ERR line and the client reconnects and re-reads the snapshot.
 * Sequence numbers are 1-based and monotonic for the node's life,
 * so a client that sees a jump in them knows it missed events
 * even when the gap is smaller than the ring.
 *
 * The log is persisted (0.38.0): the node appends each event to
 * <data-dir>/events as it records it, and on startup loads the
 * file back into the ring, so sequence numbers continue across a
 * restart instead of resetting to 1 and a WATCH FROM <seq> issued
 * after a restart can still replay what happened before it. The
 * file is compacted to the last <cap> events on startup and on
 * clean shutdown, so it never grows past one run's worth of events
 * plus the ring; a missing file loads zero events (first start),
 * malformed lines are skipped, and a load failure never stops the
 * node from starting — it just starts at sequence 1.
 *
 * The cap is set at init time, not compile time: a node takes
 * --event-log-cap (0.36.0) so an operator watching a busy cluster
 * can hold more history for replay, or a small node can hold less.
 * The default is PULSE_EVENT_LOG_CAP (128). */

#define PULSE_EVENT_LOG_CAP 128 /* default ring capacity */

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
    /* Phi at threshold crossing (0.60.0): the phi value that crossed
     * the suspect threshold, carried only on a SUSPECTED event this
     * node raised itself in phi-accrual mode — the phi the probe
     * cycle measured when it marked the peer SUSPECT. -1 on every
     * other event: a fixed-timeout suspicion has no phi, and a
     * suspicion learned from gossip was measured on another node,
     * so this node must not claim a value it never saw. A WATCH
     * consumer reads it as a trailing " phi=<value>" on the EVENT
     * line, and the persisted log carries the same trailing field,
     * so the crossing survives a restart and a replay. */
    double phi;
} pulse_event_t;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pulse_event_t *events; /* heap ring, cap entries */
    int cap;       /* ring capacity, set at init (default 128) */
    int head;      /* index of the next slot to write */
    int count;     /* events held, at most cap */
    uint64_t next_seq; /* seq the next recorded event gets */
} pulse_event_log_t;

/* Allocate the ring with room for cap events (cap <= 0 means the
 * default PULSE_EVENT_LOG_CAP). Call pulse_event_log_free when the
 * log is no longer needed; a node that runs for its whole process
 * lifetime never needs to. */
void pulse_event_log_init(pulse_event_log_t *log, int cap);
void pulse_event_log_free(pulse_event_log_t *log);

/* The ring capacity this log was initialised with. */
int pulse_event_log_cap(pulse_event_log_t *log);

/* "JOINED", "SUSPECTED", "DEAD", "REVIVED", "DROPPED", "REFUTED":
 * the word that rides in the EVENT line for a type. */
const char *pulse_event_type_name(pulse_event_type_t type);

/* The reverse: the type for a wire word ("JOINED" -> JOINED), or -1
 * when the word names no event type. Used by the WATCH filter
 * (0.30.0), which takes the same words on the command line. */
int pulse_event_type_parse(const char *name, pulse_event_type_t *out);

/* The member state an event leaves its member in (0.39.0): JOINED,
 * REVIVED and REFUTED land on ALIVE, SUSPECTED on SUSPECT, DEAD on
 * DEAD. DROPPED leaves no state — the member left the table — so
 * it returns 0 and never matches a state filter. Used by the WATCH
 * state filter (0.39.0), which subscribes to events by the state
 * they land on. */
int pulse_event_result_state(pulse_event_type_t type, member_state_t *out);

/* Append one event (assigning it the next sequence number) and wake
 * any waiter. Thread-safe; never blocks. Returns the sequence
 * number assigned, so the caller can append the same event to the
 * persisted log (0.38.0). The phi is -1: no phi was measured for
 * this event. */
uint64_t pulse_event_record(pulse_event_log_t *log, pulse_event_type_t type,
                            const char *id, uint32_t incarnation,
                            int64_t unix_ts);

/* The same, carrying the phi at a threshold crossing (0.60.0):
 * the caller passes the phi the probe cycle measured when it
 * suspected the peer, or -1 when no phi was measured. */
uint64_t pulse_event_record_phi(pulse_event_log_t *log,
                                pulse_event_type_t type, const char *id,
                                uint32_t incarnation, int64_t unix_ts,
                                double phi);

/* Write every event the ring currently holds, in sequence order,
 * to path (one "<seq> <TYPE> <id> <incarnation> <unix_ts>" line per
 * event, with a trailing " phi=<value>" on a phi-mode SUSPECTED
 * event — 0.60.0), atomically via a tmp file + rename, so a crash
 * mid-write never leaves a half-written file. Returns the number
 * of events written, or -1 if the file could not be written. Used
 * to compact the persisted log on startup and on clean shutdown
 * (0.38.0). */
int pulse_event_log_save(pulse_event_log_t *log, const char *path);

/* Load events from path into the ring, in file order, restoring
 * their sequence numbers: a restarted node continues from the last
 * recorded sequence instead of starting at 1. Only the last <cap>
 * events survive (the ring drops older ones as usual), and the
 * next sequence is one past the last loaded event. Returns the
 * number of events loaded, 0 when there is no file, or -1 if the
 * file exists but could not be read at all. Malformed lines are
 * skipped, never fatal (0.38.0). A line without the trailing
 * " phi=<value>" — written before 0.60.0, or any event with no phi
 * measured — loads with phi -1, so an old log still loads. */
int pulse_event_log_load(pulse_event_log_t *log, const char *path);

/* The latest sequence number recorded so far (0 when the log is
 * empty): a WATCH with no argument starts live from here. */
uint64_t pulse_event_latest_seq(pulse_event_log_t *log);

/* The oldest sequence number still held in the ring (0 when the log
 * is empty): a WATCH FROM <seq> below this one has been evicted and
 * can only replay from here, with a gap signalled. */
uint64_t pulse_event_oldest_seq(pulse_event_log_t *log);

/* The earliest event with seq > after_seq. Returns 1 with *out
 * filled, 0 when no such event arrived within timeout_ms, and -1
 * when the event after after_seq has already scrolled out of the
 * ring — the caller fell too far behind and must resync (re-read
 * the snapshot) instead of trusting the stream. A future after_seq
 * (nothing recorded that far yet) just waits for the next event. */
int pulse_event_wait(pulse_event_log_t *log, uint64_t after_seq,
                     pulse_event_t *out, int timeout_ms);

#endif
