#ifndef PULSE_CONTROL_H
#define PULSE_CONTROL_H

#include <stdint.h>

#include "events.h"
#include "member.h"
#include "metrics.h"

/* TCP control plane for pulse-node: a tiny line-based protocol on a
 * separate TCP port, one command per connection, plain text in and out
 * (netcat-friendly). Commands:
 *
 *   MEMBERS
 *       One line per member, then END:
 *       MEMBER <id> <incarnation> <ALIVE|SUSPECT|DEAD> <phi|'-'>
 *           <annotations|'-'>
 *       phi is the peer's current phi value (two decimals) when
 *       phi-accrual is on and the peer has heartbeat history, '-'
 *       otherwise (including self, which has no phi history).
 *       Annotations are the member's comma-separated "k=v,k=v" list,
 *       '-' when it carries none.
 *   PEERS
 *       One line per member, then END:
 *       PEER <id> <incarnation> <ALIVE|SUSPECT|DEAD> <last-seen-ms|'-'>
 *           <seed|peer-file|discovered|self>
 *       last-seen-ms is how many ms since this node last heard
 *       directly from the peer; '-' for self (it never probes
 *       itself) and for a peer never heard from (a seed that never
 *       answered). The source says where the peer came from: a
 *       --seed flag, the --peer-file roster, a peer removed from
 *       the file but kept because it was still ALIVE, or gossip /
 *       a JOIN from a peer the node was never told about
 *       (discovered). A peer named with both --seed and --peer-file
 *       reports as seed: the flag outranks the file.
 *   META <key=value>
 *       Set one annotation on this node itself (at most 8): OK
 *       <key=value>. The change is gossiped to the cluster with the
 *       next digest and persisted to the data dir, so it survives a
 *       restart. Bad syntax: ERR invalid meta <arg>; a ninth distinct
 *       key: ERR meta full (max 8); missing argument:
 *       ERR usage: META <key=value>.
 *   RELOAD
 *       Re-read --peer-file and merge it into the seed list — the
 *       same reload SIGHUP triggers, for an operator who cannot send
 *       the node a signal: OK peers=N added=A dropped=D kept=K
 *       bad=B. peers is how many peers the file lists; added/dropped/
 *       kept are the merge outcome (a still-ALIVE peer removed from
 *       the file is kept, never dropped on the strength of a file
 *       edit alone); bad is how many lines were skipped as malformed.
 *       No --peer-file: ERR no peer file. Unreadable file:
 *       ERR cannot read peer file <path>: <reason>, and the old list
 *       is kept. See "Live peer-file reload".
 *   STATUS
 *       STATUS id=<self-id> uptime=<seconds> alive=N suspect=N dead=N
 *       total=N, then END.
 *   LEAVE <member-id>
 *       Force-remove a member from the table: OK <id>. The removal is
 *       recorded as a tombstone and gossiped as DEAD, so peers mark the
 *       member DEAD and GC it on their own retention timer; a live
 *       member refutes by bumping its incarnation and comes back.
 *       Unknown member: ERR unknown member <id>. Self: ERR cannot
 *       leave self. Missing id: ERR usage: LEAVE <member-id>.
 *   METRICS
 *       One line of failure-detector metrics, then END:
 *       METRICS suspected=N dead=N refutes=N joins=N
 *           adaptive_fast_cycles=N
 *           detect_latency_ms mean=X max=Y samples=N
 *           suspect_to_dead_ms mean=X max=Y samples=N
 *           slow_peers=N
 *           peer_file=<path|'-'> last_reload=<unix|'-'>
 *           last_mtime=<unix|'-'> pf_peers=<N|'-'> pf_added=<N|'-'>
 *           pf_dropped=<N|'-'> pf_kept=<N|'-'> pf_bad=<N|'-'>
 *           watch_subscribers=<N> watch_sessions=<N>
 *       detect_latency_ms is last-direct-contact -> SUSPECT for
 *       suspicions this node detected itself (mean/max in ms, "-" when
 *       there are no samples); suspect_to_dead_ms is SUSPECT -> DEAD.
 *       adaptive_fast_cycles counts probe cycles that ran at the fast
 *       interval because a peer was SUSPECT (see "Adaptive probe
 *       interval").
 *       Transitions learned from gossip bump the counts but add no
 *       latency sample, so samples can be smaller than the counts.
 *       slow_peers is how many members currently count as slow: mean
 *       probe RTT above --slow-peer-rtt-ms (0 when the warning is
 *       disabled; see "Slow-peer warning").
 *       The peer-file fields (0.28.0) are the --peer-file state: the
 *       path, the wall-clock time of the last successful load or
 *       reload, the file's mtime at that load, and the counts from
 *       that load (the same numbers as the PEERFILE RELOAD log line).
 *       All of them are "-" when the node runs without --peer-file.
 *       watch_subscribers (0.33.0) is how many WATCH streams are
 *       connected right now, and watch_sessions is how many WATCH
 *       sessions the node has served since startup — so an operator
 *       can see how many consumers are attached to the event
 *       stream, and whether any ever were. A client that went away
 *       drops out of the current count within about a second; the
 *       session count only ever grows.
 *   JSON
 *       STATUS, MEMBERS and METRICS combined into one machine-readable
 *       JSON object on a single line, then END. Member phi is a number
 *       when phi-accrual is on and the peer has heartbeat history,
 *       null otherwise (including self). Latency means are null when
 *       there are no samples; max is 0 then. Each member also carries
 *       "meta": its annotations as a JSON object ({} when it has
 *       none), "rtt_ms" / "rtt_mean_ms" (null until a probe has been
 *       answered), "slow" (true when the peer's mean probe RTT is
 *       above --slow-peer-rtt-ms, false otherwise), "source" (where
 *       the peer came from: "seed", "peer-file", "discovered" or
 *       "self" — the same provenance PEERS reports, so a peer-file
 *       peer is distinguishable from a discovered one), and the
 *       metrics object carries "slow_peers" (how many members are
 *       slow). A top-level "peer_file" object carries the --peer-file
 *       state (path, last_reload / last_mtime as unix seconds, and
 *       the counts from the last load), or null when the node runs
 *       without --peer-file. A top-level "watch" object (0.33.0)
 *       carries "subscribers" (how many WATCH streams are connected
 *       right now) and "sessions" (how many WATCH sessions the node
 *       has served since startup).
 *   WATCH [types] [ids] [FROM <seq>]
 *       A long-lived stream of membership events, for tooling that
 *       watches a cluster instead of polling MEMBERS or JSON once a
 *       second. The reply starts with "OK watching", then one
 *       SNAPSHOT line per member — id, incarnation, state,
 *       last-seen-ms (or '-') and source, the PEERS shape — then
 *       END, then one EVENT line per membership event as it happens:
 *       EVENT <seq> <JOINED|SUSPECTED|DEAD|REVIVED|DROPPED|REFUTED>
 *       <id> <incarnation> <unix-seconds>. JOINED is a peer
 *       appearing in the table, SUSPECTED is ALIVE -> SUSPECT, DEAD
 *       is SUSPECT -> DEAD, REVIVED is a SUSPECT/DEAD peer coming
 *       back ALIVE, DROPPED is a peer leaving the table (GC after
 *       its retention, a LEAVE, or a peer-file reload drop), and
 *       REFUTED is this node hearing itself SUSPECT/DEAD and bumping
 *       its incarnation. The connection stays open until the client
 *       closes it (or falls too far behind: the event log is a
 *       bounded ring, so a slow consumer gets "ERR gap: events lost,
 *       reconnect" and must re-read the snapshot). Sequence numbers
 *       are 1-based and monotonic for the node's life; a jump in
 *       them means events were missed. The snapshot is taken after
 *       the latest sequence is read, so no event is ever lost
 *       between the two — an event in that window may appear both
 *       in the snapshot and in the stream, and a client dedupes by
 *       sequence number. WATCH runs on its own thread, so other
 *       control-plane commands keep answering while a client
 *       watches. No watch hooks: ERR watch not supported.
 *       A filter (0.30.0) narrows the stream server-side: an
 *       optional type list, an id list, or both, in any order —
 *       "WATCH JOINED,DEAD" (only those event types),
 *       "WATCH 127.0.0.1:17102" (only that peer), or
 *       "WATCH JOINED,DEAD 127.0.0.1:17102" (both). A token with a
 *       ':' is an id list (comma-separated host:port, IPv6
 *       bracketed); a token without one is a type list
 *       (comma-separated EVENT words). The snapshot is narrowed by
 *       the id list too, so a watcher subscribed to one peer sees
 *       only that peer; a type filter does not narrow the snapshot,
 *       which has no event type. A filtered-out event still
 *       consumes its sequence number, so a filtered stream's
 *       numbers jump by design — a jump means filtered-out events,
 *       not lost ones. A bad filter is refused up front:
 *       ERR invalid watch filter <reason>.
 *       Replay / resume (0.31.0): "WATCH FROM <seq>" replays the
 *       buffered events from that sequence number — in order,
 *       after the snapshot and END, before live events — so a
 *       client that reconnects can catch up instead of starting
 *       from now. FROM 0 replays from the oldest buffered event.
 *       A FROM past the latest sequence starts live (nothing to
 *       replay yet). A FROM below the oldest buffered event — the
 *       requested event has been evicted from the 128-event ring
 *       — replays from the oldest buffered event instead, after
 *       a "GAP <requested> <oldest>" line that says how much was
 *       lost. FROM composes with the type/id filters, in any
 *       order ("WATCH FROM 5 JOINED,DEAD"); a bad FROM is refused
 *       up front: ERR invalid watch filter <reason>.
 *       Heartbeat (0.32.0): a stream that has sent nothing for 5 s
 *       sends "HEARTBEAT <unix-seconds>", so a client on a quiet
 *       cluster can tell "nothing happened" from "the connection
 *       died silently". The heartbeat carries no sequence number
 *       and is sent regardless of the filter; any line sent —
 *       EVENT, GAP, or HEARTBEAT — resets the 5 s clock, so a
 *       busy cluster never sees one.
 *       Subscriber count (0.33.0): METRICS and JSON report how many
 *       WATCH streams are connected right now ("watch_subscribers"
 *       / "watch"."subscribers") and how many sessions the node has
 *       served since startup ("watch_sessions" / "watch"."sessions"),
 *       so an operator can see how many consumers are attached to
 *       the event stream without counting connections by hand.
 *   anything else
 *       ERR unknown command
 *
 * The server is deliberately dumb: it knows nothing about the node's
 * threads or locks. All state access goes through the hooks, which the
 * node implements (snapshot copies the table under the node's lock;
 * force_leave removes under the lock). One-shot commands are served
 * one connection at a time; the server is a control interface, not
 * a data path. WATCH is the exception: it is a long-lived stream,
 * so it runs on its own thread and the accept loop keeps serving
 * other commands while a client watches.
 */

/* RELOAD control-plane command (0.25.0): the outcome of re-reading
 * --peer-file, filled in by the node's reload hook. have_file is 0
 * when the node was started without --peer-file; read_ok is 0 when
 * the file could not be read, and error then holds the reason
 * ("cannot read peer file <path>: <strerror>"). peers is how many
 * peers the file lists; added/dropped/kept are the merge outcome
 * (the same numbers as the PEERFILE RELOAD log line); bad is how many
 * lines were skipped as malformed. */
typedef struct {
    int have_file;
    int read_ok;
    int peers;
    int added;
    int dropped;
    int kept;
    int bad;
    char error[640];
} pulse_reload_result_t;

/* Peer-file state (0.28.0): what --peer-file the node is using and
 * what the last successful load or reload of it did, for METRICS
 * and JSON. have_file is 0 when the node was started without
 * --peer-file; last_reload_unix is the wall-clock time (seconds
 * since the epoch) of the last successful load or reload — the
 * initial load at startup counts as the first one, with added =
 * the peers the file contributed to the seed list — and
 * last_mtime_unix is the file's mtime (seconds) at that load.
 * peers / added / dropped / kept / bad are the counts from that
 * load: the same numbers as the PEERFILE RELOAD log line (kept =
 * peers still held in the kept list after the reload). A reload
 * that cannot read the file leaves the state at the last successful
 * load, so the numbers always describe a roster the node actually
 * holds. */
typedef struct {
    int have_file;
    char path[512];
    int64_t last_reload_unix; /* 0 = never loaded */
    int64_t last_mtime_unix;  /* 0 = unknown */
    int peers;
    int added;
    int dropped;
    int kept;
    int bad;
} pulse_peer_file_state_t;

typedef struct {
    void *ctx;
    /* Copy the membership table into out (hook locks as needed). */
    void (*snapshot)(void *ctx, member_table_t *out);
    /* Force-remove a member. Returns 1 if removed, 0 if the member is
     * unknown, -1 if the id is this node itself. */
    int (*force_leave)(void *ctx, const char *id);
    /* Re-read --peer-file and merge it into the seed list — the same
     * reload SIGHUP triggers — filling out with the outcome. NULL
     * when the node does not support it: RELOAD then replies ERR. */
    void (*reload_peer_file)(void *ctx, pulse_reload_result_t *out);
    /* Copy the --peer-file state (path, last reload time/mtime,
     * counts from the last successful load) into out. NULL when the
     * node does not track it: METRICS and JSON then report no peer
     * file. */
    void (*peer_file_state)(void *ctx, pulse_peer_file_state_t *out);
    /* Copy the failure-detector metrics into out (hook locks as needed). */
    void (*metrics)(void *ctx, pulse_metrics_t *out);
    /* Set one annotation on this node itself. Returns 0 on success,
     * -1 on invalid key/value, -2 when the node already carries the
     * maximum number of annotations. NULL when the node does not
     * support it: META then replies ERR. */
    int (*set_meta)(void *ctx, const char *key, const char *value);
    /* 1 when phi-accrual suspicion is on (phi values are meaningful). */
    int (*phi_enabled)(void *ctx);
    /* Where a member came from, for the PEERS command: "seed" (a
     * --seed flag), "peer-file" (the --peer-file roster, or a peer
     * removed from it but kept because it was still ALIVE),
     * "discovered" (learned from gossip or a JOIN, never seeded),
     * or "self". NULL when the node does not track provenance:
     * PEERS then reports every non-self member as "discovered". */
    const char *(*peer_source)(void *ctx, const char *id);
    /* Slow-peer RTT threshold in ms (0 = warning disabled): a peer
     * whose mean probe RTT is above this counts as slow, and is
     * reported as "slow" per member in JSON and as slow_peers in
     * METRICS and JSON. */
    int64_t slow_rtt_threshold_ms;
    /* Membership events for the WATCH command (0.29.0): the next
     * event after last_seq, or a wait for one. Returns 1 with *out
     * filled, 0 when no new event arrived within timeout_ms, -1
     * when the event after last_seq has already scrolled out of
     * the node's bounded event log (the consumer fell too far
     * behind and must reconnect and re-read the snapshot). NULL
     * when the node does not support watching: WATCH then replies
     * ERR watch not supported. */
    int (*watch_next)(void *ctx, uint64_t last_seq, pulse_event_t *out,
                      int timeout_ms);
    /* The latest event sequence number (0 when no event has been
     * recorded yet): a WATCH stream starts live from here, after
     * its snapshot. NULL when the node does not support watching. */
    uint64_t (*watch_latest_seq)(void *ctx);
    /* The oldest event sequence number still held in the log (0
     * when no event has been recorded yet): a WATCH FROM below
     * this one has been evicted, so the stream replays from here
     * after a GAP line. NULL when the node does not support
     * watching. */
    uint64_t (*watch_oldest_seq)(void *ctx);
    const char *self_id;
    int64_t start_ms; /* monotonic ms when the node started */
} control_hooks_t;

/* Create, bind and listen on a TCP socket for the control plane, on the
 * loopback address of the given family (AF_INET or AF_INET6). The
 * control plane follows the node's family: a v6 node answers on ::1,
 * a v4 node on 127.0.0.1. Returns the listen fd, or -1 on error (with
 * errno set). */
int control_listen(int port, int family);

/* Serve connections on fd until accept() fails (e.g. the fd is shut
 * down from another thread). Returns when the server stops. */
void control_serve(int fd, const control_hooks_t *hooks);

#endif
