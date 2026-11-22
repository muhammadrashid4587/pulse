#ifndef PULSE_MEMBER_H
#define PULSE_MEMBER_H

#include <netinet/in.h>
#include <stdint.h>

#include "phi.h"

#define PULSE_MAX_MEMBERS 64
#define PULSE_ID_LEN 64
#define PULSE_MAX_TOMBSTONES 16

typedef enum {
    MEMBER_ALIVE = 0,
    MEMBER_SUSPECT = 1,
    MEMBER_DEAD = 2
} member_state_t;

typedef struct {
    char id[PULSE_ID_LEN]; /* "host:port" */
    uint32_t incarnation;  /* incarnation the member had when removed */
    int64_t removed_ms;    /* monotonic ms when the tombstone was recorded */
} tombstone_t;

typedef struct {
    char id[PULSE_ID_LEN]; /* "host:port", IPv6 hosts bracketed: "[::1]:port" */
    struct sockaddr_storage addr;
    uint32_t incarnation;
    member_state_t state;
    int64_t last_seen_ms;   /* monotonic ms of last direct contact */
    int64_t state_since_ms; /* when the current state started */
    /* Phi-accrual history for this peer: fed by every direct contact
     * (member_mark_alive). Gossip about a peer is not a heartbeat —
     * only a packet from the peer itself proves it is alive. */
    phi_accrual_t phi;
} member_t;

typedef struct {
    member_t members[PULSE_MAX_MEMBERS];
    int count;
    /* Tombstones: members garbage-collected while DEAD. Gossip about a
     * tombstoned id at an incarnation <= the recorded one is ignored, so
     * a peer that still holds the DEAD entry cannot resurrect it here.
     * A higher incarnation clears the tombstone (the member rejoined).
     * Tombstones expire after their retention so the table stays bounded;
     * by then every peer has had time to GC the entry itself. */
    tombstone_t tombstones[PULSE_MAX_TOMBSTONES];
    int tombstone_count;
} member_table_t;

int64_t pulse_now_ms(void);
void member_table_init(member_table_t *t);
member_t *member_find(member_table_t *t, const char *id);
member_t *member_add(member_table_t *t, const char *id,
                     const struct sockaddr_storage *addr,
                     uint32_t incarnation);

/* Parse a "host:port" id into a sockaddr_storage. IPv4 ids are bare
 * ("127.0.0.1:17101"); IPv6 ids are bracketed ("[::1]:17101"). A bare
 * IPv6 literal ("::1:17101") also parses: the port is the part after
 * the last colon. Returns 0 on success. */
int member_parse_addr(const char *id, struct sockaddr_storage *addr);
/* Parse a bare host literal (no port) into a sockaddr_storage: an IPv4
 * or IPv6 address, port left 0. Returns 0 on success. */
int member_parse_host(const char *host, struct sockaddr_storage *addr);
/* Format an address back to an id: "127.0.0.1:17101" or "[::1]:17101". */
void member_format_id(const struct sockaddr_storage *addr, char *id,
                      size_t len);
/* sizeof(struct sockaddr_in) or sizeof(struct sockaddr_in6) for the
 * address's family; 0 for an unknown family. Use as the length argument
 * to sendto/connect/bind. */
socklen_t pulse_addr_len(const struct sockaddr_storage *addr);
/* 1 when two addresses are the same family, port and address bytes. */
int pulse_addr_equal(const struct sockaddr_storage *a,
                     const struct sockaddr_storage *b);

/* Merge results (bit flags). */
#define MERGE_NONE 0
#define MERGE_NEW 1
#define MERGE_CHANGED 2
#define MERGE_SELF_SUSPECTED 4

/* Merge one remote membership entry into the table, SWIM-style:
 * higher incarnation wins; at equal incarnation DEAD > SUSPECT > ALIVE.
 * Entries about self are never applied; if one claims self is SUSPECT or
 * DEAD at an incarnation >= our own, MERGE_SELF_SUSPECTED is returned so
 * the caller can refute by bumping its incarnation. */
int member_merge(member_table_t *t, const char *self_id, const char *id,
                 const struct sockaddr_storage *addr, uint32_t incarnation,
                 member_state_t state, int64_t now);

int member_mark_suspect(member_table_t *t, const char *id, int64_t now);
int member_mark_dead(member_table_t *t, const char *id, int64_t now);
/* Direct contact: the member is alive. Returns 1 if the member was new or
 * its state changed, 0 if it was already ALIVE. */
int member_mark_alive(member_table_t *t, const char *id,
                      const struct sockaddr_storage *addr,
                      uint32_t incarnation, int64_t now);

int member_count_state(const member_table_t *t, member_state_t state);
/* Force-remove a member (operator action, e.g. the control plane's
 * LEAVE): drop the entry from the table and record a tombstone so
 * gossip cannot resurrect it at the recorded incarnation or below.
 * A live member that hears about its own removal refutes by bumping
 * its incarnation and comes back; a dead one stays gone. Returns 1
 * if the member was present, 0 if unknown. */
int member_force_remove(member_table_t *t, const char *id, int64_t now);
/* Garbage-collect DEAD members whose state is older than
 * dead_retention_ms. Each removal records a tombstone so gossip cannot
 * resurrect the entry, and tombstones older than
 * tombstone_retention_ms are dropped. Removed ids are written into
 * `removed` (up to max_removed); returns the number removed. */
int member_gc(member_table_t *t, int64_t now, int64_t dead_retention_ms,
              int64_t tombstone_retention_ms,
              char removed[][PULSE_ID_LEN], int max_removed);
/* Random ALIVE or SUSPECT peer other than self, or NULL if there is none.
 * The returned pointer is only valid while the caller holds the table lock. */
member_t *member_random_peer(member_table_t *t, const char *self_id);
/* Gossip fanout for a cluster with peer_count gossipable peers
 * (other members, excluding self and DEAD entries): 1 + floor(log2(n)),
 * capped at 4. See member.c for the reasoning. */
int member_gossip_fanout(int peer_count);

const char *member_state_name(member_state_t s);
char member_state_code(member_state_t s);
member_state_t member_state_from_code(char c);

#endif
