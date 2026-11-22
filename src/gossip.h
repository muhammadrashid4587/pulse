#ifndef PULSE_GOSSIP_H
#define PULSE_GOSSIP_H

#include "member.h"
#include <sys/socket.h>

#define PULSE_MAX_MSG 1400
#define PULSE_GOSSIP_ENTRIES 16

/* Wire protocol: one text message per UDP datagram.
 *
 *   PING <seq> <from-id> <from-inc>
 *   ACK <seq> <from-id> <from-inc>
 *   PING_REQ <seq> <target-id> <from-id>
 *   JOIN <from-id> <from-inc>
 *   GOSSIP <from-id> <from-inc> <count> <id:inc:state> ...
 *
 * State codes in gossip entries: A alive, S suspect, D dead.
 * Member ids are always "host:port", so a gossip entry is self-contained.
 */

typedef enum {
    GMSG_PING,
    GMSG_ACK,
    GMSG_PING_REQ,
    GMSG_JOIN,
    GMSG_GOSSIP,
    GMSG_UNKNOWN
} gmsg_type_t;

typedef struct {
    gmsg_type_t type;
    uint32_t seq;
    char from_id[PULSE_ID_LEN];
    uint32_t from_inc;
    char target_id[PULSE_ID_LEN];
    struct sockaddr_in target_addr;
    int has_target;
    int entry_count;
    member_t entries[PULSE_MAX_MEMBERS];
} gmsg_t;

/* Parse a received datagram. Returns 0 on success, -1 on malformed input. */
int gossip_parse(const char *buf, size_t len, gmsg_t *out);

int gossip_send_ping(int sock, const struct sockaddr_in *to, uint32_t seq,
                     const char *from_id, uint32_t inc);
int gossip_send_ack(int sock, const struct sockaddr_in *to, uint32_t seq,
                    const char *from_id, uint32_t inc);
int gossip_send_ping_req(int sock, const struct sockaddr_in *to, uint32_t seq,
                         const char *target_id, const char *from_id);
int gossip_send_join(int sock, const struct sockaddr_in *to,
                     const char *from_id, uint32_t inc);
/* Send a digest of up to PULSE_GOSSIP_ENTRIES table entries to a peer. */
int gossip_send_digest(int sock, const struct sockaddr_in *to,
                       const char *from_id, uint32_t inc,
                       const member_table_t *t);

#endif
