#ifndef PULSE_GOSSIP_H
#define PULSE_GOSSIP_H

#include "member.h"
#include <sys/socket.h>

#define PULSE_MAX_MSG 1400
#define PULSE_GOSSIP_ENTRIES 16
/* Leading slots of a digest reserved for the most urgent news: members
 * that recently turned SUSPECT or DEAD go first (most recently changed
 * first, DEAD before SUSPECT on a tie), so failure news is never stuck
 * behind the random window when the table is larger than the digest.
 * The rest of the digest is the usual random window, so entries beyond
 * the urgent set still rotate through. */
#define PULSE_GOSSIP_URGENT 4

/* Wire protocol: one text message per UDP datagram.
 *
 *   PING <seq> <from-id> <from-inc>
 *   ACK <seq> <from-id> <from-inc>
 *   PING_REQ <seq> <target-id> <from-id>
 *   JOIN <from-id> <from-inc>
 *   GOSSIP <from-id> <from-inc> <count> <id:inc:state> ...
 *   SYNC <from-id> <from-inc> <count> <id:inc:state> ...
 *
 * State codes in gossip entries: A alive, S suspect, D dead.
 * Member ids are always "host:port", so a gossip entry is self-contained.
 *
 * SYNC is the anti-entropy message: unlike GOSSIP, which carries a
 * 16-entry digest (urgent entries first, then a random window), SYNC
 * carries the sender's whole membership table, so a peer that missed
 * many incremental digests converges in one exchange instead of waiting
 * for the random window to rotate the missing entries through. A node
 * that receives a SYNC answers with its own full table, so one exchange
 * converges both sides. Entries use the same "id:inc:state" encoding
 * and the same merge rule as GOSSIP.
 *
 * When a shared secret is configured (gossip_set_secret), every message
 * carries a trailing " <tag>" token: the first 8 bytes of
 * HMAC-SHA256(secret, message), hex-encoded. Receivers with a secret
 * drop datagrams whose tag is missing or wrong; receivers without one
 * ignore the trailing token. See secret.h for what the gate is and is
 * not.
 */

typedef enum {
    GMSG_PING,
    GMSG_ACK,
    GMSG_PING_REQ,
    GMSG_JOIN,
    GMSG_GOSSIP,
    GMSG_SYNC,
    GMSG_UNKNOWN
} gmsg_type_t;

typedef struct {
    gmsg_type_t type;
    uint32_t seq;
    char from_id[PULSE_ID_LEN];
    uint32_t from_inc;
    char target_id[PULSE_ID_LEN];
    struct sockaddr_storage target_addr;
    int has_target;
    int entry_count;
    member_t entries[PULSE_MAX_MEMBERS];
} gmsg_t;

/* Parse a received datagram. Returns 0 on success, -1 on malformed input.
 * A trailing authentication tag token, if present, is ignored here: the
 * receiver checks it with pulse_secret_check before parsing. */
int gossip_parse(const char *buf, size_t len, gmsg_t *out);

/* Set the shared secret used to tag outgoing messages. Call once at
 * startup, before any thread sends. NULL or "" clears it (the default:
 * untagged messages, accept anything). */
void gossip_set_secret(const char *secret);

int gossip_send_ping(int sock, const struct sockaddr_storage *to, uint32_t seq,
                     const char *from_id, uint32_t inc);
int gossip_send_ack(int sock, const struct sockaddr_storage *to, uint32_t seq,
                    const char *from_id, uint32_t inc);
int gossip_send_ping_req(int sock, const struct sockaddr_storage *to, uint32_t seq,
                         const char *target_id, const char *from_id);
int gossip_send_join(int sock, const struct sockaddr_storage *to,
                     const char *from_id, uint32_t inc);
/* Send a digest of up to PULSE_GOSSIP_ENTRIES table entries to a peer.
 * Recently-changed SUSPECT/DEAD entries go first (up to
 * PULSE_GOSSIP_URGENT of them, most recently changed first): a digest
 * is only 16 entries and the table holds up to 64 members, so a purely
 * random window can miss a fresh SUSPECT for several rounds and delay
 * the news that matters most. The remaining slots are the usual random
 * contiguous window, skipping entries already sent as urgent, so the
 * rest of the table still rotates through. */
int gossip_send_digest(int sock, const struct sockaddr_storage *to,
                       const char *from_id, uint32_t inc,
                       const member_table_t *t);
/* Send the sender's whole membership table (anti-entropy SYNC): every
 * table entry, then tombstones as DEAD while there is room, in table
 * order — no 16-entry cap, no prioritization, no random window. One
 * datagram holds at most PULSE_MAX_MSG bytes, so a table of long ids
 * (a full 64-member IPv6 table) does not fit in one SYNC: the entries
 * start at a rotating offset that advances by the number sent, so
 * successive SYNCs walk the whole table and a stale peer still
 * converges within a couple of exchanges. Returns 0 on success (or
 * when the table is empty and nothing is sent), -1 on send failure. */
int gossip_send_full_table(int sock, const struct sockaddr_storage *to,
                           const char *from_id, uint32_t inc,
                           const member_table_t *t);

#endif
