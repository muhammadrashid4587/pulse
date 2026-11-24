#ifndef PULSE_GOSSIP_H
#define PULSE_GOSSIP_H

#include "member.h"
#include <sys/socket.h>

#define PULSE_MAX_MSG 1400
/* Default digest size (0.41.0): how many table entries one gossip
 * round's digest covers. The table holds up to PULSE_MAX_MEMBERS
 * (64) members, so the digest is a window, not the whole table —
 * but a cluster can now trade convergence speed for bandwidth with
 * --gossip-digest-size (1..PULSE_GOSSIP_ENTRIES_MAX), and the size
 * rides in METRICS / JSON / PROMETHEUS. */
#define PULSE_GOSSIP_ENTRIES 16
#define PULSE_GOSSIP_ENTRIES_MAX 64
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
 * Member metadata (0.20.0): an entry may carry the member's
 * annotations after the state code, ";k=v,k=v" — e.g.
 * "127.0.0.1:17102:3:A;role=worker,zone=us-east-1". Keys and values
 * are [A-Za-z0-9._-], so the suffix holds no space, colon or comma
 * that could confuse the parser; entries without it (older nodes)
 * parse exactly as before, and a malformed suffix is dropped while
 * the entry itself still merges.
 *
 * SYNC is the anti-entropy message: unlike GOSSIP, which carries a
 * 16-entry digest (urgent entries first, then a random window), SYNC
 * carries the sender's whole membership table, so a peer that missed
 * many incremental digests converges in one exchange instead of waiting
 * for the random window to rotate the missing entries through. A node
 * that receives a SYNC answers with its own full table when the
 * exchange taught it something new (a new or revived sender, a
 * refuted verdict about itself, or a merged entry that added or
 * changed a member), so one exchange converges both sides; a SYNC
 * that changes nothing gets no reply, which is what stops two
 * converged nodes answering each other with identical tables forever.
 * Entries use the same "id:inc:state" encoding and the same merge
 * rule as GOSSIP. A JOIN is answered with a SYNC for the same reason:
 * a joiner gets the seed's whole table at once instead of waiting for
 * gossip rounds to rotate the missing members through.
 *
 * When a shared secret is configured (gossip_set_secret), every message
 * carries a trailing " <tag>" token: the first 8 bytes of
 * HMAC-SHA256(secret, message), hex-encoded. Receivers with a secret
 * drop datagrams whose tag is missing or wrong; receivers without one
 * ignore the trailing token. See secret.h for what the gate is and is
 * not.
 *
 * Delta encoding (0.16.0): every membership-table change bumps a
 * table version, and each entry records the version at which it last
 * changed (see member.h). A digest to a peer that already knows the
 * table up to version V carries only entries changed after V — the
 * delta — oldest change first, so a converged cluster sends
 * no gossip bytes at all until something actually changes. A peer
 * with no recorded version (since_version 0) gets the old digest
 * instead: urgent entries first, then the random window. Delta
 * encoding changes nothing on the wire: a GOSSIP datagram has the
 * same verb and the same "id:inc:state" entries, only fewer of them,
 * so an older node merges a delta exactly as it merged a full digest.
 * Delivery is UDP, so a lost delta can leave a peer behind the
 * version recorded for it; that is bounded by the periodic
 * anti-entropy SYNC, which still exchanges full tables every ~10 s
 * and heals any missed change within one interval. A peer more than
 * one digest's worth of changes behind gets a full SYNC instead of
 * a delta — the caller decides that with member_changed_count.
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
/* Send a JOIN, retrying once immediately on failure (0.57.0): the
 * send socket is connect()ed to the first seed, so a stale ICMP
 * port-unreachable from a dead seed can fail a one-shot JOIN even
 * though the datagram would go out fine a moment later — the error
 * is consumed by the failed call, so the immediate retry succeeds.
 * Returns 0 when a send succeeded, -1 when both attempts failed. */
int gossip_send_join_retry(int sock, const struct sockaddr_storage *to,
                           const char *from_id, uint32_t inc);
/* Send a digest of up to max_entries table entries to a peer.
 * max_entries is the configured gossip digest size (1 to
 * PULSE_GOSSIP_ENTRIES_MAX, PULSE_GOSSIP_ENTRIES by default): a
 * large cluster trades convergence speed for bandwidth by shrinking
 * it, a small one can grow it to the whole table. The urgent
 * leading slots (up to PULSE_GOSSIP_URGENT) are clamped to it, so
 * a digest of 1 still leads with the single most urgent entry.
 * Delta encoding: when since_version > 0, only entries (members and
 * tombstones) that changed after that version are sent, oldest change
 * first, and nothing is sent at all when nothing changed — so
 * a converged cluster is quiet until something actually happens. When
 * since_version is 0 (the peer's version is unknown), the old digest
 * goes out: recently-changed SUSPECT/DEAD entries first (up to
 * PULSE_GOSSIP_URGENT of them, most recently changed first): a digest
 * is only max_entries entries and the table holds up to 64 members, so a purely
 * random window can miss a fresh SUSPECT for several rounds and delay
 * the news that matters most. The remaining slots are the usual random
 * contiguous window, skipping entries already sent as urgent, so the
 * rest of the table still rotates through.
 * sent_through_version, when non-NULL, is set to the highest
 * changed_version among the entries actually sent — the version the
 * peer can now be recorded as knowing — or to t->version when
 * nothing was sent because nothing changed. It is only advanced as
 * far as the entries that fit: when more entries changed than the
 * digest holds, the rest go out on the next digest, and the peer's
 * recorded version advances only to what it actually received. */
int gossip_send_digest(int sock, const struct sockaddr_storage *to,
                       const char *from_id, uint32_t inc,
                       const member_table_t *t,
                       uint64_t since_version,
                       uint64_t *sent_through_version,
                       int max_entries);
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
