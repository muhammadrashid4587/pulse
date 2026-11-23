/* Unit tests for gossip digest prioritization: recently-changed
 * SUSPECT/DEAD entries go first in each digest (most recently changed
 * first, DEAD before SUSPECT on a tie), the rest of the digest is the
 * usual random window, and tombstones still ride along as DEAD.
 * Plus delta encoding (0.16.0): a digest to a peer at version V
 * carries only entries changed after V, oldest change first, and
 * nothing at all when nothing changed; the reported through-version
 * is an exact prefix bound. Digests are sent over real UDP loopback
 * sockets and parsed back. */
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "gossip.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

static struct sockaddr_storage addr_of(const char *id) {
    struct sockaddr_storage a;
    if (member_parse_addr(id, &a) != 0) {
        memset(&a, 0, sizeof a);
    }
    return a;
}

/* Build a table of n members, 127.0.0.1:18000 .. 18000+n-1. */
static void build_table(member_table_t *t, int n) {
    member_table_init(t);
    for (int i = 0; i < n; i++) {
        char id[PULSE_ID_LEN];
        snprintf(id, sizeof id, "127.0.0.1:%d", 18000 + i);
        struct sockaddr_storage a = addr_of(id);
        member_add(t, id, &a, 1);
    }
}

static const char *id_at(int i) {
    static char buf[8][PULSE_ID_LEN];
    static int next = 0;
    char *id = buf[next++ % 8];
    snprintf(id, PULSE_ID_LEN, "127.0.0.1:%d", 18000 + i);
    return id;
}

/* Send one digest from t to a loopback receiver and parse it back.
 * Returns the parsed entry count, or -1 if nothing arrived. since is
 * the peer version the digest is a delta against (0 = the old full
 * digest); through, when non-NULL, receives the through-version the
 * send reported. */
static int msg_roundtrip_digest(const member_table_t *t, uint64_t since,
                                uint64_t *through, gmsg_t *out) {
    int recv_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (recv_sock < 0) return -1;
    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof bind_addr);
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;
    if (bind(recv_sock, (struct sockaddr *)&bind_addr,
             sizeof bind_addr) < 0) {
        close(recv_sock);
        return -1;
    }
    socklen_t len = sizeof bind_addr;
    if (getsockname(recv_sock, (struct sockaddr *)&bind_addr, &len) < 0) {
        close(recv_sock);
        return -1;
    }
    struct timeval tv = { .tv_sec = 2, .tv_usec = 0 };
    setsockopt(recv_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);

    int send_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (send_sock < 0) {
        close(recv_sock);
        return -1;
    }
    struct sockaddr_storage dest;
    memset(&dest, 0, sizeof dest);
    struct sockaddr_in *d4 = (struct sockaddr_in *)&dest;
    d4->sin_family = AF_INET;
    d4->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    d4->sin_port = bind_addr.sin_port;
    /* The sandbox refuses sendto() on a UDP socket that has never been
     * connect()ed (see README, "Note on sending"); connect the send
     * socket once, exactly as pulse-node does at startup. A connected
     * UDP socket still sendto()s any destination. */
    if (connect(send_sock, (struct sockaddr *)&dest,
                sizeof(struct sockaddr_in)) < 0) {
        close(send_sock);
        close(recv_sock);
        return -1;
    }
    int rc = gossip_send_digest(send_sock, &dest, "127.0.0.1:17999", 1,
                                t, since, through);
    if (rc != 0) {
        close(send_sock);
        close(recv_sock);
        return -1;
    }
    char buf[PULSE_MAX_MSG];
    ssize_t n = recvfrom(recv_sock, buf, sizeof buf, 0, NULL, NULL);
    close(send_sock);
    close(recv_sock);
    if (n <= 0) return -1;
    if (gossip_parse(buf, (size_t)n, out) != 0) return -1;
    return out->entry_count;
}

/* Send one digest from t and check that nothing arrives within 200 ms:
 * the delta case where the peer is already current. Returns 1 when
 * nothing arrived, 0 otherwise. through, when non-NULL, receives the
 * through-version the send reported. */
static int nothing_sent(const member_table_t *t, uint64_t since,
                        uint64_t *through) {
    int recv_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (recv_sock < 0) return 0;
    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof bind_addr);
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;
    if (bind(recv_sock, (struct sockaddr *)&bind_addr,
             sizeof bind_addr) < 0) {
        close(recv_sock);
        return 0;
    }
    socklen_t len = sizeof bind_addr;
    if (getsockname(recv_sock, (struct sockaddr *)&bind_addr, &len) < 0) {
        close(recv_sock);
        return 0;
    }
    int send_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (send_sock < 0) {
        close(recv_sock);
        return 0;
    }
    struct sockaddr_storage dest;
    memset(&dest, 0, sizeof dest);
    struct sockaddr_in *d4 = (struct sockaddr_in *)&dest;
    d4->sin_family = AF_INET;
    d4->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    d4->sin_port = bind_addr.sin_port;
    if (connect(send_sock, (struct sockaddr *)&dest,
                sizeof(struct sockaddr_in)) < 0) {
        close(send_sock);
        close(recv_sock);
        return 0;
    }
    int rc = gossip_send_digest(send_sock, &dest, "127.0.0.1:17999", 1,
                                t, since, through);
    if (rc != 0) {
        close(send_sock);
        close(recv_sock);
        return 0;
    }
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(recv_sock, &rfds);
    struct timeval tv = { .tv_sec = 0, .tv_usec = 200000 };
    int ready = select(recv_sock + 1, &rfds, NULL, NULL, &tv);
    close(send_sock);
    close(recv_sock);
    return ready == 0;
}

/* Send one full table (SYNC) from t to a loopback receiver and parse
 * it back. Returns the parsed entry count, or -1 if nothing arrived. */
static int msg_roundtrip_sync(const member_table_t *t, gmsg_t *out) {
    int recv_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (recv_sock < 0) return -1;
    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof bind_addr);
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;
    if (bind(recv_sock, (struct sockaddr *)&bind_addr,
             sizeof bind_addr) < 0) {
        close(recv_sock);
        return -1;
    }
    socklen_t len = sizeof bind_addr;
    if (getsockname(recv_sock, (struct sockaddr *)&bind_addr, &len) < 0) {
        close(recv_sock);
        return -1;
    }
    struct timeval tv = { .tv_sec = 2, .tv_usec = 0 };
    setsockopt(recv_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);

    int send_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (send_sock < 0) {
        close(recv_sock);
        return -1;
    }
    struct sockaddr_storage dest;
    memset(&dest, 0, sizeof dest);
    struct sockaddr_in *d4 = (struct sockaddr_in *)&dest;
    d4->sin_family = AF_INET;
    d4->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    d4->sin_port = bind_addr.sin_port;
    /* The sandbox refuses sendto() on a UDP socket that has never been
     * connect()ed (see README, "Note on sending"); connect the send
     * socket once, exactly as pulse-node does at startup. A connected
     * UDP socket still sendto()s any destination. */
    if (connect(send_sock, (struct sockaddr *)&dest,
                sizeof(struct sockaddr_in)) < 0) {
        close(send_sock);
        close(recv_sock);
        return -1;
    }
    int rc = gossip_send_full_table(send_sock, &dest, "127.0.0.1:17999",
                                    1, t);
    if (rc != 0) {
        close(send_sock);
        close(recv_sock);
        return -1;
    }
    char buf[PULSE_MAX_MSG];
    ssize_t n = recvfrom(recv_sock, buf, sizeof buf, 0, NULL, NULL);
    close(send_sock);
    close(recv_sock);
    if (n <= 0) return -1;
    if (gossip_parse(buf, (size_t)n, out) != 0) return -1;
    return out->entry_count;
}

static int has_entry(const gmsg_t *msg, const char *id) {
    for (int i = 0; i < msg->entry_count; i++) {
        if (strcmp(msg->entries[i].id, id) == 0) return i;
    }
    return -1;
}

int main(void) {
    srand(1234);
    gmsg_t msg;

    /* One SUSPECT in a small table goes first; everything fits. */
    {
        member_table_t t;
        build_table(&t, 3);
        member_mark_suspect(&t, id_at(1), 1000);
        int n = msg_roundtrip_digest(&t, 0, NULL, &msg);
        CHECK(n == 3);
        CHECK(msg.type == GMSG_GOSSIP);
        CHECK(strcmp(msg.entries[0].id, id_at(1)) == 0);
        CHECK(msg.entries[0].state == MEMBER_SUSPECT);
        CHECK(has_entry(&msg, id_at(0)) >= 0);
        CHECK(has_entry(&msg, id_at(2)) >= 0);
    }

    /* A 20-member table with 6 urgent entries: the digest is capped at
     * 16, and the 4 most recently changed urgent entries lead, most
     * recent first — regardless of where the random window falls. */
    {
        member_table_t t;
        build_table(&t, 20);
        member_mark_suspect(&t, id_at(0), 1000); /* A: SUSPECT @1000 */
        member_mark_dead(&t, id_at(1), 2000);    /* B: DEAD @2000 */
        member_mark_suspect(&t, id_at(2), 1500); /* C: SUSPECT @1500 */
        member_mark_dead(&t, id_at(3), 1200);    /* D: DEAD @1200 */
        member_mark_suspect(&t, id_at(4), 1800); /* E: SUSPECT @1800 */
        member_mark_dead(&t, id_at(5), 1100);    /* F: DEAD @1100 */
        int n = msg_roundtrip_digest(&t, 0, NULL, &msg);
        CHECK(n == PULSE_GOSSIP_ENTRIES);
        /* Expected lead order: B(2000), E(1800), C(1500), D(1200). */
        CHECK(strcmp(msg.entries[0].id, id_at(1)) == 0);
        CHECK(msg.entries[0].state == MEMBER_DEAD);
        CHECK(strcmp(msg.entries[1].id, id_at(4)) == 0);
        CHECK(msg.entries[1].state == MEMBER_SUSPECT);
        CHECK(strcmp(msg.entries[2].id, id_at(2)) == 0);
        CHECK(msg.entries[2].state == MEMBER_SUSPECT);
        CHECK(strcmp(msg.entries[3].id, id_at(3)) == 0);
        CHECK(msg.entries[3].state == MEMBER_DEAD);
        /* No entry is sent twice. */
        for (int i = 0; i < msg.entry_count; i++) {
            for (int j = i + 1; j < msg.entry_count; j++) {
                CHECK(strcmp(msg.entries[i].id, msg.entries[j].id) != 0);
            }
        }
    }

    /* Same-instant tie: DEAD beats SUSPECT. */
    {
        member_table_t t;
        build_table(&t, 3);
        member_mark_suspect(&t, id_at(0), 5000);
        member_mark_suspect(&t, id_at(1), 5000);
        member_mark_dead(&t, id_at(1), 5000);
        int n = msg_roundtrip_digest(&t, 0, NULL, &msg);
        CHECK(n == 3);
        CHECK(strcmp(msg.entries[0].id, id_at(1)) == 0);
        CHECK(msg.entries[0].state == MEMBER_DEAD);
        CHECK(strcmp(msg.entries[1].id, id_at(0)) == 0);
        CHECK(msg.entries[1].state == MEMBER_SUSPECT);
    }

    /* No urgent entries: a 20-member table still caps at 16, all
     * distinct, all ALIVE — the random window is unchanged. */
    {
        member_table_t t;
        build_table(&t, 20);
        int n = msg_roundtrip_digest(&t, 0, NULL, &msg);
        CHECK(n == PULSE_GOSSIP_ENTRIES);
        for (int i = 0; i < msg.entry_count; i++) {
            CHECK(msg.entries[i].state == MEMBER_ALIVE);
            for (int j = i + 1; j < msg.entry_count; j++) {
                CHECK(strcmp(msg.entries[i].id, msg.entries[j].id) != 0);
            }
        }
    }

    /* A tombstone still rides along as a DEAD entry after the table. */
    {
        member_table_t t;
        build_table(&t, 3);
        member_mark_dead(&t, id_at(1), 1000);
        member_force_remove(&t, id_at(1), 2000);
        CHECK(t.tombstone_count == 1);
        int n = msg_roundtrip_digest(&t, 0, NULL, &msg);
        CHECK(n == 3); /* 2 live members + 1 tombstone */
        int idx = has_entry(&msg, id_at(1));
        CHECK(idx >= 0);
        CHECK(msg.entries[idx].state == MEMBER_DEAD);
    }

    /* An empty table sends nothing. */
    {
        member_table_t t;
        member_table_init(&t);
        int recv_sock = socket(AF_INET, SOCK_DGRAM, 0);
        int send_sock = socket(AF_INET, SOCK_DGRAM, 0);
        struct sockaddr_storage dest;
        memset(&dest, 0, sizeof dest);
        struct sockaddr_in *d4 = (struct sockaddr_in *)&dest;
        d4->sin_family = AF_INET;
        d4->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        d4->sin_port = htons(1);
        CHECK(gossip_send_digest(send_sock, &dest, "127.0.0.1:17999", 1,
                                 &t, 0, NULL) == 0);
        close(send_sock);
        close(recv_sock);
    }

    /* Anti-entropy: a SYNC carries the whole table, not a 16-entry
     * digest — a 20-member table arrives in full, in table order. */
    {
        member_table_t t;
        build_table(&t, 20);
        member_mark_suspect(&t, id_at(2), 1000);
        member_mark_dead(&t, id_at(5), 2000);
        int n = msg_roundtrip_sync(&t, &msg);
        CHECK(n == 20);
        CHECK(msg.type == GMSG_SYNC);
        for (int i = 0; i < 20; i++)
            CHECK(has_entry(&msg, id_at(i)) >= 0);
        int idx = has_entry(&msg, id_at(2));
        CHECK(idx >= 0 && msg.entries[idx].state == MEMBER_SUSPECT);
        idx = has_entry(&msg, id_at(5));
        CHECK(idx >= 0 && msg.entries[idx].state == MEMBER_DEAD);
    }

    /* A stale node converges to the full table after one SYNC exchange:
     * the receiver knows only 5 of 20 members, one at a lower
     * incarnation, one with a stale state — after merging the parsed
     * SYNC entries it holds all 20 with the sender's states. */
    {
        member_table_t sender, receiver;
        build_table(&sender, 20);
        member_mark_suspect(&sender, id_at(2), 1000);
        member_mark_dead(&sender, id_at(5), 2000);
        /* One member rejoined at a higher incarnation: the stale
         * receiver holds the old one. */
        member_t *m = member_find(&sender, id_at(7));
        m->incarnation = 3;
        build_table(&receiver, 5);
        member_mark_suspect(&receiver, id_at(2), 500);
        int n = msg_roundtrip_sync(&sender, &msg);
        CHECK(n == 20);
        CHECK(msg.type == GMSG_SYNC);
        /* Merge exactly as the node does on receipt. */
        for (int i = 0; i < msg.entry_count; i++) {
            const member_t *e = &msg.entries[i];
            member_merge(&receiver, "127.0.0.1:17998", e->id, &e->addr,
                         e->incarnation, e->state, 5000);
        }
        CHECK(receiver.count == 20);
        for (int i = 0; i < 20; i++) {
            member_t *got = member_find(&receiver, id_at(i));
            member_t *want = member_find(&sender, id_at(i));
            CHECK(got != NULL);
            if (got && want) {
                CHECK(got->state == want->state);
                CHECK(got->incarnation == want->incarnation);
            }
        }
    }

    /* A tombstone still rides along in a SYNC as a DEAD entry. */
    {
        member_table_t t;
        build_table(&t, 3);
        member_mark_dead(&t, id_at(1), 1000);
        member_force_remove(&t, id_at(1), 2000);
        CHECK(t.tombstone_count == 1);
        int n = msg_roundtrip_sync(&t, &msg);
        CHECK(n == 3); /* 2 live members + 1 tombstone */
        int idx = has_entry(&msg, id_at(1));
        CHECK(idx >= 0);
        CHECK(msg.entries[idx].state == MEMBER_DEAD);
    }

    /* An empty table sends no SYNC either. */
    {
        member_table_t t;
        member_table_init(&t);
        int send_sock = socket(AF_INET, SOCK_DGRAM, 0);
        struct sockaddr_storage dest;
        memset(&dest, 0, sizeof dest);
        struct sockaddr_in *d4 = (struct sockaddr_in *)&dest;
        d4->sin_family = AF_INET;
        d4->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        d4->sin_port = htons(1);
        CHECK(gossip_send_full_table(send_sock, &dest, "127.0.0.1:17999",
                                     1, &t) == 0);
        close(send_sock);
    }

    /* A table too big for one datagram rotates: a 64-member table of
     * long IPv6 ids does not fit in a single SYNC, but two consecutive
     * SYNCs cover every member between them, with no entry sent twice
     * in one message. */
    {
        member_table_t t;
        member_table_init(&t);
        char id[PULSE_ID_LEN];
        for (int i = 0; i < 64; i++) {
            snprintf(id, sizeof id, "[2001:db8::%x]:%d", i + 1,
                     18000 + i);
            struct sockaddr_storage a = addr_of(id);
            member_add(&t, id, &a, 1);
        }
        CHECK(t.count == 64);
        gmsg_t first, second;
        int n1 = msg_roundtrip_sync(&t, &first);
        int n2 = msg_roundtrip_sync(&t, &second);
        CHECK(n1 > 0 && n1 < 64); /* truncated: the table is too big */
        CHECK(n2 > 0 && n2 < 64);
        for (int i = 0; i < first.entry_count; i++) {
            for (int j = i + 1; j < first.entry_count; j++) {
                CHECK(strcmp(first.entries[i].id,
                             first.entries[j].id) != 0);
            }
        }
        /* Union of the two exchanges covers the whole table. */
        int seen = 0;
        for (int i = 0; i < 64; i++) {
            snprintf(id, sizeof id, "[2001:db8::%x]:%d", i + 1,
                     18000 + i);
            if (has_entry(&first, id) >= 0 ||
                has_entry(&second, id) >= 0)
                seen++;
        }
        CHECK(seen == 64);
    }

    /* Delta encoding: table versions stamp every change, so a digest
     * to a peer at version V carries only what changed after V. */
    {
        member_table_t t;
        build_table(&t, 5);
        /* Five adds: one version bump each, starting from 0. */
        CHECK(t.version == 5);
        for (int i = 0; i < 5; i++) {
            member_t *m = member_find(&t, id_at(i));
            CHECK(m != NULL);
            if (m) CHECK(m->changed_version == (uint64_t)(i + 1));
        }
        /* A peer that already knows version 5 gets nothing at all: no
         * datagram, and the reported through-version is current. */
        uint64_t through = 0;
        CHECK(nothing_sent(&t, t.version, &through));
        CHECK(through == t.version);
        CHECK(member_changed_count(&t, t.version) == 0);
        /* One change after version 5: the delta is exactly that entry,
         * and the through-version advances to it. */
        member_mark_suspect(&t, id_at(2), 1000);
        CHECK(t.version == 6);
        CHECK(member_changed_count(&t, 5) == 1);
        through = 0;
        int n = msg_roundtrip_digest(&t, 5, &through, &msg);
        CHECK(n == 1);
        CHECK(msg.type == GMSG_GOSSIP);
        CHECK(strcmp(msg.entries[0].id, id_at(2)) == 0);
        CHECK(msg.entries[0].state == MEMBER_SUSPECT);
        CHECK(through == 6);
        /* The same delta re-sent against version 6 is empty again. */
        CHECK(nothing_sent(&t, 6, &through));
        CHECK(through == 6);
    }

    /* Several changes since the peer's version arrive oldest first,
     * and the through-version lands on the newest change. */
    {
        member_table_t t;
        build_table(&t, 5);
        uint64_t base = t.version; /* 5 */
        member_mark_suspect(&t, id_at(1), 1000); /* version 6 */
        member_mark_dead(&t, id_at(3), 2000);    /* version 7 */
        member_mark_suspect(&t, id_at(0), 3000); /* version 8 */
        CHECK(t.version == 8);
        CHECK(member_changed_count(&t, base) == 3);
        uint64_t through = 0;
        int n = msg_roundtrip_digest(&t, base, &through, &msg);
        CHECK(n == 3);
        CHECK(strcmp(msg.entries[0].id, id_at(1)) == 0);
        CHECK(msg.entries[0].state == MEMBER_SUSPECT);
        CHECK(strcmp(msg.entries[1].id, id_at(3)) == 0);
        CHECK(msg.entries[1].state == MEMBER_DEAD);
        CHECK(strcmp(msg.entries[2].id, id_at(0)) == 0);
        CHECK(msg.entries[2].state == MEMBER_SUSPECT);
        CHECK(through == 8);
    }

    /* A tombstone recorded after the peer's version rides the delta as
     * a DEAD entry; a tombstone the peer already knows does not. */
    {
        member_table_t t;
        build_table(&t, 3);
        uint64_t base = t.version; /* 3 */
        member_mark_dead(&t, id_at(1), 1000);
        member_force_remove(&t, id_at(1), 2000);
        CHECK(t.tombstone_count == 1);
        /* One change, not two: the DEAD entry left the table when the
         * tombstone was recorded, so only the tombstone is left to
         * send — the version bump for the removal is the tombstone's. */
        CHECK(member_changed_count(&t, base) == 1);
        uint64_t through = 0;
        int n = msg_roundtrip_digest(&t, base, &through, &msg);
        CHECK(n == 1); /* the tombstone only: the member is gone */
        int idx = has_entry(&msg, id_at(1));
        CHECK(idx >= 0);
        CHECK(msg.entries[idx].state == MEMBER_DEAD);
        CHECK(through == t.version);
        /* Against the current version the tombstone is old news. */
        CHECK(nothing_sent(&t, t.version, &through));
    }

    /* A delta bigger than one digest is a prefix, not a lie: with 18
     * changes the digest carries the 16 oldest, and the reported
     * through-version is the 16th change — the newest two wait for the
     * next digest instead of being recorded as known. (The node never
     * sends such a digest in production: more than 16 changes since a
     * peer's version falls back to a full SYNC.) */
    {
        member_table_t t;
        build_table(&t, 20);
        uint64_t base = t.version; /* 20 */
        for (int i = 0; i < 18; i++)
            member_mark_suspect(&t, id_at(i), 1000 + i);
        CHECK(t.version == 38);
        CHECK(member_changed_count(&t, base) == 18);
        uint64_t through = 0;
        int n = msg_roundtrip_digest(&t, base, &through, &msg);
        CHECK(n == PULSE_GOSSIP_ENTRIES);
        /* Entries 0..15 changed at versions 21..36, oldest first. */
        CHECK(strcmp(msg.entries[0].id, id_at(0)) == 0);
        CHECK(strcmp(msg.entries[15].id, id_at(15)) == 0);
        CHECK(has_entry(&msg, id_at(16)) < 0);
        CHECK(has_entry(&msg, id_at(17)) < 0);
        CHECK(through == 36);
        /* The next digest against 36 carries exactly the two left. */
        through = 0;
        n = msg_roundtrip_digest(&t, 36, &through, &msg);
        CHECK(n == 2);
        CHECK(strcmp(msg.entries[0].id, id_at(16)) == 0);
        CHECK(strcmp(msg.entries[1].id, id_at(17)) == 0);
        CHECK(through == 38);
    }

    /* A merge that changes an entry bumps the version; a merge that
     * changes nothing does not, so routine re-gossip of an unchanged
     * entry never grows the delta. */
    {
        member_table_t t;
        build_table(&t, 3);
        uint64_t base = t.version; /* 3 */
        struct sockaddr_storage a = addr_of(id_at(0));
        int r = member_merge(&t, "127.0.0.1:17998", id_at(0), &a, 1,
                             MEMBER_ALIVE, 5000);
        CHECK(r == MERGE_NONE);
        CHECK(t.version == base);
        r = member_merge(&t, "127.0.0.1:17998", id_at(0), &a, 1,
                         MEMBER_SUSPECT, 5000);
        CHECK(r == MERGE_CHANGED);
        CHECK(t.version == base + 1);
        member_t *m = member_find(&t, id_at(0));
        CHECK(m != NULL);
        if (m) CHECK(m->changed_version == t.version);
    }

    if (failures == 0) {
        printf("test_gossip: all %d checks passed\n", checks);
        return 0;
    }
    printf("test_gossip: %d/%d checks FAILED\n", failures, checks);
    return 1;
}
