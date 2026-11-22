/* Unit tests for IPv6 support (no networking): address parsing and
 * formatting for both families, address helpers, and gossip parsing
 * of messages carrying IPv6 ids. */
#include <stdio.h>
#include <string.h>

#include "gossip.h"
#include "member.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

static int parse_ok(const char *id, struct sockaddr_storage *a) {
    return member_parse_addr(id, a) == 0;
}

int main(void) {
    struct sockaddr_storage a, b;
    char idbuf[PULSE_ID_LEN];

    /* IPv4 round-trip, unchanged behaviour. */
    CHECK(parse_ok("127.0.0.1:17101", &a));
    CHECK(a.ss_family == AF_INET);
    CHECK(pulse_addr_len(&a) == sizeof(struct sockaddr_in));
    member_format_id(&a, idbuf, sizeof idbuf);
    CHECK(strcmp(idbuf, "127.0.0.1:17101") == 0);

    /* IPv6, bracketed: parse, family, port, round-trip. */
    CHECK(parse_ok("[::1]:17101", &a));
    CHECK(a.ss_family == AF_INET6);
    CHECK(pulse_addr_len(&a) == sizeof(struct sockaddr_in6));
    member_format_id(&a, idbuf, sizeof idbuf);
    CHECK(strcmp(idbuf, "[::1]:17101") == 0);

    /* A full IPv6 address round-trips (inet_ntop compresses it the
     * same way inet_pton read it). */
    CHECK(parse_ok("[2001:db8:85a3::8a2e:370:7334]:65535", &a));
    member_format_id(&a, idbuf, sizeof idbuf);
    CHECK(strcmp(idbuf, "[2001:db8:85a3::8a2e:370:7334]:65535") == 0);

    /* A bare IPv6 literal parses too (port after the last colon) and
     * formats back bracketed, the canonical form. */
    CHECK(parse_ok("::1:17101", &a));
    CHECK(a.ss_family == AF_INET6);
    member_format_id(&a, idbuf, sizeof idbuf);
    CHECK(strcmp(idbuf, "[::1]:17101") == 0);
    CHECK(parse_ok("2001:db8::1:17101", &a));
    member_format_id(&a, idbuf, sizeof idbuf);
    CHECK(strcmp(idbuf, "[2001:db8::1]:17101") == 0);

    /* Rejects: no port, bad port, empty host, bad host, junk. */
    CHECK(!parse_ok("not-an-address", &a));
    CHECK(!parse_ok("127.0.0.1:99999", &a));
    CHECK(!parse_ok("127.0.0.1:0", &a));
    CHECK(!parse_ok("127.0.0.1:12x", &a));
    CHECK(!parse_ok("[::1]", &a));
    CHECK(!parse_ok("[]:17101", &a));
    CHECK(!parse_ok("[::1]:abc", &a));
    CHECK(!parse_ok("[::1]:17101:extra", &a));
    CHECK(!parse_ok(":17101", &a));
    CHECK(!parse_ok("", &a));
    CHECK(!parse_ok("[gggg::1]:17101", &a));
    CHECK(!parse_ok("999.999.999.999:17101", &a));

    /* Address equality: same address, different port, cross-family. */
    CHECK(parse_ok("[::1]:17101", &a));
    CHECK(parse_ok("[::1]:17101", &b));
    CHECK(pulse_addr_equal(&a, &b));
    CHECK(parse_ok("[::1]:17102", &b));
    CHECK(!pulse_addr_equal(&a, &b));
    CHECK(parse_ok("127.0.0.1:17101", &b));
    CHECK(!pulse_addr_equal(&a, &b));
    CHECK(parse_ok("127.0.0.1:17101", &a));
    CHECK(parse_ok("127.0.0.1:17101", &b));
    CHECK(pulse_addr_equal(&a, &b));

    /* Bare-host parsing, for --bind. */
    CHECK(member_parse_host("::1", &a) == 0);
    CHECK(a.ss_family == AF_INET6);
    CHECK(member_parse_host("2001:db8::5", &a) == 0);
    CHECK(a.ss_family == AF_INET6);
    CHECK(member_parse_host("127.0.0.1", &a) == 0);
    CHECK(a.ss_family == AF_INET);
    CHECK(member_parse_host("garbage", &a) != 0);
    CHECK(member_parse_host("", &a) != 0);

    /* Gossip parsing carries IPv6 ids through unchanged: PING, JOIN,
     * PING_REQ (target address parsed), and a GOSSIP digest entry. */
    gmsg_t msg;
    const char *ping = "PING 7 [::1]:17102 3";
    CHECK(gossip_parse(ping, strlen(ping), &msg) == 0);
    CHECK(msg.type == GMSG_PING);
    CHECK(msg.seq == 7);
    CHECK(strcmp(msg.from_id, "[::1]:17102") == 0);
    CHECK(msg.from_inc == 3);

    const char *join = "JOIN [::1]:17102 3";
    CHECK(gossip_parse(join, strlen(join), &msg) == 0);
    CHECK(msg.type == GMSG_JOIN);
    CHECK(strcmp(msg.from_id, "[::1]:17102") == 0);

    const char *req = "PING_REQ 5 [::1]:17103 [::1]:17101";
    CHECK(gossip_parse(req, strlen(req), &msg) == 0);
    CHECK(msg.type == GMSG_PING_REQ);
    CHECK(msg.has_target == 1);
    CHECK(msg.target_addr.ss_family == AF_INET6);
    CHECK(strcmp(msg.target_id, "[::1]:17103") == 0);

    const char *gos = "GOSSIP [::1]:17101 1 1 [::1]:17102:1:A";
    CHECK(gossip_parse(gos, strlen(gos), &msg) == 0);
    CHECK(msg.type == GMSG_GOSSIP);
    CHECK(msg.entry_count == 1);
    CHECK(strcmp(msg.entries[0].id, "[::1]:17102") == 0);
    CHECK(msg.entries[0].incarnation == 1);
    CHECK(msg.entries[0].state == MEMBER_ALIVE);
    CHECK(msg.entries[0].addr.ss_family == AF_INET6);

    /* Membership table works with IPv6 ids end to end: add, direct
     * contact, and a gossip merge of a v6 entry. */
    member_table_t t;
    member_table_init(&t);
    struct sockaddr_storage a1, a2;
    CHECK(parse_ok("[::1]:17101", &a1));
    CHECK(parse_ok("[::1]:17102", &a2));
    member_t *m = member_add(&t, "[::1]:17101", &a1, 1);
    CHECK(m != NULL);
    CHECK(member_mark_alive(&t, "[::1]:17102", &a2, 1, 1000) == 1);
    CHECK(t.count == 2);
    int r = member_merge(&t, "[::1]:17101", "[::1]:17102", &a2, 1,
                         MEMBER_SUSPECT, 2000);
    CHECK(r & MERGE_CHANGED);
    CHECK(member_find(&t, "[::1]:17102")->state == MEMBER_SUSPECT);
    CHECK(member_find(&t, "[::1]:17102")->addr.ss_family == AF_INET6);

    printf("test_ipv6: %d checks, %d failures\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
