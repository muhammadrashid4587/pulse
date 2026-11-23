/* Unit tests for member metadata/annotations (no networking): parsing
 * and validation, set/format round-trips, gossip wire encoding and
 * parsing, merge propagation, and control-plane JSON rendering. */
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

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

static struct sockaddr_storage addr(const char *id) {
    struct sockaddr_storage a;
    if (member_parse_addr(id, &a) != 0) {
        memset(&a, 0, sizeof a);
    }
    return a;
}

/* Send one digest from t to a loopback receiver and parse it back.
 * Returns the parsed entry count, or -1 if nothing arrived. Same
 * real-UDP pattern as test_gossip.c: the sandbox refuses sendto() on
 * a UDP socket that has never been connect()ed, so the send socket is
 * connected once first. */
static int msg_roundtrip_digest(const member_table_t *t, gmsg_t *out) {
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
    if (connect(send_sock, (struct sockaddr *)&dest,
                sizeof(struct sockaddr_in)) < 0) {
        close(send_sock);
        close(recv_sock);
        return -1;
    }
    int rc = gossip_send_digest(send_sock, &dest, "127.0.0.1:17101", 3,
                                t, 0, NULL);
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

int main(void) {
    char key[PULSE_META_KEY_LEN + 1];
    char value[PULSE_META_VAL_LEN + 1];

    /* Parsing and validation. */
    CHECK(member_meta_parse("role=worker", key, sizeof key, value,
                            sizeof value) == 0);
    CHECK(strcmp(key, "role") == 0);
    CHECK(strcmp(value, "worker") == 0);
    CHECK(member_meta_parse("zone=us-east-1", key, sizeof key, value,
                            sizeof value) == 0);
    CHECK(strcmp(value, "us-east-1") == 0);
    CHECK(member_meta_parse("version=1.2.3", key, sizeof key, value,
                            sizeof value) == 0);
    CHECK(member_meta_parse("novalue", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("=novalue", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("key=", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("a=b=c", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("bad key=v", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("key=bad value", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("key=bad,value", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("key=bad;value", key, sizeof key, value,
                            sizeof value) != 0);
    CHECK(member_meta_parse("key=bad:value", key, sizeof key, value,
                            sizeof value) != 0);
    /* Keys and values are at most 32 chars; "key=value" at most 64. */
    char longkey[40];
    memset(longkey, 'k', sizeof longkey - 1);
    longkey[sizeof longkey - 1] = '\0';
    char longpair[80];
    snprintf(longpair, sizeof longpair, "%s=v", longkey);
    CHECK(member_meta_parse(longpair, key, sizeof key, value,
                            sizeof value) != 0);
    char longval[40];
    memset(longval, 'v', sizeof longval - 1);
    longval[sizeof longval - 1] = '\0';
    snprintf(longpair, sizeof longpair, "k=%s", longval);
    CHECK(member_meta_parse(longpair, key, sizeof key, value,
                            sizeof value) != 0);
    /* 32-char key + 31-char value = 64 chars total: allowed. */
    char k32[33], v31[32];
    memset(k32, 'k', 32); k32[32] = '\0';
    memset(v31, 'v', 31); v31[31] = '\0';
    snprintf(longpair, sizeof longpair, "%s=%s", k32, v31);
    CHECK(strlen(longpair) == 64);
    CHECK(member_meta_parse(longpair, key, sizeof key, value,
                            sizeof value) == 0);
    /* One char more (65 total) is rejected. */
    char v32[33];
    memset(v32, 'v', 32); v32[32] = '\0';
    snprintf(longpair, sizeof longpair, "%s=%s", k32, v32);
    CHECK(strlen(longpair) == 65);
    CHECK(member_meta_parse(longpair, key, sizeof key, value,
                            sizeof value) != 0);

    /* Set / format round-trip on a member. */
    member_table_t t;
    member_table_init(&t);
    struct sockaddr_storage a1 = addr("127.0.0.1:17101");
    member_t *m = member_add(&t, "127.0.0.1:17101", &a1, 1);
    CHECK(m->meta_count == 0);
    CHECK(member_meta_set(m, "role", "worker") == 0);
    CHECK(member_meta_set(m, "zone", "us-east-1") == 0);
    CHECK(m->meta_count == 2);
    char buf[600];
    CHECK(member_meta_format(m, buf, sizeof buf) == 26);
    CHECK(strcmp(buf, "role=worker,zone=us-east-1") == 0);
    /* Updating an existing key replaces the value, adds no pair. */
    CHECK(member_meta_set(m, "role", "gateway") == 0);
    CHECK(m->meta_count == 2);
    CHECK(member_meta_format(m, buf, sizeof buf) >= 0);
    CHECK(strcmp(buf, "role=gateway,zone=us-east-1") == 0);
    /* Invalid pairs are rejected and change nothing. */
    CHECK(member_meta_set(m, "bad key", "v") == -1);
    CHECK(member_meta_set(m, "k", "bad value") == -1);
    CHECK(m->meta_count == 2);
    /* The cap is 8 distinct keys: fill to 8, the 9th fails. */
    CHECK(member_meta_set(m, "k1", "v") == 0);
    CHECK(member_meta_set(m, "k2", "v") == 0);
    CHECK(member_meta_set(m, "k3", "v") == 0);
    CHECK(member_meta_set(m, "k4", "v") == 0);
    CHECK(member_meta_set(m, "k5", "v") == 0);
    CHECK(member_meta_set(m, "k6", "v") == 0);
    CHECK(m->meta_count == 8);
    CHECK(member_meta_set(m, "k7", "v") == -2);
    CHECK(m->meta_count == 8);
    /* A member with no annotations formats to "". */
    member_t *plain = member_add(&t, "127.0.0.1:17102", &a1, 1);
    CHECK(member_meta_format(plain, buf, sizeof buf) == 0);
    CHECK(strcmp(buf, "") == 0);

    /* List parsing. */
    meta_kv_t list[PULSE_MAX_META];
    int nlist = -1;
    CHECK(member_meta_parse_list("role=worker,zone=us-east-1", list,
                                 PULSE_MAX_META, &nlist) == 0);
    CHECK(nlist == 2);
    CHECK(strcmp(list[0].key, "role") == 0);
    CHECK(strcmp(list[1].value, "us-east-1") == 0);
    CHECK(member_meta_parse_list("", list, PULSE_MAX_META, &nlist) == 0);
    CHECK(nlist == 0);
    CHECK(member_meta_parse_list("role=worker,bad", list, PULSE_MAX_META,
                                 &nlist) != 0);
    CHECK(member_meta_parse_list("role=a,role=b", list, PULSE_MAX_META,
                                 &nlist) != 0);

    /* Equality ignores order. */
    member_table_t t2;
    member_table_init(&t2);
    member_t *x = member_add(&t2, "127.0.0.1:17101", &a1, 1);
    member_t *y = member_add(&t2, "127.0.0.1:17102", &a1, 1);
    member_meta_set(x, "role", "worker");
    member_meta_set(x, "zone", "us-east-1");
    member_meta_set(y, "zone", "us-east-1");
    member_meta_set(y, "role", "worker");
    CHECK(member_meta_equal(x, y));
    member_meta_set(y, "role", "gateway");
    CHECK(!member_meta_equal(x, y));

    /* JSON rendering: a JSON object, {} when there are none. */
    char json[600];
    member_meta_json(x, json, sizeof json);
    CHECK(strcmp(json, "{\"role\":\"worker\",\"zone\":\"us-east-1\"}") == 0);
    member_meta_json(plain, json, sizeof json);
    CHECK(strcmp(json, "{}") == 0);

    /* Wire round-trip: a digest carrying annotations parses back with
     * them, and entries without the suffix parse as before. */
    member_table_t tw;
    member_table_init(&tw);
    struct sockaddr_storage aw = addr("127.0.0.1:17101");
    member_t *w = member_add(&tw, "127.0.0.1:17101", &aw, 3);
    member_meta_set(w, "role", "worker");
    member_meta_set(w, "zone", "us-east-1");
    member_add(&tw, "127.0.0.1:17102", &aw, 1); /* no annotations */
    gmsg_t msg;
    CHECK(msg_roundtrip_digest(&tw, &msg) == 2);
    CHECK(msg.type == GMSG_GOSSIP);
    int wi = -1;
    for (int i = 0; i < msg.entry_count; i++) {
        if (strcmp(msg.entries[i].id, "127.0.0.1:17101") == 0) wi = i;
    }
    CHECK(wi >= 0);
    CHECK(msg.entries[wi].meta_count == 2);
    CHECK(strcmp(msg.entries[wi].meta[0].key, "role") == 0);
    CHECK(strcmp(msg.entries[wi].meta[1].value, "us-east-1") == 0);
    int pi = -1;
    for (int i = 0; i < msg.entry_count; i++) {
        if (strcmp(msg.entries[i].id, "127.0.0.1:17102") == 0) pi = i;
    }
    CHECK(pi >= 0);
    CHECK(msg.entries[pi].meta_count == 0);
    /* A malformed meta suffix is dropped; the entry still merges. */
    const char *badmeta =
        "GOSSIP 127.0.0.1:17101 3 1 127.0.0.1:17102:1:A;bad";
    CHECK(gossip_parse(badmeta, strlen(badmeta), &msg) == 0);
    CHECK(msg.entry_count == 1);
    CHECK(msg.entries[0].meta_count == 0);
    CHECK(strcmp(msg.entries[0].id, "127.0.0.1:17102") == 0);
    /* A plain entry without any suffix still parses. */
    const char *plainmsg =
        "GOSSIP 127.0.0.1:17101 3 1 127.0.0.1:17102:1:S";
    CHECK(gossip_parse(plainmsg, strlen(plainmsg), &msg) == 0);
    CHECK(msg.entry_count == 1);
    CHECK(msg.entries[0].state == MEMBER_SUSPECT);

    /* Merge propagation: annotations learned from gossip land on the
     * entry and stamp it changed; an entry without annotations never
     * wipes stored ones; self entries are untouched. */
    member_table_t tm;
    member_table_init(&tm);
    struct sockaddr_storage am = addr("127.0.0.1:17101");
    member_add(&tm, "127.0.0.1:17101", &am, 1); /* self */
    member_add(&tm, "127.0.0.1:17102", &am, 1);
    uint64_t v_before = tm.version;
    meta_kv_t incoming[2];
    int ninc = 0;
    CHECK(member_meta_parse_list("role=worker,zone=us-east-1", incoming,
                                 PULSE_MAX_META, &ninc) == 0);
    CHECK(member_merge_meta(&tm, "127.0.0.1:17101", "127.0.0.1:17102",
                            incoming, ninc) == 1);
    member_t *learned = member_find(&tm, "127.0.0.1:17102");
    CHECK(learned->meta_count == 2);
    CHECK(tm.version > v_before); /* stamped changed: re-gossips */
    /* Same annotations again: no change, no version bump. */
    v_before = tm.version;
    CHECK(member_merge_meta(&tm, "127.0.0.1:17101", "127.0.0.1:17102",
                            incoming, ninc) == 0);
    CHECK(tm.version == v_before);
    /* An entry with no annotations leaves stored ones alone. */
    CHECK(member_merge_meta(&tm, "127.0.0.1:17101", "127.0.0.1:17102",
                            incoming, 0) == 0);
    CHECK(learned->meta_count == 2);
    /* A changed value propagates. */
    CHECK(member_meta_parse_list("role=gateway,zone=us-east-1", incoming,
                                 PULSE_MAX_META, &ninc) == 0);
    CHECK(member_merge_meta(&tm, "127.0.0.1:17101", "127.0.0.1:17102",
                            incoming, ninc) == 1);
    CHECK(strcmp(learned->meta[0].value, "gateway") == 0);
    /* Self entries are never overwritten from gossip. */
    CHECK(member_merge_meta(&tm, "127.0.0.1:17101", "127.0.0.1:17101",
                            incoming, ninc) == 0);
    CHECK(member_find(&tm, "127.0.0.1:17101")->meta_count == 0);
    /* Unknown members are ignored. */
    CHECK(member_merge_meta(&tm, "127.0.0.1:17101", "127.0.0.1:17999",
                            incoming, ninc) == 0);

    printf("test_meta: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
