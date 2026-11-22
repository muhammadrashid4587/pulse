#define _POSIX_C_SOURCE 200809L
#include "gossip.h"
#include "secret.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Optional shared secret, set once at startup by gossip_set_secret.
 * When set, every outgoing message carries a trailing " <tag>" token;
 * when unset, messages go out exactly as before. */
static char g_secret[PULSE_SECRET_MAX];
static int g_has_secret = 0;

void gossip_set_secret(const char *secret) {
    if (secret && secret[0] != '\0') {
        snprintf(g_secret, sizeof g_secret, "%s", secret);
        g_has_secret = 1;
    } else {
        g_secret[0] = '\0';
        g_has_secret = 0;
    }
}

static int send_str(int sock, const struct sockaddr_storage *to, const char *s) {
    /* Tagging needs a little headroom past PULSE_MAX_MSG; the buffer is
     * stack-local, so concurrent sends from the receiver and prober
     * threads cannot clobber each other. */
    char tagged[PULSE_MAX_MSG + 32];
    const char *out = s;
    if (g_has_secret) {
        size_t n = strlen(s);
        if (n >= sizeof tagged - PULSE_TAG_HEX_LEN - 2)
            n = sizeof tagged - PULSE_TAG_HEX_LEN - 2;
        memcpy(tagged, s, n);
        tagged[n] = '\0';
        char tag[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag(g_secret, (const unsigned char *)tagged, n, tag);
        snprintf(tagged + n, sizeof tagged - n, " %s", tag);
        out = tagged;
    }
    /* The length must match the family: passing sizeof(storage) for an
     * IPv4 destination is harmless on Linux, but passing it for an
     * IPv6 destination is required, and the reverse is wrong. */
    ssize_t n = sendto(sock, out, strlen(out), 0,
                       (const struct sockaddr *)to, pulse_addr_len(to));
    return n < 0 ? -1 : 0;
}

int gossip_send_ping(int sock, const struct sockaddr_storage *to, uint32_t seq,
                     const char *from_id, uint32_t inc) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "PING %u %s %u", seq, from_id, inc);
    return send_str(sock, to, buf);
}

int gossip_send_ack(int sock, const struct sockaddr_storage *to, uint32_t seq,
                    const char *from_id, uint32_t inc) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "ACK %u %s %u", seq, from_id, inc);
    return send_str(sock, to, buf);
}

int gossip_send_ping_req(int sock, const struct sockaddr_storage *to, uint32_t seq,
                         const char *target_id, const char *from_id) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "PING_REQ %u %s %s", seq, target_id, from_id);
    return send_str(sock, to, buf);
}

int gossip_send_join(int sock, const struct sockaddr_storage *to,
                     const char *from_id, uint32_t inc) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "JOIN %s %u", from_id, inc);
    return send_str(sock, to, buf);
}

int gossip_send_digest(int sock, const struct sockaddr_storage *to,
                       const char *from_id, uint32_t inc,
                       const member_table_t *t) {
    /* Entry text is built first so the header count can cover both the
     * member window and the tombstones appended after it. */
    char entries[1280];
    int off = 0;
    int n = t->count;
    if (n == 0) return 0;
    int start = rand() % n;
    int count = n < PULSE_GOSSIP_ENTRIES ? n : PULSE_GOSSIP_ENTRIES;
    for (int k = 0; k < count; k++) {
        const member_t *m = &t->members[(start + k) % n];
        int w = snprintf(entries + off, sizeof entries - (size_t)off,
                         " %s:%u:%c", m->id, m->incarnation,
                         member_state_code(m->state));
        if (w < 0 || (size_t)(off + w) >= sizeof entries) break;
        off += w;
    }
    /* Tombstones ride along as DEAD entries when there is room: a
     * force-leave (or a GC removal) then spreads to peers, which mark
     * the member DEAD and GC it on their own retention timer. A live
     * member that hears itself DEAD refutes by bumping its incarnation,
     * exactly as for a DEAD entry already in the table. */
    int total = count;
    for (int k = 0; k < t->tombstone_count && total < PULSE_GOSSIP_ENTRIES; k++) {
        const tombstone_t *ts = &t->tombstones[k];
        int w = snprintf(entries + off, sizeof entries - (size_t)off,
                         " %s:%u:%c", ts->id, ts->incarnation,
                         member_state_code(MEMBER_DEAD));
        if (w < 0 || (size_t)(off + w) >= sizeof entries) break;
        off += w;
        total++;
    }
    char buf[PULSE_MAX_MSG];
    int hoff = snprintf(buf, sizeof buf, "GOSSIP %s %u %d%s", from_id, inc,
                        total, entries);
    if (hoff < 0 || (size_t)hoff >= sizeof buf) return -1;
    return send_str(sock, to, buf);
}

int gossip_parse(const char *buf, size_t len, gmsg_t *out) {
    memset(out, 0, sizeof(*out));
    char tmp[PULSE_MAX_MSG];
    if (len >= sizeof tmp) len = sizeof tmp - 1;
    memcpy(tmp, buf, len);
    tmp[len] = '\0';

    char *save = NULL;
    char *tok = strtok_r(tmp, " \t\r\n", &save);
    if (!tok) return -1;

    if (strcmp(tok, "PING") == 0) {
        out->type = GMSG_PING;
    } else if (strcmp(tok, "ACK") == 0) {
        out->type = GMSG_ACK;
    } else if (strcmp(tok, "PING_REQ") == 0) {
        out->type = GMSG_PING_REQ;
    } else if (strcmp(tok, "JOIN") == 0) {
        out->type = GMSG_JOIN;
    } else if (strcmp(tok, "GOSSIP") == 0) {
        out->type = GMSG_GOSSIP;
    } else {
        out->type = GMSG_UNKNOWN;
        return -1;
    }

    char *s;
    if (out->type == GMSG_PING || out->type == GMSG_ACK) {
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        out->seq = (uint32_t)strtoul(s, NULL, 10);
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        snprintf(out->from_id, sizeof out->from_id, "%s", s);
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        out->from_inc = (uint32_t)strtoul(s, NULL, 10);
    } else if (out->type == GMSG_PING_REQ) {
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        out->seq = (uint32_t)strtoul(s, NULL, 10);
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        snprintf(out->target_id, sizeof out->target_id, "%s", s);
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        snprintf(out->from_id, sizeof out->from_id, "%s", s);
        if (member_parse_addr(out->target_id, &out->target_addr) == 0)
            out->has_target = 1;
    } else if (out->type == GMSG_JOIN) {
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        snprintf(out->from_id, sizeof out->from_id, "%s", s);
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        out->from_inc = (uint32_t)strtoul(s, NULL, 10);
    } else if (out->type == GMSG_GOSSIP) {
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        snprintf(out->from_id, sizeof out->from_id, "%s", s);
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        out->from_inc = (uint32_t)strtoul(s, NULL, 10);
        s = strtok_r(NULL, " \t\r\n", &save);
        if (!s) return -1;
        int count = atoi(s);
        for (int i = 0; i < count && i < PULSE_MAX_MEMBERS; i++) {
            s = strtok_r(NULL, " \t\r\n", &save);
            if (!s) break;
            /* Entry is "host:port:inc:state"; split from the right. */
            char *last = strrchr(s, ':');
            if (!last || last == s) continue;
            char code = last[1];
            *last = '\0';
            char *prev = strrchr(s, ':');
            if (!prev || prev == s) continue;
            uint32_t inc = (uint32_t)strtoul(prev + 1, NULL, 10);
            *prev = '\0';
            member_t *e = &out->entries[out->entry_count];
            snprintf(e->id, sizeof e->id, "%s", s);
            e->incarnation = inc;
            e->state = member_state_from_code(code);
            if (member_parse_addr(e->id, &e->addr) != 0) continue;
            e->last_seen_ms = 0;
            e->state_since_ms = 0;
            out->entry_count++;
        }
    }
    return 0;
}
