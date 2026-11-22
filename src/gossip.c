#define _POSIX_C_SOURCE 200809L
#include "gossip.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int send_str(int sock, const struct sockaddr_in *to, const char *s) {
    ssize_t n = sendto(sock, s, strlen(s), 0,
                       (const struct sockaddr *)to, sizeof(*to));
    return n < 0 ? -1 : 0;
}

int gossip_send_ping(int sock, const struct sockaddr_in *to, uint32_t seq,
                     const char *from_id, uint32_t inc) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "PING %u %s %u", seq, from_id, inc);
    return send_str(sock, to, buf);
}

int gossip_send_ack(int sock, const struct sockaddr_in *to, uint32_t seq,
                    const char *from_id, uint32_t inc) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "ACK %u %s %u", seq, from_id, inc);
    return send_str(sock, to, buf);
}

int gossip_send_ping_req(int sock, const struct sockaddr_in *to, uint32_t seq,
                         const char *target_id, const char *from_id) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "PING_REQ %u %s %s", seq, target_id, from_id);
    return send_str(sock, to, buf);
}

int gossip_send_join(int sock, const struct sockaddr_in *to,
                     const char *from_id, uint32_t inc) {
    char buf[PULSE_MAX_MSG];
    snprintf(buf, sizeof buf, "JOIN %s %u", from_id, inc);
    return send_str(sock, to, buf);
}

int gossip_send_digest(int sock, const struct sockaddr_in *to,
                       const char *from_id, uint32_t inc,
                       const member_table_t *t) {
    char buf[PULSE_MAX_MSG];
    int n = t->count;
    if (n == 0) return 0;
    int start = rand() % n;
    int count = n < PULSE_GOSSIP_ENTRIES ? n : PULSE_GOSSIP_ENTRIES;
    int off = snprintf(buf, sizeof buf, "GOSSIP %s %u %d", from_id, inc, count);
    if (off < 0 || (size_t)off >= sizeof buf) return -1;
    for (int k = 0; k < count; k++) {
        const member_t *m = &t->members[(start + k) % n];
        int w = snprintf(buf + off, sizeof buf - (size_t)off, " %s:%u:%c",
                         m->id, m->incarnation, member_state_code(m->state));
        if (w < 0 || (size_t)(off + w) >= sizeof buf) break;
        off += w;
    }
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
