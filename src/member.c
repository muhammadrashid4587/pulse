#define _POSIX_C_SOURCE 200809L
#include "member.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int64_t pulse_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

void member_table_init(member_table_t *t) {
    t->count = 0;
    t->tombstone_count = 0;
}

/* Tombstones: a GC'd member leaves a record so gossip cannot bring it
 * back. A tombstone is keyed by id; a rejoin at a higher incarnation
 * clears it, and direct contact clears it too (a packet from the member
 * is proof of life). Tombstones expire after their retention: by then
 * every peer has had a chance to GC the entry itself, so nobody is left
 * to re-gossip it. */
static tombstone_t *tombstone_find(member_table_t *t, const char *id) {
    for (int i = 0; i < t->tombstone_count; i++) {
        if (strcmp(t->tombstones[i].id, id) == 0) return &t->tombstones[i];
    }
    return NULL;
}

static void tombstone_remove(member_table_t *t, const char *id) {
    for (int i = 0; i < t->tombstone_count; i++) {
        if (strcmp(t->tombstones[i].id, id) == 0) {
            t->tombstones[i] = t->tombstones[--t->tombstone_count];
            return;
        }
    }
}

static void tombstone_add(member_table_t *t, const char *id,
                          uint32_t incarnation, int64_t now) {
    if (tombstone_find(t, id)) return; /* already recorded */
    if (t->tombstone_count >= PULSE_MAX_TOMBSTONES) {
        /* Full: evict the oldest tombstone to make room. */
        int oldest = 0;
        for (int i = 1; i < t->tombstone_count; i++) {
            if (t->tombstones[i].removed_ms < t->tombstones[oldest].removed_ms)
                oldest = i;
        }
        t->tombstones[oldest] = t->tombstones[--t->tombstone_count];
    }
    tombstone_t *ts = &t->tombstones[t->tombstone_count++];
    /* Copy through a local buffer: id may point into the table itself
     * (member_gc passes m->id), and snprintf warns about the overlap. */
    char idbuf[PULSE_ID_LEN];
    snprintf(idbuf, sizeof idbuf, "%s", id);
    snprintf(ts->id, sizeof(ts->id), "%s", idbuf);
    ts->incarnation = incarnation;
    ts->removed_ms = now;
}

int member_gc(member_table_t *t, int64_t now, int64_t dead_retention_ms,
              int64_t tombstone_retention_ms,
              char removed[][PULSE_ID_LEN], int max_removed) {
    /* Expire old tombstones first. */
    for (int i = 0; i < t->tombstone_count;) {
        if (now - t->tombstones[i].removed_ms >= tombstone_retention_ms) {
            t->tombstones[i] = t->tombstones[--t->tombstone_count];
        } else {
            i++;
        }
    }
    int n_removed = 0;
    for (int i = 0; i < t->count;) {
        member_t *m = &t->members[i];
        if (m->state == MEMBER_DEAD &&
            now - m->state_since_ms >= dead_retention_ms) {
            tombstone_add(t, m->id, m->incarnation, now);
            if (n_removed < max_removed) {
                snprintf(removed[n_removed], PULSE_ID_LEN, "%s", m->id);
            }
            n_removed++;
            /* Swap-remove: the last entry moves into this slot. */
            t->members[i] = t->members[--t->count];
        } else {
            i++;
        }
    }
    return n_removed;
}

member_t *member_find(member_table_t *t, const char *id) {
    for (int i = 0; i < t->count; i++) {
        if (strcmp(t->members[i].id, id) == 0) return &t->members[i];
    }
    return NULL;
}

member_t *member_add(member_table_t *t, const char *id,
                     const struct sockaddr_storage *addr,
                     uint32_t incarnation) {
    member_t *m = member_find(t, id);
    if (m) return m;
    if (t->count >= PULSE_MAX_MEMBERS) return NULL;
    m = &t->members[t->count++];
    memset(m, 0, sizeof(*m));
    snprintf(m->id, sizeof(m->id), "%s", id);
    if (addr) {
        m->addr = *addr;
    } else {
        memset(&m->addr, 0, sizeof(m->addr));
    }
    m->incarnation = incarnation;
    m->state = MEMBER_ALIVE;
    m->last_seen_ms = 0;
    m->state_since_ms = 0;
    phi_init(&m->phi);
    return m;
}

/* Strict port check: all digits, 1..65535. atoi would silently accept
 * "12x" as 12 and "99999" as a wrapped value; ids must not do that. */
static int parse_port(const char *s, uint16_t *port) {
    if (!s || *s == '\0') return -1;
    long v = 0;
    for (const char *p = s; *p; p++) {
        if (*p < '0' || *p > '9') return -1;
        v = v * 10 + (*p - '0');
        if (v > 65535) return -1;
    }
    if (v <= 0) return -1;
    *port = (uint16_t)v;
    return 0;
}

int member_parse_host(const char *host, struct sockaddr_storage *addr) {
    memset(addr, 0, sizeof(*addr));
    if (!host || *host == '\0') return -1;
    struct sockaddr_in *a4 = (struct sockaddr_in *)addr;
    if (inet_pton(AF_INET, host, &a4->sin_addr) == 1) {
        a4->sin_family = AF_INET;
        return 0;
    }
    struct sockaddr_in6 *a6 = (struct sockaddr_in6 *)addr;
    if (inet_pton(AF_INET6, host, &a6->sin6_addr) == 1) {
        a6->sin6_family = AF_INET6;
        return 0;
    }
    return -1;
}

int member_parse_addr(const char *id, struct sockaddr_storage *addr) {
    if (!id || *id == '\0') return -1;
    const char *host_start, *host_end, *port_str;
    if (id[0] == '[') {
        /* Bracketed IPv6: "[::1]:17101". */
        const char *close = strchr(id, ']');
        if (!close || close[1] != ':' || close == id + 1) return -1;
        host_start = id + 1;
        host_end = close;
        port_str = close + 2;
    } else {
        /* Bare form: the port is after the last colon. For an IPv4
         * id that is the only colon; for a bare IPv6 literal
         * ("::1:17101") the host part still contains colons. */
        const char *colon = strrchr(id, ':');
        if (!colon || colon == id) return -1;
        host_start = id;
        host_end = colon;
        port_str = colon + 1;
    }
    char host[64];
    size_t hlen = (size_t)(host_end - host_start);
    if (hlen == 0 || hlen >= sizeof(host)) return -1;
    memcpy(host, host_start, hlen);
    host[hlen] = '\0';
    uint16_t port;
    if (parse_port(port_str, &port) != 0) return -1;
    /* A host part with a colon in it is IPv6; without, IPv4. */
    int v6 = strchr(host, ':') != NULL;
    memset(addr, 0, sizeof(*addr));
    if (v6) {
        struct sockaddr_in6 *a6 = (struct sockaddr_in6 *)addr;
        if (inet_pton(AF_INET6, host, &a6->sin6_addr) != 1) return -1;
        a6->sin6_family = AF_INET6;
        a6->sin6_port = htons(port);
    } else {
        struct sockaddr_in *a4 = (struct sockaddr_in *)addr;
        if (inet_pton(AF_INET, host, &a4->sin_addr) != 1) return -1;
        a4->sin_family = AF_INET;
        a4->sin_port = htons(port);
    }
    return 0;
}

void member_format_id(const struct sockaddr_storage *addr, char *id,
                      size_t len) {
    if (addr->ss_family == AF_INET6) {
        const struct sockaddr_in6 *a6 =
            (const struct sockaddr_in6 *)addr;
        char host[INET6_ADDRSTRLEN];
        inet_ntop(AF_INET6, &a6->sin6_addr, host, sizeof(host));
        snprintf(id, len, "[%s]:%d", host, ntohs(a6->sin6_port));
    } else {
        const struct sockaddr_in *a4 = (const struct sockaddr_in *)addr;
        char host[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &a4->sin_addr, host, sizeof(host));
        snprintf(id, len, "%s:%d", host, ntohs(a4->sin_port));
    }
}

socklen_t pulse_addr_len(const struct sockaddr_storage *addr) {
    switch (addr->ss_family) {
        case AF_INET: return sizeof(struct sockaddr_in);
        case AF_INET6: return sizeof(struct sockaddr_in6);
        default: return 0;
    }
}

int pulse_addr_equal(const struct sockaddr_storage *a,
                     const struct sockaddr_storage *b) {
    if (a->ss_family != b->ss_family) return 0;
    if (a->ss_family == AF_INET6) {
        const struct sockaddr_in6 *x = (const struct sockaddr_in6 *)a;
        const struct sockaddr_in6 *y = (const struct sockaddr_in6 *)b;
        return x->sin6_port == y->sin6_port &&
               memcmp(&x->sin6_addr, &y->sin6_addr,
                      sizeof(struct in6_addr)) == 0;
    }
    if (a->ss_family == AF_INET) {
        const struct sockaddr_in *x = (const struct sockaddr_in *)a;
        const struct sockaddr_in *y = (const struct sockaddr_in *)b;
        return x->sin_port == y->sin_port &&
               memcmp(&x->sin_addr, &y->sin_addr,
                      sizeof(struct in_addr)) == 0;
    }
    return 0;
}

static int state_rank(member_state_t s) {
    switch (s) {
        case MEMBER_DEAD: return 2;
        case MEMBER_SUSPECT: return 1;
        default: return 0;
    }
}

int member_merge(member_table_t *t, const char *self_id, const char *id,
                 const struct sockaddr_storage *addr, uint32_t incarnation,
                 member_state_t state, int64_t now) {
    if (strcmp(id, self_id) == 0) {
        member_t *self = member_find(t, self_id);
        if (self && state != MEMBER_ALIVE && incarnation >= self->incarnation)
            return MERGE_SELF_SUSPECTED;
        return MERGE_NONE;
    }
    member_t *m = member_find(t, id);
    if (!m) {
        /* A tombstoned id only comes back at a higher incarnation: the
         * member rejoined (its incarnation file bumps on every restart).
         * Gossip at the recorded incarnation or below is a peer that
         * still holds the stale DEAD entry; ignore it. */
        tombstone_t *ts = tombstone_find(t, id);
        if (ts) {
            if (incarnation <= ts->incarnation) return MERGE_NONE;
            tombstone_remove(t, id);
        }
        m = member_add(t, id, addr, incarnation);
        if (!m) return MERGE_NONE;
        m->state = state;
        m->state_since_ms = now;
        return MERGE_NEW;
    }
    if (incarnation > m->incarnation) {
        m->incarnation = incarnation;
        m->state = state;
        m->state_since_ms = now;
        if (addr) m->addr = *addr;
        return MERGE_CHANGED;
    }
    if (incarnation == m->incarnation && state_rank(state) > state_rank(m->state)) {
        m->state = state;
        m->state_since_ms = now;
        return MERGE_CHANGED;
    }
    return MERGE_NONE;
}

int member_mark_suspect(member_table_t *t, const char *id, int64_t now) {
    member_t *m = member_find(t, id);
    if (!m || m->state != MEMBER_ALIVE) return 0;
    m->state = MEMBER_SUSPECT;
    m->state_since_ms = now;
    return 1;
}

int member_mark_dead(member_table_t *t, const char *id, int64_t now) {
    member_t *m = member_find(t, id);
    if (!m || m->state == MEMBER_DEAD) return 0;
    m->state = MEMBER_DEAD;
    m->state_since_ms = now;
    return 1;
}

int member_mark_alive(member_table_t *t, const char *id,
                      const struct sockaddr_storage *addr,
                      uint32_t incarnation, int64_t now) {
    member_t *m = member_find(t, id);
    if (!m) {
        /* Direct contact is proof of life: a member that talks to us is
         * alive even if we garbage-collected its DEAD entry. Drop any
         * tombstone and re-add it, whatever its incarnation. (The
         * supported restart path bumps the incarnation anyway; a node
         * that restarts with a wiped data dir reuses incarnation 1 and
         * still comes back through this path.) */
        tombstone_remove(t, id);
        m = member_add(t, id, addr, incarnation);
        if (!m) return 0;
        m->last_seen_ms = now;
        m->state_since_ms = now;
        phi_heartbeat(&m->phi, now);
        return 1;
    }
    int changed = (m->state != MEMBER_ALIVE);
    m->state = MEMBER_ALIVE;
    if (incarnation > m->incarnation) m->incarnation = incarnation;
    if (addr) m->addr = *addr;
    m->last_seen_ms = now;
    if (changed) m->state_since_ms = now;
    phi_heartbeat(&m->phi, now);
    return changed;
}

int member_force_remove(member_table_t *t, const char *id, int64_t now) {
    member_t *m = member_find(t, id);
    if (!m) return 0;
    tombstone_add(t, m->id, m->incarnation, now);
    /* Swap-remove: the last entry moves into this slot. */
    *m = t->members[--t->count];
    return 1;
}

int member_count_state(const member_table_t *t, member_state_t state) {
    int n = 0;
    for (int i = 0; i < t->count; i++) {
        if (t->members[i].state == state) n++;
    }
    return n;
}

member_t *member_random_peer(member_table_t *t, const char *self_id) {
    int idx[PULSE_MAX_MEMBERS];
    int n = 0;
    for (int i = 0; i < t->count; i++) {
        if (strcmp(t->members[i].id, self_id) == 0) continue;
        if (t->members[i].state == MEMBER_DEAD) continue;
        idx[n++] = i;
    }
    if (n == 0) return NULL;
    return &t->members[idx[rand() % n]];
}

const char *member_state_name(member_state_t s) {
    switch (s) {
        case MEMBER_ALIVE: return "ALIVE";
        case MEMBER_SUSPECT: return "SUSPECT";
        case MEMBER_DEAD: return "DEAD";
        default: return "UNKNOWN";
    }
}

char member_state_code(member_state_t s) {
    switch (s) {
        case MEMBER_ALIVE: return 'A';
        case MEMBER_SUSPECT: return 'S';
        case MEMBER_DEAD: return 'D';
        default: return 'A';
    }
}

member_state_t member_state_from_code(char c) {
    switch (c) {
        case 'S': return MEMBER_SUSPECT;
        case 'D': return MEMBER_DEAD;
        default: return MEMBER_ALIVE;
    }
}
