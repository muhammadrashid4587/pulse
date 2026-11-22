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
}

member_t *member_find(member_table_t *t, const char *id) {
    for (int i = 0; i < t->count; i++) {
        if (strcmp(t->members[i].id, id) == 0) return &t->members[i];
    }
    return NULL;
}

member_t *member_add(member_table_t *t, const char *id,
                     const struct sockaddr_in *addr, uint32_t incarnation) {
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
    return m;
}

int member_parse_addr(const char *id, struct sockaddr_in *addr) {
    const char *colon = strrchr(id, ':');
    if (!colon) return -1;
    char host[64];
    size_t hlen = (size_t)(colon - id);
    if (hlen == 0 || hlen >= sizeof(host)) return -1;
    memcpy(host, id, hlen);
    host[hlen] = '\0';
    int port = atoi(colon + 1);
    if (port <= 0 || port > 65535) return -1;
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr->sin_addr) != 1) return -1;
    return 0;
}

void member_format_id(const struct sockaddr_in *addr, char *id, size_t len) {
    char host[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr->sin_addr, host, sizeof(host));
    snprintf(id, len, "%s:%d", host, ntohs(addr->sin_port));
}

static int state_rank(member_state_t s) {
    switch (s) {
        case MEMBER_DEAD: return 2;
        case MEMBER_SUSPECT: return 1;
        default: return 0;
    }
}

int member_merge(member_table_t *t, const char *self_id, const char *id,
                 const struct sockaddr_in *addr, uint32_t incarnation,
                 member_state_t state, int64_t now) {
    if (strcmp(id, self_id) == 0) {
        member_t *self = member_find(t, self_id);
        if (self && state != MEMBER_ALIVE && incarnation >= self->incarnation)
            return MERGE_SELF_SUSPECTED;
        return MERGE_NONE;
    }
    member_t *m = member_find(t, id);
    if (!m) {
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
                      const struct sockaddr_in *addr, uint32_t incarnation,
                      int64_t now) {
    member_t *m = member_find(t, id);
    if (!m) {
        m = member_add(t, id, addr, incarnation);
        if (!m) return 0;
        m->last_seen_ms = now;
        m->state_since_ms = now;
        return 1;
    }
    int changed = (m->state != MEMBER_ALIVE);
    m->state = MEMBER_ALIVE;
    if (incarnation > m->incarnation) m->incarnation = incarnation;
    if (addr) m->addr = *addr;
    m->last_seen_ms = now;
    if (changed) m->state_since_ms = now;
    return changed;
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
