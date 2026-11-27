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
    t->version = 0;
}

/* Record a change to one entry: bump the table version and stamp the
 * entry with it, so a later digest can tell whether this peer already
 * knows about the change. Every function below that changes what
 * gossip would send goes through here (directly or via
 * note_tombstone_change). A pure heartbeat — last_seen_ms moving —
 * is not a change: the entry's gossip content is identical. */
static void note_change(member_table_t *t, member_t *m) {
    t->version++;
    m->changed_version = t->version;
}

void member_note_changed(member_table_t *t, member_t *m) {
    note_change(t, m);
}

/* Same, for a tombstone: bumping the version also covers the table
 * entry that was removed to make room for it (one observable change:
 * the member is gone and the tombstone says DEAD at its incarnation). */
static void note_tombstone_change(member_table_t *t, tombstone_t *ts) {
    t->version++;
    ts->changed_version = t->version;
}

int member_changed_count(const member_table_t *t, uint64_t since_version) {
    int n = 0;
    for (int i = 0; i < t->count; i++) {
        if (t->members[i].changed_version > since_version) n++;
    }
    for (int i = 0; i < t->tombstone_count; i++) {
        if (t->tombstones[i].changed_version > since_version) n++;
    }
    return n;
}

/* Metadata: a key or value is 1..32 chars of [A-Za-z0-9._-]. The
 * charset keeps annotations safe in the wire format (no spaces,
 * commas, semicolons, colons or '=' inside a key or value) and in
 * JSON (no quotes or backslashes to escape). */
static int meta_token_ok(const char *s, size_t max_len) {
    size_t n = strlen(s);
    if (n == 0 || n > max_len) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'))
            return 0;
    }
    return 1;
}

int member_meta_parse(const char *s, char *key, size_t klen, char *value,
                      size_t vlen) {
    if (!s || strlen(s) > PULSE_META_MAX_LEN) return -1;
    const char *eq = strchr(s, '=');
    if (!eq || eq == s || eq[1] == '\0') return -1;
    /* Exactly one '=': a second one would be inside the value. */
    if (strchr(eq + 1, '=') != NULL) return -1;
    size_t k = (size_t)(eq - s);
    size_t v = strlen(eq + 1);
    if (k >= klen || v >= vlen) return -1;
    memcpy(key, s, k);
    key[k] = '\0';
    memcpy(value, eq + 1, v);
    value[v] = '\0';
    if (!meta_token_ok(key, PULSE_META_KEY_LEN)) return -1;
    if (!meta_token_ok(value, PULSE_META_VAL_LEN)) return -1;
    return 0;
}

int member_meta_set(member_t *m, const char *key, const char *value) {
    if (!meta_token_ok(key, PULSE_META_KEY_LEN)) return -1;
    if (!meta_token_ok(value, PULSE_META_VAL_LEN)) return -1;
    if (strlen(key) + 1 + strlen(value) > PULSE_META_MAX_LEN) return -1;
    /* Validated above: key and value fit their buffers, so memcpy is
     * exact — snprintf would warn about truncation it cannot see. */
    size_t klen = strlen(key);
    size_t vlen = strlen(value);
    for (int i = 0; i < m->meta_count; i++) {
        if (strcmp(m->meta[i].key, key) == 0) {
            memcpy(m->meta[i].value, value, vlen + 1);
            return 0;
        }
    }
    if (m->meta_count >= PULSE_MAX_META) return -2;
    memcpy(m->meta[m->meta_count].key, key, klen + 1);
    memcpy(m->meta[m->meta_count].value, value, vlen + 1);
    m->meta_count++;
    return 0;
}

int member_meta_format(const member_t *m, char *buf, size_t len) {
    size_t off = 0;
    for (int i = 0; i < m->meta_count; i++) {
        int w = snprintf(buf + off, len - off, "%s%s=%s",
                         i == 0 ? "" : ",", m->meta[i].key,
                         m->meta[i].value);
        if (w < 0 || (size_t)w >= len - off) return -1;
        off += (size_t)w;
    }
    if (len > 0) buf[off] = '\0';
    return (int)off;
}

int member_meta_parse_list(const char *s, meta_kv_t *out, int max,
                           int *out_count) {
    *out_count = 0;
    if (!s || s[0] == '\0') return 0;
    char tmp[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
    if (strlen(s) >= sizeof tmp) return -1;
    snprintf(tmp, sizeof tmp, "%s", s);
    char *save = NULL;
    char *tok = strtok_r(tmp, ",", &save);
    while (tok) {
        if (*out_count >= max) return -1;
        char key[PULSE_META_KEY_LEN + 1];
        char value[PULSE_META_VAL_LEN + 1];
        if (member_meta_parse(tok, key, sizeof key, value, sizeof value) != 0)
            return -1;
        /* A list with the same key twice is ambiguous: reject it. */
        for (int i = 0; i < *out_count; i++) {
            if (strcmp(out[i].key, key) == 0) return -1;
        }
        snprintf(out[*out_count].key, sizeof out[*out_count].key, "%s",
                 key);
        snprintf(out[*out_count].value, sizeof out[*out_count].value,
                 "%s", value);
        (*out_count)++;
        tok = strtok_r(NULL, ",", &save);
    }
    return 0;
}

int member_meta_equal(const member_t *a, const member_t *b) {
    if (a->meta_count != b->meta_count) return 0;
    for (int i = 0; i < a->meta_count; i++) {
        int found = 0;
        for (int j = 0; j < b->meta_count; j++) {
            if (strcmp(a->meta[i].key, b->meta[j].key) == 0 &&
                strcmp(a->meta[i].value, b->meta[j].value) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) return 0;
    }
    return 1;
}

void member_meta_json(const member_t *m, char *buf, size_t len) {
    size_t off = 0;
    int w = snprintf(buf + off, len - off, "{");
    if (w < 0 || (size_t)w >= len - off) goto done;
    off += (size_t)w;
    for (int i = 0; i < m->meta_count; i++) {
        w = snprintf(buf + off, len - off, "%s\"%s\":\"%s\"",
                     i == 0 ? "" : ",", m->meta[i].key, m->meta[i].value);
        if (w < 0 || (size_t)w >= len - off) goto done;
        off += (size_t)w;
    }
    w = snprintf(buf + off, len - off, "}");
    if (w < 0 || (size_t)w >= len - off) goto done;
    off += (size_t)w;
done:
    if (len > 0) buf[off < len ? off : len - 1] = '\0';
}

int member_merge_meta(member_table_t *t, const char *self_id, const char *id,
                      const meta_kv_t *meta, int meta_count) {
    if (meta_count <= 0) return 0; /* nothing carried: keep what we have */
    if (strcmp(id, self_id) == 0) return 0; /* our own meta is ours */
    member_t *m = member_find(t, id);
    if (!m) return 0;
    if (meta_count > PULSE_MAX_META) return 0;
    member_t tmp;
    memset(&tmp, 0, sizeof tmp);
    tmp.meta_count = meta_count;
    for (int i = 0; i < meta_count; i++) tmp.meta[i] = meta[i];
    if (member_meta_equal(m, &tmp)) return 0;
    m->meta_count = meta_count;
    for (int i = 0; i < meta_count; i++) m->meta[i] = meta[i];
    /* Annotations are gossip-visible: stamp the change so the next
     * digest carries the entry as a delta. */
    note_change(t, m);
    return 1;
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
    note_tombstone_change(t, ts);
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
    m->rtt_ms = -1;
    m->rtt_sum_ms = 0;
    m->rtt_samples = 0;
    m->rtt_max_ms = -1;
    m->probes_sent = 0;
    m->probes_answered = 0;
    phi_init(&m->phi);
    note_change(t, m);
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
        note_change(t, m);
        return MERGE_CHANGED;
    }
    if (incarnation == m->incarnation && state_rank(state) > state_rank(m->state)) {
        m->state = state;
        m->state_since_ms = now;
        note_change(t, m);
        return MERGE_CHANGED;
    }
    return MERGE_NONE;
}

void member_note_probe_sent(member_table_t *t, const char *id,
                            uint32_t seq) {
    member_t *m = member_find(t, id);
    if (!m) return;
    m->probes_sent++;
    /* Recent-probe window (0.68.0): the probe goes in as an
     * unanswered slot at the ring head, evicting the oldest
     * outcome when the window is full — the window always holds
     * the last PULSE_PROBE_WINDOW direct probes, no older. */
    int slot = m->probe_window_head;
    m->probe_window_seq[slot] = seq;
    m->probe_window_answered[slot] = 0;
    m->probe_window_head = (slot + 1) % PULSE_PROBE_WINDOW;
    if (m->probe_window_count < PULSE_PROBE_WINDOW)
        m->probe_window_count++;
}

void member_note_probe_answered(member_table_t *t, const char *id,
                                uint32_t seq) {
    member_t *m = member_find(t, id);
    if (!m) return;
    for (int i = 0; i < m->probe_window_count; i++) {
        if (m->probe_window_seq[i] == seq &&
            !m->probe_window_answered[i]) {
            m->probe_window_answered[i] = 1;
            return;
        }
    }
    /* No slot for this sequence: the probe never went out
     * through this node, or its slot already slid out of the
     * window — either way there is nothing to flip. */
}

double member_probe_rate(const member_t *m) {
    if (!m || m->probe_window_count == 0) return -1;
    int answered = 0;
    for (int i = 0; i < m->probe_window_count; i++)
        if (m->probe_window_answered[i]) answered++;
    return (double)answered / (double)m->probe_window_count;
}

void member_sample_probe_rate(member_t *m, int64_t now) {
    if (!m) return;
    double rate = member_probe_rate(m);
    if (rate < 0) return; /* no probe in the window yet: no rate */
    /* Same-millisecond repeat: the probe cycle is at least 25 ms,
     * so this only fires when two callers sample in one instant —
     * the ring must hold distinct times, not duplicates. */
    if (m->probe_rate_hist_count > 0) {
        int last = (m->probe_rate_hist_head - 1 +
                    PULSE_PROBE_RATE_HISTORY) %
                   PULSE_PROBE_RATE_HISTORY;
        if (m->probe_rate_hist_ms[last] == now) return;
    }
    m->probe_rate_hist[m->probe_rate_hist_head] = rate;
    m->probe_rate_hist_ms[m->probe_rate_hist_head] = now;
    m->probe_rate_hist_head =
        (m->probe_rate_hist_head + 1) % PULSE_PROBE_RATE_HISTORY;
    if (m->probe_rate_hist_count < PULSE_PROBE_RATE_HISTORY)
        m->probe_rate_hist_count++;
}

int member_probe_rate_history_json(const member_t *m, int64_t start_ms,
                                   char *buf, size_t len) {
    if (!m) return -1;
    size_t used = 0;
    int w = snprintf(buf + used, len - used, "[");
    if (w < 0 || (size_t)w >= len - used) return -1;
    used += (size_t)w;
    /* Oldest first: the ring's oldest slot is hist_head -
     * hist_count (wrapped), then forward — the same walk
     * phi_history_json uses. */
    int start = m->probe_rate_hist_head - m->probe_rate_hist_count;
    if (start < 0) start += PULSE_PROBE_RATE_HISTORY;
    for (int k = 0; k < m->probe_rate_hist_count; k++) {
        int idx = (start + k) % PULSE_PROBE_RATE_HISTORY;
        w = snprintf(buf + used, len - used, "%s[%lld,%.2f]",
                     k == 0 ? "" : ",",
                     (long long)(m->probe_rate_hist_ms[idx] - start_ms),
                     m->probe_rate_hist[idx]);
        if (w < 0 || (size_t)w >= len - used) return -1;
        used += (size_t)w;
    }
    w = snprintf(buf + used, len - used, "]");
    if (w < 0 || (size_t)w >= len - used) return -1;
    used += (size_t)w;
    return (int)used;
}

int member_is_low_probe_rate(const member_t *m, double threshold_frac) {
    if (!m || threshold_frac <= 0) return 0;
    if (m->state != MEMBER_ALIVE) return 0;
    if (m->probe_window_count < PULSE_PROBE_WINDOW_MIN) return 0;
    double rate = member_probe_rate(m);
    return rate >= 0 && rate < threshold_frac;
}

int member_count_low_probe_rate(const member_table_t *t,
                                const char *self_id,
                                double threshold_frac) {
    int n = 0;
    for (int i = 0; i < t->count; i++) {
        /* Self never probes itself (see member_is_low_probe_rate),
         * so it is excluded here, not in the predicate: the
         * predicate cannot tell self from a peer. */
        if (self_id && strcmp(t->members[i].id, self_id) == 0) continue;
        if (member_is_low_probe_rate(&t->members[i], threshold_frac))
            n++;
    }
    return n;
}

int member_note_rtt(member_table_t *t, const char *id, int64_t rtt_ms,
                    int64_t slow_threshold_ms) {
    if (rtt_ms < 0) return 0;
    member_t *m = member_find(t, id);
    if (!m) return 0;
    m->rtt_ms = rtt_ms;
    m->rtt_sum_ms += rtt_ms;
    m->rtt_samples++;
    /* Per-member probe counts (0.66.0): an answered probe is one
     * that produced an RTT sample, so the answered count rides
     * with the sample — it always equals rtt_samples, and a
     * probe never answered adds to sent only. */
    m->probes_answered++;
    if (m->rtt_max_ms < 0 || rtt_ms > m->rtt_max_ms)
        m->rtt_max_ms = rtt_ms;
    /* Slow-peer warning (0.22.0): a first crossing above the threshold
     * returns 2 so the caller warns once for the episode; the flag
     * clears when the mean recovers, so a later crossing warns again.
     * A threshold of 0 disables the warning entirely. */
    if (!member_is_slow(m, slow_threshold_ms)) {
        m->rtt_slow_warned = 0;
        return 1;
    }
    if (m->rtt_slow_warned) return 1;
    m->rtt_slow_warned = 1;
    return 2;
}

int64_t member_rtt_mean_ms(const member_t *m) {
    if (!m || m->rtt_samples == 0) return -1;
    return m->rtt_sum_ms / m->rtt_samples;
}

int64_t member_rtt_max_ms(const member_t *m) {
    if (!m || m->rtt_samples == 0) return -1;
    return m->rtt_max_ms;
}

int member_is_slow(const member_t *m, int64_t slow_threshold_ms) {
    if (!m || slow_threshold_ms <= 0) return 0;
    int64_t mean = member_rtt_mean_ms(m);
    return mean >= 0 && mean > slow_threshold_ms;
}

int member_count_slow(const member_table_t *t, int64_t slow_threshold_ms) {
    int n = 0;
    for (int i = 0; i < t->count; i++) {
        if (member_is_slow(&t->members[i], slow_threshold_ms)) n++;
    }
    return n;
}

int member_is_quiet(const member_t *m, int64_t quiet_threshold_ms,
                    int64_t now) {
    if (!m || quiet_threshold_ms <= 0) return 0;
    if (m->state != MEMBER_ALIVE) return 0;
    int64_t age = member_last_seen_age_ms(m, now);
    return age >= 0 && age > quiet_threshold_ms;
}

int member_count_quiet(const member_table_t *t, const char *self_id,
                       int64_t quiet_threshold_ms, int64_t now) {
    int n = 0;
    for (int i = 0; i < t->count; i++) {
        /* Self's last-contact age is the node's uptime (see
         * member_is_quiet), so it is excluded here, not in the
         * predicate: the predicate cannot tell self from a peer. */
        if (self_id && strcmp(t->members[i].id, self_id) == 0) continue;
        if (member_is_quiet(&t->members[i], quiet_threshold_ms, now))
            n++;
    }
    return n;
}

int64_t member_suspect_age_ms(const member_t *m, int64_t now) {
    if (!m || m->state != MEMBER_SUSPECT) return -1;
    return now - m->state_since_ms;
}

int64_t member_last_seen_age_ms(const member_t *m, int64_t now) {
    if (!m || m->last_seen_ms <= 0) return -1;
    return now - m->last_seen_ms;
}

int member_mark_suspect(member_table_t *t, const char *id, int64_t now) {
    member_t *m = member_find(t, id);
    if (!m || m->state != MEMBER_ALIVE) return 0;
    m->state = MEMBER_SUSPECT;
    m->state_since_ms = now;
    note_change(t, m);
    return 1;
}

int member_mark_dead(member_table_t *t, const char *id, int64_t now) {
    member_t *m = member_find(t, id);
    if (!m || m->state == MEMBER_DEAD) return 0;
    m->state = MEMBER_DEAD;
    m->state_since_ms = now;
    note_change(t, m);
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
    int inc_raised = (incarnation > m->incarnation);
    m->state = MEMBER_ALIVE;
    if (inc_raised) m->incarnation = incarnation;
    if (addr) m->addr = *addr;
    m->last_seen_ms = now;
    /* Direct contact re-arms the quiet-peer warning (0.62.0): the
     * peer answered again, so a later silence is a fresh episode
     * and warns again instead of staying silent because the first
     * crossing already fired. */
    m->quiet_warned = 0;
    if (changed) m->state_since_ms = now;
    /* A state change or a higher incarnation is gossip-visible, so it
     * bumps the table version; a routine heartbeat that only moves
     * last_seen_ms is not, and leaves the version alone. */
    if (changed || inc_raised) note_change(t, m);
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

/* Gossip fanout: how many distinct peers receive a digest each gossip
 * round. One peer is enough for a tiny cluster, but gossip only
 * spreads in O(log n) rounds when each round reaches O(log n) peers,
 * so the fanout grows as 1 + floor(log2(peer_count)) and caps at 4 to
 * bound traffic on large clusters: 0-1 peers -> 1, 2-3 -> 2, 4-7 -> 3,
 * 8+ -> 4. */
int member_gossip_fanout(int peer_count) {
    if (peer_count <= 1) return 1;
    int f = 1;
    int n = peer_count;
    while (n > 1) {
        f++;
        n >>= 1;
    }
    if (f > 4) f = 4;
    return f;
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
