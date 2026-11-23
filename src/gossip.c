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

/* Append one " id:inc:state" entry to the digest buffer, with the
 * member's annotations when it has any: ";k=v,k=v" after the state
 * code. Returns 0 on success, -1 when the buffer is full. */
static int append_entry(char *entries, size_t size, int *off,
                        const char *id, uint32_t inc, member_state_t state,
                        const char *meta) {
    int w = snprintf(entries + *off, size - (size_t)*off, " %s:%u:%c", id,
                     inc, member_state_code(state));
    if (w < 0 || (size_t)(*off + w) >= size) return -1;
    *off += w;
    if (meta && meta[0] != '\0') {
        w = snprintf(entries + *off, size - (size_t)*off, ";%s", meta);
        if (w < 0 || (size_t)(*off + w) >= size) return -1;
        *off += w;
    }
    return 0;
}

/* 1 when a should be gossiped before b: the more recently changed entry
 * first, DEAD before SUSPECT when both changed at the same instant. */
static int urgent_before(const member_t *a, const member_t *b) {
    if (a->state_since_ms != b->state_since_ms)
        return a->state_since_ms > b->state_since_ms;
    return a->state > b->state;
}

int gossip_send_digest(int sock, const struct sockaddr_storage *to,
                       const char *from_id, uint32_t inc,
                       const member_table_t *t,
                       uint64_t since_version,
                       uint64_t *sent_through_version) {
    /* Entry text is built first so the header count can cover both the
     * member window and the tombstones appended after it. */
    char entries[1280];
    int off = 0;
    int n = t->count;
    if (n == 0) {
        if (sent_through_version) *sent_through_version = t->version;
        return 0;
    }
    int total = 0;
    /* The highest changed_version actually sent: the peer can be
     * recorded as knowing the table up to here. It only advances as
     * far as the entries that fit in the digest. */
    uint64_t through = since_version;

    if (since_version > 0) {
        /* Delta: only entries that changed after the peer's version,
         * oldest change first, so the reported through-version is an
         * exact prefix bound: the digest sends a prefix of version
         * order, so every entry at or below the reported version went
         * out and the newest changes simply wait for the next digest.
         * (Sending newest-first instead would over-claim: a truncated
         * digest would record the peer as knowing the newest change
         * while older unsent changes below that version never went out
         * again.) Members and tombstones share one version-ordered
         * list: a tombstone appended after the members could otherwise
         * be squeezed out by a full digest while its version sat below
         * the reported bound, and the peer would never be sent it
         * again. Nothing changed means nothing to say: send no
         * datagram at all, and report the peer as current — a
         * converged cluster stays quiet until something actually
         * happens. */
        typedef struct {
            int is_tombstone;
            int idx;
            uint64_t ver;
        } delta_item_t;
        delta_item_t items[PULSE_MAX_MEMBERS + PULSE_MAX_TOMBSTONES];
        int n_items = 0;
        for (int i = 0; i < n; i++) {
            if (t->members[i].changed_version > since_version) {
                items[n_items].is_tombstone = 0;
                items[n_items].idx = i;
                items[n_items].ver = t->members[i].changed_version;
                n_items++;
            }
        }
        for (int i = 0; i < t->tombstone_count; i++) {
            if (t->tombstones[i].changed_version > since_version) {
                items[n_items].is_tombstone = 1;
                items[n_items].idx = i;
                items[n_items].ver = t->tombstones[i].changed_version;
                n_items++;
            }
        }
        /* Insertion sort by version ascending — the delta is at most
         * 16 + 16 items and usually tiny. */
        for (int i = 1; i < n_items; i++) {
            delta_item_t key = items[i];
            int j = i - 1;
            while (j >= 0 && items[j].ver > key.ver) {
                items[j + 1] = items[j];
                j--;
            }
            items[j + 1] = key;
        }
        for (int k = 0; k < n_items && total < PULSE_GOSSIP_ENTRIES; k++) {
            if (items[k].is_tombstone) {
                const tombstone_t *ts = &t->tombstones[items[k].idx];
                if (append_entry(entries, sizeof entries, &off, ts->id,
                                 ts->incarnation, MEMBER_DEAD, "") != 0)
                    break;
            } else {
                const member_t *m = &t->members[items[k].idx];
                char meta[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
                if (member_meta_format(m, meta, sizeof meta) < 0)
                    meta[0] = '\0';
                if (append_entry(entries, sizeof entries, &off, m->id,
                                 m->incarnation, m->state, meta) != 0)
                    break;
            }
            total++;
            if (items[k].ver > through)
                through = items[k].ver;
        }
        if (total == 0) {
            if (sent_through_version) *sent_through_version = t->version;
            return 0;
        }
        char buf[PULSE_MAX_MSG];
        int hoff = snprintf(buf, sizeof buf, "GOSSIP %s %u %d%s", from_id,
                            inc, total, entries);
        if (hoff < 0 || (size_t)hoff >= sizeof buf) return -1;
        if (sent_through_version) *sent_through_version = through;
        return send_str(sock, to, buf);
    }

    /* since_version 0: the peer's version is unknown, so the old
     * digest goes out — urgent entries first, then the random window.
     * A peer we know nothing about gets the full picture, not a delta
     * against a version it may never have had. */
    /* Urgent entries first: SUSPECT/DEAD members, most recently changed
     * first, capped at PULSE_GOSSIP_URGENT. Insertion sort — the table
     * holds at most 64 members and the urgent set is usually tiny. */
    int urgent[PULSE_MAX_MEMBERS];
    int n_urgent = 0;
    for (int i = 0; i < n; i++) {
        if (t->members[i].state != MEMBER_ALIVE)
            urgent[n_urgent++] = i;
    }
    for (int i = 1; i < n_urgent; i++) {
        int key = urgent[i];
        int j = i - 1;
        while (j >= 0 &&
               urgent_before(&t->members[key], &t->members[urgent[j]])) {
            urgent[j + 1] = urgent[j];
            j--;
        }
        urgent[j + 1] = key;
    }
    int n_first = n_urgent < PULSE_GOSSIP_URGENT ? n_urgent
                                                 : PULSE_GOSSIP_URGENT;
    if (n_first > PULSE_GOSSIP_ENTRIES) n_first = PULSE_GOSSIP_ENTRIES;
    for (int k = 0; k < n_first; k++) {
        const member_t *m = &t->members[urgent[k]];
        char meta[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
        if (member_meta_format(m, meta, sizeof meta) < 0)
            meta[0] = '\0';
        if (append_entry(entries, sizeof entries, &off, m->id,
                         m->incarnation, m->state, meta) != 0)
            break;
        total++;
        if (m->changed_version > through)
            through = m->changed_version;
    }

    /* The rest of the digest is the usual random contiguous window,
     * skipping entries already sent as urgent, so the remainder of the
     * table still rotates through instead of starving. */
    int start = rand() % n;
    for (int k = 0; k < n && total < PULSE_GOSSIP_ENTRIES; k++) {
        int idx = (start + k) % n;
        int already = 0;
        for (int j = 0; j < n_first; j++) {
            if (urgent[j] == idx) {
                already = 1;
                break;
            }
        }
        if (already) continue;
        const member_t *m = &t->members[idx];
        char meta[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
        if (member_meta_format(m, meta, sizeof meta) < 0)
            meta[0] = '\0';
        if (append_entry(entries, sizeof entries, &off, m->id,
                         m->incarnation, m->state, meta) != 0)
            break;
        total++;
        if (m->changed_version > through)
            through = m->changed_version;
    }
    /* Tombstones ride along as DEAD entries when there is room: a
     * force-leave (or a GC removal) then spreads to peers, which mark
     * the member DEAD and GC it on their own retention timer. A live
     * member that hears itself DEAD refutes by bumping its incarnation,
     * exactly as for a DEAD entry already in the table. */
    for (int k = 0; k < t->tombstone_count && total < PULSE_GOSSIP_ENTRIES; k++) {
        const tombstone_t *ts = &t->tombstones[k];
        if (append_entry(entries, sizeof entries, &off, ts->id,
                         ts->incarnation, MEMBER_DEAD, "") != 0)
            break;
        total++;
        if (ts->changed_version > through)
            through = ts->changed_version;
    }
    char buf[PULSE_MAX_MSG];
    int hoff = snprintf(buf, sizeof buf, "GOSSIP %s %u %d%s", from_id, inc,
                        total, entries);
    if (hoff < 0 || (size_t)hoff >= sizeof buf) return -1;
    if (sent_through_version) *sent_through_version = through;
    return send_str(sock, to, buf);
}

/* Anti-entropy offset: where the next SYNC starts in the table. A
 * full IPv6 table does not fit in one datagram, so successive SYNCs
 * walk the table from where the last one stopped; a stale peer then
 * converges within a couple of exchanges instead of re-receiving the
 * same head of the table forever. */
static int g_sync_offset = 0;

int gossip_send_full_table(int sock, const struct sockaddr_storage *to,
                           const char *from_id, uint32_t inc,
                           const member_table_t *t) {
    /* Entries must leave room for the header and a possible auth tag
     * inside PULSE_MAX_MSG; the parse side truncates at the same size. */
    char entries[PULSE_MAX_MSG - 64];
    int off = 0;
    int n = t->count;
    if (n == 0) return 0;
    int total = 0;

    /* Every table entry in table order, starting at the rotating
     * offset and wrapping, until the datagram is full. */
    int start = g_sync_offset % n;
    for (int k = 0; k < n; k++) {
        const member_t *m = &t->members[(start + k) % n];
        char meta[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
        if (member_meta_format(m, meta, sizeof meta) < 0)
            meta[0] = '\0';
        if (append_entry(entries, sizeof entries, &off, m->id,
                         m->incarnation, m->state, meta) != 0)
            break;
        total++;
    }
    g_sync_offset = (start + total) % n;
    /* Tombstones ride along as DEAD entries when there is room, exactly
     * as in a digest. */
    for (int k = 0; k < t->tombstone_count; k++) {
        const tombstone_t *ts = &t->tombstones[k];
        if (append_entry(entries, sizeof entries, &off, ts->id,
                         ts->incarnation, MEMBER_DEAD, "") != 0)
            break;
        total++;
    }
    char buf[PULSE_MAX_MSG];
    int hoff = snprintf(buf, sizeof buf, "SYNC %s %u %d%s", from_id, inc,
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
    } else if (strcmp(tok, "SYNC") == 0) {
        out->type = GMSG_SYNC;
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
    } else if (out->type == GMSG_GOSSIP || out->type == GMSG_SYNC) {
        /* SYNC carries the same "id:inc:state" entries as GOSSIP, so
         * the two verbs share one parser branch. */
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
            /* An entry is "host:port:inc:state", optionally followed
             * by ";k=v,k=v" annotations (0.20.0). The meta suffix is
             * split off first: it contains no ':' — keys and values
             * are [A-Za-z0-9._-] — so the remaining token parses
             * exactly as before. A malformed meta list is dropped,
             * not fatal: the entry still merges, without annotations.
             * Entries from older nodes carry no suffix at all. */
            char *semi = strchr(s, ';');
            char *meta_str = NULL;
            if (semi) {
                *semi = '\0';
                meta_str = semi + 1;
            }
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
            e->meta_count = 0;
            if (meta_str) {
                int nmeta = 0;
                if (member_meta_parse_list(meta_str, e->meta,
                                           PULSE_MAX_META, &nmeta) == 0)
                    e->meta_count = nmeta;
            }
            out->entry_count++;
        }
    }
    return 0;
}
