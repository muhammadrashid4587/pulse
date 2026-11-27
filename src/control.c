#define _POSIX_C_SOURCE 200809L
#include "control.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "phi.h"
#include "gossip.h"

#define CONTROL_MAX_LINE 1024

/* WATCH heartbeat (0.32.0): ms a stream may go without sending a
 * line before it sends a HEARTBEAT line of its own, so a client on
 * a quiet cluster can tell "nothing happened" from "the connection
 * died silently". Five seconds: long enough that a busy cluster
 * never sees one (events reset the clock), short enough that a
 * half-open connection is noticed quickly. */
#define WATCH_HEARTBEAT_IDLE_MS 5000

/* WATCH subscribers (0.33.0): how many WATCH streams are currently
 * connected, and how many WATCH sessions the node has served since
 * startup — so an operator polling METRICS or JSON can see how many
 * consumers are attached to the event stream. Counted here, in the
 * control plane, because the watch threads live here: the current
 * count is live state, like slow_peers, not a failure-detector
 * metric, so it needs no hook. A session is counted when its thread
 * starts (a WATCH refused up front — bad filter, no watch hooks —
 * never becomes a session); the count drops when the thread ends,
 * so a client that went away is gone from the count within about a
 * second of the disconnect being noticed.
 *
 * WATCH delivery / drop counts (0.34.0): per stream, how many
 * EVENT lines were actually sent (delivered), how many events the
 * stream consumed but skipped because its type/id filter excluded
 * them (filtered), and how many events it lost because they had
 * already scrolled out of the bounded event ring before the stream
 * could consume them (dropped — a replay GAP, or a mid-stream gap
 * that ends the stream). The per-stream counts live in a small
 * fixed registry (one slot per stream, at most WATCH_MAX_STREAMS
 * concurrent streams — a 65th stream still works and still feeds
 * the totals, it just has no slot to list), and the lifetime
 * totals accumulate every counted event, so METRICS and JSON
 * report the totals including what live streams have done so far,
 * not only what finished streams did. All of it is guarded by the
 * one watch mutex: the counts are updated once per event, which
 * is cheap next to the send itself. */
#define WATCH_MAX_STREAMS 64

typedef struct {
    uint64_t session; /* session number assigned at stream start */
    uint64_t delivered; /* EVENT lines sent on this stream */
    uint64_t filtered; /* events consumed but skipped by the filter */
    uint64_t dropped; /* events lost to a gap, never deliverable */
    int paused; /* 1 when this stream is paused (0.37.0) */
    int active;
} watch_stream_stat_t;

/* WATCH pause (0.37.0): one entry per live WATCH stream, keyed by
 * its session number, holding the stream's paused flag. A paused
 * stream stops consuming events — its thread waits instead of
 * calling watch_next — so a slow consumer can stop the flow
 * without closing the connection, and resume later with the ring
 * replaying what it missed (or the gap signal when the ring has
 * scrolled past it). The entries live in a list of their own,
 * separate from the 64-slot counts registry above: any live
 * session can be paused, listed or not. The list is guarded by
 * g_watch_mu; an entry is freed only by its own stream's thread
 * when the stream ends, so a PAUSE / RESUME command can only ever
 * find entries for streams that are still alive. */
typedef struct watch_pause_entry {
    uint64_t session;
    int paused; /* 1 when the stream is paused */
    struct watch_pause_entry *next;
} watch_pause_entry_t;

static pthread_mutex_t g_watch_mu = PTHREAD_MUTEX_INITIALIZER;
static watch_pause_entry_t *g_watch_pause_list; /* under g_watch_mu */

/* Find a pause entry by session. The caller must hold g_watch_mu. */
static watch_pause_entry_t *watch_pause_find_locked(uint64_t session) {
    for (watch_pause_entry_t *e = g_watch_pause_list; e; e = e->next)
        if (e->session == session) return e;
    return NULL;
}

/* Register a pause entry for a new stream. Returns the entry, or
 * NULL on allocation failure: the stream still runs, it just
 * cannot be paused (a PAUSE for its session then answers
 * "unknown watch session"). */
static watch_pause_entry_t *watch_pause_register(uint64_t session) {
    watch_pause_entry_t *e = malloc(sizeof *e);
    if (!e) return NULL;
    e->session = session;
    e->paused = 0;
    pthread_mutex_lock(&g_watch_mu);
    e->next = g_watch_pause_list;
    g_watch_pause_list = e;
    pthread_mutex_unlock(&g_watch_mu);
    return e;
}

static void watch_pause_unregister(watch_pause_entry_t *e) {
    if (!e) return;
    pthread_mutex_lock(&g_watch_mu);
    watch_pause_entry_t **p = &g_watch_pause_list;
    while (*p && *p != e) p = &(*p)->next;
    if (*p) *p = e->next;
    pthread_mutex_unlock(&g_watch_mu);
    free(e);
}

/* Set the paused flag for the stream with this session number.
 * Returns 0 on success, -1 when no live stream has that session. */
static int watch_pause_set(uint64_t session, int paused) {
    pthread_mutex_lock(&g_watch_mu);
    watch_pause_entry_t *e = watch_pause_find_locked(session);
    if (e) e->paused = paused;
    pthread_mutex_unlock(&g_watch_mu);
    return e ? 0 : -1;
}

/* Is this stream paused? A watch thread reads its own entry's
 * flag; a NULL entry (allocation failed at stream start) is never
 * paused. */
static int watch_pause_is_paused(watch_pause_entry_t *e) {
    if (!e) return 0;
    pthread_mutex_lock(&g_watch_mu);
    int p = e->paused;
    pthread_mutex_unlock(&g_watch_mu);
    return p;
}

static int g_watch_current = 0;
static uint64_t g_watch_total = 0;
static uint64_t g_watch_delivered_total = 0;
static uint64_t g_watch_filtered_total = 0;
static uint64_t g_watch_dropped_total = 0;
static watch_stream_stat_t g_watch_streams[WATCH_MAX_STREAMS];

typedef struct {
    int current;
    uint64_t sessions;
    uint64_t delivered;
    uint64_t filtered;
    uint64_t dropped;
    int paused; /* WATCH streams paused right now (0.37.0) */
    watch_stream_stat_t streams[WATCH_MAX_STREAMS];
    int n_streams;
} watch_stats_t;

static void watch_stats_snapshot(watch_stats_t *out) {
    pthread_mutex_lock(&g_watch_mu);
    out->current = g_watch_current;
    out->sessions = g_watch_total;
    out->delivered = g_watch_delivered_total;
    out->filtered = g_watch_filtered_total;
    out->dropped = g_watch_dropped_total;
    out->paused = 0;
    for (watch_pause_entry_t *e = g_watch_pause_list; e; e = e->next)
        if (e->paused) out->paused++;
    out->n_streams = 0;
    for (int i = 0; i < WATCH_MAX_STREAMS; i++) {
        if (g_watch_streams[i].active) {
            watch_stream_stat_t s = g_watch_streams[i];
            /* The paused flag lives in the pause list, keyed by
             * session: a stream with no entry (allocation failed
             * at stream start) is never paused. */
            watch_pause_entry_t *e =
                watch_pause_find_locked(s.session);
            s.paused = e ? e->paused : 0;
            out->streams[out->n_streams++] = s;
        }
    }
    pthread_mutex_unlock(&g_watch_mu);
}

/* Claim a registry slot for a new stream (its session number).
 * Returns the slot, or -1 when every slot is taken: the stream
 * still runs and still feeds the totals, it just is not listed
 * per-stream in JSON until a slot frees up. */
static int watch_stream_register(uint64_t session) {
    pthread_mutex_lock(&g_watch_mu);
    for (int i = 0; i < WATCH_MAX_STREAMS; i++) {
        if (!g_watch_streams[i].active) {
            memset(&g_watch_streams[i], 0, sizeof g_watch_streams[i]);
            g_watch_streams[i].session = session;
            g_watch_streams[i].active = 1;
            pthread_mutex_unlock(&g_watch_mu);
            return i;
        }
    }
    pthread_mutex_unlock(&g_watch_mu);
    return -1;
}

/* Add to one stream's counts and to the lifetime totals. A slot
 * of -1 (registry full) still feeds the totals: the events
 * happened, only the per-stream listing is missing. */
static void watch_stream_add(int slot, uint64_t delivered,
                             uint64_t filtered, uint64_t dropped) {
    pthread_mutex_lock(&g_watch_mu);
    if (slot >= 0 && g_watch_streams[slot].active) {
        g_watch_streams[slot].delivered += delivered;
        g_watch_streams[slot].filtered += filtered;
        g_watch_streams[slot].dropped += dropped;
    }
    g_watch_delivered_total += delivered;
    g_watch_filtered_total += filtered;
    g_watch_dropped_total += dropped;
    pthread_mutex_unlock(&g_watch_mu);
}

static void watch_stream_unregister(int slot) {
    if (slot < 0) return;
    pthread_mutex_lock(&g_watch_mu);
    g_watch_streams[slot].active = 0;
    pthread_mutex_unlock(&g_watch_mu);
}

/* The WATCH event ring capacity (0.36.0): the --event-log-cap the
 * node started with. A NULL hook — an older harness, or a node
 * that does not support watching — reports the default, so
 * METRICS / JSON / PROMETHEUS never print a nonsense cap. */
static int event_log_cap_or_default(const control_hooks_t *hooks) {
    if (hooks->event_log_cap) return hooks->event_log_cap(hooks->ctx);
    return PULSE_EVENT_LOG_CAP;
}

/* The gossip digest size (0.41.0): how many members one gossip
 * round's digest covers — the --gossip-digest-size the node
 * started with. A NULL hook reports the default, so METRICS /
 * JSON / PROMETHEUS never print a nonsense size. */
static int gossip_digest_size_or_default(const control_hooks_t *hooks) {
    if (hooks->gossip_digest_size)
        return hooks->gossip_digest_size(hooks->ctx);
    return PULSE_GOSSIP_ENTRIES;
}

/* The phi suspicion threshold (0.43.0): the phi value at which a
 * peer is suspected — the --phi-threshold the node started with.
 * A NULL hook reports the default, so PROMETHEUS never prints a
 * nonsense threshold. */
static double phi_threshold_or_default(const control_hooks_t *hooks) {
    if (hooks->phi_threshold)
        return hooks->phi_threshold(hooks->ctx);
    return PULSE_PHI_THRESHOLD_DEFAULT;
}

int control_listen(int port, int family) {
    int fd = socket(family, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_storage addr;
    memset(&addr, 0, sizeof addr);
    if (family == AF_INET6) {
        struct sockaddr_in6 *a6 = (struct sockaddr_in6 *)&addr;
        a6->sin6_family = AF_INET6;
        a6->sin6_addr = in6addr_loopback;
        a6->sin6_port = htons((uint16_t)port);
    } else {
        struct sockaddr_in *a4 = (struct sockaddr_in *)&addr;
        a4->sin_family = AF_INET;
        a4->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a4->sin_port = htons((uint16_t)port);
    }
    if (bind(fd, (struct sockaddr *)&addr, pulse_addr_len(&addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 8) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int send_all(int fd, const char *s) {
    size_t len = strlen(s);
    size_t sent = 0;
    while (sent < len) {
        /* MSG_NOSIGNAL: a client that went away mid-send must fail
         * this send (and close the stream), not kill the node with
         * SIGPIPE — a WATCH client disconnecting just as an EVENT
         * is being written would otherwise take the whole process
         * down with it. */
        ssize_t n = send(fd, s + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

/* Read one line (up to \n, or EOF). Returns the length, 0 on EOF with
 * nothing read, -1 on error. The line is NUL-terminated without the
 * trailing newline. */
static int read_line(int fd, char *buf, size_t cap) {
    size_t n = 0;
    while (n + 1 < cap) {
        char c;
        ssize_t r = recv(fd, &c, 1, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) break; /* EOF */
        if (c == '\n') break;
        if (c == '\r') continue;
        buf[n++] = c;
    }
    buf[n] = '\0';
    return (int)n;
}

/* Where a member came from, for PEERS and JSON (0.27.0 / 0.28.0):
 * the node's peer_source hook when it has one, else "self" for this
 * node and "discovered" for everyone else. Returns a string literal;
 * the caller must not free it. */
static const char *peer_source_str(const control_hooks_t *hooks,
                                   const char *id) {
    if (hooks->peer_source) {
        const char *s = hooks->peer_source(hooks->ctx, id);
        if (s && s[0] != '\0') return s;
    }
    return strcmp(id, hooks->self_id) == 0 ? "self" : "discovered";
}

/* Peer-file state (0.28.0), shared by METRICS and JSON: fill out
 * from the hook, or report "no peer file" when the node has no
 * hook for it. */
static void peer_file_state_or_none(const control_hooks_t *hooks,
                                    pulse_peer_file_state_t *out) {
    memset(out, 0, sizeof *out);
    if (hooks->peer_file_state)
        hooks->peer_file_state(hooks->ctx, out);
}

/* Copy s into dst, escaping backslash, quote and control characters
 * for JSON. Returns 0 on success, -1 when dst is too small (dst is
 * still NUL-terminated). Member ids need no escaping (they are
 * host:port), but a peer-file path is user-supplied and may contain
 * either. */
static int json_escape(const char *s, char *dst, size_t cap) {
    size_t n = 0;
    int truncated = 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        char esc[8];
        int m = 0;
        if (*p == '"' || *p == '\\') {
            esc[0] = '\\';
            esc[1] = (char)*p;
            m = 2;
        } else if (*p < 0x20) {
            m = snprintf(esc, sizeof esc, "\\u%04x", *p);
        } else {
            esc[0] = (char)*p;
            m = 1;
        }
        if (n + (size_t)m >= cap) {
            truncated = 1;
            break;
        }
        memcpy(dst + n, esc, (size_t)m);
        n += (size_t)m;
    }
    if (cap > 0) dst[n < cap ? n : cap - 1] = '\0';
    return truncated ? -1 : 0;
}

static void handle_members(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    int phi_on = hooks->phi_enabled(hooks->ctx);
    int64_t now = pulse_now_ms();
    /* A member line can carry up to 8 annotations at 64 chars each,
     * plus the id, incarnation, state and phi. */
    char line[768];
    for (int i = 0; i < t.count; i++) {
        const member_t *m = &t.members[i];
        /* Phi is meaningful only in phi mode, only for peers with
         * heartbeat history, and never for self. */
        char phibuf[32];
        if (!phi_on || strcmp(m->id, hooks->self_id) == 0 ||
            !m->phi.have_last || m->phi.count < PULSE_PHI_MIN_SAMPLES) {
            snprintf(phibuf, sizeof phibuf, "-");
        } else {
            snprintf(phibuf, sizeof phibuf, "%.2f",
                     phi_value(&m->phi, now));
        }
        char meta[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
        if (member_meta_format(m, meta, sizeof meta) < 0 || meta[0] == '\0')
            snprintf(meta, sizeof meta, "-");
        snprintf(line, sizeof line, "MEMBER %s %u %s %s %s\n", m->id,
                 m->incarnation, member_state_name(m->state), phibuf,
                 meta);
        if (send_all(fd, line) != 0) return;
    }
    send_all(fd, "END\n");
}

/* One member line in the PEERS shape — id, incarnation, state,
 * last-seen-ms (or '-') and source — with the caller's prefix:
 * "PEER" for PEERS, "SNAPSHOT" for the WATCH snapshot. Last-seen
 * is ms since direct contact. Self never probes itself (its
 * last_seen is just startup time), and a peer never heard from
 * has last_seen_ms 0 — both report '-'. */
static void format_peer_line(char *line, size_t cap, const char *prefix,
                             const member_t *m,
                             const control_hooks_t *hooks, int64_t now) {
    char seenbuf[32];
    if (strcmp(m->id, hooks->self_id) == 0 || m->last_seen_ms <= 0) {
        snprintf(seenbuf, sizeof seenbuf, "-");
    } else {
        snprintf(seenbuf, sizeof seenbuf, "%lld",
                 (long long)(now - m->last_seen_ms));
    }
    const char *source = peer_source_str(hooks, m->id);
    snprintf(line, cap, "%s %s %u %s %s %s\n", prefix, m->id,
             m->incarnation, member_state_name(m->state), seenbuf,
             source);
}

static void handle_peers(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    int64_t now = pulse_now_ms();
    /* A peer line is short: id, incarnation, state, last-seen, source. */
    char line[256];
    for (int i = 0; i < t.count; i++) {
        format_peer_line(line, sizeof line, "PEER", &t.members[i],
                         hooks, now);
        if (send_all(fd, line) != 0) return;
    }
    send_all(fd, "END\n");
}

static void handle_status(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    int64_t uptime = (pulse_now_ms() - hooks->start_ms) / 1000;
    char line[256];
    snprintf(line, sizeof line,
             "STATUS id=%s uptime=%lld alive=%d suspect=%d dead=%d total=%d\n"
             "END\n",
             hooks->self_id, (long long)uptime,
             member_count_state(&t, MEMBER_ALIVE),
             member_count_state(&t, MEMBER_SUSPECT),
             member_count_state(&t, MEMBER_DEAD), t.count);
    send_all(fd, line);
}

static void handle_metrics(int fd, const control_hooks_t *hooks) {
    pulse_metrics_t m;
    hooks->metrics(hooks->ctx, &m);
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    int64_t now = pulse_now_ms(); /* quiet-peer ages are measured now */
    /* The metrics line plus slow_peers plus the peer-file fields
     * (path up to 512 chars) plus the per-seed rejoin breakdown
     * (16 seeds at most, each "host:port=N") can run long; 4 KB
     * holds the whole line for any realistic path. snprintf
     * truncates safely. */
    char line[4096];
    metrics_format(&m, line, sizeof line);
    /* Slow-peer warning (0.22.0): how many members currently count
     * as slow — mean probe RTT above the configured threshold. The
     * count is derived from the table, not a metrics counter, since
     * slowness is current state, not a transition. */
    size_t used = strlen(line);
    snprintf(line + used, sizeof line - used, " slow_peers=%d",
             member_count_slow(&t, hooks->slow_rtt_threshold_ms));
    /* Quiet-peer warning (0.62.0): how many members currently
     * count as quiet — ALIVE with a last-contact age above the
     * configured threshold. Like slow_peers, the count is derived
     * from the table, not a metrics counter, since quietness is
     * current state, not a transition. */
    used = strlen(line);
    snprintf(line + used, sizeof line - used, " quiet_peers=%d",
             member_count_quiet(&t, hooks->self_id, hooks->quiet_threshold_ms, now));
    /* Peer-file state (0.28.0): the roster this node seeds from and
     * what the last load of it did. A path with a space breaks the
     * space-separated format — JSON is the machine-readable form —
     * but a plain path reads fine here. */
    pulse_peer_file_state_t pf;
    peer_file_state_or_none(hooks, &pf);
    used = strlen(line);
    if (pf.have_file) {
        snprintf(line + used, sizeof line - used,
                 " peer_file=%s last_reload=%lld last_mtime=%lld "
                 "pf_peers=%d pf_added=%d pf_dropped=%d pf_kept=%d "
                 "pf_bad=%d",
                 pf.path, (long long)pf.last_reload_unix,
                 (long long)pf.last_mtime_unix, pf.peers, pf.added,
                 pf.dropped, pf.kept, pf.bad);
    } else {
        snprintf(line + used, sizeof line - used,
                 " peer_file=- last_reload=- last_mtime=- pf_peers=- "
                 "pf_added=- pf_dropped=- pf_kept=- pf_bad=-");
    }
    /* WATCH subscribers (0.33.0): how many WATCH streams are
     * connected right now, and how many sessions the node has
     * served since startup. The current count is live state — a
     * client that went away drops out of it within about a second
     * — while the session count only ever grows. Delivery / drop
     * counts (0.34.0): how many EVENT lines WATCH streams have
     * sent in total (delivered), how many events streams consumed
     * but skipped because their filter excluded them (filtered),
     * and how many events streams lost to a gap in the bounded
     * event ring (dropped) — all lifetime totals, including what
     * live streams have done so far. */
    watch_stats_t ws;
    watch_stats_snapshot(&ws);
    used = strlen(line);
    snprintf(line + used, sizeof line - used,
             " watch_subscribers=%d watch_sessions=%llu "
             "watch_delivered=%llu watch_filtered=%llu "
             "watch_dropped=%llu watch_paused=%d "
             "watch_event_log_cap=%d gossip_digest_size=%d",
             ws.current, (unsigned long long)ws.sessions,
             (unsigned long long)ws.delivered,
             (unsigned long long)ws.filtered,
             (unsigned long long)ws.dropped,
             ws.paused,
             event_log_cap_or_default(hooks),
             gossip_digest_size_or_default(hooks));
    /* Phi suspicion threshold (0.45.0): the phi value at which a
     * peer is suspected — the --phi-threshold the node started
     * with, 8.0 by default, two decimals like PROMETHEUS's
     * pulse_phi_threshold gauge. Emitted always, like the other
     * config gauges (gossip_digest_size, watch_event_log_cap),
     * whether or not --phi-accrual is on, so a text-protocol
     * consumer can judge a rising PHI line against the threshold
     * that actually fires the suspicion instead of hard-coding
     * 8.0. */
    used = strlen(line);
    snprintf(line + used, sizeof line - used, " phi_threshold=%.2f",
             phi_threshold_or_default(hooks));
    if (send_all(fd, line) != 0) return;
    /* Per-member phi (0.44.0): the failure-detector value JSON and
     * PROMETHEUS already report per member, as one "PHI <id>
     * <value>" line per member that has a phi, under the same rule
     * as JSON: phi mode on, not self, heartbeat history with at
     * least PULSE_PHI_MIN_SAMPLES intervals. The value is the live
     * phi at reply time, two decimals like JSON and PROMETHEUS, so
     * it rises as a peer goes quiet. A node without --phi-accrual
     * emits no PHI line at all, and a peer never heard from has
     * none either; a dead member keeps its last-known phi until
     * GC, the same as JSON reports it. */
    {
        int phi_on = hooks->phi_enabled(hooks->ctx);
        int64_t now = pulse_now_ms();
        for (int i = 0; i < t.count; i++) {
            const member_t *mem = &t.members[i];
            if (!phi_on || strcmp(mem->id, hooks->self_id) == 0 ||
                !mem->phi.have_last ||
                mem->phi.count < PULSE_PHI_MIN_SAMPLES) continue;
            char philine[128];
            snprintf(philine, sizeof philine, "\nPHI %s %.2f",
                     mem->id, phi_value(&mem->phi, now));
            if (send_all(fd, philine) != 0) return;
        }
    }
    /* Suspicion age per member (0.58.0): how long each SUSPECT
     * member has been suspected, in ms, as one "SUSPECT <id> <ms>"
     * line per SUSPECT member, under the same rule as JSON's
     * "suspect_ms" — the age is now minus when the member's
     * current state started, so it grows while the member sits
     * SUSPECT and the member drops off the list the moment it
     * revives or goes DEAD. ALIVE and DEAD members have no line:
     * an absent line is how the text protocol says "not
     * suspected". A text-protocol consumer judges the age against
     * the suspect timeout the same way a JSON consumer judges
     * "suspect_ms". */
    {
        int64_t now = pulse_now_ms();
        for (int i = 0; i < t.count; i++) {
            const member_t *mem = &t.members[i];
            int64_t age = member_suspect_age_ms(mem, now);
            if (age < 0) continue;
            char susline[128];
            snprintf(susline, sizeof susline, "\nSUSPECT %s %lld",
                     mem->id, (long long)age);
            if (send_all(fd, susline) != 0) return;
        }
    }
    /* Time since last contact per member (0.59.0): how long since
     * this node last heard from each member, in ms, as one
     * "LASTSEEN <id> <ms>" line per member heard from, after the
     * SUSPECT lines, under the same rule as JSON's "last_seen_ms"
     * — the age is now minus the member's last_seen_ms. Self is
     * excluded (its last_seen is just startup time, so the age
     * would be uptime, not a contact age) and a peer never heard
     * from has no line: an absent line is how the text protocol
     * says "never heard from". A DEAD member keeps its line until
     * GC, the age growing — how long it has been silent. A
     * text-protocol consumer judges the age against the probe
     * interval and the suspect timeout the same way a JSON
     * consumer judges "last_seen_ms": a live peer's age stays
     * under a few probe intervals, a dying peer's climbs. */
    {
        int64_t now = pulse_now_ms();
        for (int i = 0; i < t.count; i++) {
            const member_t *mem = &t.members[i];
            if (strcmp(mem->id, hooks->self_id) == 0) continue;
            int64_t age = member_last_seen_age_ms(mem, now);
            if (age < 0) continue;
            char seenline[128];
            snprintf(seenline, sizeof seenline, "\nLASTSEEN %s %lld",
                     mem->id, (long long)age);
            if (send_all(fd, seenline) != 0) return;
        }
    }
    /* Per-member probe RTT (0.63.0): the last and mean direct-probe
     * round-trip time JSON and PROMETHEUS already report per member,
     * as one "RTT <id> <last_ms> <mean_ms>" line per member that has
     * answered a probe, after the LASTSEEN lines, under the same rule
     * as JSON's "rtt_ms" / "rtt_mean_ms" and PROMETHEUS's
     * pulse_member_rtt_ms / pulse_member_rtt_mean_ms gauges —
     * rtt_ms is -1 until a probe has been answered, so self (which
     * never probes itself) and a peer never heard from have no line:
     * an absent line is how the text protocol says "never probed".
     * A value of 0 is a real sub-millisecond probe, not a missing
     * one. A DEAD member keeps its last RTT until GC, the same as
     * JSON reports it, so a text-protocol consumer can see the last
     * measured RTT of a peer that just died instead of only the
     * global probe_rtt_ms histogram. */
    for (int i = 0; i < t.count; i++) {
        const member_t *mem = &t.members[i];
        if (mem->rtt_ms < 0 || mem->rtt_samples == 0) continue;
        char rttline[128];
        snprintf(rttline, sizeof rttline, "\nRTT %s %lld %lld",
                 mem->id, (long long)mem->rtt_ms,
                 (long long)member_rtt_mean_ms(mem));
        if (send_all(fd, rttline) != 0) return;
    }
    send_all(fd, "\nEND\n");
}

/* JSON: one machine-readable line combining STATUS, MEMBERS and
 * METRICS, then END. Member ids are host:port (IPv6 bracketed), so
 * they contain no quote or backslash and need no escaping. Phi is a
 * number when phi-accrual is on and the peer has heartbeat history,
 * null otherwise (including self). Latency means are null when there
 * are no samples. Built into a fixed buffer: 64 members at ~120
 * bytes each plus up to ~520 bytes of annotations each plus up to
 * ~360 bytes of phi history each is under 64 KB, so 128 KB has ample
 * headroom. */
static void handle_json(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    pulse_metrics_t m;
    hooks->metrics(hooks->ctx, &m);
    int phi_on = hooks->phi_enabled(hooks->ctx);
    int64_t now = pulse_now_ms();
    int64_t uptime = (now - hooks->start_ms) / 1000;
    static char buf[131072];
    size_t used = 0;
    int w = snprintf(buf + used, sizeof buf - used,
        "{\"id\":\"%s\",\"uptime\":%lld,\"alive\":%d,\"suspect\":%d,"
        "\"dead\":%d,\"total\":%d,\"members\":[",
        hooks->self_id, (long long)uptime,
        member_count_state(&t, MEMBER_ALIVE),
        member_count_state(&t, MEMBER_SUSPECT),
        member_count_state(&t, MEMBER_DEAD), t.count);
    if (w < 0) return;
    used += (size_t)w;
    for (int i = 0; i < t.count; i++) {
        const member_t *mem = &t.members[i];
        char phibuf[32];
        /* Phi history (0.47.0): the last 16 sampled phi values with
         * their age in ms since this node started, oldest first —
         * [[age_ms, phi], ...] — so a JSON consumer can see how a
         * peer's suspicion evolved instead of only its live phi.
         * Same rule as "phi" itself: null when phi-accrual is off,
         * for self, or with no heartbeat history; "[]" when the
         * peer has history but no probe cycle has sampled it yet.
         * The samples are taken by the prober thread, never by a
         * control-plane read: polling JSON must not move the
         * history it is reading. */
        char histbuf[512];
        if (!phi_on || strcmp(mem->id, hooks->self_id) == 0 ||
            !mem->phi.have_last || mem->phi.count < PULSE_PHI_MIN_SAMPLES) {
            snprintf(phibuf, sizeof phibuf, "null");
            snprintf(histbuf, sizeof histbuf, "null");
        } else {
            snprintf(phibuf, sizeof phibuf, "%.2f",
                     phi_value(&mem->phi, now));
            if (phi_history_json(&mem->phi, hooks->start_ms, histbuf,
                                 sizeof histbuf) < 0)
                snprintf(histbuf, sizeof histbuf, "[]");
        }
        /* Annotations are a JSON object: {"role":"worker"} — keys and
         * values are restricted to JSON-safe characters, so no
         * escaping is needed (see member_meta_json). */
        char metajson[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1) + 32];
        member_meta_json(mem, metajson, sizeof metajson);
        /* Probe RTT (0.21.0): the last measured PING->ACK round trip
         * in ms, and the mean over all answered probes. Both are
         * null until a probe has been answered; self never probes
         * itself, so self stays null. "slow" (0.22.0) is true when
         * the peer's mean RTT is above the configured slow-peer
         * threshold (false when the warning is disabled, or the peer
         * has answered no probe yet). The worst RTT (0.64.0) is the
         * largest single probe round trip over all answered probes —
         * null under the same rule as the last and mean — so one
         * 60 ms probe among sub-millisecond ones is visible instead
         * of averaged away. */
        char rttbuf[32], rttmeanbuf[32], rttmaxbuf[32];
        if (mem->rtt_ms < 0 || mem->rtt_samples == 0) {
            snprintf(rttbuf, sizeof rttbuf, "null");
            snprintf(rttmeanbuf, sizeof rttmeanbuf, "null");
            snprintf(rttmaxbuf, sizeof rttmaxbuf, "null");
        } else {
            snprintf(rttbuf, sizeof rttbuf, "%lld",
                     (long long)mem->rtt_ms);
            snprintf(rttmeanbuf, sizeof rttmeanbuf, "%lld",
                     (long long)(mem->rtt_sum_ms / mem->rtt_samples));
            snprintf(rttmaxbuf, sizeof rttmaxbuf, "%lld",
                     (long long)member_rtt_max_ms(mem));
        }
        /* Suspicion age (0.58.0): how long this member has been
         * SUSPECT, in ms — now minus when its current state started.
         * Null unless the member is SUSPECT: an ALIVE member has no
         * suspicion to age, and a DEAD member's suspicion is over.
         * A consumer judges it against the suspect timeout
         * (--suspect-timeout-ms, 300 by default) to see how close
         * the member is to being marked DEAD. */
        char suspectbuf[32];
        int64_t age = member_suspect_age_ms(mem, now);
        if (age < 0)
            snprintf(suspectbuf, sizeof suspectbuf, "null");
        else
            snprintf(suspectbuf, sizeof suspectbuf, "%lld",
                     (long long)age);
        /* Time since last contact (0.59.0): how long since this
         * node last heard from this member, in ms — now minus its
         * last_seen_ms. Null for self (its last_seen is just
         * startup time, so the age would be uptime, not a contact
         * age) and for a peer never heard from (last_seen_ms 0):
         * an absent value, not a zero — a zero would claim
         * contact just now. A live peer's age stays under a few
         * probe intervals; a dying peer's climbs toward the
         * suspect timeout, and a DEAD member's keeps growing
         * until GC — how long it has been silent. */
        char seenbuf[32];
        if (strcmp(mem->id, hooks->self_id) == 0)
            snprintf(seenbuf, sizeof seenbuf, "null");
        else {
            int64_t seen_age = member_last_seen_age_ms(mem, now);
            if (seen_age < 0)
                snprintf(seenbuf, sizeof seenbuf, "null");
            else
                snprintf(seenbuf, sizeof seenbuf, "%lld",
                         (long long)seen_age);
        }
        /* "source" (0.28.0) is where this node got the peer from —
         * "seed", "peer-file", "discovered" or "self" — the same
         * provenance PEERS reports, so a peer-file peer is
         * distinguishable from a discovered one in JSON too.
         * "quiet" (0.62.0) is true when the peer is ALIVE and its
         * last-contact age is above --quiet-peer-ms — the peer
         * has gone quiet but the suspicion has not fired yet —
         * false when the warning is disabled, the peer was never
         * heard from, or it is SUSPECT or DEAD. Self is always
         * false: self's last_seen is startup time, so its age
         * would be the node's uptime, not a contact age. */
        w = snprintf(buf + used, sizeof buf - used,
                     "%s{\"id\":\"%s\",\"incarnation\":%u,\"state\":\"%s\","
                     "\"phi\":%s,\"phi_history\":%s,\"meta\":%s,\"rtt_ms\":%s,"
                     "\"rtt_mean_ms\":%s,\"rtt_max_ms\":%s,\"slow\":%s,\"quiet\":%s,"
                     "\"suspect_ms\":%s,"
                     "\"last_seen_ms\":%s,"
                     "\"source\":\"%s\"}",
                     i == 0 ? "" : ",", mem->id, mem->incarnation,
                     member_state_name(mem->state), phibuf, histbuf, metajson,
                     rttbuf, rttmeanbuf, rttmaxbuf,
                     member_is_slow(mem, hooks->slow_rtt_threshold_ms)
                         ? "true"
                         : "false",
                     (strcmp(mem->id, hooks->self_id) != 0 &&
                      member_is_quiet(mem, hooks->quiet_threshold_ms,
                                      now))
                         ? "true"
                         : "false",
                     suspectbuf, seenbuf,
                     peer_source_str(hooks, mem->id));
        if (w < 0 || (size_t)w >= sizeof buf - used) return;
        used += (size_t)w;
    }
    /* Peer-file state (0.28.0): the same state METRICS reports, as
     * a JSON object, or null when the node runs without --peer-file.
     * The path is escaped: unlike member ids it is user-supplied
     * and may contain a quote or a backslash. */
    pulse_peer_file_state_t pf;
    peer_file_state_or_none(hooks, &pf);
    /* 1 KB holds the object for any realistic path: the fixed fields
     * are ~110 bytes, so the escaped path is capped at 700 (a longer
     * path is shortened, never cut mid-escape — json_escape only
     * copies whole escape sequences). */
    char pfjson[1024];
    if (!pf.have_file) {
        snprintf(pfjson, sizeof pfjson, "null");
    } else {
        char escpath[700];
        json_escape(pf.path, escpath, sizeof escpath);
        snprintf(pfjson, sizeof pfjson,
                 "{\"path\":\"%s\",\"last_reload\":%lld,"
                 "\"last_mtime\":%lld,\"peers\":%d,\"added\":%d,"
                 "\"dropped\":%d,\"kept\":%d,\"bad\":%d}",
                 escpath, (long long)pf.last_reload_unix,
                 (long long)pf.last_mtime_unix, pf.peers, pf.added,
                 pf.dropped, pf.kept, pf.bad);
    }
    char dmean[16], smean[16], rmean[16], pmean[16], dhist[64],
        prhist[64];
    if (m.detect_samples > 0)
        snprintf(dmean, sizeof dmean, "%lld",
                 (long long)(m.detect_sum_ms / m.detect_samples));
    else
        snprintf(dmean, sizeof dmean, "null");
    if (m.suspect_dead_samples > 0)
        snprintf(smean, sizeof smean, "%lld",
                 (long long)(m.suspect_dead_sum_ms / m.suspect_dead_samples));
    else
        snprintf(smean, sizeof smean, "null");
    if (m.refute_samples > 0)
        snprintf(rmean, sizeof rmean, "%lld",
                 (long long)(m.refute_sum_ms / m.refute_samples));
    else
        snprintf(rmean, sizeof rmean, "null");
    /* Probe-RTT histogram (0.48.0): the same mean/max/samples/hist
     * shape as detect_latency_ms, over answered direct probes —
     * mean is null with no samples yet, like the other latency
     * groups. */
    if (m.rtt_samples > 0)
        snprintf(pmean, sizeof pmean, "%lld",
                 (long long)(m.rtt_sum_ms / m.rtt_samples));
    else
        snprintf(pmean, sizeof pmean, "null");
    metrics_detect_hist_json(&m, dhist, sizeof dhist);
    metrics_rtt_hist_json(&m, prhist, sizeof prhist);
    /* Per-seed seed-rejoin breakdown (0.54.0): the same counts
     * METRICS reports as seed_rejoins_by_seed, as a JSON object —
     * {"127.0.0.1:17101":2} — or {} when no seed has been
     * re-joined yet. Member ids carry no quote or backslash, so
     * no escaping is needed (see metrics_seed_rejoins_json).
     * Per-seed last rejoin (0.55.0): the same shape, but the
     * wall-clock time of each seed's most recent re-JOIN as unix
     * milliseconds — {"127.0.0.1:17101":1700000100000} — or {}
     * when no seed has been re-joined yet, so a JSON consumer can
     * tell when each seed was last retried, not just how often;
     * a seed that was never re-joined has no key in either.
     * Per-seed rejoin backoff (0.56.0): the same shape again,
     * but the delay in ms the node will wait before the next
     * re-JOIN to each seed — {"127.0.0.1:17101":10000} — or {}
     * when no seed has been re-joined yet; 0 while the seed is
     * ALIVE (no backoff pending), the scheduled delay while it
     * is down and being backed off, so a JSON consumer can tell
     * a seed that is merely quiet from one whose retry has
     * backed off to the 30 s cap. */
    char srjson[1408];
    metrics_seed_rejoins_json(&m, srjson, sizeof srjson);
    char srljson[1408];
    metrics_seed_last_rejoin_json(&m, srljson, sizeof srljson);
    char srbjson[1408];
    metrics_seed_rejoin_backoff_json(&m, srbjson, sizeof srbjson);
    /* WATCH subscribers (0.33.0): the same counts METRICS reports,
     * as a top-level "watch" object — how many WATCH streams are
     * connected right now, and how many sessions since startup —
     * plus the delivery / drop counts (0.34.0): how many EVENT
     * lines streams have sent in total, how many events were
     * skipped by a stream's filter, and how many were lost to a
     * gap, and a "streams" array with the same three counts per
     * currently-connected stream, keyed by its session number, so
     * an operator can tell which consumer is falling behind
     * instead of only that some consumer is. The streams array is
     * built into its own buffer first: 64 streams at ~110 bytes
     * each fit in 8 KB with room to spare. */
    watch_stats_t ws;
    watch_stats_snapshot(&ws);
    char streams_json[8192];
    size_t s_used = 0;
    int sw = snprintf(streams_json, sizeof streams_json, "[");
    if (sw < 0) return;
    s_used += (size_t)sw;
    for (int i = 0; i < ws.n_streams; i++) {
        sw = snprintf(streams_json + s_used,
                      sizeof streams_json - s_used,
                      "%s{\"session\":%llu,\"delivered\":%llu,"
                      "\"filtered\":%llu,\"dropped\":%llu,"
                      "\"paused\":%s}",
                      i == 0 ? "" : ",",
                      (unsigned long long)ws.streams[i].session,
                      (unsigned long long)ws.streams[i].delivered,
                      (unsigned long long)ws.streams[i].filtered,
                      (unsigned long long)ws.streams[i].dropped,
                      ws.streams[i].paused ? "true" : "false");
        if (sw < 0 || (size_t)sw >= sizeof streams_json - s_used) return;
        s_used += (size_t)sw;
    }
    sw = snprintf(streams_json + s_used, sizeof streams_json - s_used,
                  "]");
    if (sw < 0 || (size_t)sw >= sizeof streams_json - s_used) return;
    /* Phi suspicion threshold (0.46.0): the same threshold METRICS
     * and PROMETHEUS report, as a top-level JSON number — the
     * --phi-threshold the node started with, 8.0 by default, two
     * decimals like the other two views — emitted always, whether
     * or not --phi-accrual is on, so a JSON consumer can judge a
     * rising per-member "phi" against the threshold that actually
     * fires the suspicion instead of hard-coding 8.0. */
    w = snprintf(buf + used, sizeof buf - used,
        "],\"metrics\":{\"suspected\":%lld,\"dead\":%lld,\"refutes\":%lld,"
        "\"joins\":%lld,\"seed_rejoins\":%lld,\"seed_rejoins_by_seed\":%s,"
        "\"seed_last_rejoin_ms\":%s,\"seed_rejoin_backoff_ms\":%s,"
        "\"adaptive_fast_cycles\":%lld,"
        "\"suspect_refuted\":%lld,\"slow_peers\":%d,\"quiet_peers\":%d,"
        "\"detect_latency_ms\":{\"mean\":%s,\"max\":%lld,"
        "\"samples\":%lld,\"hist\":[%s]},\"suspect_to_dead_ms\":{\"mean\":%s,"
        "\"max\":%lld,\"samples\":%lld},\"refute_latency_ms\":{\"mean\":%s,"
        "\"max\":%lld,\"samples\":%lld},\"probe_rtt_ms\":{\"mean\":%s,"
        "\"max\":%lld,\"samples\":%lld,\"hist\":[%s]}},\"peer_file\":%s,"
        "\"watch\":{\"subscribers\":%d,\"sessions\":%llu,"
        "\"delivered\":%llu,\"filtered\":%llu,\"dropped\":%llu,"
        "\"paused\":%d,\"event_log_cap\":%d,\"streams\":%s},"
        "\"gossip_digest_size\":%d,\"phi_threshold\":%.2f}\nEND\n",
        (long long)m.suspected, (long long)m.dead, (long long)m.refutes,
        (long long)m.joins, (long long)m.seed_rejoins, srjson, srljson,
        srbjson,
        (long long)m.adaptive_fast_cycles,
        (long long)m.suspect_refuted,
        member_count_slow(&t, hooks->slow_rtt_threshold_ms),
        member_count_quiet(&t, hooks->self_id, hooks->quiet_threshold_ms, now),
        dmean, (long long)m.detect_max_ms,
        (long long)m.detect_samples, dhist, smean,
        (long long)m.suspect_dead_max_ms,
        (long long)m.suspect_dead_samples,
        rmean, (long long)m.refute_max_ms,
        (long long)m.refute_samples,
        pmean, (long long)m.rtt_max_ms,
        (long long)m.rtt_samples, prhist,
        pfjson, ws.current,
        (unsigned long long)ws.sessions,
        (unsigned long long)ws.delivered,
        (unsigned long long)ws.filtered,
        (unsigned long long)ws.dropped,
        ws.paused,
        event_log_cap_or_default(hooks), streams_json,
        gossip_digest_size_or_default(hooks),
        phi_threshold_or_default(hooks));
    if (w < 0 || (size_t)w >= sizeof buf - used) return;
    send_all(fd, buf);
}

/* Prometheus exposition format (0.35.0): the same values METRICS and
 * JSON report, as Prometheus text format — one "# HELP" / "# TYPE"
 * pair per metric family, then "name value" lines, then END (the
 * control plane's usual trailer; this is the exposition format, not
 * an HTTP endpoint — scrape it with pulse-ctl, or point a scraper
 * at a tiny exporter that forwards the command). Counters are the
 * lifetime transition totals (suspected, dead, refutes, joins, seed
 * rejoins, adaptive fast cycles, suspect refuted, watch sessions and
 * the watch delivery / drop counts); gauges are the current membership
 * counts, slow peers, uptime and current watch subscribers.
 * Latencies are mean/max gauges (0 with no samples — Prometheus
 * has no null, and the matching _samples gauge says whether the
 * mean is real), and the detection latency also rides as a real
 * histogram: the same five buckets METRICS reports as hist, with
 * _sum and _count, so a scraper can compute quantiles instead of
 * trusting the mean. The probe RTT rides the same way since
 * 0.48.0: mean/max/samples gauges plus a real histogram over the
 * same five buckets METRICS reports as probe_rtt_ms hist
 * (<1, 1-4, 5-9, 10-49, >=50 ms). Each member rides as a pulse_member_info
 * gauge (1, labelled by member, state and source), so a scraper
 * can alert on a member's state without polling JSON. The
 * peer-file gauges are only emitted when the node runs with
 * --peer-file: a node without one has no roster state to report,
 * and an absent metric is the Prometheus way of saying "not
 * applicable". Member ids are host:port (IPv6 bracketed), so they
 * carry no quote or backslash and need no label escaping. */
static int prom_add(char *buf, size_t cap, size_t *used,
                    const char *fmt, ...) {
    if (*used >= cap) return -1;
    va_list ap;
    va_start(ap, fmt);
    int w = vsnprintf(buf + *used, cap - *used, fmt, ap);
    va_end(ap);
    if (w < 0 || (size_t)w >= cap - *used) return -1;
    *used += (size_t)w;
    return 0;
}

static void handle_prometheus(int fd, const control_hooks_t *hooks) {
    pulse_metrics_t m;
    hooks->metrics(hooks->ctx, &m);
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    int64_t uptime = (pulse_now_ms() - hooks->start_ms) / 1000;
    /* Phi (0.42.0): the live phi values, at scrape time — the same
     * now MEMBERS and JSON use, so the gauge agrees with JSON's
     * "phi" to the second decimal. */
    int phi_on = hooks->phi_enabled(hooks->ctx);
    int64_t now = pulse_now_ms();
    watch_stats_t ws;
    watch_stats_snapshot(&ws);
    pulse_peer_file_state_t pf;
    peer_file_state_or_none(hooks, &pf);

    /* 64 members at ~150 bytes each plus the fixed families is
     * under 16 KB; 64 KB has ample headroom, as in handle_json. */
    static char buf[65536];
    size_t used = 0;
    int ok = 1;
    /* Membership counts: current state, so gauges. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_members_alive Members currently ALIVE.\n"
        "# TYPE pulse_members_alive gauge\n"
        "pulse_members_alive %d\n",
        member_count_state(&t, MEMBER_ALIVE)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_members_suspect Members currently SUSPECT.\n"
        "# TYPE pulse_members_suspect gauge\n"
        "pulse_members_suspect %d\n",
        member_count_state(&t, MEMBER_SUSPECT)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_members_dead Members currently DEAD.\n"
        "# TYPE pulse_members_dead gauge\n"
        "pulse_members_dead %d\n",
        member_count_state(&t, MEMBER_DEAD)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_members_total Members in the table.\n"
        "# TYPE pulse_members_total gauge\n"
        "pulse_members_total %d\n", t.count) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_slow_peers Members whose mean probe RTT is "
        "above --slow-peer-rtt-ms.\n"
        "# TYPE pulse_slow_peers gauge\n"
        "pulse_slow_peers %d\n",
        member_count_slow(&t, hooks->slow_rtt_threshold_ms)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_quiet_peers ALIVE members whose last-contact "
        "age is above --quiet-peer-ms.\n"
        "# TYPE pulse_quiet_peers gauge\n"
        "pulse_quiet_peers %d\n",
        member_count_quiet(&t, hooks->self_id, hooks->quiet_threshold_ms, now)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_uptime_seconds Seconds since this node "
        "started.\n"
        "# TYPE pulse_uptime_seconds gauge\n"
        "pulse_uptime_seconds %lld\n", (long long)uptime) == 0;
    /* Transition counts: lifetime totals, so counters. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_suspected_total ALIVE to SUSPECT transitions "
        "seen.\n"
        "# TYPE pulse_suspected_total counter\n"
        "pulse_suspected_total %lld\n", (long long)m.suspected) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_dead_total SUSPECT to DEAD transitions seen.\n"
        "# TYPE pulse_dead_total counter\n"
        "pulse_dead_total %lld\n", (long long)m.dead) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_refutes_total Times this node refuted gossip "
        "about itself.\n"
        "# TYPE pulse_refutes_total counter\n"
        "pulse_refutes_total %lld\n", (long long)m.refutes) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_joins_total First sightings of a member.\n"
        "# TYPE pulse_joins_total counter\n"
        "pulse_joins_total %lld\n", (long long)m.joins) == 0;
    /* Seed rejoins (0.53.0, per-seed 0.54.0): one labelled sample
     * per seed that has been re-joined, so a scraper can see which
     * seed a node keeps rejoining — sum over the labels is the
     * old total. A seed never re-joined has no line (its count is
     * implicitly 0), and a node that re-joined nothing emits the
     * HELP/TYPE pair with no samples: an absent metric is how
     * Prometheus says "not applicable". Member ids carry no quote
     * or backslash, so no label escaping is needed. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_seed_rejoin_total Re-JOINs sent to a seed "
        "that was not ALIVE, by seed.\n"
        "# TYPE pulse_seed_rejoin_total counter\n") == 0;
    for (int i = 0; i < m.seed_rejoin_seed_count; i++) {
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_seed_rejoin_total{seed=\"%s\"} %lld\n",
            m.seed_rejoins_by_seed[i].id,
            (long long)m.seed_rejoins_by_seed[i].count) == 0;
    }
    /* Per-seed last rejoin (0.55.0): the wall-clock time of each
     * seed's most recent re-JOIN, as unix milliseconds, one
     * labelled gauge sample per re-joined seed — so a scraper can
     * alert on a seed whose last retry is getting stale, or tell
     * a retry that is still firing from one that stopped when the
     * cluster merged. A seed never re-joined has no line (it was
     * never retried, so there is no time to report — never a
     * fabricated zero), and a node that re-joined nothing emits
     * the HELP/TYPE pair with no samples, like the counter above.
     * Unix ms, not seconds: the retry fires every ~5 s, and
     * second resolution would hide whether the last attempt was
     * this interval or the one before. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_seed_last_rejoin_ms Wall-clock time of the "
        "most recent re-JOIN sent to each seed, as unix "
        "milliseconds, by seed.\n"
        "# TYPE pulse_seed_last_rejoin_ms gauge\n") == 0;
    for (int i = 0; i < m.seed_rejoin_seed_count; i++) {
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_seed_last_rejoin_ms{seed=\"%s\"} %lld\n",
            m.seed_rejoins_by_seed[i].id,
            (long long)m.seed_rejoins_by_seed[i].last_rejoin_unix_ms) == 0;
    }
    /* Per-seed rejoin backoff (0.56.0): the delay in ms this node
     * will wait before the next re-JOIN to each seed, one labelled
     * gauge sample per re-joined seed — so a scraper can tell a
     * seed whose retry is still at the 5 s base from one whose
     * retry has backed off to the 30 s cap, and a seed that came
     * back (backoff reset, 0) from one still being retried. A
     * seed never re-joined has no line, never a fabricated zero,
     * and a node that re-joined nothing emits the HELP/TYPE pair
     * with no samples, like the counter and the last-rejoin
     * gauge above. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_seed_rejoin_backoff_ms Delay in ms before the "
        "next re-JOIN to each seed: the current exponential "
        "backoff, 0 while the seed is ALIVE.\n"
        "# TYPE pulse_seed_rejoin_backoff_ms gauge\n") == 0;
    for (int i = 0; i < m.seed_rejoin_seed_count; i++) {
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_seed_rejoin_backoff_ms{seed=\"%s\"} %lld\n",
            m.seed_rejoins_by_seed[i].id,
            (long long)m.seed_rejoins_by_seed[i].next_delay_ms) == 0;
    }
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_adaptive_fast_cycles_total Probe cycles that "
        "ran at the fast interval because a peer was SUSPECT.\n"
        "# TYPE pulse_adaptive_fast_cycles_total counter\n"
        "pulse_adaptive_fast_cycles_total %lld\n",
        (long long)m.adaptive_fast_cycles) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_suspect_refuted_total SUSPECT to ALIVE "
        "transitions: suspicions that turned out wrong.\n"
        "# TYPE pulse_suspect_refuted_total counter\n"
        "pulse_suspect_refuted_total %lld\n",
        (long long)m.suspect_refuted) == 0;
    /* Latency groups: mean/max/samples as gauges. A mean of 0
     * with 0 samples means "no sample yet" — Prometheus has no
     * null, and the samples gauge says whether the mean is real. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_detect_latency_ms_mean Mean last-direct-"
        "contact to SUSPECT latency in ms.\n"
        "# TYPE pulse_detect_latency_ms_mean gauge\n"
        "pulse_detect_latency_ms_mean %lld\n",
        (long long)(m.detect_samples > 0
                        ? m.detect_sum_ms / m.detect_samples
                        : 0)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_detect_latency_ms_max Max last-direct-"
        "contact to SUSPECT latency in ms.\n"
        "# TYPE pulse_detect_latency_ms_max gauge\n"
        "pulse_detect_latency_ms_max %lld\n",
        (long long)m.detect_max_ms) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_detect_latency_ms_samples Suspicions with a "
        "measured detection latency.\n"
        "# TYPE pulse_detect_latency_ms_samples gauge\n"
        "pulse_detect_latency_ms_samples %lld\n",
        (long long)m.detect_samples) == 0;
    /* Detection-latency histogram: the same five buckets METRICS
     * reports as hist (<100, 100-249, 250-499, 500-999, >=1000 ms),
     * cumulative, with _sum and _count — a real histogram, so a
     * scraper can compute quantiles instead of trusting the mean. */
    {
        static const int64_t bounds[] = PULSE_DETECT_HIST_BOUNDS;
        int64_t cumulative = 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_detect_latency_ms Detection latency in "
            "ms, last direct contact to SUSPECT.\n"
            "# TYPE pulse_detect_latency_ms histogram\n") == 0;
        for (int b = 0; b < PULSE_DETECT_HIST_BUCKETS; b++) {
            cumulative += m.detect_hist[b];
            if (b < PULSE_DETECT_HIST_BUCKETS - 1) {
                ok &= prom_add(buf, sizeof buf, &used,
                    "pulse_detect_latency_ms_bucket{le=\"%lld\"} "
                    "%lld\n",
                    (long long)bounds[b],
                    (long long)cumulative) == 0;
            } else {
                ok &= prom_add(buf, sizeof buf, &used,
                    "pulse_detect_latency_ms_bucket{le=\"+Inf\"} "
                    "%lld\n",
                    (long long)cumulative) == 0;
            }
        }
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_detect_latency_ms_sum %lld\n"
            "pulse_detect_latency_ms_count %lld\n",
            (long long)m.detect_sum_ms,
            (long long)m.detect_samples) == 0;
    }
    /* Probe-RTT histogram (0.48.0): the same shape as the
     * detection-latency histogram — mean/max/samples gauges plus a
     * real histogram, cumulative, with _sum and _count — but over
     * answered direct probes: buckets <1, 1-4, 5-9, 10-49, >=50 ms,
     * the same five buckets METRICS reports as probe_rtt_ms hist.
     * A mean of 0 with 0 samples means "no probe answered yet". */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_probe_rtt_ms_mean Mean direct-probe "
        "round-trip time in ms.\n"
        "# TYPE pulse_probe_rtt_ms_mean gauge\n"
        "pulse_probe_rtt_ms_mean %lld\n",
        (long long)(m.rtt_samples > 0
                        ? m.rtt_sum_ms / m.rtt_samples
                        : 0)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_probe_rtt_ms_max Max direct-probe "
        "round-trip time in ms.\n"
        "# TYPE pulse_probe_rtt_ms_max gauge\n"
        "pulse_probe_rtt_ms_max %lld\n",
        (long long)m.rtt_max_ms) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_probe_rtt_ms_samples Direct probes answered.\n"
        "# TYPE pulse_probe_rtt_ms_samples gauge\n"
        "pulse_probe_rtt_ms_samples %lld\n",
        (long long)m.rtt_samples) == 0;
    {
        static const int64_t bounds[] = PULSE_RTT_HIST_BOUNDS;
        int64_t cumulative = 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_probe_rtt_ms Direct-probe round-trip "
            "time in ms.\n"
            "# TYPE pulse_probe_rtt_ms histogram\n") == 0;
        for (int b = 0; b < PULSE_RTT_HIST_BUCKETS; b++) {
            cumulative += m.rtt_hist[b];
            if (b < PULSE_RTT_HIST_BUCKETS - 1) {
                ok &= prom_add(buf, sizeof buf, &used,
                    "pulse_probe_rtt_ms_bucket{le=\"%lld\"} "
                    "%lld\n",
                    (long long)bounds[b],
                    (long long)cumulative) == 0;
            } else {
                ok &= prom_add(buf, sizeof buf, &used,
                    "pulse_probe_rtt_ms_bucket{le=\"+Inf\"} "
                    "%lld\n",
                    (long long)cumulative) == 0;
            }
        }
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_probe_rtt_ms_sum %lld\n"
            "pulse_probe_rtt_ms_count %lld\n",
            (long long)m.rtt_sum_ms,
            (long long)m.rtt_samples) == 0;
    }
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_suspect_to_dead_ms_mean Mean SUSPECT to DEAD "
        "duration in ms.\n"
        "# TYPE pulse_suspect_to_dead_ms_mean gauge\n"
        "pulse_suspect_to_dead_ms_mean %lld\n",
        (long long)(m.suspect_dead_samples > 0
                        ? m.suspect_dead_sum_ms / m.suspect_dead_samples
                        : 0)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_suspect_to_dead_ms_max Max SUSPECT to DEAD "
        "duration in ms.\n"
        "# TYPE pulse_suspect_to_dead_ms_max gauge\n"
        "pulse_suspect_to_dead_ms_max %lld\n",
        (long long)m.suspect_dead_max_ms) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_suspect_to_dead_ms_samples Deaths with a "
        "measured SUSPECT to DEAD duration.\n"
        "# TYPE pulse_suspect_to_dead_ms_samples gauge\n"
        "pulse_suspect_to_dead_ms_samples %lld\n",
        (long long)m.suspect_dead_samples) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_refute_latency_ms_mean Mean time a refuted "
        "suspicion sat SUSPECT, in ms.\n"
        "# TYPE pulse_refute_latency_ms_mean gauge\n"
        "pulse_refute_latency_ms_mean %lld\n",
        (long long)(m.refute_samples > 0
                        ? m.refute_sum_ms / m.refute_samples
                        : 0)) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_refute_latency_ms_max Max time a refuted "
        "suspicion sat SUSPECT, in ms.\n"
        "# TYPE pulse_refute_latency_ms_max gauge\n"
        "pulse_refute_latency_ms_max %lld\n",
        (long long)m.refute_max_ms) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_refute_latency_ms_samples Refuted suspicions "
        "with a measured SUSPECT duration.\n"
        "# TYPE pulse_refute_latency_ms_samples gauge\n"
        "pulse_refute_latency_ms_samples %lld\n",
        (long long)m.refute_samples) == 0;
    /* Watch stream state: current subscribers are a gauge, the
     * session and delivery / drop counts are lifetime totals. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_watch_subscribers WATCH streams connected "
        "right now.\n"
        "# TYPE pulse_watch_subscribers gauge\n"
        "pulse_watch_subscribers %d\n", ws.current) == 0;
    /* Paused streams (0.37.0): how many WATCH streams are paused
     * right now — a paused stream is still connected and still
     * counts as a subscriber, it just is not consuming events. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_watch_paused WATCH streams paused right "
        "now.\n"
        "# TYPE pulse_watch_paused gauge\n"
        "pulse_watch_paused %d\n", ws.paused) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_watch_sessions_total WATCH sessions served "
        "since startup.\n"
        "# TYPE pulse_watch_sessions_total counter\n"
        "pulse_watch_sessions_total %llu\n",
        (unsigned long long)ws.sessions) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_watch_delivered_total EVENT lines WATCH "
        "streams have sent in total.\n"
        "# TYPE pulse_watch_delivered_total counter\n"
        "pulse_watch_delivered_total %llu\n",
        (unsigned long long)ws.delivered) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_watch_filtered_total Events WATCH streams "
        "consumed but skipped because their filter excluded them.\n"
        "# TYPE pulse_watch_filtered_total counter\n"
        "pulse_watch_filtered_total %llu\n",
        (unsigned long long)ws.filtered) == 0;
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_watch_dropped_total Events WATCH streams "
        "lost to a gap in the bounded event ring.\n"
        "# TYPE pulse_watch_dropped_total counter\n"
        "pulse_watch_dropped_total %llu\n",
        (unsigned long long)ws.dropped) == 0;
    /* The event ring capacity (0.36.0): how many events the WATCH
     * ring holds — the --event-log-cap the node started with, 128
     * by default — as a gauge, so a scraper can see how far back
     * a replay can reach. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_watch_event_log_cap Events the WATCH event "
        "ring holds.\n"
        "# TYPE pulse_watch_event_log_cap gauge\n"
        "pulse_watch_event_log_cap %d\n",
        event_log_cap_or_default(hooks)) == 0;
    /* The gossip digest size (0.41.0): how many members one gossip
     * round's digest covers — the --gossip-digest-size the node
     * started with, 16 by default — as a gauge, so a scraper can
     * see whether the node is trading convergence speed for
     * bandwidth. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_gossip_digest_size Members one gossip round's "
        "digest covers.\n"
        "# TYPE pulse_gossip_digest_size gauge\n"
        "pulse_gossip_digest_size %d\n",
        gossip_digest_size_or_default(hooks)) == 0;
    /* The phi suspicion threshold (0.43.0): the phi value at which
     * a peer is suspected — the --phi-threshold the node started
     * with, 8.0 by default — as a gauge, so a scraper can alert on
     * a rising pulse_member_phi against the threshold that
     * actually fires the suspicion, instead of hard-coding 8.0.
     * It is emitted always, like the other config gauges: the
     * threshold is the node's configured value whether or not
     * --phi-accrual is on. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_phi_threshold Phi value at which a peer is "
        "suspected.\n"
        "# TYPE pulse_phi_threshold gauge\n"
        "pulse_phi_threshold %.2f\n",
        phi_threshold_or_default(hooks)) == 0;
    /* One info gauge per member, labelled by member, state and
     * source — the Prometheus way of exposing per-member state
     * without a separate metric per peer. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_info One per member, labelled by "
        "member, state and source.\n"
        "# TYPE pulse_member_info gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_info{member=\"%s\",state=\"%s\","
            "source=\"%s\"} 1\n",
            t.members[i].id, member_state_name(t.members[i].state),
            peer_source_str(hooks, t.members[i].id)) == 0;
    }
    /* Probe RTT per member (0.40.0): the last and mean direct-probe
     * round-trip time JSON already reports as rtt_ms /
     * rtt_mean_ms, as gauges a scraper can alert on — one line per
     * member that has answered a probe. Self never probes itself
     * and a peer never heard from has no RTT, so both are absent:
     * an absent series is how Prometheus says "not applicable",
     * the same as the peer-file gauges below. A value of 0 is a
     * real sub-millisecond probe, not a missing one. A member that
     * goes DEAD keeps its last RTT — the same as JSON, which
     * reports the last measured value too — so the gauge is the
     * last-known RTT, not a live one, for a dead member. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_rtt_ms Last direct-probe round-trip "
        "time in ms, per member.\n"
        "# TYPE pulse_member_rtt_ms gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (t.members[i].rtt_ms < 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_rtt_ms{member=\"%s\"} %lld\n",
            t.members[i].id, (long long)t.members[i].rtt_ms) == 0;
    }
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_rtt_mean_ms Mean direct-probe "
        "round-trip time in ms, per member.\n"
        "# TYPE pulse_member_rtt_mean_ms gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (t.members[i].rtt_ms < 0 ||
            t.members[i].rtt_samples == 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_rtt_mean_ms{member=\"%s\"} %lld\n",
            t.members[i].id,
            (long long)member_rtt_mean_ms(&t.members[i])) == 0;
    }
    /* Worst probe RTT per member (0.64.0): the largest single
     * direct-probe round-trip time JSON now reports as
     * rtt_max_ms, as a gauge a scraper can alert on — one line
     * per member that has answered a probe, under the same rule
     * as the last and mean gauges. The mean can hide one 60 ms
     * probe among sub-millisecond ones; the max cannot. A value
     * of 0 is a real worst case of sub-millisecond probes, not a
     * missing one, and a dead member keeps its worst RTT until
     * GC, like its last and mean. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_rtt_max_ms Worst direct-probe "
        "round-trip time in ms, per member.\n"
        "# TYPE pulse_member_rtt_max_ms gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (t.members[i].rtt_ms < 0 ||
            t.members[i].rtt_samples == 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_rtt_max_ms{member=\"%s\"} %lld\n",
            t.members[i].id,
            (long long)member_rtt_max_ms(&t.members[i])) == 0;
    }
    /* Suspicion age per member (0.58.0): how long each SUSPECT
     * member has been suspected, in ms — the same value JSON
     * reports as "suspect_ms" and METRICS as a SUSPECT line — as
     * a gauge a scraper can alert on, one line per SUSPECT
     * member. ALIVE and DEAD members have no line: an absent
     * series is how Prometheus says "not suspected", the same as
     * the RTT gauges say "never probed". The gauge grows while
     * the member sits SUSPECT and disappears the moment it
     * revives or goes DEAD, so a scraper can alert on a suspicion
     * nearing the suspect timeout instead of only on the DEAD
     * transition. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_suspect_ms How long a member has been "
        "suspected, in ms, per member.\n"
        "# TYPE pulse_member_suspect_ms gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        int64_t age = member_suspect_age_ms(&t.members[i], now);
        if (age < 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_suspect_ms{member=\"%s\"} %lld\n",
            t.members[i].id, (long long)age) == 0;
    }
    /* Time since last contact per member (0.59.0): how long since
     * this node last heard from each member, in ms — the same
     * value JSON reports as "last_seen_ms" and METRICS as a
     * LASTSEEN line — as a gauge a scraper can alert on, one line
     * per member heard from. Self is excluded (its last_seen is
     * just startup time) and a peer never heard from has no line:
     * an absent series is how Prometheus says "never heard from",
     * the same as the RTT gauges say "never probed". A live
     * peer's gauge stays under a few probe intervals; a dying
     * peer's climbs toward the suspect timeout, and a DEAD
     * member's keeps growing until GC, so a scraper can alert on
     * a peer going quiet before the suspicion even fires. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_last_seen_ms How long since this node "
        "last heard from a member, in ms, per member.\n"
        "# TYPE pulse_member_last_seen_ms gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (strcmp(t.members[i].id, hooks->self_id) == 0) continue;
        int64_t age = member_last_seen_age_ms(&t.members[i], now);
        if (age < 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_last_seen_ms{member=\"%s\"} %lld\n",
            t.members[i].id, (long long)age) == 0;
    }
    /* Phi per member (0.42.0): the failure-detector value JSON
     * reports as "phi" per member, as a gauge a scraper can alert
     * on — one line per member that has a phi, under the same
     * rule as JSON: phi mode on, not self, heartbeat history with
     * at least PULSE_PHI_MIN_SAMPLES intervals. Self has no phi
     * history and a peer never heard from has none either, so
     * both are absent; a node without --phi-accrual emits no
     * pulse_member_phi line at all. The value is the live phi at
     * scrape time, two decimals like JSON, so it rises as a peer
     * goes quiet — a scraper can alert well before the threshold
     * suspicion fires. A member that goes DEAD keeps its last
     * heartbeat, so its phi keeps growing until GC, the same as
     * JSON reports it. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_phi Phi-accrual failure-detector "
        "value, per member.\n"
        "# TYPE pulse_member_phi gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (!phi_on || strcmp(t.members[i].id, hooks->self_id) == 0 ||
            !t.members[i].phi.have_last ||
            t.members[i].phi.count < PULSE_PHI_MIN_SAMPLES) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_phi{member=\"%s\"} %.2f\n",
            t.members[i].id, phi_value(&t.members[i].phi, now)) == 0;
    }
    /* Phi history per member (0.51.0): the last 16 sampled phi
     * values JSON reports as "phi_history", as gauges a scraper
     * can alert on — the latest sampled phi (the most recent
     * probe-cycle sample, not the live value recomputed at scrape
     * time), the max over the window, and how many samples the
     * window holds (1-16). Same rule as pulse_member_phi: phi
     * mode on, not self, heartbeat history with at least
     * PULSE_PHI_MIN_SAMPLES intervals — and at least one sample
     * taken, so a peer with history but no sample yet has no
     * lines; a member that goes DEAD keeps its frozen history,
     * so its gauges stay at the last-known values until GC, like
     * the live phi gauge. The samples gauge says whether the
     * latest/max are real, the same convention as the latency
     * groups' samples gauges. */
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_phi_history_latest Latest sampled "
        "phi in the member's phi history.\n"
        "# TYPE pulse_member_phi_history_latest gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (!phi_on || strcmp(t.members[i].id, hooks->self_id) == 0 ||
            !t.members[i].phi.have_last ||
            t.members[i].phi.count < PULSE_PHI_MIN_SAMPLES) continue;
        double latest, mx;
        int n;
        phi_history_stats(&t.members[i].phi, &latest, &mx, &n);
        if (n == 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_phi_history_latest{member=\"%s\"} %.2f\n",
            t.members[i].id, latest) == 0;
    }
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_phi_history_max Max sampled phi in "
        "the member's phi history.\n"
        "# TYPE pulse_member_phi_history_max gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (!phi_on || strcmp(t.members[i].id, hooks->self_id) == 0 ||
            !t.members[i].phi.have_last ||
            t.members[i].phi.count < PULSE_PHI_MIN_SAMPLES) continue;
        double latest, mx;
        int n;
        phi_history_stats(&t.members[i].phi, &latest, &mx, &n);
        if (n == 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_phi_history_max{member=\"%s\"} %.2f\n",
            t.members[i].id, mx) == 0;
    }
    ok &= prom_add(buf, sizeof buf, &used,
        "# HELP pulse_member_phi_history_samples Samples in the "
        "member's phi history (at most 16).\n"
        "# TYPE pulse_member_phi_history_samples gauge\n") == 0;
    for (int i = 0; i < t.count; i++) {
        if (!phi_on || strcmp(t.members[i].id, hooks->self_id) == 0 ||
            !t.members[i].phi.have_last ||
            t.members[i].phi.count < PULSE_PHI_MIN_SAMPLES) continue;
        double latest, mx;
        int n;
        phi_history_stats(&t.members[i].phi, &latest, &mx, &n);
        if (n == 0) continue;
        ok &= prom_add(buf, sizeof buf, &used,
            "pulse_member_phi_history_samples{member=\"%s\"} %d\n",
            t.members[i].id, n) == 0;
    }
    /* Phi history histogram per member (0.52.0): the same 16
     * sampled phi values the gauges above summarise, as a real
     * histogram — cumulative buckets at le 0.5, 1, 2, 4, 8 and
     * +Inf, plus _sum and _count — so a scraper can compute
     * quantiles of a peer's phi history instead of trusting the
     * latest and max alone: a peer that spent most of its window
     * near 0 with one spike to 7.9 reads differently from one
     * that climbed steadily. Same rule as the history gauges:
     * phi mode on, not self, heartbeat history with at least
     * PULSE_PHI_MIN_SAMPLES intervals, and at least one sample
     * taken; a member that goes DEAD keeps its frozen histogram
     * until GC, like its gauges, so the shape of the rise at
     * suspicion stays visible after the peer is gone. The sum is
     * the sum of the sampled values to two decimals like the
     * gauges, and the +Inf bucket always equals the samples
     * gauge for the same member. */
    {
        static const double bounds[] = PULSE_PHI_HIST_BOUNDS;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_member_phi_history Sampled phi values "
            "in the member's phi history, cumulative histogram.\n"
            "# TYPE pulse_member_phi_history histogram\n") == 0;
        for (int i = 0; i < t.count; i++) {
            if (!phi_on ||
                strcmp(t.members[i].id, hooks->self_id) == 0 ||
                !t.members[i].phi.have_last ||
                t.members[i].phi.count < PULSE_PHI_MIN_SAMPLES)
                continue;
            int counts[PULSE_PHI_HIST_NBOUNDS + 1];
            double sum;
            int n;
            phi_history_hist(&t.members[i].phi, bounds,
                             PULSE_PHI_HIST_NBOUNDS, counts, &sum, &n);
            if (n == 0) continue;
            for (int b = 0; b <= PULSE_PHI_HIST_NBOUNDS; b++) {
                if (b < PULSE_PHI_HIST_NBOUNDS) {
                    ok &= prom_add(buf, sizeof buf, &used,
                        "pulse_member_phi_history_bucket{member="
                        "\"%s\",le=\"%g\"} %d\n",
                        t.members[i].id, bounds[b], counts[b]) == 0;
                } else {
                    ok &= prom_add(buf, sizeof buf, &used,
                        "pulse_member_phi_history_bucket{member="
                        "\"%s\",le=\"+Inf\"} %d\n",
                        t.members[i].id, counts[b]) == 0;
                }
            }
            ok &= prom_add(buf, sizeof buf, &used,
                "pulse_member_phi_history_sum{member=\"%s\"} %.2f\n"
                "pulse_member_phi_history_count{member=\"%s\"} %d\n",
                t.members[i].id, sum, t.members[i].id, n) == 0;
        }
    }
    /* Peer-file state (0.28.0): only when the node runs with
     * --peer-file — an absent metric is how Prometheus says "not
     * applicable", matching METRICS reporting "-" and JSON null. */
    if (pf.have_file) {
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_peer_file_peers Peers listed in "
            "--peer-file at the last load.\n"
            "# TYPE pulse_peer_file_peers gauge\n"
            "pulse_peer_file_peers %d\n", pf.peers) == 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_peer_file_added Peers added by the last "
            "peer-file load.\n"
            "# TYPE pulse_peer_file_added gauge\n"
            "pulse_peer_file_added %d\n", pf.added) == 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_peer_file_dropped Peers dropped by the "
            "last peer-file load.\n"
            "# TYPE pulse_peer_file_dropped gauge\n"
            "pulse_peer_file_dropped %d\n", pf.dropped) == 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_peer_file_kept Peers kept by the last "
            "peer-file load (removed from the file but still "
            "ALIVE).\n"
            "# TYPE pulse_peer_file_kept gauge\n"
            "pulse_peer_file_kept %d\n", pf.kept) == 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_peer_file_bad Malformed lines skipped "
            "by the last peer-file load.\n"
            "# TYPE pulse_peer_file_bad gauge\n"
            "pulse_peer_file_bad %d\n", pf.bad) == 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_peer_file_last_reload_seconds Unix time "
            "of the last successful peer-file load.\n"
            "# TYPE pulse_peer_file_last_reload_seconds gauge\n"
            "pulse_peer_file_last_reload_seconds %lld\n",
            (long long)pf.last_reload_unix) == 0;
        ok &= prom_add(buf, sizeof buf, &used,
            "# HELP pulse_peer_file_last_mtime_seconds Mtime of "
            "--peer-file at the last successful load.\n"
            "# TYPE pulse_peer_file_last_mtime_seconds gauge\n"
            "pulse_peer_file_last_mtime_seconds %lld\n",
            (long long)pf.last_mtime_unix) == 0;
    }
    if (!ok) return; /* buffer full: send nothing, as in handle_json */
    if (send_all(fd, buf) != 0) return;
    send_all(fd, "END\n");
}

static void handle_leave(int fd, const control_hooks_t *hooks,
                         const char *arg) {
    if (!arg || arg[0] == '\0') {
        send_all(fd, "ERR usage: LEAVE <member-id>\n");
        return;
    }
    int r = hooks->force_leave(hooks->ctx, arg);
    char line[128];
    if (r == 1) {
        snprintf(line, sizeof line, "OK %s\n", arg);
    } else if (r == -1) {
        snprintf(line, sizeof line, "ERR cannot leave self\n");
    } else {
        snprintf(line, sizeof line, "ERR unknown member %s\n", arg);
    }
    send_all(fd, line);
}

static void handle_meta(int fd, const control_hooks_t *hooks,
                        const char *arg) {
    if (!arg || arg[0] == '\0') {
        send_all(fd, "ERR usage: META <key=value>\n");
        return;
    }
    if (!hooks->set_meta) {
        send_all(fd, "ERR meta not supported\n");
        return;
    }
    char key[PULSE_META_KEY_LEN + 1];
    char value[PULSE_META_VAL_LEN + 1];
    if (member_meta_parse(arg, key, sizeof key, value, sizeof value) != 0) {
        char line[128];
        snprintf(line, sizeof line, "ERR invalid meta %s\n", arg);
        send_all(fd, line);
        return;
    }
    int r = hooks->set_meta(hooks->ctx, key, value);
    char line[128];
    if (r == 0) {
        snprintf(line, sizeof line, "OK %s=%s\n", key, value);
    } else if (r == -2) {
        snprintf(line, sizeof line,
                 "ERR meta full (max %d)\n", PULSE_MAX_META);
    } else {
        snprintf(line, sizeof line, "ERR invalid meta %s\n", arg);
    }
    send_all(fd, line);
}

/* RELOAD (0.25.0): re-read --peer-file through the node's reload hook
 * — the same reload SIGHUP triggers — and report the outcome. The
 * error buffer is 640 bytes, so a 768-byte line holds "ERR " plus the
 * whole reason. */
static void handle_reload(int fd, const control_hooks_t *hooks) {
    if (!hooks->reload_peer_file) {
        send_all(fd, "ERR reload not supported\n");
        return;
    }
    pulse_reload_result_t r;
    hooks->reload_peer_file(hooks->ctx, &r);
    char line[768];
    if (!r.have_file) {
        send_all(fd, "ERR no peer file\n");
    } else if (!r.read_ok) {
        snprintf(line, sizeof line, "ERR %s\n", r.error);
        send_all(fd, line);
    } else {
        snprintf(line, sizeof line,
                 "OK peers=%d added=%d dropped=%d kept=%d bad=%d\n",
                 r.peers, r.added, r.dropped, r.kept, r.bad);
        send_all(fd, line);
    }
}

/* WATCH filter (0.30.0): what a filtered WATCH subscribes to. A
 * WATCH with no filter matches everything; a filter narrows it to
 * certain event types, certain peer ids, or both. The type mask has
 * one bit per pulse_event_type_t; the id list holds canonical
 * "host:port" ids (parsed and re-formatted, so an equivalent
 * spelling still matches). At most PULSE_MAX_MEMBERS ids: a filter
 * naming more peers than the table can hold is nonsense. */
typedef struct {
    int have_types;
    uint32_t type_mask;
    int have_ids;
    char ids[PULSE_MAX_MEMBERS][PULSE_ID_LEN];
    int n_ids;
    /* State filter (0.39.0): "STATE <list>" narrows the stream to
     * events that leave their member in one of these states —
     * JOINED/REVIVED/REFUTED/QUIET land on ALIVE, SUSPECTED on SUSPECT,
     * DEAD on DEAD — and narrows the snapshot the same way, so a
     * watcher subscribed to DEAD snapshots only the dead members.
     * One bit per member_state_t; DROPPED lands on no state, so it
     * never matches a state filter. */
    int have_states;
    uint32_t state_mask;
    /* Replay / resume (0.31.0): "FROM <seq>" asks the stream to
     * replay buffered events from that sequence number before
     * going live. have_from is 0 for a plain live WATCH. from_seq
     * 0 means "from the oldest buffered event". */
    int have_from;
    uint64_t from_seq;
} watch_filter_t;

/* Parse one comma-separated filter token. A token containing ':' is
 * a peer-id list: every part must parse as a host:port address
 * (IPv6 bracketed), and is stored canonicalised. A token without
 * ':' is an event-type list: every part must be one of the six
 * EVENT words. Returns 0 on success, -1 with err filled on a bad
 * token (unknown type, unparsable id, or too many ids). */
static int parse_watch_filter_token(const char *token, watch_filter_t *f,
                                    char *err, size_t errcap) {
    char copy[CONTROL_MAX_LINE];
    snprintf(copy, sizeof copy, "%s", token);
    int is_ids = strchr(copy, ':') != NULL;
    char *save = NULL;
    for (char *part = strtok_r(copy, ",", &save); part;
         part = strtok_r(NULL, ",", &save)) {
        if (is_ids) {
            struct sockaddr_storage addr;
            if (member_parse_addr(part, &addr) != 0) {
                snprintf(err, errcap, "invalid peer id %s", part);
                return -1;
            }
            if (f->n_ids >= PULSE_MAX_MEMBERS) {
                snprintf(err, errcap, "too many peer ids (max %d)",
                         PULSE_MAX_MEMBERS);
                return -1;
            }
            /* Canonicalise through parse + format, so an id typed
             * in an equivalent spelling still matches the ids
             * the events carry. */
            member_format_id(&addr, f->ids[f->n_ids], PULSE_ID_LEN);
            f->n_ids++;
            f->have_ids = 1;
        } else {
            pulse_event_type_t type;
            if (pulse_event_type_parse(part, &type) != 0) {
                snprintf(err, errcap, "unknown event type %s", part);
                return -1;
            }
            f->type_mask |= (uint32_t)(1u << type);
            f->have_types = 1;
        }
    }
    return 0;
}

/* Parse one STATE token: the token after "STATE" must be a
 * comma-separated list of member states (ALIVE, SUSPECT, DEAD).
 * Returns 0 on success, -1 with err filled. A second STATE is
 * refused: one state list per stream. */
static int parse_watch_state(const char *tok, watch_filter_t *f,
                             char *err, size_t errcap) {
    if (f->have_states) {
        snprintf(err, errcap, "duplicate STATE");
        return -1;
    }
    if (!tok || tok[0] == '\0') {
        snprintf(err, errcap, "STATE requires a state list");
        return -1;
    }
    char copy[CONTROL_MAX_LINE];
    snprintf(copy, sizeof copy, "%s", tok);
    char *save = NULL;
    for (char *part = strtok_r(copy, ",", &save); part;
         part = strtok_r(NULL, ",", &save)) {
        member_state_t state;
        if (strcmp(part, "ALIVE") == 0) {
            state = MEMBER_ALIVE;
        } else if (strcmp(part, "SUSPECT") == 0) {
            state = MEMBER_SUSPECT;
        } else if (strcmp(part, "DEAD") == 0) {
            state = MEMBER_DEAD;
        } else {
            snprintf(err, errcap, "unknown member state %s", part);
            return -1;
        }
        f->state_mask |= (uint32_t)(1u << state);
        f->have_states = 1;
    }
    return 0;
}

/* Parse one FROM token: the token after "FROM" must be a decimal
 * sequence number (digits only, fits in a uint64_t). Returns 0 on
 * success, -1 with err filled. A second FROM is refused: one
 * resume point per stream. */
static int parse_watch_from(const char *tok, watch_filter_t *f,
                            char *err, size_t errcap) {
    if (f->have_from) {
        snprintf(err, errcap, "duplicate FROM");
        return -1;
    }
    if (!tok || tok[0] == '\0') {
        snprintf(err, errcap, "FROM requires a sequence number");
        return -1;
    }
    for (const char *p = tok; *p; p++) {
        if (*p < '0' || *p > '9') {
            snprintf(err, errcap, "invalid FROM seq %s", tok);
            return -1;
        }
    }
    errno = 0;
    char *end = NULL;
    unsigned long long v = strtoull(tok, &end, 10);
    if (errno != 0 || end == tok || *end != '\0') {
        snprintf(err, errcap, "invalid FROM seq %s", tok);
        return -1;
    }
    f->have_from = 1;
    f->from_seq = (uint64_t)v;
    return 0;
}

/* Parse the argument of a WATCH command into a filter: a type list,
 * an id list, or both, in any order, each list comma-separated and
 * each token whitespace-separated, plus an optional "STATE <list>"
 * state filter (0.39.0) and an optional "FROM <seq>"
 * replay point (0.31.0). A NULL or empty argument is an unfiltered
 * live WATCH. Returns 0 on success, -1 with err filled. */
static int parse_watch_filter(const char *arg, watch_filter_t *f,
                              char *err, size_t errcap) {
    memset(f, 0, sizeof *f);
    if (!arg || arg[0] == '\0') return 0;
    char copy[CONTROL_MAX_LINE];
    snprintf(copy, sizeof copy, "%s", arg);
    char *save = NULL;
    for (char *tok = strtok_r(copy, " \t", &save); tok;
         tok = strtok_r(NULL, " \t", &save)) {
        if (strcmp(tok, "FROM") == 0) {
            /* Consume the sequence number right after FROM: the
             * loop's next strtok_r call then continues past it. */
            char *seq_tok = strtok_r(NULL, " \t", &save);
            if (parse_watch_from(seq_tok, f, err, errcap) != 0)
                return -1;
            continue;
        }
        if (strcmp(tok, "STATE") == 0) {
            /* Consume the state list right after STATE, the same
             * way FROM consumes its sequence number. */
            char *state_tok = strtok_r(NULL, " \t", &save);
            if (parse_watch_state(state_tok, f, err, errcap) != 0)
                return -1;
            continue;
        }
        if (parse_watch_filter_token(tok, f, err, errcap) != 0)
            return -1;
    }
    return 0;
}

/* Does this event pass the filter? A filtered-out type, state, or
 * an id outside the list is dropped server-side, before sending.
 * The state filter (0.39.0) matches on the state the event leaves
 * its member in: an event that lands on no state (DROPPED) never
 * matches it. */
static int watch_filter_match(const watch_filter_t *f,
                              const pulse_event_t *ev) {
    if (f->have_types && !(f->type_mask & (uint32_t)(1u << ev->type)))
        return 0;
    if (f->have_states) {
        member_state_t state;
        if (!pulse_event_result_state(ev->type, &state))
            return 0;
        if (!(f->state_mask & (uint32_t)(1u << state)))
            return 0;
    }
    if (f->have_ids) {
        for (int i = 0; i < f->n_ids; i++)
            if (strcmp(f->ids[i], ev->id) == 0) return 1;
        return 0;
    }
    return 1;
}

/* Does this member pass the filter's snapshot side (0.39.0): the
 * snapshot is narrowed by the id list and by the state list — a
 * type filter does not narrow the snapshot, which has no event
 * type, but a state filter does, since a SNAPSHOT line carries the
 * member's current state. With no id or state filter, everything
 * passes. */
static int watch_filter_match_member(const watch_filter_t *f,
                                     const member_t *m) {
    if (f->have_ids) {
        int found = 0;
        for (int i = 0; i < f->n_ids; i++)
            if (strcmp(f->ids[i], m->id) == 0) found = 1;
        if (!found) return 0;
    }
    if (f->have_states &&
        !(f->state_mask & (uint32_t)(1u << m->state)))
        return 0;
    return 1;
}

/* WATCH PAUSE / WATCH RESUME (0.37.0): one-shot commands on a
 * separate connection that pause or resume an existing WATCH
 * stream by its session number (the number JSON reports per
 * stream). Pausing stops the stream consuming events — it waits
 * instead of calling watch_next — so a slow consumer can stop
 * the flow without closing the connection; resuming picks up
 * from the last consumed sequence, replaying what the ring still
 * holds, or the stream ends with ERR gap when the ring has
 * scrolled past it. Both are idempotent: pausing an already
 * paused stream (or resuming a running one) answers OK again —
 * the state is observable in METRICS / JSON, so the reply does
 * not need to say whether anything changed. A session that is
 * not a live stream — never existed, or already closed — answers
 * ERR unknown watch session; a session that is not a number, or
 * 0 (sessions start at 1), answers ERR invalid watch session;
 * a missing session answers ERR usage. */
static void handle_watch_pause(int fd, const char *arg) {
    int pause = strncmp(arg, "PAUSE", 5) == 0;
    const char *rest = arg + (pause ? 5 : 6);
    while (*rest == ' ' || *rest == '\t') rest++;
    if (*rest == '\0') {
        char line[64];
        snprintf(line, sizeof line, "ERR usage: WATCH %s <session>\n",
                 pause ? "PAUSE" : "RESUME");
        send_all(fd, line);
        return;
    }
    /* The session is digits only, with trailing blanks allowed —
     * anything else is not a session number. */
    const char *p = rest;
    while (*p >= '0' && *p <= '9') p++;
    const char *digits_end = p;
    while (*p == ' ' || *p == '\t') p++;
    if (*p != '\0' || digits_end == rest) {
        char line[128];
        snprintf(line, sizeof line, "ERR invalid watch session %s\n",
                 rest);
        send_all(fd, line);
        return;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long v = strtoull(rest, &end, 10);
    if (errno != 0 || v == 0) {
        char line[128];
        snprintf(line, sizeof line, "ERR invalid watch session %s\n",
                 rest);
        send_all(fd, line);
        return;
    }
    if (watch_pause_set((uint64_t)v, pause) != 0) {
        char line[128];
        snprintf(line, sizeof line,
                 "ERR unknown watch session %llu\n", v);
        send_all(fd, line);
        return;
    }
    char line[64];
    snprintf(line, sizeof line, "OK %s %llu\n",
             pause ? "paused" : "resumed", v);
    send_all(fd, line);
}

/* Wait while this stream is paused (0.37.0): sleep in 100 ms
 * slices so a resume lands within a tenth of a second, checking
 * each slice whether the client went away (a closed socket ends
 * the stream — a paused stream whose client is gone must not
 * linger as a thread forever) and sending the heartbeat when the
 * stream has sent nothing for 5 s. The heartbeat keeps its usual
 * meaning while paused — the stream is alive, it is just not
 * consuming events — and it is the only line a paused stream
 * sends, so a paused consumer sees no EVENT lines at all.
 * Returns 0 when the stream resumed (or was never paused), -1
 * when the client went away or a heartbeat send failed. */
static int watch_wait_paused(int fd, watch_pause_entry_t *pause_entry,
                             int64_t *last_sent_ms) {
    char line[256];
    while (watch_pause_is_paused(pause_entry)) {
        struct timespec ts = { .tv_sec = 0,
                               .tv_nsec = 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
        char c;
        ssize_t n = recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n == 0) return -1;
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
            errno != EINTR)
            return -1;
        int64_t now_ms = pulse_now_ms();
        if (now_ms - *last_sent_ms >= WATCH_HEARTBEAT_IDLE_MS) {
            snprintf(line, sizeof line, "HEARTBEAT %lld\n",
                     (long long)time(NULL));
            if (send_all(fd, line) != 0) return -1;
            *last_sent_ms = now_ms;
        }
    }
    return 0;
}

/* WATCH (0.29.0): stream membership events on a long-lived
 * connection. Runs on its own thread (see start_watch): the accept
 * loop keeps serving one-shot commands while a client watches.
 * The stream starts with the current table as SNAPSHOT lines (the
 * PEERS shape) so a late subscriber sees the cluster as it is,
 * then one EVENT line per membership event as it happens. The
 * latest sequence is read BEFORE the snapshot, so no event is ever
 * lost between the two: an event that lands in that window may
 * appear both in the snapshot and in the stream, and the client
 * dedupes by sequence number. The wait times out every second so
 * a client that went away without closing is noticed promptly (a
 * peek that returns 0 is a closed socket). A consumer that fell
 * more than a ring behind gets an ERR line and the connection
 * closes: it reconnects and re-reads the snapshot.
 *
 * A filter (0.30.0) narrows the stream: WATCH takes an optional
 * type list, an id list, or both, e.g. "WATCH JOINED,DEAD" (only
 * those event types), "WATCH 127.0.0.1:17102" (only that peer), or
 * "WATCH JOINED,DEAD 127.0.0.1:17102" (both). The filter is
 * applied server-side before sending, and the snapshot is narrowed
 * by the id list too, so a watcher subscribed to one peer sees
 * only that peer. An event that does not match is skipped but its
 * sequence number is still consumed, so a filtered stream's numbers
 * jump by design — a jump means filtered-out events, not lost ones,
 * and only a gap ERR means the client fell behind. A bad filter is
 * refused up front with "ERR invalid watch filter <reason>" and
 * the connection closes.
 *
 * A state filter (0.39.0) narrows the stream by the member state
 * an event leaves its member in: "WATCH STATE SUSPECT,DEAD"
 * carries only events that land on SUSPECT or DEAD, so a watcher
 * that only cares about trouble never sees joins or revivals.
 * JOINED, REVIVED, REFUTED and QUIET land on ALIVE, SUSPECTED on SUSPECT,
 * DEAD on DEAD, and DROPPED lands on no state at all — the member
 * left the table — so it never matches a state filter; a watcher
 * that wants removals subscribes to the DROPPED type instead.
 * The state filter narrows the snapshot too, by the member's
 * current state, and composes with the type/id filters and FROM,
 * in any order. A second STATE is refused, like a second FROM,
 * and a bad state is refused up front with "ERR invalid watch
 * filter unknown member state <name>".
 *
 * Replay / resume (0.31.0): "WATCH FROM <seq>" replays the
 * buffered events from that sequence number — in order, after the
 * snapshot and END, before live events — so a client that
 * reconnects catches up instead of starting from now. The
 * snapshot still goes first: it is the current table, the replay
 * is the transitions the client missed, and a client dedupes by
 * sequence number when an event appears in both. FROM 0 replays
 * from the oldest buffered event; a FROM past the latest sequence
 * starts live, with nothing to replay yet. A FROM below the oldest
 * buffered event — the requested event has been evicted from the
 * event ring (whose capacity is --event-log-cap, 128 by default)
 * — replays from the oldest buffered event instead,
 * after a "GAP <requested> <oldest>" line that says how much was
 * lost; the stream stays open, unlike the mid-stream gap below.
 * FROM composes with the type/id filters, in any order, and the
 * filter applies to replayed events exactly as to live ones.
 *
 * Heartbeat (0.32.0): a stream that has sent nothing for 5 s sends
 * "HEARTBEAT <unix-seconds>", so a client on a quiet cluster can
 * tell "nothing happened" from "the connection died silently" —
 * a half-open TCP connection otherwise looks exactly like a quiet
 * cluster. The heartbeat carries no sequence number (it is not an
 * event) and is sent regardless of the filter: it is a property of
 * the stream, not of the membership. Any line sent — EVENT, GAP,
 * or HEARTBEAT — resets the 5 s clock, so a busy cluster never
 * sees one. A client that only understands EVENT lines ignores
 * anything else, so the heartbeat is safe to add to an existing
 * watcher.
 *
 * Delivery / drop counts (0.34.0): the stream counts what it does
 * with every event it consumes — delivered when an EVENT line is
 * sent successfully (live and replayed alike), filtered when the
 * type/id filter skips it, dropped when the event is gone before
 * the stream can have it: a replay GAP at stream start (the
 * events between the requested seq and the oldest buffered one),
 * a replay GAP on the first wait (a burst evicted the resume
 * point between the snapshot and the wait), or a mid-stream gap
 * (the events between the last consumed seq and the oldest
 * buffered one). A send that fails ends the stream and is counted
 * neither way — the client is gone, so there is no consumer left
 * to report it to. The counts are per stream and feed lifetime
 * totals, exposed in METRICS and JSON.
 *
 * Pause / resume (0.37.0): a stream paused with "WATCH PAUSE
 * <session>" stops consuming events — its thread waits instead of
 * calling watch_next — so a slow consumer can stop the flow
 * without closing the connection. The events it misses while
 * paused stay in the bounded ring: on "WATCH RESUME <session>"
 * the stream picks up from the last sequence it consumed,
 * replaying what the ring still holds, in order, exactly as a
 * live stream does. When the ring has scrolled past the resume
 * point the same gap signal covers it: the stream ends with
 * "ERR gap: events lost, reconnect" and the lost events count as
 * dropped, as with any mid-stream gap. A paused stream still
 * sends its HEARTBEAT when 5 s pass with nothing sent — the
 * stream is alive, it is just not consuming — and a paused stream
 * whose client goes away ends, like any other stream, instead of
 * lingering as a thread. Pausing never stalls the node and never
 * grows memory: the node keeps recording events into the same
 * bounded ring, and the paused stream simply does not read from
 * it. */
static void handle_watch(int fd, const control_hooks_t *hooks,
                         const char *arg, int slot,
                         watch_pause_entry_t *pause_entry) {
    watch_filter_t filter;
    char ferr[128];
    if (parse_watch_filter(arg, &filter, ferr, sizeof ferr) != 0) {
        char line[256];
        snprintf(line, sizeof line, "ERR invalid watch filter %s\n",
                 ferr);
        send_all(fd, line);
        close(fd);
        return;
    }
    /* Capture the hooks this thread needs up front: the thread is
     * detached and outlives the connection setup, so it must not
     * re-read the hooks struct on every loop iteration. */
    int (*watch_next)(void *ctx, uint64_t last_seq, pulse_event_t *out,
                      int timeout_ms) = hooks->watch_next;
    uint64_t (*watch_latest_seq)(void *ctx) = hooks->watch_latest_seq;
    uint64_t (*watch_oldest_seq)(void *ctx) = hooks->watch_oldest_seq;
    void (*snapshot)(void *ctx, member_table_t *out) = hooks->snapshot;
    uint64_t latest = watch_latest_seq(hooks->ctx);
    uint64_t oldest = watch_oldest_seq(hooks->ctx);
    /* The first sequence number to deliver: latest + 1 means live
     * only (nothing buffered to send). With FROM, the requested
     * seq — clamped to what the ring still holds, with a gap
     * signalled when the request fell off the ring. */
    uint64_t start = latest + 1;
    int gap = 0;
    uint64_t gap_from = 0;
    if (filter.have_from) {
        if (oldest == 0) {
            start = 1; /* log empty: wait for the first event */
        } else if (filter.from_seq == 0) {
            start = oldest;
        } else if (filter.from_seq < oldest) {
            gap = 1;
            gap_from = filter.from_seq;
            start = oldest;
        } else if (filter.from_seq > latest) {
            start = latest + 1; /* future: start live */
        } else {
            start = filter.from_seq;
        }
    }
    uint64_t seq = start - 1;
    member_table_t t;
    snapshot(hooks->ctx, &t);
    if (send_all(fd, "OK watching\n") != 0) {
        close(fd);
        return;
    }
    int64_t now = pulse_now_ms();
    char line[256];
    for (int i = 0; i < t.count; i++) {
        if (!watch_filter_match_member(&filter, &t.members[i])) continue;
        format_peer_line(line, sizeof line, "SNAPSHOT", &t.members[i],
                         hooks, now);
        if (send_all(fd, line) != 0) {
            close(fd);
            return;
        }
    }
    if (send_all(fd, "END\n") != 0) {
        close(fd);
        return;
    }
    /* last_sent_ms is the wall clock (ms) of the last line sent on
     * this stream: the heartbeat (0.32.0) fires when it is 5 s old.
     * Every send below — EVENT, GAP, HEARTBEAT — refreshes it. */
    int64_t last_sent_ms = pulse_now_ms();
    if (gap) {
        snprintf(line, sizeof line, "GAP %llu %llu\n",
                 (unsigned long long)gap_from,
                 (unsigned long long)oldest);
        if (send_all(fd, line) != 0) {
            close(fd);
            return;
        }
        /* Delivery / drop counts (0.34.0): the events between the
         * requested seq and the oldest buffered one are gone —
         * this stream asked for them and will never see them. */
        watch_stream_add(slot, 0, 0, oldest - gap_from);
        last_sent_ms = pulse_now_ms();
    }
    /* first_wait is still set while no event has been consumed:
     * a gap on the very first wait means the replay point was
     * evicted between the snapshot and the wait (a burst of more
     * events than the ring holds in that window) — re-read the
     * ring and resume from the new oldest with a GAP line, instead
     * of dropping a stream that only just opened. Once an event has been
     * consumed the client is live, and a gap then means it fell
     * behind mid-stream: ERR and close, as before. */
    int first_wait = 1;
    for (;;) {
        /* Paused (0.37.0): stop consuming events until the stream
         * is resumed — the sequence number stays where it was, so
         * the resume replays from the ring (or ends with ERR gap
         * when the ring has scrolled past it). A client that went
         * away while paused ends the stream here. */
        if (watch_wait_paused(fd, pause_entry, &last_sent_ms) != 0)
            break;
        pulse_event_t ev;
        int r = watch_next(hooks->ctx, seq, &ev, 1000);
        if (r == 1) {
            /* Paused (0.37.0): the event arrived while a PAUSE
             * was landing — the thread was already inside the
             * wait when the flag flipped, so the loop-top check
             * above never saw it. Do not send it and do not
             * consume it: rewind the cursor so the resume
             * replays it from the ring (or ends with ERR gap
             * when the ring has scrolled past it), then wait
             * paused. An event already being sent when the
             * pause lands — returned before the flag flipped —
             * still arrives: the pause stops the flow from the
             * moment it takes effect, it does not recall a line
             * already on the wire. */
            if (watch_pause_is_paused(pause_entry)) {
                seq = ev.seq - 1;
                continue;
            }
            first_wait = 0;
            /* Always consume the sequence number, even for a
             * filtered-out event: the next wait is for the event
             * after this one, or the same event would be returned
             * again forever. Only a matching event is sent. */
            seq = ev.seq;
            if (!watch_filter_match(&filter, &ev)) {
                /* Delivery / drop counts (0.34.0): the event was
                 * consumed but the filter kept it off the wire. */
                watch_stream_add(slot, 0, 1, 0);
                continue;
            }
            /* Phi at threshold crossing (0.60.0): a SUSPECTED
             * event this node raised in phi-accrual mode carries
             * the phi that crossed the threshold as a trailing
             * " phi=<value>" — two decimals, the same value the
             * "SUSPECT ... phi=" log line printed — so a watcher
             * sees why the suspicion fired without polling
             * METRICS for a phi that has already moved on.
             * Last-contact age (0.61.0): an event whose member was
             * heard from carries the ms since this node last
             * heard from it when the event was recorded, as a
             * trailing " last_seen_ms=<ms>" — the same value
             * JSON reports as "last_seen_ms", frozen at the
             * event so a replay still shows how quiet the peer
             * was when the transition happened, and a DEAD event
             * shows a larger age than the SUSPECTED event before
             * it — after the phi field when both are present. An
             * event with neither writes the plain five fields,
             * so existing consumers see byte-identical lines. */
            size_t used = (size_t)snprintf(
                line, sizeof line, "EVENT %llu %s %s %u %lld",
                (unsigned long long)ev.seq,
                pulse_event_type_name(ev.type), ev.id,
                ev.incarnation, (long long)ev.unix_ts);
            if (ev.phi >= 0) {
                snprintf(line + used, sizeof line - used,
                         " phi=%.2f", ev.phi);
                used = strlen(line);
            }
            if (ev.last_seen_ms >= 0) {
                snprintf(line + used, sizeof line - used,
                         " last_seen_ms=%lld",
                         (long long)ev.last_seen_ms);
                used = strlen(line);
            }
            if (used < sizeof line - 1) {
                line[used++] = '\n';
                line[used] = '\0';
            }
            if (send_all(fd, line) != 0) break;
            watch_stream_add(slot, 1, 0, 0);
            last_sent_ms = pulse_now_ms();
        } else if (r == -1) {
            if (first_wait && filter.have_from) {
                uint64_t new_oldest = watch_oldest_seq(hooks->ctx);
                if (new_oldest == 0) {
                    send_all(fd, "ERR gap: events lost, reconnect\n");
                    break;
                }
                snprintf(line, sizeof line, "GAP %llu %llu\n",
                         (unsigned long long)start,
                         (unsigned long long)new_oldest);
                if (send_all(fd, line) != 0) break;
                /* Delivery / drop counts (0.34.0): the events
                 * between the replay point and the new oldest
                 * scrolled out while the stream was opening. */
                watch_stream_add(slot, 0, 0, new_oldest - start);
                last_sent_ms = pulse_now_ms();
                seq = new_oldest - 1;
                first_wait = 0;
                continue;
            }
            /* Delivery / drop counts (0.34.0): the events between
             * the last one this stream consumed and the oldest
             * still buffered scrolled out while the consumer was
             * behind — they are lost to this stream, which is
             * why it is being dropped with an ERR. */
            {
                uint64_t oldest_now = watch_oldest_seq(hooks->ctx);
                if (oldest_now > seq + 1)
                    watch_stream_add(slot, 0, 0,
                                     oldest_now - (seq + 1));
            }
            send_all(fd, "ERR gap: events lost, reconnect\n");
            break;
        } else {
            /* Timed out with nothing new: check the client is
             * still there. A peek that returns 0 is a closed
             * socket; EAGAIN means nothing to read (and the
             * client sent nothing, as expected); anything the
             * client did send is ignored — WATCH takes no input
             * after the command line. */
            char c;
            ssize_t n = recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT);
            if (n == 0) break;
            if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
                errno != EINTR)
                break;
            /* Heartbeat (0.32.0): nothing sent for 5 s means the
             * stream looks dead to a client that cannot tell a
             * quiet cluster from a half-open connection, so send
             * a HEARTBEAT line with the current unix time. It
             * carries no sequence number and ignores the filter:
             * it is the stream saying it is alive, not an event.
             * The wait above times out every second, so the
             * heartbeat lands within a second of the 5 s mark. */
            int64_t now_ms = pulse_now_ms();
            if (now_ms - last_sent_ms >= WATCH_HEARTBEAT_IDLE_MS) {
                /* Wall-clock unix seconds, like the EVENT
                 * timestamps — not pulse_now_ms, which is
                 * CLOCK_MONOTONIC and means nothing to a client. */
                snprintf(line, sizeof line, "HEARTBEAT %lld\n",
                         (long long)time(NULL));
                if (send_all(fd, line) != 0) break;
                last_sent_ms = now_ms;
            }
        }
    }
    close(fd);
}

typedef struct {
    int fd;
    const control_hooks_t *hooks;
    /* The WATCH argument, copied: the thread outlives the
     * connection's line buffer, so it must own its copy. */
    char arg[CONTROL_MAX_LINE];
    /* Delivery / drop counts (0.34.0): the registry slot this
     * stream counts into (-1 when the registry was full), and
     * the session number it was assigned at start. */
    int slot;
    uint64_t session;
    /* Pause / resume (0.37.0): this stream's entry in the pause
     * list (NULL when its allocation failed — the stream then
     * runs unpausable). The thread frees it when the stream
     * ends. */
    watch_pause_entry_t *pause_entry;
} watch_arg_t;

static void *watch_thread(void *arg) {
    watch_arg_t *w = arg;
    int slot = w->slot;
    watch_pause_entry_t *pause_entry = w->pause_entry;
    handle_watch(w->fd, w->hooks, w->arg, slot, pause_entry);
    free(w);
    /* The stream is over: give its pause entry back (a later
     * PAUSE for this session must answer "unknown session") and
     * its registry slot, and leave the subscriber count. */
    watch_pause_unregister(pause_entry);
    /* The stream is over: this subscriber leaves the count and
     * gives its registry slot back. The increment happened in
     * start_watch before the thread was created, so the count can
     * never dip below zero here. */
    watch_stream_unregister(slot);
    pthread_mutex_lock(&g_watch_mu);
    g_watch_current--;
    pthread_mutex_unlock(&g_watch_mu);
    return NULL;
}

/* Start a WATCH stream on its own detached thread. Returns 1 when
 * the thread owns the fd now (the caller must not close it), 0
 * when the reply was already sent and the caller should close. */
static int start_watch(int fd, const control_hooks_t *hooks,
                       const char *arg) {
    if (!hooks->watch_next || !hooks->watch_latest_seq ||
        !hooks->watch_oldest_seq) {
        send_all(fd, "ERR watch not supported\n");
        return 0;
    }
    watch_arg_t *w = malloc(sizeof *w);
    if (!w) {
        send_all(fd, "ERR watch not supported\n");
        return 0;
    }
    w->fd = fd;
    w->hooks = hooks;
    snprintf(w->arg, sizeof w->arg, "%s", arg ? arg : "");
    /* Count the session before the thread starts: a thread that
     * runs and exits fast must not decrement before this increment
     * lands. On a create failure the session never ran, so the
     * count is given back. The session number doubles as the
     * stream's key in the delivery / drop registry (0.34.0). */
    pthread_mutex_lock(&g_watch_mu);
    g_watch_current++;
    g_watch_total++;
    w->session = g_watch_total;
    pthread_mutex_unlock(&g_watch_mu);
    w->slot = watch_stream_register(w->session);
    /* Pause / resume (0.37.0): register the stream's pause entry
     * before the thread starts, so a PAUSE sent the moment the
     * stream opens already finds it. */
    w->pause_entry = watch_pause_register(w->session);
    pthread_t th;
    if (pthread_create(&th, NULL, watch_thread, w) != 0) {
        watch_pause_unregister(w->pause_entry);
        watch_stream_unregister(w->slot);
        pthread_mutex_lock(&g_watch_mu);
        g_watch_current--;
        g_watch_total--;
        pthread_mutex_unlock(&g_watch_mu);
        free(w);
        send_all(fd, "ERR watch not supported\n");
        return 0;
    }
    pthread_detach(th);
    return 1;
}

/* Handle one connection. Returns 1 when the fd was handed to a
 * WATCH thread (the caller must not close it), 0 otherwise. */
static int handle_connection(int fd, const control_hooks_t *hooks) {
    char line[CONTROL_MAX_LINE];
    int n = read_line(fd, line, sizeof line);
    if (n < 0) return 0;
    if (n == 0) {
        send_all(fd, "ERR empty command\n");
        return 0;
    }
    /* Split the command word from its argument. */
    char *sp = strchr(line, ' ');
    char *arg = NULL;
    if (sp) {
        *sp = '\0';
        arg = sp + 1;
        while (*arg == ' ') arg++;
    }
    if (strcmp(line, "MEMBERS") == 0) {
        handle_members(fd, hooks);
    } else if (strcmp(line, "PEERS") == 0) {
        handle_peers(fd, hooks);
    } else if (strcmp(line, "STATUS") == 0) {
        handle_status(fd, hooks);
    } else if (strcmp(line, "LEAVE") == 0) {
        handle_leave(fd, hooks, arg);
    } else if (strcmp(line, "META") == 0) {
        handle_meta(fd, hooks, arg);
    } else if (strcmp(line, "RELOAD") == 0) {
        handle_reload(fd, hooks);
    } else if (strcmp(line, "METRICS") == 0) {
        handle_metrics(fd, hooks);
    } else if (strcmp(line, "PROMETHEUS") == 0) {
        handle_prometheus(fd, hooks);
    } else if (strcmp(line, "JSON") == 0) {
        handle_json(fd, hooks);
    } else if (strcmp(line, "WATCH") == 0) {
        /* WATCH PAUSE <session> / WATCH RESUME <session>
         * (0.37.0) are one-shot commands on a separate
         * connection, not new streams: they pause or resume an
         * existing WATCH stream by its session number. Anything
         * else starts a stream. (A bare "PAUSE" was never a valid
         * filter token, so no valid WATCH is affected.) */
        if (arg && (strcmp(arg, "PAUSE") == 0 ||
                    strncmp(arg, "PAUSE ", 6) == 0 ||
                    strcmp(arg, "RESUME") == 0 ||
                    strncmp(arg, "RESUME ", 7) == 0)) {
            handle_watch_pause(fd, arg);
        } else {
            return start_watch(fd, hooks, arg);
        }
    } else {
        send_all(fd, "ERR unknown command\n");
    }
    return 0;
}

void control_serve(int fd, const control_hooks_t *hooks) {
    for (;;) {
        struct sockaddr_storage peer;
        socklen_t peerlen = sizeof peer;
        int conn = accept(fd, (struct sockaddr *)&peer, &peerlen);
        if (conn < 0) {
            if (errno == EINTR) continue;
            return; /* listen socket shut down: stop serving */
        }
        /* A WATCH hands its fd to its own thread; every other
         * command is served here and the connection closes. */
        if (!handle_connection(conn, hooks)) close(conn);
    }
}
