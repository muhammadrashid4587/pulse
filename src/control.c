#define _POSIX_C_SOURCE 200809L
#include "control.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "phi.h"

#define CONTROL_MAX_LINE 1024

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
        ssize_t n = send(fd, s + sent, len - sent, 0);
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

static void handle_peers(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    int64_t now = pulse_now_ms();
    /* A peer line is short: id, incarnation, state, last-seen, source. */
    char line[256];
    for (int i = 0; i < t.count; i++) {
        const member_t *m = &t.members[i];
        /* Last-seen is ms since direct contact. Self never probes
         * itself (its last_seen is just startup time), and a peer
         * never heard from has last_seen_ms 0 — both report '-'. */
        char seenbuf[32];
        if (strcmp(m->id, hooks->self_id) == 0 || m->last_seen_ms <= 0) {
            snprintf(seenbuf, sizeof seenbuf, "-");
        } else {
            snprintf(seenbuf, sizeof seenbuf, "%lld",
                     (long long)(now - m->last_seen_ms));
        }
        const char *source = peer_source_str(hooks, m->id);
        snprintf(line, sizeof line, "PEER %s %u %s %s %s\n", m->id,
                 m->incarnation, member_state_name(m->state), seenbuf,
                 source);
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
    /* The metrics line plus slow_peers plus the peer-file fields
     * (path up to 512 chars) can run long; 1 KB holds the whole
     * line for any realistic path. snprintf truncates safely. */
    char line[1024];
    metrics_format(&m, line, sizeof line);
    /* Slow-peer warning (0.22.0): how many members currently count
     * as slow — mean probe RTT above the configured threshold. The
     * count is derived from the table, not a metrics counter, since
     * slowness is current state, not a transition. */
    size_t used = strlen(line);
    snprintf(line + used, sizeof line - used, " slow_peers=%d",
             member_count_slow(&t, hooks->slow_rtt_threshold_ms));
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
    if (send_all(fd, line) != 0) return;
    send_all(fd, "\nEND\n");
}

/* JSON: one machine-readable line combining STATUS, MEMBERS and
 * METRICS, then END. Member ids are host:port (IPv6 bracketed), so
 * they contain no quote or backslash and need no escaping. Phi is a
 * number when phi-accrual is on and the peer has heartbeat history,
 * null otherwise (including self). Latency means are null when there
 * are no samples. Built into a fixed buffer: 64 members at ~120
 * bytes each plus up to ~520 bytes of annotations each is under
 * 48 KB, so 64 KB has ample headroom. */
static void handle_json(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    pulse_metrics_t m;
    hooks->metrics(hooks->ctx, &m);
    int phi_on = hooks->phi_enabled(hooks->ctx);
    int64_t now = pulse_now_ms();
    int64_t uptime = (now - hooks->start_ms) / 1000;
    static char buf[65536];
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
        if (!phi_on || strcmp(mem->id, hooks->self_id) == 0 ||
            !mem->phi.have_last || mem->phi.count < PULSE_PHI_MIN_SAMPLES) {
            snprintf(phibuf, sizeof phibuf, "null");
        } else {
            snprintf(phibuf, sizeof phibuf, "%.2f",
                     phi_value(&mem->phi, now));
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
         * has answered no probe yet). */
        char rttbuf[32], rttmeanbuf[32];
        if (mem->rtt_ms < 0 || mem->rtt_samples == 0) {
            snprintf(rttbuf, sizeof rttbuf, "null");
            snprintf(rttmeanbuf, sizeof rttmeanbuf, "null");
        } else {
            snprintf(rttbuf, sizeof rttbuf, "%lld",
                     (long long)mem->rtt_ms);
            snprintf(rttmeanbuf, sizeof rttmeanbuf, "%lld",
                     (long long)(mem->rtt_sum_ms / mem->rtt_samples));
        }
        /* "source" (0.28.0) is where this node got the peer from —
         * "seed", "peer-file", "discovered" or "self" — the same
         * provenance PEERS reports, so a peer-file peer is
         * distinguishable from a discovered one in JSON too. */
        w = snprintf(buf + used, sizeof buf - used,
                     "%s{\"id\":\"%s\",\"incarnation\":%u,\"state\":\"%s\","
                     "\"phi\":%s,\"meta\":%s,\"rtt_ms\":%s,"
                     "\"rtt_mean_ms\":%s,\"slow\":%s,\"source\":\"%s\"}",
                     i == 0 ? "" : ",", mem->id, mem->incarnation,
                     member_state_name(mem->state), phibuf, metajson,
                     rttbuf, rttmeanbuf,
                     member_is_slow(mem, hooks->slow_rtt_threshold_ms)
                         ? "true"
                         : "false",
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
    char dmean[16], smean[16], rmean[16], dhist[64];
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
    metrics_detect_hist_json(&m, dhist, sizeof dhist);
    w = snprintf(buf + used, sizeof buf - used,
        "],\"metrics\":{\"suspected\":%lld,\"dead\":%lld,\"refutes\":%lld,"
        "\"joins\":%lld,\"adaptive_fast_cycles\":%lld,"
        "\"suspect_refuted\":%lld,\"slow_peers\":%d,"
        "\"detect_latency_ms\":{\"mean\":%s,\"max\":%lld,"
        "\"samples\":%lld,\"hist\":[%s]},\"suspect_to_dead_ms\":{\"mean\":%s,"
        "\"max\":%lld,\"samples\":%lld},\"refute_latency_ms\":{\"mean\":%s,"
        "\"max\":%lld,\"samples\":%lld}},\"peer_file\":%s}\nEND\n",
        (long long)m.suspected, (long long)m.dead, (long long)m.refutes,
        (long long)m.joins, (long long)m.adaptive_fast_cycles,
        (long long)m.suspect_refuted,
        member_count_slow(&t, hooks->slow_rtt_threshold_ms),
        dmean, (long long)m.detect_max_ms,
        (long long)m.detect_samples, dhist, smean,
        (long long)m.suspect_dead_max_ms,
        (long long)m.suspect_dead_samples,
        rmean, (long long)m.refute_max_ms,
        (long long)m.refute_samples, pfjson);
    if (w < 0 || (size_t)w >= sizeof buf - used) return;
    send_all(fd, buf);
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

static void handle_connection(int fd, const control_hooks_t *hooks) {
    char line[CONTROL_MAX_LINE];
    int n = read_line(fd, line, sizeof line);
    if (n < 0) return;
    if (n == 0) {
        send_all(fd, "ERR empty command\n");
        return;
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
    } else if (strcmp(line, "JSON") == 0) {
        handle_json(fd, hooks);
    } else {
        send_all(fd, "ERR unknown command\n");
    }
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
        handle_connection(conn, hooks);
        close(conn);
    }
}
