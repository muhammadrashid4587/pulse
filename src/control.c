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

static void handle_members(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    int phi_on = hooks->phi_enabled(hooks->ctx);
    int64_t now = pulse_now_ms();
    char line[256];
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
        snprintf(line, sizeof line, "MEMBER %s %u %s %s\n", m->id,
                 m->incarnation, member_state_name(m->state), phibuf);
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
    char line[512];
    metrics_format(&m, line, sizeof line);
    if (send_all(fd, line) != 0) return;
    send_all(fd, "\nEND\n");
}

/* JSON: one machine-readable line combining STATUS, MEMBERS and
 * METRICS, then END. Member ids are host:port (IPv6 bracketed), so
 * they contain no quote or backslash and need no escaping. Phi is a
 * number when phi-accrual is on and the peer has heartbeat history,
 * null otherwise (including self). Latency means are null when there
 * are no samples. Built into a fixed buffer: 64 members at ~120
 * bytes each is under 8 KB, so 16 KB has ample headroom. */
static void handle_json(int fd, const control_hooks_t *hooks) {
    member_table_t t;
    hooks->snapshot(hooks->ctx, &t);
    pulse_metrics_t m;
    hooks->metrics(hooks->ctx, &m);
    int phi_on = hooks->phi_enabled(hooks->ctx);
    int64_t now = pulse_now_ms();
    int64_t uptime = (now - hooks->start_ms) / 1000;
    static char buf[16384];
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
        w = snprintf(buf + used, sizeof buf - used,
                     "%s{\"id\":\"%s\",\"incarnation\":%u,\"state\":\"%s\","
                     "\"phi\":%s}",
                     i == 0 ? "" : ",", mem->id, mem->incarnation,
                     member_state_name(mem->state), phibuf);
        if (w < 0 || (size_t)w >= sizeof buf - used) return;
        used += (size_t)w;
    }
    char dmean[16], smean[16], dhist[64];
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
    metrics_detect_hist_json(&m, dhist, sizeof dhist);
    w = snprintf(buf + used, sizeof buf - used,
        "],\"metrics\":{\"suspected\":%lld,\"dead\":%lld,\"refutes\":%lld,"
        "\"joins\":%lld,\"adaptive_fast_cycles\":%lld,"
        "\"detect_latency_ms\":{\"mean\":%s,\"max\":%lld,"
        "\"samples\":%lld,\"hist\":[%s]},\"suspect_to_dead_ms\":{\"mean\":%s,"
        "\"max\":%lld,\"samples\":%lld}}}\nEND\n",
        (long long)m.suspected, (long long)m.dead, (long long)m.refutes,
        (long long)m.joins, (long long)m.adaptive_fast_cycles,
        dmean, (long long)m.detect_max_ms,
        (long long)m.detect_samples, dhist, smean,
        (long long)m.suspect_dead_max_ms,
        (long long)m.suspect_dead_samples);
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
    } else if (strcmp(line, "STATUS") == 0) {
        handle_status(fd, hooks);
    } else if (strcmp(line, "LEAVE") == 0) {
        handle_leave(fd, hooks, arg);
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
