/* Unit tests for the TCP control plane dispatch (RELOAD, 0.25.0):
 * fake hooks serve canned state, and the test drives the real
 * control_listen / control_serve over loopback, checking the exact
 * reply lines for RELOAD success, no peer file, an unreadable file,
 * and a node with no reload hook — plus MEMBERS on an empty table
 * and an unknown command, so the dispatch around RELOAD is covered
 * too. The node-level merge itself is covered by the integration
 * test (tests/test_reload.sh) and the SIGHUP reload tests. */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "control.h"
#include "member.h"
#include "metrics.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

/* What the fake reload hook reports. 0 = success, 1 = no peer file,
 * 2 = unreadable file. */
static int g_reload_mode = 0;

static void snapshot_hook(void *ctx, member_table_t *out) {
    (void)ctx;
    memset(out, 0, sizeof *out);
}

static void metrics_hook(void *ctx, pulse_metrics_t *out) {
    (void)ctx;
    memset(out, 0, sizeof *out);
}

static int force_leave_hook(void *ctx, const char *id) {
    (void)ctx;
    (void)id;
    return 0;
}

static int phi_enabled_hook(void *ctx) {
    (void)ctx;
    return 0;
}

static void reload_hook(void *ctx, pulse_reload_result_t *out) {
    (void)ctx;
    memset(out, 0, sizeof *out);
    if (g_reload_mode == 0) {
        out->have_file = 1;
        out->read_ok = 1;
        out->peers = 3;
        out->added = 1;
        out->dropped = 0;
        out->kept = 0;
        out->bad = 0;
    } else if (g_reload_mode == 1) {
        out->have_file = 0;
    } else {
        out->have_file = 1;
        out->read_ok = 0;
        snprintf(out->error, sizeof out->error,
                 "cannot read peer file /tmp/nope: No such file or directory");
    }
}

static control_hooks_t g_hooks;

static void *serve_thread(void *arg) {
    int fd = *(int *)arg;
    control_serve(fd, &g_hooks);
    return NULL;
}

/* Send one command line to the control port and read the reply until
 * the server closes the connection. Returns the reply length. */
static int send_cmd(int port, const char *cmd, char *out, size_t cap) {
    char addrbuf[64];
    snprintf(addrbuf, sizeof addrbuf, "127.0.0.1:%d", port);
    struct sockaddr_storage dst;
    if (member_parse_addr(addrbuf, &dst) != 0) return -1;
    int fd = socket(dst.ss_family, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    if (connect(fd, (struct sockaddr *)&dst, pulse_addr_len(&dst)) < 0) {
        close(fd);
        return -1;
    }
    size_t len = strlen(cmd);
    if (send(fd, cmd, len, 0) != (ssize_t)len) {
        close(fd);
        return -1;
    }
    size_t n = 0;
    for (;;) {
        ssize_t r = recv(fd, out + n, cap - n - 1, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (r == 0) break;
        n += (size_t)r;
        if (n + 1 >= cap) break;
    }
    out[n] = '\0';
    close(fd);
    return (int)n;
}

int main(void) {
    g_hooks.ctx = NULL;
    g_hooks.snapshot = snapshot_hook;
    g_hooks.metrics = metrics_hook;
    g_hooks.force_leave = force_leave_hook;
    g_hooks.reload_peer_file = reload_hook;
    g_hooks.set_meta = NULL;
    g_hooks.phi_enabled = phi_enabled_hook;
    g_hooks.slow_rtt_threshold_ms = 0;
    g_hooks.self_id = "127.0.0.1:17999";
    g_hooks.start_ms = pulse_now_ms();

    /* Port 18991: no other suite uses it. */
    int fd = control_listen(18991, AF_INET);
    CHECK(fd >= 0);
    if (fd < 0) {
        printf("test_control: %d passed, %d failed\n", checks - failures,
               failures);
        return 1;
    }
    pthread_t server;
    CHECK(pthread_create(&server, NULL, serve_thread, &fd) == 0);

    char reply[1024];

    /* RELOAD success: the exact outcome line, no END trailer (like
     * LEAVE/META, a single-line reply). */
    g_reload_mode = 0;
    CHECK(send_cmd(18991, "RELOAD\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "OK peers=3 added=1 dropped=0 kept=0 bad=0\n") == 0);

    /* RELOAD with no --peer-file. */
    g_reload_mode = 1;
    CHECK(send_cmd(18991, "RELOAD\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR no peer file\n") == 0);

    /* RELOAD with an unreadable file: the reason rides in the ERR. */
    g_reload_mode = 2;
    CHECK(send_cmd(18991, "RELOAD\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR cannot read peer file /tmp/nope: No such file or directory\n") == 0);

    /* MEMBERS on an empty table is just END; an unknown command is
     * still a clean ERR, so the dispatch around RELOAD is intact. */
    CHECK(send_cmd(18991, "MEMBERS\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "END\n") == 0);
    CHECK(send_cmd(18991, "BOGUS\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR unknown command\n") == 0);

    shutdown(fd, SHUT_RDWR);
    close(fd);
    pthread_join(server, NULL);

    /* A node with no reload hook at all: RELOAD is refused cleanly
     * instead of crashing on a NULL hook. */
    g_hooks.reload_peer_file = NULL;
    fd = control_listen(18992, AF_INET);
    CHECK(fd >= 0);
    if (fd >= 0) {
        CHECK(pthread_create(&server, NULL, serve_thread, &fd) == 0);
        CHECK(send_cmd(18992, "RELOAD\n", reply, sizeof reply) > 0);
        CHECK(strcmp(reply, "ERR reload not supported\n") == 0);
        shutdown(fd, SHUT_RDWR);
        close(fd);
        pthread_join(server, NULL);
    }

    printf("test_control: %d passed, %d failed\n", checks - failures,
           failures);
    return failures ? 1 : 0;
}
