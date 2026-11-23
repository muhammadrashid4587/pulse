/* Unit tests for the TCP control plane dispatch (RELOAD, 0.25.0;
 * PEERS, 0.27.0; peer-file state in METRICS/JSON, 0.28.0):
 * fake hooks serve canned state, and the test drives the real
 * control_listen / control_serve over loopback, checking the exact
 * reply lines for RELOAD success, no peer file, an unreadable file,
 * and a node with no reload hook — plus PEERS lines (id, state,
 * last-seen, source) for a canned table, and MEMBERS on an empty
 * table and an unknown command, so the dispatch around RELOAD is
 * covered too. METRICS and JSON (0.28.0) are covered the same way:
 * the peer-file path, last reload time / last mtime, and the counts
 * from the last load ride in the METRICS line and as a "peer_file"
 * object in JSON, each member carries its "source" in JSON, and a
 * node with no peer file (or no peer-file hook) reports "-" / null
 * instead. The node-level merge itself is covered by the
 * integration test (tests/test_reload.sh) and the SIGHUP reload
 * tests; PEERS provenance end to end is covered by
 * tests/test_peers.sh, and the METRICS/JSON exposure end to end by
 * tests/test_peerfile_metrics.sh. */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <ctype.h>
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

/* What the fake peer-file-state hook reports (0.28.0). 0 = a peer
 * file with a last load, 1 = no peer file. */
static int g_peerfile_mode = 0;

static void peer_file_state_hook(void *ctx, pulse_peer_file_state_t *out) {
    (void)ctx;
    memset(out, 0, sizeof *out);
    if (g_peerfile_mode == 0) {
        out->have_file = 1;
        snprintf(out->path, sizeof out->path, "/tmp/peers");
        out->last_reload_unix = 1700000000;
        out->last_mtime_unix = 1699999000;
        out->peers = 3;
        out->added = 1;
        out->dropped = 0;
        out->kept = 1;
        out->bad = 0;
    }
}

/* Canned membership for PEERS: self, a --seed peer (heard from
 * recently), a --peer-file peer (heard from recently), and a
 * discovered peer never heard from (last_seen_ms 0). g_peers_empty
 * makes the snapshot return an empty table instead. */
static int g_peers_empty = 0;

static void snapshot_hook(void *ctx, member_table_t *out) {
    (void)ctx;
    member_table_init(out);
    if (g_peers_empty) return;
    int64_t now = pulse_now_ms();
    struct sockaddr_storage addr;
    memset(&addr, 0, sizeof addr);
    member_t *m;
    m = member_add(out, "127.0.0.1:17999", &addr, 1);
    if (m) m->last_seen_ms = now;
    m = member_add(out, "127.0.0.1:17901", &addr, 1);
    if (m) m->last_seen_ms = now;
    m = member_add(out, "127.0.0.1:17903", &addr, 2);
    if (m) m->last_seen_ms = now;
    m = member_add(out, "127.0.0.1:17902", &addr, 1);
    if (m) {
        m->state = MEMBER_DEAD;
        m->last_seen_ms = 0; /* never heard from */
    }
}

static const char *peer_source_hook(void *ctx, const char *id) {
    (void)ctx;
    if (strcmp(id, "127.0.0.1:17999") == 0) return "self";
    if (strcmp(id, "127.0.0.1:17901") == 0) return "seed";
    if (strcmp(id, "127.0.0.1:17903") == 0) return "peer-file";
    return "discovered";
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
    g_hooks.peer_file_state = peer_file_state_hook;
    g_hooks.set_meta = NULL;
    g_hooks.phi_enabled = phi_enabled_hook;
    g_hooks.peer_source = peer_source_hook;
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

    char reply[4096];

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
    g_peers_empty = 1;
    CHECK(send_cmd(18991, "MEMBERS\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "END\n") == 0);
    CHECK(send_cmd(18991, "BOGUS\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR unknown command\n") == 0);

    /* PEERS on an empty table is just END too. */
    CHECK(send_cmd(18991, "PEERS\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "END\n") == 0);
    g_peers_empty = 0;

    /* PEERS on the canned table: one line per member, then END.
     * Last-seen is timing-dependent (0 or 1 ms for a peer heard from
     * just now), so it is checked as "-" or all digits, never an
     * exact number; id, incarnation, state and source are exact. */
    CHECK(send_cmd(18991, "PEERS\n", reply, sizeof reply) > 0);
    {
        const char *want_id[4] = {"127.0.0.1:17999", "127.0.0.1:17901",
                                  "127.0.0.1:17903", "127.0.0.1:17902"};
        unsigned want_inc[4] = {1, 1, 2, 1};
        const char *want_state[4] = {"ALIVE", "ALIVE", "ALIVE", "DEAD"};
        const char *want_seen[4] = {"-", "digits", "digits", "-"};
        const char *want_src[4] = {"self", "seed", "peer-file",
                                   "discovered"};
        int nlines = 0;
        char *save = NULL;
        char *line = strtok_r(reply, "\n", &save);
        while (line) {
            if (strcmp(line, "END") == 0) break;
            CHECK(nlines < 4);
            if (nlines < 4) {
                char id[64], state[16], seen[32], src[16];
                unsigned inc = 0;
                CHECK(sscanf(line, "PEER %63s %u %15s %31s %15s", id,
                             &inc, state, seen, src) == 5);
                CHECK(strcmp(id, want_id[nlines]) == 0);
                CHECK(inc == want_inc[nlines]);
                CHECK(strcmp(state, want_state[nlines]) == 0);
                if (strcmp(want_seen[nlines], "-") == 0) {
                    CHECK(strcmp(seen, "-") == 0);
                } else {
                    int all_digits = seen[0] != '\0';
                    for (int i = 0; seen[i]; i++)
                        if (!isdigit((unsigned char)seen[i]))
                            all_digits = 0;
                    CHECK(all_digits);
                }
                CHECK(strcmp(src, want_src[nlines]) == 0);
            }
            nlines++;
            line = strtok_r(NULL, "\n", &save);
        }
        CHECK(nlines == 4);
        CHECK(line != NULL && strcmp(line, "END") == 0);
    }

    /* A node with no peer_source hook: PEERS still answers, with
     * every non-self member as "discovered" and self as "self". */
    g_hooks.peer_source = NULL;
    CHECK(send_cmd(18991, "PEERS\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply, "PEER 127.0.0.1:17999 1 ALIVE - self\n") != NULL);
    CHECK(strstr(reply, "PEER 127.0.0.1:17901 1 ALIVE ") != NULL);
    CHECK(strstr(reply, " discovered\n") != NULL);
    CHECK(strstr(reply, " seed\n") == NULL);
    CHECK(strstr(reply, " peer-file\n") == NULL);
    g_hooks.peer_source = peer_source_hook;

    /* METRICS (0.28.0): the peer-file state rides at the end of the
     * line — path, last reload time / last mtime as unix seconds,
     * and the counts from the last load. Exact line: the canned
     * metrics are all zero, so every other field is known too. */
    g_peerfile_mode = 0;
    CHECK(send_cmd(18991, "METRICS\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "METRICS suspected=0 dead=0 refutes=0 joins=0 "
                 "adaptive_fast_cycles=0 suspect_refuted=0 "
                 "detect_latency_ms mean=- max=- samples=0 "
                 "hist=[0,0,0,0,0] suspect_to_dead_ms mean=- max=- "
                 "samples=0 refute_latency_ms mean=- max=- samples=0 "
                 "slow_peers=0 peer_file=/tmp/peers "
                 "last_reload=1700000000 last_mtime=1699999000 "
                 "pf_peers=3 pf_added=1 pf_dropped=0 pf_kept=1 "
                 "pf_bad=0\nEND\n") == 0);

    /* JSON (0.28.0): the same state as a "peer_file" object, and
     * each member carries its "source" — the peer-file peer is
     * distinguishable from the discovered one. */
    CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply,
                 "\"peer_file\":{\"path\":\"/tmp/peers\","
                 "\"last_reload\":1700000000,\"last_mtime\":1699999000,"
                 "\"peers\":3,\"added\":1,\"dropped\":0,\"kept\":1,"
                 "\"bad\":0}") != NULL);
    CHECK(strstr(reply, "\"source\":\"self\"") != NULL);
    CHECK(strstr(reply, "\"source\":\"seed\"") != NULL);
    CHECK(strstr(reply, "\"source\":\"peer-file\"") != NULL);
    CHECK(strstr(reply, "\"source\":\"discovered\"") != NULL);
    /* The peer_file object sits at the top level, after metrics. */
    {
        const char *metrics = strstr(reply, "\"metrics\":{");
        const char *pf = strstr(reply, "\"peer_file\":{");
        CHECK(metrics != NULL && pf != NULL && pf > metrics);
    }

    /* No peer file: METRICS reports "-" for every peer-file field,
     * and JSON reports "peer_file":null. */
    g_peerfile_mode = 1;
    CHECK(send_cmd(18991, "METRICS\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply,
                 "peer_file=- last_reload=- last_mtime=- pf_peers=- "
                 "pf_added=- pf_dropped=- pf_kept=- pf_bad=-") != NULL);
    CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply, "\"peer_file\":null") != NULL);
    CHECK(strstr(reply, "\"peer_file\":{") == NULL);

    /* A node with no peer-file-state hook at all reports no peer
     * file instead of crashing on a NULL hook. */
    g_hooks.peer_file_state = NULL;
    CHECK(send_cmd(18991, "METRICS\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply, "peer_file=- last_reload=-") != NULL);
    CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply, "\"peer_file\":null") != NULL);
    g_hooks.peer_file_state = peer_file_state_hook;
    g_peerfile_mode = 0;

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
