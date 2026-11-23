/* Unit tests for the TCP control plane dispatch (RELOAD, 0.25.0;
 * PEERS, 0.27.0; peer-file state in METRICS/JSON, 0.28.0; WATCH,
 * 0.29.0):
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
 * tests/test_peerfile_metrics.sh. WATCH (0.29.0) is covered two
 * ways: the event log itself (record / wait order, timeout, wakeup
 * on record, and the gap when the ring overflows), and the stream
 * over the control plane (OK, a SNAPSHOT of the canned table, then
 * live EVENT lines for events recorded after connecting, with
 * one-shot commands still answering while the stream is open, and
 * a clean ERR with no watch hooks). The WATCH events end to end —
 * a join, a suspect and a revival, a death, a LEAVE drop — are
 * covered by tests/test_watch.sh. WATCH filtering (0.30.0) is
 * covered here too: a filtered stream's snapshot holds only the
 * filtered peer, only matching events arrive, and a bad filter is
 * refused up front; the filter end to end is covered by
 * tests/test_watch_filter.sh. WATCH replay / resume (0.31.0) is
 * covered here too: FROM <seq> replays the buffered events from
 * that seq after the snapshot and END, before live events; a FROM
 * below the oldest buffered event replays from the oldest instead,
 * after a GAP line; FROM 0 replays from the oldest; a FROM past
 * the latest starts live; and a bad FROM is refused up front; the
 * replay end to end is covered by tests/test_watch_replay.sh. WATCH
 * heartbeat (0.32.0) is covered here too: a live WATCH on a quiet
 * stream sends a HEARTBEAT line with the current unix time once 5 s
 * pass with nothing sent, and a second one about 5 s later, with no
 * EVENT line in between; the heartbeat end to end is covered by
 * tests/test_watch_heartbeat.sh. WATCH subscriber count (0.33.0) is
 * covered here too: METRICS and JSON report the number of WATCH
 * streams connected right now and the sessions served since startup,
 * rising as watchers connect and falling as they go away; the count
 * end to end is covered by tests/test_watch_subscribers.sh. */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "control.h"
#include "events.h"
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

/* WATCH (0.29.0): the fake watch hooks are backed by a real event
 * log, so the stream test records events after connecting and
 * reads them off the wire. */
static pulse_event_log_t g_watch_log;

static int watch_next_hook(void *ctx, uint64_t last_seq,
                           pulse_event_t *out, int timeout_ms) {
    (void)ctx;
    return pulse_event_wait(&g_watch_log, last_seq, out, timeout_ms);
}

static uint64_t watch_latest_seq_hook(void *ctx) {
    (void)ctx;
    return pulse_event_latest_seq(&g_watch_log);
}

static uint64_t watch_oldest_seq_hook(void *ctx) {
    (void)ctx;
    return pulse_event_oldest_seq(&g_watch_log);
}

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

/* Connect to the control port without sending anything: the
 * caller drives the conversation (a WATCH stream stays open). */
static int watch_connect(int port) {
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
    return fd;
}

/* Read from a WATCH connection until buf holds needle (or the
 * timeout elapses). Returns 1 when found. The socket has a
 * receive timeout, so a stuck stream fails the test instead of
 * hanging it. buf is NUL-terminated on entry and after each read. */
static int watch_read_until(int fd, char *buf, size_t cap, size_t *len,
                            const char *needle, int timeout_ms) {
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (long)(timeout_ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    for (;;) {
        if (strstr(buf, needle) != NULL) return 1;
        if (*len + 1 >= cap) return 0;
        ssize_t r = recv(fd, buf + *len, cap - *len - 1, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return 0; /* timeout or error */
        }
        if (r == 0) return 0; /* server closed */
        *len += (size_t)r;
        buf[*len] = '\0';
    }
}

/* Read until buf holds a complete line starting with prefix (or the
 * timeout elapses). watch_read_until returns as soon as the prefix
 * is present, which can be a partial line when the send arrived in
 * pieces; this one waits for the newline that ends the line. */
static int watch_read_line_with_prefix(int fd, char *buf, size_t cap,
                                       size_t *len, const char *prefix,
                                       int timeout_ms) {
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (long)(timeout_ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    for (;;) {
        const char *p = strstr(buf, prefix);
        if (p != NULL && strchr(p, '\n') != NULL) return 1;
        if (*len + 1 >= cap) return 0;
        ssize_t r = recv(fd, buf + *len, cap - *len - 1, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return 0; /* timeout or error */
        }
        if (r == 0) return 0; /* server closed */
        *len += (size_t)r;
        buf[*len] = '\0';
    }
}

/* Poll METRICS until its line contains needle (or the timeout
 * elapses). A WATCH thread notices a closed client only on its next
 * 1 s wait timeout, so the subscriber count lags a close by up to
 * a second and an exact read right after closing is racy — the
 * poll waits for the count to settle instead. Returns 1 when
 * found. */
static int metrics_wait_contains(int port, const char *needle,
                                 int timeout_ms) {
    char reply[4096];
    int64_t deadline = pulse_now_ms() + timeout_ms;
    for (;;) {
        if (send_cmd(port, "METRICS\n", reply, sizeof reply) > 0 &&
            strstr(reply, needle) != NULL)
            return 1;
        if (pulse_now_ms() >= deadline) return 0;
        struct timespec ts = {0, 100 * 1000000L};
        nanosleep(&ts, NULL);
    }
}

/* Records one event into the log after a short sleep, so the
 * wakeup path of pulse_event_wait is exercised from another
 * thread instead of only the already-buffered path. */
static void *record_later_thread(void *arg) {
    pulse_event_log_t *log = arg;
    struct timespec ts = {0, 50 * 1000000L};
    nanosleep(&ts, NULL);
    pulse_event_record(log, PULSE_EVENT_REVIVED, "127.0.0.1:17901", 2,
                       1700000003);
    return NULL;
}

/* The event log itself (0.29.0): record / wait order, timeout,
 * cross-thread wakeup, and the gap when the ring overflows. */
static void test_event_log(void) {
    pulse_event_log_t log;
    pulse_event_log_init(&log);

    /* Type names are the words on the wire. */
    CHECK(strcmp(pulse_event_type_name(PULSE_EVENT_JOINED), "JOINED") == 0);
    CHECK(strcmp(pulse_event_type_name(PULSE_EVENT_SUSPECTED), "SUSPECTED") == 0);
    CHECK(strcmp(pulse_event_type_name(PULSE_EVENT_DEAD), "DEAD") == 0);
    CHECK(strcmp(pulse_event_type_name(PULSE_EVENT_REVIVED), "REVIVED") == 0);
    CHECK(strcmp(pulse_event_type_name(PULSE_EVENT_DROPPED), "DROPPED") == 0);
    CHECK(strcmp(pulse_event_type_name(PULSE_EVENT_REFUTED), "REFUTED") == 0);

    /* Empty log: no latest seq, and a short wait times out. */
    pulse_event_t ev;
    CHECK(pulse_event_latest_seq(&log) == 0);
    CHECK(pulse_event_wait(&log, 0, &ev, 20) == 0);

    /* Recorded events come back in order, with 1-based seqs. */
    pulse_event_record(&log, PULSE_EVENT_JOINED, "127.0.0.1:17901", 1,
                       1700000000);
    pulse_event_record(&log, PULSE_EVENT_SUSPECTED, "127.0.0.1:17901", 1,
                       1700000001);
    pulse_event_record(&log, PULSE_EVENT_DEAD, "127.0.0.1:17901", 1,
                       1700000002);
    CHECK(pulse_event_latest_seq(&log) == 3);
    CHECK(pulse_event_wait(&log, 0, &ev, 20) == 1);
    CHECK(ev.seq == 1 && ev.type == PULSE_EVENT_JOINED &&
          strcmp(ev.id, "127.0.0.1:17901") == 0 &&
          ev.incarnation == 1 && ev.unix_ts == 1700000000);
    CHECK(pulse_event_wait(&log, 1, &ev, 20) == 1);
    CHECK(ev.seq == 2 && ev.type == PULSE_EVENT_SUSPECTED);
    CHECK(pulse_event_wait(&log, 2, &ev, 20) == 1);
    CHECK(ev.seq == 3 && ev.type == PULSE_EVENT_DEAD);
    /* Caught up: a short wait times out instead of repeating. */
    CHECK(pulse_event_wait(&log, 3, &ev, 20) == 0);

    /* A wait for a future seq wakes when the event is recorded on
     * another thread. */
    pthread_t th;
    CHECK(pthread_create(&th, NULL, record_later_thread, &log) == 0);
    CHECK(pulse_event_wait(&log, 3, &ev, 2000) == 1);
    CHECK(ev.seq == 4 && ev.type == PULSE_EVENT_REVIVED &&
          ev.incarnation == 2 && ev.unix_ts == 1700000003);
    pthread_join(th, NULL);

    /* Overflow: past the ring capacity the oldest events scroll
     * out, and a wait for one of them reports the gap instead of
     * returning a stale event. Seqs 1-4 are already recorded, so
     * after CAP+5 more the next seq is CAP+10 and the oldest held
     * is 10: seqs below 10 are gone. */
    for (int i = 0; i < PULSE_EVENT_LOG_CAP + 5; i++)
        pulse_event_record(&log, PULSE_EVENT_JOINED, "127.0.0.1:17902",
                           1, 1700000100 + i);
    CHECK(pulse_event_latest_seq(&log) ==
          (uint64_t)(PULSE_EVENT_LOG_CAP + 9));
    CHECK(pulse_event_wait(&log, 8, &ev, 20) == -1);
    /* The oldest surviving event is still readable. */
    CHECK(pulse_event_wait(&log, 9, &ev, 20) == 1);
    CHECK(ev.seq == 10 && ev.type == PULSE_EVENT_JOINED &&
          strcmp(ev.id, "127.0.0.1:17902") == 0);
}

int main(void) {
    test_event_log();

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
                 "pf_bad=0 watch_subscribers=0 watch_sessions=0\nEND\n") == 0);

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

    /* WATCH (0.29.0): the stream starts with OK, a SNAPSHOT of the
     * canned table in the PEERS shape, and END — then live events
     * recorded after connecting arrive as EVENT lines, in order,
     * with 1-based sequence numbers. While the WATCH is open, a
     * one-shot command on another connection still answers: the
     * stream does not starve the control plane. */
    pulse_event_log_init(&g_watch_log);
    g_hooks.watch_next = watch_next_hook;
    g_hooks.watch_latest_seq = watch_latest_seq_hook;
    g_hooks.watch_oldest_seq = watch_oldest_seq_hook;
    int wfd = watch_connect(18991);
    CHECK(wfd >= 0);
    if (wfd >= 0) {
        const char *wcmd = "WATCH\n";
        CHECK(send(wfd, wcmd, strlen(wcmd), 0) == (ssize_t)strlen(wcmd));
        char wbuf[4096];
        size_t wlen = 0;
        wbuf[0] = '\0';
        CHECK(watch_read_until(wfd, wbuf, sizeof wbuf, &wlen, "END\n",
                               3000));
        CHECK(strncmp(wbuf, "OK watching\n", 12) == 0);
        CHECK(strstr(wbuf, "SNAPSHOT 127.0.0.1:17999 1 ALIVE - self\n") != NULL);
        CHECK(strstr(wbuf, "SNAPSHOT 127.0.0.1:17901 1 ALIVE ") != NULL);
        CHECK(strstr(wbuf, " seed\n") != NULL);
        CHECK(strstr(wbuf, "SNAPSHOT 127.0.0.1:17903 2 ALIVE ") != NULL);
        CHECK(strstr(wbuf, " peer-file\n") != NULL);
        /* MEMBERS still answers while the WATCH is open. */
        CHECK(send_cmd(18991, "MEMBERS\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply, "MEMBER 127.0.0.1:17999 1 ALIVE") != NULL);
        /* Events recorded after the snapshot stream live. */
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17901", 1, 1700000000);
        pulse_event_record(&g_watch_log, PULSE_EVENT_SUSPECTED,
                           "127.0.0.1:17903", 2, 1700000001);
        CHECK(watch_read_until(wfd, wbuf, sizeof wbuf, &wlen,
                               "EVENT 1 JOINED 127.0.0.1:17901 1 1700000000\n",
                               3000));
        CHECK(watch_read_until(wfd, wbuf, sizeof wbuf, &wlen,
                               "EVENT 2 SUSPECTED 127.0.0.1:17903 2 1700000001\n",
                               3000));
        close(wfd);
    }

    /* WATCH subscriber count (0.33.0): METRICS and JSON report how
     * many WATCH streams are connected right now and how many
     * sessions the node has served since startup. The wfd stream
     * above was session 1; this one is session 2, so the counts
     * are subscribers=1 sessions=2 while it is open — the poll
     * waits for the closed wfd thread to drop out of the count
     * first (up to a second after its client closed). A second
     * stream takes it to subscribers=2 sessions=3, closing the
     * second drops it back to 1, and closing the first leaves 0
     * with the session count still at 3: sessions only ever grow.
     * A WATCH refused up front (bad filter, checked below) never
     * becomes a session, so it counts in neither number. */
    int sfd = watch_connect(18991);
    CHECK(sfd >= 0);
    if (sfd >= 0) {
        const char *scmd = "WATCH\n";
        CHECK(send(sfd, scmd, strlen(scmd), 0) == (ssize_t)strlen(scmd));
        char sbuf[4096];
        size_t slen = 0;
        sbuf[0] = '\0';
        CHECK(watch_read_until(sfd, sbuf, sizeof sbuf, &slen, "END\n",
                               3000));
        CHECK(strncmp(sbuf, "OK watching\n", 12) == 0);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=1 watch_sessions=2", 5000));
        CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply,
                     "\"watch\":{\"subscribers\":1,\"sessions\":2}") != NULL);
        int tfd = watch_connect(18991);
        CHECK(tfd >= 0);
        if (tfd >= 0) {
            const char *tcmd = "WATCH\n";
            CHECK(send(tfd, tcmd, strlen(tcmd), 0) ==
                  (ssize_t)strlen(tcmd));
            char tbuf[4096];
            size_t tlen = 0;
            tbuf[0] = '\0';
            CHECK(watch_read_until(tfd, tbuf, sizeof tbuf, &tlen,
                                   "END\n", 3000));
            CHECK(metrics_wait_contains(
                18991, "watch_subscribers=2 watch_sessions=3", 5000));
            close(tfd);
            CHECK(metrics_wait_contains(
                18991, "watch_subscribers=1 watch_sessions=3", 5000));
        }
        close(sfd);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=0 watch_sessions=3", 5000));
        CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply,
                     "\"watch\":{\"subscribers\":0,\"sessions\":3}") != NULL);
    }

    /* WATCH filter (0.30.0): a type list, an id list, or both narrows
     * the stream server-side, and the snapshot is narrowed by the id
     * list too. A filtered-out event still consumes its sequence
     * number, so the numbers the client sees jump. The log already
     * holds seqs 1-2, so the events below are 3-6. */
    int ffd = watch_connect(18991);
    CHECK(ffd >= 0);
    if (ffd >= 0) {
        const char *fcmd = "WATCH JOINED 127.0.0.1:17901\n";
        CHECK(send(ffd, fcmd, strlen(fcmd), 0) == (ssize_t)strlen(fcmd));
        char fbuf[4096];
        size_t flen = 0;
        fbuf[0] = '\0';
        CHECK(watch_read_until(ffd, fbuf, sizeof fbuf, &flen, "END\n",
                               3000));
        CHECK(strncmp(fbuf, "OK watching\n", 12) == 0);
        /* The snapshot holds only the filtered peer. */
        CHECK(strstr(fbuf, "SNAPSHOT 127.0.0.1:17901 1 ALIVE ") != NULL);
        CHECK(strstr(fbuf, "SNAPSHOT 127.0.0.1:17903") == NULL);
        CHECK(strstr(fbuf, "SNAPSHOT 127.0.0.1:17999") == NULL);
        /* Only JOINED for 17901 arrives: SUSPECTED for 17901 fails
         * the type filter, JOINED for 17903 fails the id filter,
         * and DEAD for 17901 fails the type filter. */
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17901", 1, 1700000010);
        pulse_event_record(&g_watch_log, PULSE_EVENT_SUSPECTED,
                           "127.0.0.1:17901", 1, 1700000011);
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17903", 2, 1700000012);
        pulse_event_record(&g_watch_log, PULSE_EVENT_DEAD,
                           "127.0.0.1:17901", 1, 1700000013);
        CHECK(watch_read_until(ffd, fbuf, sizeof fbuf, &flen,
                               "EVENT 3 JOINED 127.0.0.1:17901 1 1700000010\n",
                               3000));
        /* Drain anything else the stream would send: the buffered
         * events are processed at once, so a 1.5 s read for a line
         * that never arrives leaves the buffer settled. */
        CHECK(watch_read_until(ffd, fbuf, sizeof fbuf, &flen,
                               "EVENT 999", 1500) == 0);
        CHECK(strstr(fbuf, "EVENT 4 SUSPECTED") == NULL);
        CHECK(strstr(fbuf, "EVENT 5 JOINED 127.0.0.1:17903") == NULL);
        CHECK(strstr(fbuf, "EVENT 6 DEAD") == NULL);
        close(ffd);
    }

    /* WATCH replay / resume (0.31.0): FROM <seq> replays the
     * buffered events from that seq, in order, after the snapshot
     * and END — and live events recorded after the replay keep
     * streaming on the same connection. The log is NOT
     * re-initialised here: the wfd/ffd blocks above already left
     * seqs 1-6 in it, so three more events are recorded (seqs
     * 7-9) and a FROM 8 replays seqs 8-9 but not 7. Re-initialising
     * the log would be wrong: the earlier WATCH threads may still
     * be inside pulse_event_wait on it (they exit within a second
     * of their client closing, and nothing waits for them), so
     * resetting its mutex/condvar under them is undefined
     * behaviour that wedges a waiter and strands the log lock. */
    pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                       "127.0.0.1:17901", 1, 1700000100);
    pulse_event_record(&g_watch_log, PULSE_EVENT_SUSPECTED,
                       "127.0.0.1:17903", 2, 1700000101);
    pulse_event_record(&g_watch_log, PULSE_EVENT_DEAD,
                       "127.0.0.1:17901", 1, 1700000102);
    int rfd = watch_connect(18991);
    CHECK(rfd >= 0);
    if (rfd >= 0) {
        const char *rcmd = "WATCH FROM 8\n";
        CHECK(send(rfd, rcmd, strlen(rcmd), 0) == (ssize_t)strlen(rcmd));
        char rbuf[4096];
        size_t rlen = 0;
        rbuf[0] = '\0';
        CHECK(watch_read_until(rfd, rbuf, sizeof rbuf, &rlen, "END\n",
                               3000));
        CHECK(strncmp(rbuf, "OK watching\n", 12) == 0);
        CHECK(watch_read_until(rfd, rbuf, sizeof rbuf, &rlen,
                               "EVENT 8 SUSPECTED 127.0.0.1:17903 2 1700000101\n",
                               3000));
        CHECK(watch_read_until(rfd, rbuf, sizeof rbuf, &rlen,
                               "EVENT 9 DEAD 127.0.0.1:17901 1 1700000102\n",
                               3000));
        CHECK(strstr(rbuf, "EVENT 7 ") == NULL);
        /* Live events after the replay keep streaming. */
        pulse_event_record(&g_watch_log, PULSE_EVENT_REVIVED,
                           "127.0.0.1:17901", 1, 1700000103);
        CHECK(watch_read_until(rfd, rbuf, sizeof rbuf, &rlen,
                               "EVENT 10 REVIVED 127.0.0.1:17901 1 1700000103\n",
                               3000));
        close(rfd);
    }

    /* FROM with a filter: the replay is filtered exactly like the
     * live stream — only JOINED for 17901 arrives, and the
     * filtered-out seqs are still consumed, so the numbers jump. */
    int pfd = watch_connect(18991);
    CHECK(pfd >= 0);
    if (pfd >= 0) {
        const char *pcmd = "WATCH FROM 7 JOINED 127.0.0.1:17901\n";
        CHECK(send(pfd, pcmd, strlen(pcmd), 0) == (ssize_t)strlen(pcmd));
        char pbuf[4096];
        size_t plen = 0;
        pbuf[0] = '\0';
        CHECK(watch_read_until(pfd, pbuf, sizeof pbuf, &plen, "END\n",
                               3000));
        CHECK(strncmp(pbuf, "OK watching\n", 12) == 0);
        CHECK(watch_read_until(pfd, pbuf, sizeof pbuf, &plen,
                               "EVENT 7 JOINED 127.0.0.1:17901 1 1700000100\n",
                               3000));
        CHECK(watch_read_until(pfd, pbuf, sizeof pbuf, &plen,
                               "EVENT 999", 1500) == 0);
        CHECK(strstr(pbuf, "EVENT 8 SUSPECTED") == NULL);
        CHECK(strstr(pbuf, "EVENT 9 DEAD") == NULL);
        CHECK(strstr(pbuf, "EVENT 10 REVIVED") == NULL);
        close(pfd);
    }

    /* The gap (0.31.0): 130 more events push the oldest held seq
     * from 1 to 13 (10 + 130 = 140 recorded, the ring holds the
     * last 128), so a FROM 2 has been evicted — the stream signals
     * "GAP 2 13" and replays from seq 13 instead of dropping the
     * connection. FROM 0 replays from the oldest with no GAP
     * line, and a FROM past the latest (9999) starts live: the
     * next recorded event arrives as seq 141 and nothing is
     * replayed. */
    for (int i = 0; i < 130; i++)
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17901", 1, 1700000200 + i);
    int gfd = watch_connect(18991);
    CHECK(gfd >= 0);
    if (gfd >= 0) {
        const char *gcmd = "WATCH FROM 2\n";
        CHECK(send(gfd, gcmd, strlen(gcmd), 0) == (ssize_t)strlen(gcmd));
        char gbuf[8192];
        size_t glen = 0;
        gbuf[0] = '\0';
        CHECK(watch_read_until(gfd, gbuf, sizeof gbuf, &glen, "END\n",
                               3000));
        CHECK(strncmp(gbuf, "OK watching\n", 12) == 0);
        CHECK(watch_read_until(gfd, gbuf, sizeof gbuf, &glen,
                               "GAP 2 13\n", 3000));
        CHECK(watch_read_until(gfd, gbuf, sizeof gbuf, &glen,
                               "EVENT 13 JOINED 127.0.0.1:17901 1 1700000202\n",
                               3000));
        CHECK(strstr(gbuf, "EVENT 2 ") == NULL);
        CHECK(strstr(gbuf, "EVENT 12 ") == NULL);
        close(gfd);
    }
    int zfd = watch_connect(18991);
    CHECK(zfd >= 0);
    if (zfd >= 0) {
        const char *zcmd = "WATCH FROM 0\n";
        CHECK(send(zfd, zcmd, strlen(zcmd), 0) == (ssize_t)strlen(zcmd));
        char zbuf[8192];
        size_t zlen = 0;
        zbuf[0] = '\0';
        CHECK(watch_read_until(zfd, zbuf, sizeof zbuf, &zlen, "END\n",
                               3000));
        CHECK(strncmp(zbuf, "OK watching\n", 12) == 0);
        CHECK(strstr(zbuf, "GAP ") == NULL);
        CHECK(watch_read_until(zfd, zbuf, sizeof zbuf, &zlen,
                               "EVENT 13 JOINED 127.0.0.1:17901 1 1700000202\n",
                               3000));
        close(zfd);
    }
    int ufd = watch_connect(18991);
    CHECK(ufd >= 0);
    if (ufd >= 0) {
        const char *ucmd = "WATCH FROM 9999\n";
        CHECK(send(ufd, ucmd, strlen(ucmd), 0) == (ssize_t)strlen(ucmd));
        char ubuf[4096];
        size_t ulen = 0;
        ubuf[0] = '\0';
        CHECK(watch_read_until(ufd, ubuf, sizeof ubuf, &ulen, "END\n",
                               3000));
        CHECK(strncmp(ubuf, "OK watching\n", 12) == 0);
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17903", 2, 1700000300);
        CHECK(watch_read_until(ufd, ubuf, sizeof ubuf, &ulen,
                               "EVENT 141 JOINED 127.0.0.1:17903 2 1700000300\n",
                               3000));
        CHECK(strstr(ubuf, "EVENT 13 ") == NULL);
        close(ufd);
    }

    /* WATCH heartbeat (0.32.0): a plain live WATCH on a quiet
     * stream — the log holds seqs 1-141 and nothing is recorded
     * after this one opens — sends a HEARTBEAT line with the
     * current unix time once 5 s pass with nothing sent, and no
     * EVENT line: the stream is alive, the cluster is just quiet.
     * The heartbeat carries no sequence number, so it never shows
     * up as an EVENT, and a second wait finds a second heartbeat
     * about 5 s later, proving it repeats instead of firing once.
     * (The 8 s read timeouts give the 5 s interval a wide margin
     * for a slow scheduler; the heartbeat lands within a second
     * of the mark because the wait times out every second.) */
    int hfd = watch_connect(18991);
    CHECK(hfd >= 0);
    if (hfd >= 0) {
        const char *hcmd = "WATCH\n";
        CHECK(send(hfd, hcmd, strlen(hcmd), 0) == (ssize_t)strlen(hcmd));
        char hbuf[4096];
        size_t hlen = 0;
        hbuf[0] = '\0';
        CHECK(watch_read_until(hfd, hbuf, sizeof hbuf, &hlen, "END\n",
                               3000));
        CHECK(strncmp(hbuf, "OK watching\n", 12) == 0);
        /* The read waits for the whole HEARTBEAT line, not just
         * its prefix: a partial line would parse as a truncated
         * timestamp. */
        CHECK(watch_read_line_with_prefix(hfd, hbuf, sizeof hbuf, &hlen,
                                          "HEARTBEAT ", 8000));
        /* The heartbeat is the only line after END: no EVENT on a
         * quiet stream, and its timestamp is a plausible unix
         * time — all digits, after 2023, not in the future. */
        CHECK(strstr(hbuf, "EVENT ") == NULL);
        const char *hb = strstr(hbuf, "HEARTBEAT ");
        CHECK(hb != NULL);
        if (hb != NULL) {
            long long ts = 0;
            int n = 0;
            CHECK(sscanf(hb, "HEARTBEAT %lld%n", &ts, &n) == 1);
            CHECK(ts > 1700000000LL);
            CHECK(ts <= (long long)time(NULL) + 60);
            CHECK(hb[n] == '\n');
        }
        /* A second heartbeat follows about 5 s later: the buffer
         * is reset first, so the read below can only be satisfied
         * by a new HEARTBEAT line, not the one already counted. */
        hlen = 0;
        hbuf[0] = '\0';
        CHECK(watch_read_line_with_prefix(hfd, hbuf, sizeof hbuf, &hlen,
                                          "HEARTBEAT ", 8000));
        CHECK(strstr(hbuf, "EVENT ") == NULL);
        CHECK(strstr(hbuf, "HEARTBEAT ") != NULL);
        close(hfd);
    }

    /* A bad filter is refused up front, and the connection closes. */
    CHECK(send_cmd(18991, "WATCH BOGUS\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter unknown event type BOGUS\n") == 0);
    CHECK(send_cmd(18991, "WATCH 127.0.0.1:notaport\n", reply,
                   sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter invalid peer id "
                 "127.0.0.1:notaport\n") == 0);

    /* A bad FROM is refused up front too (0.31.0): a missing
     * sequence number, a non-numeric one, and a duplicate FROM. */
    CHECK(send_cmd(18991, "WATCH FROM\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter FROM requires a sequence "
                 "number\n") == 0);
    CHECK(send_cmd(18991, "WATCH FROM abc\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter invalid FROM seq abc\n") == 0);
    CHECK(send_cmd(18991, "WATCH FROM 1 FROM 2\n", reply,
                   sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter duplicate FROM\n") == 0);

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
        /* A node with no watch hooks at all: WATCH is refused
         * cleanly instead of crashing on a NULL hook. */
        g_hooks.watch_next = NULL;
        g_hooks.watch_latest_seq = NULL;
        g_hooks.watch_oldest_seq = NULL;
        CHECK(send_cmd(18992, "WATCH\n", reply, sizeof reply) > 0);
        CHECK(strcmp(reply, "ERR watch not supported\n") == 0);
        shutdown(fd, SHUT_RDWR);
        close(fd);
        pthread_join(server, NULL);
    }

    printf("test_control: %d passed, %d failed\n", checks - failures,
           failures);
    return failures ? 1 : 0;
}
