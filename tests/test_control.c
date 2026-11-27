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
 * tests/test_watch_filter.sh. WATCH state filtering (0.39.0) is
 * covered here too: a STATE SUSPECT,DEAD stream snapshots only
 * the canned DEAD member and carries only SUSPECTED and DEAD
 * events — JOINED, DROPPED and REVIVED are filtered out —
 * while a STATE ALIVE stream snapshots the three ALIVE members
 * and not the dead one, and a bad STATE is refused up front; the
 * state filter end to end is covered by
 * tests/test_watch_state_filter.sh. WATCH replay / resume (0.31.0) is
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
 * end to end is covered by tests/test_watch_subscribers.sh. WATCH
 * delivery / drop counts (0.34.0) are covered here too: each stream
 * counts the EVENT lines it sent, the events its filter skipped,
 * and the events it lost to a gap in the event ring, and METRICS
 * and JSON report the totals plus a per-stream breakdown keyed by
 * session number; the counts end to end are covered by
 * tests/test_watch_delivery.sh. WATCH backpressure / pause
 * (0.37.0) is covered here too: a stream paused with "WATCH PAUSE
 * <session>" stops consuming events — one recorded while it is
 * paused never reaches it — and METRICS / JSON report it paused,
 * and "WATCH RESUME <session>" resumes it with the held event
 * replayed from the ring; pausing an already-paused stream and
 * resuming a running one answer OK again, and a bad pause /
 * resume (unknown session, non-numeric session, missing session)
 * is refused up front. The pause end to end — including the gap
 * when a paused stream's resume point scrolls out of a small ring
 * — is covered by tests/test_watch_backpressure.sh. Prometheus exposition format
 * (0.35.0) is covered here too: the same values as counters,
 * gauges, a real detection-latency histogram and one
 * pulse_member_info gauge per member, with the peer-file gauges
 * present only when the node runs with --peer-file, and zero
 * samples meaning 0 means/maxima and an all-zero histogram; the
 * exposition end to end is covered by tests/test_prometheus.sh.
 * Per-member probe RTT (0.40.0) is covered here too: the last and
 * mean direct-probe RTT ride as pulse_member_rtt_ms and
 * pulse_member_rtt_mean_ms gauges, one line per member that
 * answered a probe, with self and a never-heard-from peer absent
 * entirely; the RTT end to end is covered by
 * tests/test_prometheus.sh. Per-member phi in METRICS (0.44.0)
 * is covered here too: with phi mode on, a "PHI <id> <value>"
 * line rides after the METRICS line for the peer with canned
 * phi history, and no PHI line at all for self, a peer with no
 * history, or a never-heard-from peer — and with phi mode off
 * there is no PHI line for anyone. The phi suspicion threshold
 * in METRICS (0.45.0) is covered here too: the METRICS line
 * carries phi_threshold from the hook (5.50 in this harness),
 * and with no hook it reports the default 8.00. The same
 * threshold in JSON (0.46.0) is covered here too: the JSON
 * object carries a top-level "phi_threshold" number from the
 * hook, and with no hook it reports the default 8.00. Per-member
 * probe RTT in METRICS (0.63.0) is covered here too: one "RTT
 * <id> <last> <mean> <max>" line rides after the LASTSEEN lines
 * for each member that answered a probe — the canned 17901 (last
 * 3, mean 2, max 7) and 17903 (last 0, mean 0, max 0) — with self
 * and the never-heard-from peer absent; the RTT end to end is
 * covered by tests/test_rtt_metrics.sh. Per-member worst probe
 * RTT (0.64.0) is covered here too: each member carries
 * "rtt_max_ms" in JSON (7 for the canned 17901, 0 for 17903,
 * null for self and the never-heard-from peer) and a
 * pulse_member_rtt_max_ms gauge in PROMETHEUS under the same rule
 * as the last and mean gauges; the worst RTT end to end is covered
 * by tests/test_prometheus.sh. Worst probe RTT in METRICS
 * (0.65.0) is the same max as the fourth RTT field, covered by
 * the exact canned lines above. */
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
    if (m) {
        m->last_seen_ms = now;
        /* Probe RTT (0.40.0): answered probes, last 3 ms, mean 2
         * over 4 samples — so PROMETHEUS has a real rtt gauge to
         * check for this peer. Worst RTT (0.64.0): 7 ms, the
         * largest of the four samples (3, 7, 0, 0 sum to 10). */
        m->rtt_ms = 3;
        m->rtt_sum_ms = 10;
        m->rtt_samples = 4;
        m->rtt_max_ms = 7;
        /* Phi history (0.42.0): two 100 ms intervals, last
         * heartbeat ~9.8 s ago — so the live phi at scrape time
         * is pinned at the 30.00 cap (the gap is ~97 floored
         * spreads, erfc underflows to 0) and PROMETHEUS has a
         * real phi gauge to check for this peer. The peer-file
         * peer below gets no phi history, so it has no gauge. */
        phi_heartbeat(&m->phi, now - 10000);
        phi_heartbeat(&m->phi, now - 9900);
        phi_heartbeat(&m->phi, now - 9800);
        /* Phi history samples (0.51.0): three samples as the peer
         * went quiet — 0.08, 0.30, 0.80 — so PROMETHEUS has real
         * history gauges to check for this peer. */
        phi_sample(&m->phi, now - 9800);
        phi_sample(&m->phi, now - 9700);
        phi_sample(&m->phi, now - 9600);
    }
    m = member_add(out, "127.0.0.1:17903", &addr, 2);
    if (m) {
        m->last_seen_ms = now;
        /* A sub-millisecond probe: last and mean both 0, which is
         * a real value, not a missing one — and the worst is 0
         * too (0.64.0), a real worst case, not a missing one. */
        m->rtt_ms = 0;
        m->rtt_sum_ms = 0;
        m->rtt_samples = 1;
        m->rtt_max_ms = 0;
    }
    m = member_add(out, "127.0.0.1:17902", &addr, 1);
    if (m) {
        m->state = MEMBER_DEAD;
        m->last_seen_ms = 0; /* never heard from */
        /* No probe ever answered: rtt_ms stays -1, so PROMETHEUS
         * has no rtt gauge for this peer (0.40.0). */
    }
}

static const char *peer_source_hook(void *ctx, const char *id) {
    (void)ctx;
    if (strcmp(id, "127.0.0.1:17999") == 0) return "self";
    if (strcmp(id, "127.0.0.1:17901") == 0) return "seed";
    if (strcmp(id, "127.0.0.1:17903") == 0) return "peer-file";
    return "discovered";
}

/* What the fake metrics hook reports (PROMETHEUS, 0.35.0). 0 = all
 * zeros (the METRICS exact-line test depends on it); 1 = nonzero
 * counts and latency samples, so the Prometheus counters, gauges
 * and histogram buckets have real values to check. */
static int g_metrics_mode = 0;

static void metrics_hook(void *ctx, pulse_metrics_t *out) {
    (void)ctx;
    memset(out, 0, sizeof *out);
    if (g_metrics_mode == 1) {
        out->suspected = 2;
        out->dead = 1;
        out->refutes = 1;
        out->joins = 3;
        out->seed_rejoins = 2;
        /* Per-seed breakdown (0.54.0): the 2 rejoins split across
         * two seeds, one each, so the labelled counter has two
         * samples that sum to the total. */
        out->seed_rejoin_seed_count = 2;
        snprintf(out->seed_rejoins_by_seed[0].id, PULSE_ID_LEN,
                 "127.0.0.1:17101");
        out->seed_rejoins_by_seed[0].count = 1;
        /* Per-seed last rejoin (0.55.0): the wall-clock time of
         * each seed's most recent re-JOIN, as unix ms — distinct
         * values, so the checks prove each seed carries its own
         * timestamp, not a shared one. */
        out->seed_rejoins_by_seed[0].last_rejoin_unix_ms = 1700000100000LL;
        /* Per-seed rejoin backoff (0.56.0): the delay in ms before
         * the next re-JOIN to each seed — 17101 is mid-backoff at
         * 10 s after one failure, 17103 came back so its backoff
         * reset to 0, so the checks prove each seed carries its
         * own delay, not a shared one, and that 0 means healthy
         * rather than missing. */
        out->seed_rejoins_by_seed[0].failures = 1;
        out->seed_rejoins_by_seed[0].next_delay_ms = 10000;
        out->seed_rejoins_by_seed[0].next_due_ms = 60000;
        out->seed_rejoins_by_seed[1].failures = 0;
        out->seed_rejoins_by_seed[1].next_delay_ms = 0;
        out->seed_rejoins_by_seed[1].next_due_ms = 0;
        snprintf(out->seed_rejoins_by_seed[1].id, PULSE_ID_LEN,
                 "127.0.0.1:17103");
        out->seed_rejoins_by_seed[1].count = 1;
        out->seed_rejoins_by_seed[1].last_rejoin_unix_ms = 1700000200000LL;
        out->adaptive_fast_cycles = 5;
        out->suspect_refuted = 1;
        /* Two detection samples, 150 and 300 ms: mean 225, max 300,
         * one in the 100-249 bucket and one in the 250-499 bucket. */
        out->detect_samples = 2;
        out->detect_sum_ms = 450;
        out->detect_max_ms = 300;
        out->detect_hist[1] = 1;
        out->detect_hist[2] = 1;
        out->suspect_dead_samples = 1;
        out->suspect_dead_sum_ms = 301;
        out->suspect_dead_max_ms = 301;
        out->refute_samples = 1;
        out->refute_sum_ms = 120;
        out->refute_max_ms = 120;
        /* Five probe-RTT samples, one per bucket (0.48.0): 0, 2, 7,
         * 25 and 80 ms — mean 22, max 80, hist [1,1,1,1,1]. */
        out->rtt_samples = 5;
        out->rtt_sum_ms = 114;
        out->rtt_max_ms = 80;
        out->rtt_hist[0] = 1;
        out->rtt_hist[1] = 1;
        out->rtt_hist[2] = 1;
        out->rtt_hist[3] = 1;
        out->rtt_hist[4] = 1;
    }
}

static int force_leave_hook(void *ctx, const char *id) {
    (void)ctx;
    (void)id;
    return 0;
}

/* Phi mode for the fake hooks (0.42.0): 0 = fixed-timeout path, so
 * MEMBERS / JSON report no phi and PROMETHEUS has no phi gauge;
 * 1 = phi-accrual on, so the seed peer's canned history produces
 * a real phi gauge. */
static int g_phi_mode = 0;

static int phi_enabled_hook(void *ctx) {
    (void)ctx;
    return g_phi_mode;
}

/* Phi suspicion threshold (0.43.0): a non-default 5.5, so the
 * PROMETHEUS gauge proves it comes from the hook, not the
 * default. */
static double phi_threshold_hook(void *ctx) {
    (void)ctx;
    return 5.5;
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

/* Poll JSON until its object contains needle (or the timeout
 * elapses). A WATCH thread consumes events on its own schedule, so
 * a per-stream count in JSON lags the record by up to a wait cycle
 * — the poll waits for the stream to have consumed instead of
 * racing a single read. Returns 1 when found. */
static int json_wait_contains(int port, const char *needle,
                              int timeout_ms) {
    char reply[4096];
    int64_t deadline = pulse_now_ms() + timeout_ms;
    for (;;) {
        if (send_cmd(port, "JSON\n", reply, sizeof reply) > 0 &&
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
    pulse_event_log_init(&log, PULSE_EVENT_LOG_CAP);

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
    /* A plain record carries no phi (0.60.0): only a phi-mode
     * threshold crossing does. It carries no last-contact age
     * either (0.61.0): only an event recorded by the node for a
     * member it heard from does. */
    CHECK(ev.phi == -1);
    CHECK(ev.last_seen_ms == -1);
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
    pulse_event_log_free(&log);

    /* Configurable cap (0.36.0): a log initialised with a small
     * cap holds exactly that many events, and the same gap rules
     * apply at the smaller size — a replay point below the oldest
     * held event is a gap, and the oldest surviving event is
     * still readable. The cap is reported back, and a cap of 0
     * means the default. */
    pulse_event_log_t small;
    pulse_event_log_init(&small, 4);
    CHECK(pulse_event_log_cap(&small) == 4);
    for (int i = 0; i < 6; i++)
        pulse_event_record(&small, PULSE_EVENT_JOINED,
                           "127.0.0.1:17903", 1, 1700000200 + i);
    CHECK(pulse_event_latest_seq(&small) == 6);
    CHECK(pulse_event_oldest_seq(&small) == 3);
    CHECK(pulse_event_wait(&small, 1, &ev, 20) == -1);
    CHECK(pulse_event_wait(&small, 2, &ev, 20) == 1);
    CHECK(ev.seq == 3 && strcmp(ev.id, "127.0.0.1:17903") == 0);
    pulse_event_log_free(&small);

    pulse_event_log_t def;
    pulse_event_log_init(&def, 0);
    CHECK(pulse_event_log_cap(&def) == PULSE_EVENT_LOG_CAP);
    pulse_event_log_free(&def);

    /* Event-log persistence (0.38.0): save the ring to a file and
     * load it back into a fresh log — the events come back with
     * their original sequence numbers, and the next record
     * continues the sequence instead of restarting at 1. */
    const char *ev_path = "/tmp/pulse-event-log-test.txt";
    pulse_event_log_t saved;
    pulse_event_log_init(&saved, 5);
    pulse_event_record(&saved, PULSE_EVENT_JOINED, "127.0.0.1:17904", 1,
                       1700000300);
    pulse_event_record(&saved, PULSE_EVENT_SUSPECTED, "127.0.0.1:17904",
                       1, 1700000301);
    pulse_event_record(&saved, PULSE_EVENT_DEAD, "127.0.0.1:17904", 1,
                       1700000302);
    /* Phi at threshold crossing (0.60.0): a SUSPECTED event
     * recorded with a phi round-trips through the file with it,
     * so a restart restores the crossing value, not just the
     * suspicion. Last-contact age (0.61.0): the same event
     * recorded with an age — 512 ms since the node last heard
     * from the member when it was suspected — round-trips with
     * it too, so a restart restores how quiet the peer was when
     * the transition happened. */
    pulse_event_record_full(&saved, PULSE_EVENT_SUSPECTED,
                            "127.0.0.1:17904", 1, 1700000303, 8.42,
                            512);
    /* Quiet-peer warning (0.62.0): a QUIET event recorded with a
     * last-contact age — 305 ms since the node last heard from
     * the member when the quiet threshold was crossed — and no
     * phi round-trips with it, so a restart restores the
     * warning, not just the suspicion. */
    pulse_event_record_full(&saved, PULSE_EVENT_QUIET,
                            "127.0.0.1:17904", 1, 1700000304, -1,
                            305);
    CHECK(pulse_event_log_save(&saved, ev_path) == 5);
    pulse_event_log_free(&saved);

    pulse_event_log_t restored;
    pulse_event_log_init(&restored, 5);
    CHECK(pulse_event_log_load(&restored, ev_path) == 5);
    CHECK(pulse_event_latest_seq(&restored) == 5);
    CHECK(pulse_event_oldest_seq(&restored) == 1);
    CHECK(pulse_event_wait(&restored, 0, &ev, 20) == 1);
    CHECK(ev.seq == 1 && ev.type == PULSE_EVENT_JOINED &&
          strcmp(ev.id, "127.0.0.1:17904") == 0 &&
          ev.incarnation == 1 && ev.unix_ts == 1700000300);
    CHECK(pulse_event_wait(&restored, 1, &ev, 20) == 1);
    CHECK(ev.seq == 2 && ev.type == PULSE_EVENT_SUSPECTED);
    CHECK(pulse_event_wait(&restored, 2, &ev, 20) == 1);
    CHECK(ev.seq == 3 && ev.type == PULSE_EVENT_DEAD);
    CHECK(pulse_event_wait(&restored, 3, &ev, 20) == 1);
    CHECK(ev.seq == 4 && ev.type == PULSE_EVENT_SUSPECTED &&
          ev.phi == 8.42);
    /* The last-contact age recorded with the crossing comes back
     * with it (0.61.0). */
    CHECK(ev.last_seen_ms == 512);
    /* The QUIET event recorded after it comes back with its age
     * and no phi (0.62.0): the warning survives the restart too. */
    CHECK(pulse_event_wait(&restored, 4, &ev, 20) == 1);
    CHECK(ev.seq == 5 && ev.type == PULSE_EVENT_QUIET &&
          strcmp(ev.id, "127.0.0.1:17904") == 0 &&
          ev.incarnation == 1 && ev.unix_ts == 1700000304);
    CHECK(ev.last_seen_ms == 305);
    CHECK(ev.phi == -1);
    /* The next record continues the sequence, not restarts it. */
    pulse_event_record(&restored, PULSE_EVENT_REVIVED, "127.0.0.1:17904",
                       2, 1700000305);
    CHECK(pulse_event_latest_seq(&restored) == 6);
    pulse_event_log_free(&restored);

    /* The QUIET word (0.62.0) parses back to the QUIET type, so a
     * WATCH filter can subscribe to it by name, and it lands on
     * ALIVE for the state filter — the peer is still alive, it
     * has just gone quiet. */
    pulse_event_type_t qtype;
    CHECK(pulse_event_type_parse("QUIET", &qtype) == 0);
    CHECK(qtype == PULSE_EVENT_QUIET);
    CHECK(strcmp(pulse_event_type_name(PULSE_EVENT_QUIET), "QUIET") == 0);
    member_state_t qstate;
    CHECK(pulse_event_result_state(PULSE_EVENT_QUIET, &qstate) == 1);
    CHECK(qstate == MEMBER_ALIVE);

    /* A missing file loads zero events, and a malformed line is
     * skipped while the good lines around it still load. */
    pulse_event_log_init(&restored, 4);
    CHECK(pulse_event_log_load(&restored,
                               "/tmp/pulse-event-log-no-such-file.txt") == 0);
    CHECK(pulse_event_latest_seq(&restored) == 0);
    FILE *mf = fopen(ev_path, "w");
    CHECK(mf != NULL);
    if (mf) {
        fprintf(mf, "not an event line\n");
        fprintf(mf, "7 REVIVED 127.0.0.1:17905 2 1700000400\n");
        fprintf(mf, "8 DEAD 127.0.0.1:17905 2 1700000401\n");
        /* An event with only a last-contact age — no phi — loads
         * with the age and phi -1 (0.61.0): the two trailing
         * fields are independent. */
        fprintf(mf,
                "9 JOINED 127.0.0.1:17905 2 1700000402 "
                "last_seen_ms=437\n");
        /* A QUIET event line (0.62.0) loads as QUIET with its
         * age and no phi. */
        fprintf(mf,
                "10 QUIET 127.0.0.1:17905 2 1700000403 "
                "last_seen_ms=305\n");
        fclose(mf);
    }
    CHECK(pulse_event_log_load(&restored, ev_path) == 4);
    CHECK(pulse_event_latest_seq(&restored) == 10);
    CHECK(pulse_event_oldest_seq(&restored) == 7);
    CHECK(pulse_event_wait(&restored, 6, &ev, 20) == 1);
    CHECK(ev.seq == 7 && ev.type == PULSE_EVENT_REVIVED &&
          strcmp(ev.id, "127.0.0.1:17905") == 0);
    /* A line without the trailing phi field — the pre-0.60.0
     * format — loads with phi -1, so an old log still loads. */
    CHECK(ev.phi == -1);
    /* ...and without a last_seen_ms field either — the
     * pre-0.61.0 format — it loads with last_seen_ms -1. */
    CHECK(ev.last_seen_ms == -1);
    CHECK(pulse_event_wait(&restored, 7, &ev, 20) == 1);
    CHECK(ev.seq == 8 && ev.type == PULSE_EVENT_DEAD);
    CHECK(ev.phi == -1);
    CHECK(ev.last_seen_ms == -1);
    CHECK(pulse_event_wait(&restored, 8, &ev, 20) == 1);
    CHECK(ev.seq == 9 && ev.type == PULSE_EVENT_JOINED &&
          ev.last_seen_ms == 437);
    CHECK(ev.phi == -1);
    CHECK(pulse_event_wait(&restored, 9, &ev, 20) == 1);
    CHECK(ev.seq == 10 && ev.type == PULSE_EVENT_QUIET &&
          ev.last_seen_ms == 305);
    CHECK(ev.phi == -1);
    pulse_event_log_free(&restored);
    unlink(ev_path);
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
    g_hooks.phi_threshold = phi_threshold_hook;
    g_hooks.peer_source = peer_source_hook;
    g_hooks.slow_rtt_threshold_ms = 0;
    g_hooks.quiet_threshold_ms = 0;
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

    /* 8 KB, not 4 KB: the JSON reply grows a field at a time
     * (0.62.0's per-member "quiet" flag and "quiet_peers" pushed
     * it past 4 KB with the canned table), and a truncated reply
     * drops the watch "streams" array at the end. */
    char reply[8192];

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
     * and the counts from the last load. Exact first line: the
     * canned metrics are all zero, so every other field is known
     * too. Time since last contact (0.59.0) rides after it as one
     * "LASTSEEN <id> <ms>" line per member heard from — the canned
     * table's two heard-from peers, 17901 and 17903, in table
     * order, with timing-dependent values checked as all digits
     * like PEERS does; self (17999) and the never-heard-from peer
     * (17902) have no line. Per-member probe RTT (0.63.0) rides
     * after the LASTSEEN lines as one "RTT <id> <last> <mean>
     * <max>" line per member that answered a probe — the canned
     * table's 17901 (last 3, mean 2, max 7 over 4 samples) and
     * 17903 (last 0, mean 0, max 0, a real sub-millisecond probe)
     * in table order, exact values since they are canned; self
     * and the never-heard-from peer (17902, rtt_ms -1) have no
     * line — then END. */
    g_peerfile_mode = 0;
    CHECK(send_cmd(18991, "METRICS\n", reply, sizeof reply) > 0);
    {
        static const char metrics_line[] =
            "METRICS suspected=0 dead=0 refutes=0 joins=0 "
            "seed_rejoins=0 seed_rejoins_by_seed=- "
            "seed_last_rejoin_ms_by_seed=- "
            "seed_rejoin_backoff_ms_by_seed=- "
            "adaptive_fast_cycles=0 suspect_refuted=0 "
            "detect_latency_ms mean=- max=- samples=0 "
            "hist=[0,0,0,0,0] suspect_to_dead_ms mean=- max=- "
            "samples=0 refute_latency_ms mean=- max=- samples=0 "
            "probe_rtt_ms mean=- max=- samples=0 "
            "hist=[0,0,0,0,0] slow_peers=0 quiet_peers=0 "
            "peer_file=/tmp/peers "
            "last_reload=1700000000 last_mtime=1699999000 "
            "pf_peers=3 pf_added=1 pf_dropped=0 pf_kept=1 "
            "pf_bad=0 watch_subscribers=0 watch_sessions=0 "
            "watch_delivered=0 watch_filtered=0 watch_dropped=0 "
            "watch_paused=0 watch_event_log_cap=128 "
            "gossip_digest_size=16 phi_threshold=5.50\n";
        CHECK(strncmp(reply, metrics_line, sizeof metrics_line - 1) == 0);
        const char *want_seen_id[2] = {"127.0.0.1:17901",
                                       "127.0.0.1:17903"};
        const char *want_rtt[2] = {"RTT 127.0.0.1:17901 3 2 7",
                                   "RTT 127.0.0.1:17903 0 0 0"};
        int nseen = 0, nrtt = 0;
        char *save = NULL;
        char *line = strtok_r(reply + sizeof metrics_line - 1, "\n",
                              &save);
        while (line) {
            if (strcmp(line, "END") == 0) break;
            if (nseen < 2) {
                char id[64], ms[32];
                CHECK(sscanf(line, "LASTSEEN %63s %31s", id, ms) == 2);
                CHECK(strcmp(id, want_seen_id[nseen]) == 0);
                int all_digits = ms[0] != '\0';
                for (int i = 0; ms[i]; i++)
                    if (!isdigit((unsigned char)ms[i])) all_digits = 0;
                CHECK(all_digits);
                nseen++;
            } else {
                CHECK(nrtt < 2);
                if (nrtt < 2) CHECK(strcmp(line, want_rtt[nrtt]) == 0);
                nrtt++;
            }
            line = strtok_r(NULL, "\n", &save);
        }
        CHECK(nseen == 2);
        CHECK(nrtt == 2);
        CHECK(line != NULL && strcmp(line, "END") == 0);
    }

    /* Phi suspicion threshold in METRICS (0.45.0): the fake hook
     * reports 5.50, so the exact line above proves the value
     * comes from the hook, not the default — and with no hook
     * the line reports the default 8.00 instead of crashing on
     * a NULL hook. */
    {
        double (*saved)(void *) = g_hooks.phi_threshold;
        g_hooks.phi_threshold = NULL;
        CHECK(send_cmd(18991, "METRICS\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply, "phi_threshold=8.00\n") != NULL);
        g_hooks.phi_threshold = saved;
    }

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
    /* Seed-rejoin count (0.53.0): the metrics object carries
     * "seed_rejoins" right after "joins" — 0 here, since the
     * canned metrics are all zeros — and the per-seed breakdown
     * (0.54.0) rides right after it as "seed_rejoins_by_seed",
     * {} when no seed has been re-joined yet. */
    CHECK(strstr(reply, "\"joins\":0,\"seed_rejoins\":0") != NULL);
    CHECK(strstr(reply, "\"seed_rejoins_by_seed\":{}") != NULL);
    /* Per-seed last rejoin (0.55.0): the same shape as the
     * breakdown, but unix-ms timestamps — {} here, since the
     * canned metrics are all zeros. */
    CHECK(strstr(reply, "\"seed_last_rejoin_ms\":{}") != NULL);
    /* Per-seed rejoin backoff (0.56.0): the same shape again,
     * but the delay in ms before the next re-JOIN to each seed
     * — {} here, since the canned metrics are all zeros. */
    CHECK(strstr(reply, "\"seed_rejoin_backoff_ms\":{}") != NULL);
    /* The event ring capacity (0.36.0) rides in the "watch"
     * object: no hook is set in this harness, so it reports the
     * default 128. */
    CHECK(strstr(reply, "\"event_log_cap\":128") != NULL);
    /* The gossip digest size (0.41.0) rides top-level: no hook is
     * set in this harness, so it reports the default 16. */
    CHECK(strstr(reply, "\"gossip_digest_size\":16") != NULL);
    /* Phi suspicion threshold (0.46.0): the same threshold rides
     * top-level in JSON as a number — the fake hook reports 5.50,
     * so the value proves it comes from the hook, not the
     * default — and with no hook it falls back to 8.00 instead
     * of crashing on a NULL hook. */
    CHECK(strstr(reply, "\"phi_threshold\":5.50") != NULL);
    {
        double (*saved)(void *) = g_hooks.phi_threshold;
        g_hooks.phi_threshold = NULL;
        CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply, "\"phi_threshold\":8.00") != NULL);
        g_hooks.phi_threshold = saved;
    }
    /* Probe-RTT histogram (0.48.0): the same mean/max/samples/
     * hist shape as detect_latency_ms, over answered direct
     * probes — here the canned metrics are all zero, so the mean
     * is null and the histogram is empty. */
    CHECK(strstr(reply,
                 "\"probe_rtt_ms\":{\"mean\":null,\"max\":0,"
                 "\"samples\":0,\"hist\":[0,0,0,0,0]}") != NULL);
    /* Time since last contact (0.59.0): each member carries
     * "last_seen_ms" — a number for a peer heard from, null for
     * self and for a peer never heard from. The values are
     * timing-dependent, so check the field exists and the
     * never-heard-from peer's entry carries null. */
    CHECK(strstr(reply, "\"last_seen_ms\":") != NULL);
    {
        const char *never = strstr(reply, "\"id\":\"127.0.0.1:17902\"");
        const char *seen_null =
            never ? strstr(never, "\"last_seen_ms\":null") : NULL;
        const char *next_id =
            never ? strstr(never + 1, "\"id\":") : NULL;
        CHECK(never != NULL && seen_null != NULL &&
              (next_id == NULL || seen_null < next_id));
    }
    /* Worst probe RTT (0.64.0): each member carries "rtt_max_ms"
     * in JSON — a number for a peer that answered a probe, null
     * for self and for a peer never heard from. Check the canned
     * 17901 carries 7 (its worst sample) and the never-heard-from
     * 17902 carries null, each inside its own member object. */
    {
        const char *peer = strstr(reply, "\"id\":\"127.0.0.1:17901\"");
        const char *max7 =
            peer ? strstr(peer, "\"rtt_max_ms\":7") : NULL;
        const char *next_id =
            peer ? strstr(peer + 1, "\"id\":") : NULL;
        CHECK(peer != NULL && max7 != NULL &&
              (next_id == NULL || max7 < next_id));
        const char *never = strstr(reply, "\"id\":\"127.0.0.1:17902\"");
        const char *max_null =
            never ? strstr(never, "\"rtt_max_ms\":null") : NULL;
        next_id = never ? strstr(never + 1, "\"id\":") : NULL;
        CHECK(never != NULL && max_null != NULL &&
              (next_id == NULL || max_null < next_id));
    }
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

    /* PROMETHEUS (0.35.0): the same values METRICS and JSON report,
     * as Prometheus text format — HELP/TYPE lines, counters for the
     * lifetime totals, gauges for the current counts and latency
     * mean/max/samples, a real histogram for the detection latency
     * (cumulative buckets, _sum, _count), one pulse_member_info
     * gauge per member labelled by member/state/source, and the
     * peer-file gauges — then END. The reply is longer than the
     * 4 KB buffer the other tests use, so it gets its own. */
    g_metrics_mode = 1;
    /* Per-seed last rejoin in JSON (0.55.0): the canned
     * timestamps ride as an object keyed by seed, right after
     * the breakdown — each seed carries its own time. */
    CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply,
                 "\"seed_last_rejoin_ms\":{\"127.0.0.1:17101\":"
                 "1700000100000,\"127.0.0.1:17103\":1700000200000}") != NULL);
    /* Per-seed rejoin backoff in JSON (0.56.0): the canned
     * delays ride as an object keyed by seed, right after the
     * last-rejoin timestamps — 17101 is mid-backoff at 10 s,
     * 17103 reset to 0 when it came back. */
    CHECK(strstr(reply,
                 "\"seed_rejoin_backoff_ms\":{\"127.0.0.1:17101\":"
                 "10000,\"127.0.0.1:17103\":0}") != NULL);
    {
        /* 16 KB, not 8 KB: the reply grows a family at a time
         * (0.55.0's pulse_seed_last_rejoin_ms pushed it past 8 KB
         * with the canned table), and a truncated reply drops
         * the peer-file gauges at the end. */
        char preply[16384];
        CHECK(send_cmd(18991, "PROMETHEUS\n", preply, sizeof preply) > 0);
        /* HELP/TYPE pairs and the value lines for the counters. */
        CHECK(strstr(preply, "# HELP pulse_suspected_total ") != NULL);
        CHECK(strstr(preply, "# TYPE pulse_suspected_total counter\n") != NULL);
        CHECK(strstr(preply, "pulse_suspected_total 2\n") != NULL);
        CHECK(strstr(preply, "pulse_dead_total 1\n") != NULL);
        CHECK(strstr(preply, "pulse_refutes_total 1\n") != NULL);
        CHECK(strstr(preply, "pulse_joins_total 3\n") != NULL);
        /* Seed-rejoin count (0.53.0, per-seed 0.54.0): the canned
         * 2 rejoins ride as one labelled sample per seed, right
         * after joins — one each for the two seeds, summing to
         * the total. */
        CHECK(strstr(preply,
                     "pulse_seed_rejoin_total{seed=\"127.0.0.1:17101\"} 1\n") != NULL);
        CHECK(strstr(preply,
                     "pulse_seed_rejoin_total{seed=\"127.0.0.1:17103\"} 1\n") != NULL);
        /* Per-seed last rejoin (0.55.0): the canned timestamps
         * ride as one labelled gauge sample per seed, right after
         * the counter — each seed carries its own time. */
        CHECK(strstr(preply,
                     "pulse_seed_last_rejoin_ms{seed=\"127.0.0.1:17101\"} "
                     "1700000100000\n") != NULL);
        CHECK(strstr(preply,
                     "pulse_seed_last_rejoin_ms{seed=\"127.0.0.1:17103\"} "
                     "1700000200000\n") != NULL);
        /* Per-seed rejoin backoff (0.56.0): the canned delays
         * ride as one labelled gauge sample per seed, right after
         * the last-rejoin gauge — 17101 is mid-backoff at 10 s,
         * 17103 reset to 0 when it came back. */
        CHECK(strstr(preply,
                     "pulse_seed_rejoin_backoff_ms{seed=\"127.0.0.1:17101\"} "
                     "10000\n") != NULL);
        CHECK(strstr(preply,
                     "pulse_seed_rejoin_backoff_ms{seed=\"127.0.0.1:17103\"} "
                     "0\n") != NULL);
        CHECK(strstr(preply, "pulse_adaptive_fast_cycles_total 5\n") != NULL);
        CHECK(strstr(preply, "pulse_suspect_refuted_total 1\n") != NULL);
        /* Membership gauges from the canned table: 3 alive, 1 dead. */
        CHECK(strstr(preply, "# TYPE pulse_members_alive gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_members_alive 3\n") != NULL);
        CHECK(strstr(preply, "pulse_members_suspect 0\n") != NULL);
        CHECK(strstr(preply, "pulse_members_dead 1\n") != NULL);
        CHECK(strstr(preply, "pulse_members_total 4\n") != NULL);
        CHECK(strstr(preply, "pulse_slow_peers 0\n") != NULL);
        /* Uptime is timing-dependent: a number, nothing more.
         * The leading newline keeps strstr off the HELP line,
         * which names the metric too. */
        {
            const char *u = strstr(preply, "\npulse_uptime_seconds ");
            CHECK(u != NULL);
            if (u) {
                u += strlen("\npulse_uptime_seconds ");
                CHECK(*u >= '0' && *u <= '9');
            }
        }
        /* Latency gauges: mean/max/samples for all three groups. */
        CHECK(strstr(preply, "pulse_detect_latency_ms_mean 225\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_max 300\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_samples 2\n") != NULL);
        CHECK(strstr(preply, "pulse_suspect_to_dead_ms_mean 301\n") != NULL);
        CHECK(strstr(preply, "pulse_suspect_to_dead_ms_max 301\n") != NULL);
        CHECK(strstr(preply, "pulse_suspect_to_dead_ms_samples 1\n") != NULL);
        CHECK(strstr(preply, "pulse_refute_latency_ms_mean 120\n") != NULL);
        CHECK(strstr(preply, "pulse_refute_latency_ms_max 120\n") != NULL);
        CHECK(strstr(preply, "pulse_refute_latency_ms_samples 1\n") != NULL);
        /* Detection-latency histogram: cumulative buckets, _sum,
         * _count — one sample in 100-249, one in 250-499. */
        CHECK(strstr(preply, "# TYPE pulse_detect_latency_ms histogram\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_bucket{le=\"100\"} 0\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_bucket{le=\"250\"} 1\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_bucket{le=\"500\"} 2\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_bucket{le=\"1000\"} 2\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_bucket{le=\"+Inf\"} 2\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_sum 450\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_count 2\n") != NULL);
        /* Probe-RTT histogram (0.48.0): the same shape as the
         * detection histogram — mean/max/samples gauges plus a
         * real histogram with cumulative buckets — over the five
         * canned samples 0, 2, 7, 25, 80 ms: mean 22, max 80,
         * one per bucket, so every cumulative bucket steps by 1. */
        CHECK(strstr(preply, "# TYPE pulse_probe_rtt_ms_mean gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_mean 22\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_max 80\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_samples 5\n") != NULL);
        CHECK(strstr(preply, "# TYPE pulse_probe_rtt_ms histogram\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_bucket{le=\"1\"} 1\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_bucket{le=\"5\"} 2\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_bucket{le=\"10\"} 3\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_bucket{le=\"50\"} 4\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_bucket{le=\"+Inf\"} 5\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_sum 114\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_count 5\n") != NULL);
        /* Watch counts: no watcher has connected yet, all zero. */
        CHECK(strstr(preply, "pulse_watch_subscribers 0\n") != NULL);
        CHECK(strstr(preply, "pulse_watch_sessions_total 0\n") != NULL);
        CHECK(strstr(preply, "pulse_watch_delivered_total 0\n") != NULL);
        CHECK(strstr(preply, "pulse_watch_filtered_total 0\n") != NULL);
        CHECK(strstr(preply, "pulse_watch_dropped_total 0\n") != NULL);
        /* Gossip digest size (0.41.0): no hook is set in this
         * harness, so it reports the default 16. */
        CHECK(strstr(preply, "# TYPE pulse_gossip_digest_size gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_gossip_digest_size 16\n") != NULL);
        /* Phi suspicion threshold (0.43.0): the fake hook reports
         * a non-default 5.5, so the gauge proves it comes from
         * the hook — and with no hook it falls back to the
         * default 8.0. */
        CHECK(strstr(preply, "# TYPE pulse_phi_threshold gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_phi_threshold 5.50\n") != NULL);
        {
            double (*saved)(void *) = g_hooks.phi_threshold;
            g_hooks.phi_threshold = NULL;
            char preply[8192];
            CHECK(send_cmd(18991, "PROMETHEUS\n", preply, sizeof preply) > 0);
            CHECK(strstr(preply, "pulse_phi_threshold 8.00\n") != NULL);
            g_hooks.phi_threshold = saved;
        }
        /* One pulse_member_info gauge per member, with state and
         * source labels. */
        CHECK(strstr(preply, "pulse_member_info{member=\"127.0.0.1:17999\",state=\"ALIVE\",source=\"self\"} 1\n") != NULL);
        CHECK(strstr(preply, "pulse_member_info{member=\"127.0.0.1:17901\",state=\"ALIVE\",source=\"seed\"} 1\n") != NULL);
        CHECK(strstr(preply, "pulse_member_info{member=\"127.0.0.1:17903\",state=\"ALIVE\",source=\"peer-file\"} 1\n") != NULL);
        CHECK(strstr(preply, "pulse_member_info{member=\"127.0.0.1:17902\",state=\"DEAD\",source=\"discovered\"} 1\n") != NULL);
        /* Probe RTT per member (0.40.0): last and mean RTT as
         * gauges, one line per member that answered a probe —
         * the seed peer at 3 / mean 2, the peer-file peer at a
         * real sub-millisecond 0 / 0 — while self, which never
         * probes itself, and the never-heard-from dead peer have
         * no rtt gauge at all. */
        CHECK(strstr(preply, "# TYPE pulse_member_rtt_ms gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_ms{member=\"127.0.0.1:17901\"} 3\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_ms{member=\"127.0.0.1:17903\"} 0\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_ms{member=\"127.0.0.1:17999\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_rtt_ms{member=\"127.0.0.1:17902\"}") == NULL);
        CHECK(strstr(preply, "# TYPE pulse_member_rtt_mean_ms gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_mean_ms{member=\"127.0.0.1:17901\"} 2\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_mean_ms{member=\"127.0.0.1:17903\"} 0\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_mean_ms{member=\"127.0.0.1:17999\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_rtt_mean_ms{member=\"127.0.0.1:17902\"}") == NULL);
        /* Worst probe RTT (0.64.0): the same rule as the last and
         * mean gauges — 7 for 17901 (its worst sample), 0 for
         * 17903 (a real worst case, not a missing one), and no
         * line at all for self or the never-heard-from peer. */
        CHECK(strstr(preply, "# TYPE pulse_member_rtt_max_ms gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_max_ms{member=\"127.0.0.1:17901\"} 7\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_max_ms{member=\"127.0.0.1:17903\"} 0\n") != NULL);
        CHECK(strstr(preply, "pulse_member_rtt_max_ms{member=\"127.0.0.1:17999\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_rtt_max_ms{member=\"127.0.0.1:17902\"}") == NULL);
        /* Time since last contact per member (0.59.0): one
         * pulse_member_last_seen_ms gauge sample per member heard
         * from — the seed peer and the peer-file peer, both heard
         * from just now — while self and the never-heard-from
         * dead peer have no sample at all. The values are
         * timing-dependent, so check the HELP/TYPE lines and the
         * sample prefixes, not the numbers. */
        CHECK(strstr(preply, "# HELP pulse_member_last_seen_ms ") != NULL);
        CHECK(strstr(preply, "# TYPE pulse_member_last_seen_ms gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_last_seen_ms{member=\"127.0.0.1:17901\"} ") != NULL);
        CHECK(strstr(preply, "pulse_member_last_seen_ms{member=\"127.0.0.1:17903\"} ") != NULL);
        CHECK(strstr(preply, "pulse_member_last_seen_ms{member=\"127.0.0.1:17999\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_last_seen_ms{member=\"127.0.0.1:17902\"}") == NULL);
        /* Phi per member (0.42.0): phi mode is off in this
         * harness, so no pulse_member_phi line at all — the
         * canned phi history on the seed peer produces no gauge
         * without --phi-accrual. */
        CHECK(strstr(preply, "pulse_member_phi{member=") == NULL);
        /* Phi history gauges (0.51.0) need phi mode too: the
         * canned samples produce no gauge without --phi-accrual
         * either — the HELP/TYPE lines are still there, so the
         * check is on the member lines, not the family name. */
        CHECK(strstr(preply, "pulse_member_phi_history_latest{member=") == NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_max{member=") == NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_samples{member=") == NULL);
        /* Peer-file gauges from the canned load. */
        CHECK(strstr(preply, "pulse_peer_file_peers 3\n") != NULL);
        CHECK(strstr(preply, "pulse_peer_file_added 1\n") != NULL);
        CHECK(strstr(preply, "pulse_peer_file_dropped 0\n") != NULL);
        CHECK(strstr(preply, "pulse_peer_file_kept 1\n") != NULL);
        CHECK(strstr(preply, "pulse_peer_file_bad 0\n") != NULL);
        CHECK(strstr(preply, "pulse_peer_file_last_reload_seconds 1700000000\n") != NULL);
        CHECK(strstr(preply, "pulse_peer_file_last_mtime_seconds 1699999000\n") != NULL);
        /* The reply ends with END, like every other one-shot. */
        size_t plen = strlen(preply);
        CHECK(plen >= 4 && strcmp(preply + plen - 4, "END\n") == 0);
    }
    /* Phi per member (0.42.0), phi mode on: the seed peer's canned
     * history produces a phi gauge at the 30.00 cap, while self
     * (no phi history), the peer-file peer (no phi history) and
     * the never-heard-from dead peer have no phi gauge at all. */
    g_phi_mode = 1;
    {
        /* 16 KB, not 8 KB: the phi-mode reply carries the phi and
         * phi-history families on top of the rest, and 0.62.0's
         * pulse_quiet_peers family pushed it past 8 KB — a
         * truncated reply drops the phi-history samples gauges
         * at the end. */
        char preply[16384];
        CHECK(send_cmd(18991, "PROMETHEUS\n", preply, sizeof preply) > 0);
        CHECK(strstr(preply, "# TYPE pulse_member_phi gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_phi{member=\"127.0.0.1:17901\"} 30.00\n") != NULL);
        CHECK(strstr(preply, "pulse_member_phi{member=\"127.0.0.1:17999\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_phi{member=\"127.0.0.1:17903\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_phi{member=\"127.0.0.1:17902\"}") == NULL);
        /* Phi history in PROMETHEUS (0.51.0): the seed peer's
         * three canned samples ride as latest / max / samples
         * gauges — latest 0.80, max 0.80, 3 samples — while self
         * (no phi history), the peer-file peer (no phi history)
         * and the never-heard-from dead peer have no history
         * gauge at all. */
        CHECK(strstr(preply, "# TYPE pulse_member_phi_history_latest gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_latest{member=\"127.0.0.1:17901\"} 0.80\n") != NULL);
        CHECK(strstr(preply, "# TYPE pulse_member_phi_history_max gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_max{member=\"127.0.0.1:17901\"} 0.80\n") != NULL);
        CHECK(strstr(preply, "# TYPE pulse_member_phi_history_samples gauge\n") != NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_samples{member=\"127.0.0.1:17901\"} 3\n") != NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_latest{member=\"127.0.0.1:17999\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_latest{member=\"127.0.0.1:17903\"}") == NULL);
        CHECK(strstr(preply, "pulse_member_phi_history_latest{member=\"127.0.0.1:17902\"}") == NULL);
    }
    g_phi_mode = 0;
    /* Per-member phi in METRICS (0.44.0): phi mode on, the seed
     * peer's canned history produces a PHI line at the 30.00 cap
     * after the METRICS line and before END, while self (no phi
     * history), the peer-file peer (no phi history) and the
     * never-heard-from dead peer have no PHI line at all — and
     * with phi mode off there is no PHI line for anyone. */
    g_phi_mode = 1;
    CHECK(send_cmd(18991, "METRICS\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply, "\nPHI 127.0.0.1:17901 30.00\n") != NULL);
    CHECK(strstr(reply, "PHI 127.0.0.1:17999") == NULL);
    CHECK(strstr(reply, "PHI 127.0.0.1:17903") == NULL);
    CHECK(strstr(reply, "PHI 127.0.0.1:17902") == NULL);
    {
        const char *metrics = strstr(reply, "METRICS ");
        const char *phi = strstr(reply, "\nPHI 127.0.0.1:17901 30.00\n");
        const char *end = strstr(reply, "\nEND\n");
        CHECK(metrics != NULL && phi != NULL && end != NULL &&
              phi > metrics && end > phi);
    }
    g_phi_mode = 0;
    CHECK(send_cmd(18991, "METRICS\n", reply, sizeof reply) > 0);
    CHECK(strstr(reply, "\nPHI ") == NULL);
    /* No peer file: the peer-file gauges are absent entirely, not
     * zero — an absent metric is how Prometheus says "not
     * applicable" — while the rest of the exposition is unchanged. */
    g_peerfile_mode = 1;
    {
        char preply[8192];
        CHECK(send_cmd(18991, "PROMETHEUS\n", preply, sizeof preply) > 0);
        CHECK(strstr(preply, "pulse_peer_file_") == NULL);
        CHECK(strstr(preply, "pulse_suspected_total 2\n") != NULL);
        CHECK(strstr(preply, "pulse_members_total 4\n") != NULL);
    }
    g_peerfile_mode = 0;
    /* With no samples at all the latency means and maxima are 0
     * and the histogram is all zeros with _sum 0 and _count 0. */
    g_metrics_mode = 0;
    {
        char preply[8192];
        CHECK(send_cmd(18991, "PROMETHEUS\n", preply, sizeof preply) > 0);
        CHECK(strstr(preply, "pulse_detect_latency_ms_mean 0\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_max 0\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_samples 0\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_bucket{le=\"+Inf\"} 0\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_sum 0\n") != NULL);
        CHECK(strstr(preply, "pulse_detect_latency_ms_count 0\n") != NULL);
        /* Probe-RTT histogram (0.48.0): no probe answered yet, so
         * the gauges are 0 and the histogram is all zeros. */
        CHECK(strstr(preply, "pulse_probe_rtt_ms_mean 0\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_max 0\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_samples 0\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_bucket{le=\"+Inf\"} 0\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_sum 0\n") != NULL);
        CHECK(strstr(preply, "pulse_probe_rtt_ms_count 0\n") != NULL);
        CHECK(strstr(preply, "pulse_suspected_total 0\n") != NULL);
    }

    /* WATCH (0.29.0): the stream starts with OK, a SNAPSHOT of the
     * canned table in the PEERS shape, and END — then live events
     * recorded after connecting arrive as EVENT lines, in order,
     * with 1-based sequence numbers. While the WATCH is open, a
     * one-shot command on another connection still answers: the
     * stream does not starve the control plane. */
    pulse_event_log_init(&g_watch_log, PULSE_EVENT_LOG_CAP);
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
                     "\"watch\":{\"subscribers\":1,\"sessions\":2,"
                     "\"delivered\":2,\"filtered\":0,\"dropped\":0,"
                     "\"paused\":0,\"event_log_cap\":128,"
                     "\"streams\":[{\"session\":2,\"delivered\":0,"
                     "\"filtered\":0,\"dropped\":0,"
                     "\"paused\":false}]}") != NULL);
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
                     "\"watch\":{\"subscribers\":0,\"sessions\":3,"
                     "\"delivered\":2,\"filtered\":0,\"dropped\":0,"
                     "\"paused\":0,\"event_log_cap\":128,"
                     "\"streams\":[]}") != NULL);
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

    /* WATCH delivery / drop counts (0.34.0): each stream counts
     * the EVENT lines it sent (delivered), the events it consumed
     * but skipped because its type/id filter excluded them
     * (filtered), and the events it lost because they had already
     * scrolled out of the bounded event ring before the stream
     * could consume them (dropped). METRICS reports the three as
     * lifetime totals; JSON carries the same three totals in the
     * "watch" object plus a "streams" array with the same three
     * counts per currently-connected stream, keyed by its session
     * number. The log holds seqs 1-141 here and sessions 1-10 are
     * done, so the streams below are sessions 11-13. A plain
     * WATCH that receives two live events reports delivered=2; a
     * WATCH DEAD that sees two non-DEAD events reports filtered=2
     * and delivered=0; and a WATCH FROM 2 against a ring whose
     * oldest held seq is 18 reports dropped=16 and replays the
     * 128 buffered events as delivered=128. The lifetime totals in
     * METRICS are not asserted exactly: a just-closed stream's
     * thread lingers up to a second before it notices the close,
     * and if events are recorded in that window the lingering
     * thread consumes them — delivering some to a dead socket,
     * filtering some, or hitting a gap when the ring scrolls past
     * it — so the totals depend on thread-exit timing. The
     * per-stream counts are exact, and the totals path itself
     * (all three zero before any WATCH connects) is covered by
     * the exact METRICS line above. */
    CHECK(metrics_wait_contains(
        18991, "watch_subscribers=0 watch_sessions=10", 5000));
    int cfd = watch_connect(18991);
    CHECK(cfd >= 0);
    if (cfd >= 0) {
        const char *ccmd = "WATCH\n";
        CHECK(send(cfd, ccmd, strlen(ccmd), 0) == (ssize_t)strlen(ccmd));
        char cbuf[4096];
        size_t clen = 0;
        cbuf[0] = '\0';
        CHECK(watch_read_until(cfd, cbuf, sizeof cbuf, &clen, "END\n",
                               3000));
        CHECK(strncmp(cbuf, "OK watching\n", 12) == 0);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=1 watch_sessions=11", 5000));
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17901", 1, 1700000400);
        pulse_event_record(&g_watch_log, PULSE_EVENT_SUSPECTED,
                           "127.0.0.1:17903", 2, 1700000401);
        CHECK(watch_read_until(cfd, cbuf, sizeof cbuf, &clen,
                               "EVENT 142 JOINED 127.0.0.1:17901 1 1700000400\n",
                               3000));
        CHECK(watch_read_until(cfd, cbuf, sizeof cbuf, &clen,
                               "EVENT 143 SUSPECTED 127.0.0.1:17903 2 1700000401\n",
                               3000));
        /* The per-stream counts ride in JSON: session 11
         * delivered both events, filtered and dropped nothing. */
        CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply,
                     "\"session\":11,\"delivered\":2,\"filtered\":0,"
                     "\"dropped\":0") != NULL);
        close(cfd);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=0 watch_sessions=11", 5000));
    }
    int dfd = watch_connect(18991);
    CHECK(dfd >= 0);
    if (dfd >= 0) {
        const char *dcmd = "WATCH DEAD\n";
        CHECK(send(dfd, dcmd, strlen(dcmd), 0) == (ssize_t)strlen(dcmd));
        char dbuf[4096];
        size_t dlen = 0;
        dbuf[0] = '\0';
        CHECK(watch_read_until(dfd, dbuf, sizeof dbuf, &dlen, "END\n",
                               3000));
        CHECK(strncmp(dbuf, "OK watching\n", 12) == 0);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=1 watch_sessions=12", 5000));
        /* Two non-DEAD events: both are consumed and skipped by
         * the filter, so filtered grows by two and delivered
         * stays at zero. The poll waits for the stream thread to
         * have consumed them — a single JSON read right after
         * recording would race it. */
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17901", 1, 1700000402);
        pulse_event_record(&g_watch_log, PULSE_EVENT_SUSPECTED,
                           "127.0.0.1:17903", 2, 1700000403);
        /* The poll waits for the stream thread to have consumed
         * both events — a single JSON read right after recording
         * would race it. */
        CHECK(json_wait_contains(
            18991,
            "\"session\":12,\"delivered\":0,\"filtered\":2,\"dropped\":0",
            5000));
        CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply,
                     "\"session\":12,\"delivered\":0,\"filtered\":2,"
                     "\"dropped\":0") != NULL);
        CHECK(strstr(dbuf, "EVENT 144 ") == NULL);
        CHECK(strstr(dbuf, "EVENT 145 ") == NULL);
        close(dfd);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=0 watch_sessions=12", 5000));
    }
    /* A WATCH FROM 2 against a ring whose oldest held seq is 18
     * (seqs 1-145 recorded, the ring holds the last 128): the
     * stream signals GAP 2 18, counts the 16 lost events as
     * dropped, and replays the 128 buffered events as delivered.
     * The buffer is 16 KB: 128 EVENT lines plus the snapshot run
     * to about 7 KB. */
    int qfd = watch_connect(18991);
    CHECK(qfd >= 0);
    if (qfd >= 0) {
        const char *qcmd = "WATCH FROM 2\n";
        CHECK(send(qfd, qcmd, strlen(qcmd), 0) == (ssize_t)strlen(qcmd));
        char qbuf[16384];
        size_t qlen = 0;
        qbuf[0] = '\0';
        CHECK(watch_read_until(qfd, qbuf, sizeof qbuf, &qlen, "END\n",
                               3000));
        CHECK(strncmp(qbuf, "OK watching\n", 12) == 0);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=1 watch_sessions=13", 5000));
        CHECK(watch_read_until(qfd, qbuf, sizeof qbuf, &qlen,
                               "GAP 2 18\n", 3000));
        CHECK(watch_read_until(qfd, qbuf, sizeof qbuf, &qlen,
                               "EVENT 145 SUSPECTED 127.0.0.1:17903 2 1700000403\n",
                               3000));
        CHECK(strstr(qbuf, "EVENT 17 ") == NULL);
        CHECK(send_cmd(18991, "JSON\n", reply, sizeof reply) > 0);
        CHECK(strstr(reply,
                     "\"session\":13,\"delivered\":128,\"filtered\":0,"
                     "\"dropped\":16") != NULL);
        close(qfd);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=0 watch_sessions=13", 5000));
    }

    /* WATCH pause / resume (0.37.0): a stream paused with
     * "WATCH PAUSE <session>" stops consuming events — one
     * recorded while it is paused never reaches it — and METRICS
     * / JSON report it paused (watch_paused=1, and the stream's
     * "paused":true); "WATCH RESUME <session>" resumes it and
     * the held event arrives, replayed from the ring, in order.
     * The log holds seqs 1-145 here and sessions 1-13 are done,
     * so this stream is session 14 and the recorded event is seq
     * 146. Pausing an already-paused stream (and resuming a
     * running one) answers OK again: the state is observable in
     * METRICS / JSON, so the reply does not need to say whether
     * anything changed. */
    int wpfd = watch_connect(18991);
    CHECK(wpfd >= 0);
    if (wpfd >= 0) {
        const char *wpcmd = "WATCH\n";
        CHECK(send(wpfd, wpcmd, strlen(wpcmd), 0) == (ssize_t)strlen(wpcmd));
        char wpbuf[4096];
        size_t wplen = 0;
        wpbuf[0] = '\0';
        CHECK(watch_read_until(wpfd, wpbuf, sizeof wpbuf, &wplen, "END\n",
                               3000));
        CHECK(strncmp(wpbuf, "OK watching\n", 12) == 0);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=1 watch_sessions=14", 5000));
        /* Pause it: the one-shot command answers OK, and the
         * paused state shows in METRICS and in the per-stream
         * JSON entry. */
        CHECK(send_cmd(18991, "WATCH PAUSE 14\n", reply,
                       sizeof reply) > 0);
        CHECK(strcmp(reply, "OK paused 14\n") == 0);
        CHECK(metrics_wait_contains(18991, "watch_paused=1", 5000));
        CHECK(json_wait_contains(
            18991,
            "\"session\":14,\"delivered\":0,\"filtered\":0,"
            "\"dropped\":0,\"paused\":true",
            5000));
        /* Pausing again is idempotent: OK again, still paused. */
        CHECK(send_cmd(18991, "WATCH PAUSE 14\n", reply,
                       sizeof reply) > 0);
        CHECK(strcmp(reply, "OK paused 14\n") == 0);
        /* An event recorded while paused never reaches the
         * stream: a 1.5 s read for it times out, and the buffer
         * holds no EVENT line past the snapshot. The read
         * timeout is set by watch_read_until itself, and the
         * stream is far short of its 5 s heartbeat, so nothing
         * else arrives in the window either. */
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17901", 1, 1700000500);
        CHECK(watch_read_until(wpfd, wpbuf, sizeof wpbuf, &wplen,
                               "EVENT 146 ", 1500) == 0);
        CHECK(strstr(wpbuf, "EVENT 146 ") == NULL);
        /* Resume it: the held event arrives, replayed from the
         * ring, and the stream is unpaused. */
        CHECK(send_cmd(18991, "WATCH RESUME 14\n", reply,
                       sizeof reply) > 0);
        CHECK(strcmp(reply, "OK resumed 14\n") == 0);
        CHECK(watch_read_until(wpfd, wpbuf, sizeof wpbuf, &wplen,
                               "EVENT 146 JOINED 127.0.0.1:17901 1 1700000500\n",
                               3000));
        CHECK(metrics_wait_contains(18991, "watch_paused=0", 5000));
        CHECK(json_wait_contains(
            18991,
            "\"session\":14,\"delivered\":1,\"filtered\":0,"
            "\"dropped\":0,\"paused\":false",
            5000));
        /* Resuming a running stream is idempotent too. */
        CHECK(send_cmd(18991, "WATCH RESUME 14\n", reply,
                       sizeof reply) > 0);
        CHECK(strcmp(reply, "OK resumed 14\n") == 0);
        close(wpfd);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=0 watch_sessions=14", 5000));
    }

    /* Bad pause / resume commands are refused up front: a
     * session that is not a live stream, a session that is not
     * a number, and a missing session. */
    CHECK(send_cmd(18991, "WATCH PAUSE 99\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR unknown watch session 99\n") == 0);
    CHECK(send_cmd(18991, "WATCH RESUME 99\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR unknown watch session 99\n") == 0);
    CHECK(send_cmd(18991, "WATCH PAUSE xyz\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR invalid watch session xyz\n") == 0);
    CHECK(send_cmd(18991, "WATCH PAUSE\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR usage: WATCH PAUSE <session>\n") == 0);
    CHECK(send_cmd(18991, "WATCH RESUME\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply, "ERR usage: WATCH RESUME <session>\n") == 0);

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

    /* WATCH state filter (0.39.0): "WATCH STATE <list>" narrows
     * the stream to events that leave their member in one of the
     * listed states — JOINED, REVIVED and REFUTED land on ALIVE,
     * SUSPECTED on SUSPECT, DEAD on DEAD — and narrows the
     * snapshot by the member's current state too. DROPPED lands
     * on no state, so it never matches a state filter. The log
     * holds seqs 1-146 here. Sessions 1-14 are streams, and the
     * five refused WATCHes just above (two bad filters, three bad
     * FROMs) each counted as a session too — start_watch counts
     * the session before the thread parses the filter — so this
     * stream is session 20 and the events below are 147-151. */
    {
        member_state_t st;
        CHECK(pulse_event_result_state(PULSE_EVENT_JOINED, &st) == 1
              && st == MEMBER_ALIVE);
        CHECK(pulse_event_result_state(PULSE_EVENT_SUSPECTED, &st) == 1
              && st == MEMBER_SUSPECT);
        CHECK(pulse_event_result_state(PULSE_EVENT_DEAD, &st) == 1
              && st == MEMBER_DEAD);
        CHECK(pulse_event_result_state(PULSE_EVENT_REVIVED, &st) == 1
              && st == MEMBER_ALIVE);
        CHECK(pulse_event_result_state(PULSE_EVENT_REFUTED, &st) == 1
              && st == MEMBER_ALIVE);
        CHECK(pulse_event_result_state(PULSE_EVENT_DROPPED, &st) == 0);
    }
    int vfd = watch_connect(18991);
    CHECK(vfd >= 0);
    if (vfd >= 0) {
        const char *vcmd = "WATCH STATE SUSPECT,DEAD\n";
        CHECK(send(vfd, vcmd, strlen(vcmd), 0) == (ssize_t)strlen(vcmd));
        char vbuf[4096];
        size_t vlen = 0;
        vbuf[0] = '\0';
        CHECK(watch_read_until(vfd, vbuf, sizeof vbuf, &vlen, "END\n",
                               3000));
        CHECK(strncmp(vbuf, "OK watching\n", 12) == 0);
        /* The snapshot holds only the canned DEAD member: the
         * other three are ALIVE, and the state filter narrows
         * the snapshot by current state. The dead member was
         * never heard from, so its last-seen is '-'. */
        CHECK(strstr(vbuf, "SNAPSHOT 127.0.0.1:17902 1 DEAD - discovered\n") != NULL);
        CHECK(strstr(vbuf, "SNAPSHOT 127.0.0.1:17901") == NULL);
        CHECK(strstr(vbuf, "SNAPSHOT 127.0.0.1:17903") == NULL);
        CHECK(strstr(vbuf, "SNAPSHOT 127.0.0.1:17999") == NULL);
        /* Only SUSPECTED and DEAD arrive: JOINED lands on ALIVE,
         * DROPPED lands on no state at all, and REVIVED lands on
         * ALIVE — all three are filtered out, but still consume
         * their sequence numbers, so the numbers jump. */
        pulse_event_record(&g_watch_log, PULSE_EVENT_SUSPECTED,
                           "127.0.0.1:17903", 2, 1700000600);
        pulse_event_record(&g_watch_log, PULSE_EVENT_JOINED,
                           "127.0.0.1:17901", 1, 1700000601);
        pulse_event_record(&g_watch_log, PULSE_EVENT_DEAD,
                           "127.0.0.1:17903", 2, 1700000602);
        pulse_event_record(&g_watch_log, PULSE_EVENT_DROPPED,
                           "127.0.0.1:17901", 1, 1700000603);
        pulse_event_record(&g_watch_log, PULSE_EVENT_REVIVED,
                           "127.0.0.1:17903", 2, 1700000604);
        CHECK(watch_read_until(vfd, vbuf, sizeof vbuf, &vlen,
                               "EVENT 147 SUSPECTED 127.0.0.1:17903 2 1700000600\n",
                               3000));
        CHECK(watch_read_until(vfd, vbuf, sizeof vbuf, &vlen,
                               "EVENT 149 DEAD 127.0.0.1:17903 2 1700000602\n",
                               3000));
        CHECK(watch_read_until(vfd, vbuf, sizeof vbuf, &vlen,
                               "EVENT 999", 1500) == 0);
        CHECK(strstr(vbuf, "EVENT 148 JOINED") == NULL);
        CHECK(strstr(vbuf, "EVENT 150 DROPPED") == NULL);
        CHECK(strstr(vbuf, "EVENT 151 REVIVED") == NULL);
        close(vfd);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=0 watch_sessions=20", 5000));
    }
    /* A STATE ALIVE stream snapshots the three ALIVE members of
     * the canned table — and not the dead one — and a bad STATE
     * is refused up front: an unknown state, a missing list, and
     * a duplicate STATE, exactly like a duplicate FROM. */
    int afd = watch_connect(18991);
    CHECK(afd >= 0);
    if (afd >= 0) {
        const char *acmd = "WATCH STATE ALIVE\n";
        CHECK(send(afd, acmd, strlen(acmd), 0) == (ssize_t)strlen(acmd));
        char abuf[4096];
        size_t alen = 0;
        abuf[0] = '\0';
        CHECK(watch_read_until(afd, abuf, sizeof abuf, &alen, "END\n",
                               3000));
        CHECK(strncmp(abuf, "OK watching\n", 12) == 0);
        CHECK(strstr(abuf, "SNAPSHOT 127.0.0.1:17999 1 ALIVE - self\n") != NULL);
        CHECK(strstr(abuf, "SNAPSHOT 127.0.0.1:17901 1 ALIVE ") != NULL);
        CHECK(strstr(abuf, "SNAPSHOT 127.0.0.1:17903 2 ALIVE ") != NULL);
        CHECK(strstr(abuf, "SNAPSHOT 127.0.0.1:17902") == NULL);
        close(afd);
        CHECK(metrics_wait_contains(
            18991, "watch_subscribers=0 watch_sessions=21", 5000));
    }
    CHECK(send_cmd(18991, "WATCH STATE BOGUS\n", reply,
                   sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter unknown member state BOGUS\n")
          == 0);
    CHECK(send_cmd(18991, "WATCH STATE\n", reply, sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter STATE requires a state "
                 "list\n") == 0);
    CHECK(send_cmd(18991, "WATCH STATE ALIVE STATE DEAD\n", reply,
                   sizeof reply) > 0);
    CHECK(strcmp(reply,
                 "ERR invalid watch filter duplicate STATE\n") == 0);

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
