/* pulse-node: a SWIM-style gossip failure detector daemon.
 *
 * One node per process. Nodes gossip membership over UDP, probe each other
 * directly, fall back to indirect probes (PING_REQ) when a direct probe
 * goes unanswered, and mark a peer SUSPECT, then DEAD, when it stops
 * responding. A node that hears gossip calling it SUSPECT or DEAD refutes
 * it by bumping its incarnation number.
 *
 * Defaults are tuned for sub-second failure detection on a LAN:
 * probe every 100 ms, give up on a direct probe after 100 ms, suspect
 * after a failed indirect probe, and declare dead 300 ms after suspicion.
 */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "adaptive.h"
#include "control.h"
#include "events.h"
#include "gossip.h"
#include "member.h"
#include "metrics.h"
#include "persist.h"
#include "peerfile.h"
#include "phi.h"
#include "secret.h"
#include "control.h"

#define MAX_PENDING 32
#define PING_REQ_FANOUT 2
/* In phi-accrual mode, a peer with no heartbeat history at all is
 * suspected after this many probe intervals of silence. A seed that
 * never answers cannot produce a phi, so without this guard it would
 * sit ALIVE forever. */
#define PHI_NO_HISTORY_INTERVALS 20

typedef struct {
    int port;
    char id[PULSE_ID_LEN];
    int has_id;
    char data_dir[512];
    char seeds[16][PULSE_ID_LEN];
    int seed_count;
    int seed_flag_count; /* seeds[0..seed_flag_count) came from --seed
                            flags; the rest came from --peer-file, so a
                            reload can rebuild the list as flags plus
                            the file's current entries */
    char peer_file[512];   /* --peer-file path, "" = none: initial peers
                              read from a file instead of --seed flags
                              (see peerfile.h) */
    char peer_file_seeds[16][PULSE_ID_LEN]; /* peers currently listed
                            in --peer-file (excluding ones also named
                            with --seed): the reload diffs against this */
    int peer_file_seed_count;
    char kept_peers[16][PULSE_ID_LEN]; /* peers removed from --peer-file
                            but kept because they were ALIVE when the
                            reload ran: a later reload drops them once
                            they stop answering, instead of forgetting
                            them the moment the file edit lands */
    int kept_peer_count;
    int probe_interval_ms;
    int probe_interval_fast_ms; /* fast probe interval while any peer
                                   is SUSPECT, 0 = half the base
                                   interval (see adaptive.h) */
    double probe_jitter_frac;  /* ±fraction of jitter on the probe
                                  interval each cycle (0.50.0): each
                                  cycle sleeps base * (1 + u) with u
                                  uniform in [-frac, +frac], so nodes
                                  that start together drift out of
                                  lockstep; 0.1 by default, 0 = none */
    int ack_timeout_ms;
    int suspect_timeout_ms;
    int64_t dead_retention_ms; /* how long a DEAD entry stays in the table */
    int phi_accrual;           /* suspect on phi instead of fixed timeouts */
    double phi_threshold;      /* phi at which a peer is suspected */
    int control_port;          /* TCP control plane port, -1 = disabled */
    int sync_interval_ms;      /* how often to exchange full tables
                                  (anti-entropy SYNC) with a random peer */
    int64_t slow_peer_rtt_ms;  /* slow-peer warning threshold: warn when
                                  a peer's mean probe RTT is above this
                                  many ms, 0 = disabled (see member.h) */
    int64_t quiet_peer_ms;     /* quiet-peer warning threshold: warn
                                  when an ALIVE peer's last-contact age
                                  is above this many ms, 0 = disabled
                                  (see member.h) */
    double low_probe_rate_frac; /* low-probe-rate warning threshold:
                                  warn when an ALIVE peer's recent
                                  probe success rate falls below this
                                  fraction, 0 = disabled (see
                                  member.h) */
    int64_t flap_rate_threshold; /* flap-rate warning threshold:
                                  warn when an ALIVE peer has
                                  completed at least this many flap
                                  cycles in the last 10 minutes, 0
                                  = disabled (see member.h) */
    int event_log_cap;         /* WATCH event ring capacity: how many
                                  membership events the log holds for
                                  replay (0.36.0), 0 = default 128 */
    int gossip_digest_size;    /* how many members one gossip round's
                                  digest covers (0.41.0), 0 = default
                                  PULSE_GOSSIP_ENTRIES (16) */
    char secret[PULSE_SECRET_MAX]; /* shared secret for joins, "" = none */
    int ipv6;                  /* use IPv6 (bind ::, ids are "[::1]:port") */
    char bind_host[128];       /* bind address override, "" = wildcard */
    /* This node's own annotations (see member.h): --meta flags, or the
     * ones saved from the last run when no flags are given. */
    meta_kv_t meta[PULSE_MAX_META];
    int meta_count;
} config_t;

typedef struct {
    uint32_t seq;
    char target_id[PULSE_ID_LEN];
    struct sockaddr_storage target_addr;
    int64_t sent_ms;
    int indirect_tried;
    /* rtt_only (0.49.0): in phi-accrual mode the pending entry exists
     * only to match the ACK and measure the probe RTT — suspicion
     * comes from phi, not from a probe timeout, so the entry is never
     * escalated to an indirect probe and never suspects on age. The
     * fixed-timeout path leaves this 0. */
    int rtt_only;
    int active;
} pending_probe_t;

typedef struct {
    uint32_t seq;
    char target_id[PULSE_ID_LEN];
    struct sockaddr_storage requester;
    int64_t sent_ms;
    int active;
} pending_indirect_t;

static config_t g_cfg;
static member_table_t g_table;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_log_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_self_id[PULSE_ID_LEN];
static uint32_t g_incarnation = 1;
static int g_sock = -1;      /* bound receiving socket, never connected */
static int g_send_sock = -1; /* dedicated sending socket (see main) */
static int g_control_sock = -1; /* TCP control plane listen socket */
static int g_family = AF_INET; /* address family of this node */
static control_hooks_t g_hooks;
static pulse_metrics_t g_metrics;
static pulse_event_log_t g_event_log; /* membership events for WATCH
    (0.29.0): every transition below records one here, next to the
    log line for the same transition, so the log and the log file
    agree. The log has its own lock; recording never takes g_lock,
    so it is safe to call after the table lock is released. */
/* Event-log persistence (0.38.0): every recorded event is appended
 * to <data-dir>/events as it happens, so the log survives a
 * restart. The append runs under this lock, not the log's own
 * lock, because two threads can record at once (the prober and
 * the control thread both record DROPPED events) and two appends
 * to the same FILE* must not interleave mid-line. The path is
 * set once at startup, next to pulse_event_log_init. */
static char g_events_path[1024];
static pthread_mutex_t g_events_file_lock = PTHREAD_MUTEX_INITIALIZER;
static int64_t g_start_ms = 0;
static volatile sig_atomic_t g_stop = 0;
/* Live peer-file reload (0.24.0): SIGHUP sets this flag and nothing
 * else — the handler does no I/O — and the prober thread picks it up
 * on its next cycle, where the reload runs under the normal lock
 * discipline instead of inside a signal handler. */
static volatile sig_atomic_t g_reload_peer_file = 0;
/* RELOAD control-plane command (0.25.0) runs the same reload from the
 * control thread, so the two entry points are serialized on this
 * lock: a SIGHUP reload and a RELOAD command must never interleave
 * their seed-list edits and JOIN sends. */
static pthread_mutex_t g_reload_lock = PTHREAD_MUTEX_INITIALIZER;
/* Auto-reload on peer-file mtime change (0.26.0): the prober thread
 * stats the peer file once a second (PEER_FILE_WATCH_INTERVAL_MS);
 * when the mtime moved since the last load or reload, it runs the
 * same reload merge SIGHUP and RELOAD trigger, so an edited roster
 * is picked up with no signal and no control-plane call. The mtime
 * is nanosecond-resolution (st_mtim), so two edits inside one second
 * still count as a change unless the file is rewritten with
 * identical content in the same nanosecond — in which case the
 * reload would have been a no-op anyway. g_peer_file_mtime_known
 * is 0 until the first successful stat, so a node whose file was
 * unreadable at startup just records the baseline on its first
 * good check instead of reloading at once. */
#define PEER_FILE_WATCH_INTERVAL_MS 1000
static struct timespec g_peer_file_mtime;
static int g_peer_file_mtime_known = 0;
static int64_t g_peer_file_last_check_ms = 0;
/* Peer-file state exposed in METRICS and JSON (0.28.0): what the
 * last successful load or reload of --peer-file did. The initial
 * load at startup counts as the first reload (added = the peers
 * the file contributed to the seed list). A reload that cannot
 * read the file leaves this at the last successful load, so the
 * numbers always describe a roster the node actually holds.
 * Written under g_lock at the end of a successful load/reload
 * (both run serialized by g_reload_lock, and the startup load
 * before any thread starts); read by the control-plane hook under
 * the same lock. */
static pulse_peer_file_state_t g_peerfile_state;

/* stat the peer file and return 1 with *ts set when it exists. */
static int peer_file_mtime(struct timespec *ts) {
    struct stat st;
    if (stat(g_cfg.peer_file, &st) != 0) return 0;
    *ts = st.st_mtim;
    return 1;
}
static uint32_t g_seq = 1;
static pending_probe_t g_pending[MAX_PENDING];
static pending_indirect_t g_indirect[MAX_PENDING];

/* Forward: the configured gossip digest size (0.41.0), defined with
 * the other control-plane hooks further down; probe_cycle uses it
 * before that definition. */
static int gossip_digest_size_hook(void *ctx);

/* Per-peer gossip versions: the highest membership-table version we
 * believe each peer has already received from us (see
 * member_table_t.version). A digest to a peer carries only the entries
 * that changed after that version (delta encoding, 0.16.0); a peer
 * with no record, or one more than a digest's worth of changes behind,
 * gets a full SYNC instead. Records are best-effort — UDP delivery is
 * not guaranteed, so a lost delta can leave a peer behind the version
 * recorded for it — but the periodic anti-entropy SYNC (full table,
 * every ~10 s) heals that within one interval, and failure detection
 * itself never depends on gossip: each node probes its peers directly.
 * Only the prober thread and the receiver thread's reply paths touch
 * this table, always through the helpers below, which take g_lock
 * themselves; no caller may hold g_lock across them. */
#define MAX_PEER_VERSIONS PULSE_MAX_MEMBERS
typedef struct {
    char id[PULSE_ID_LEN];
    uint64_t known_version;
} peer_version_t;
static peer_version_t g_peer_versions[MAX_PEER_VERSIONS];
static int g_n_peer_versions;

static void log_line(const char *fmt, ...) {
    pthread_mutex_lock(&g_log_lock);
    double t = (pulse_now_ms() - g_start_ms) / 1000.0;
    fprintf(stdout, "[%8.3f] ", t);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);
    fputc('\n', stdout);
    fflush(stdout);
    pthread_mutex_unlock(&g_log_lock);
}

/* Copy an id between two PULSE_ID_LEN slots. Every id is
 * NUL-terminated inside its slot by construction (all producers
 * snprintf with PULSE_ID_LEN), so a whole-slot copy is exact.
 * snprintf would work too, but gcc's fortify check cannot bound a
 * variable-indexed array element and warns on it. */
static void copy_id(char dst[PULSE_ID_LEN], const char *src) {
    memcpy(dst, src, PULSE_ID_LEN);
}

/* Remove ids[idx] from a 16-slot id list by swapping in the last
 * entry (order does not matter for seed lists). */
static void list_swap_remove(char ids[][PULSE_ID_LEN], int *count,
                             int idx) {
    (*count)--;
    if (idx != *count) copy_id(ids[idx], ids[*count]);
}

static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
    if (g_sock >= 0) shutdown(g_sock, SHUT_RDWR);
    if (g_control_sock >= 0) shutdown(g_control_sock, SHUT_RDWR);
}

static void on_reload(int sig) {
    (void)sig;
    g_reload_peer_file = 1;
}

/* Incarnation survives restarts: the data dir holds a file with the next
 * incarnation to use. A restarted node therefore rejoins with a higher
 * incarnation and can refute any stale SUSPECT/DEAD gossip about itself. */
static uint32_t load_incarnation(const char *data_dir) {
    char path[600];
    snprintf(path, sizeof path, "%s/incarnation", data_dir);
    FILE *f = fopen(path, "r");
    uint32_t v = 1;
    if (f) {
        if (fscanf(f, "%u", &v) != 1) v = 1;
        fclose(f);
    }
    f = fopen(path, "w");
    if (f) {
        fprintf(f, "%u\n", v + 1);
        fclose(f);
    }
    return v;
}

static void save_incarnation(const char *data_dir, uint32_t next) {
    char path[600];
    snprintf(path, sizeof path, "%s/incarnation", data_dir);
    FILE *f = fopen(path, "w");
    if (f) {
        fprintf(f, "%u\n", next);
        fclose(f);
    }
}

/* The table version a peer is believed to already know. *found is 0
 * when we have never sent this peer anything (a new peer, or a peer
 * whose record was evicted): the caller treats that as infinitely
 * behind and answers with a full SYNC. */
static uint64_t peer_known_version(const char *id, int *found) {
    uint64_t v = 0;
    *found = 0;
    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < g_n_peer_versions; i++) {
        if (strcmp(g_peer_versions[i].id, id) == 0) {
            v = g_peer_versions[i].known_version;
            *found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_lock);
    return v;
}

/* Record that a peer now knows the table up to version: call after a
 * digest (with the through-version the send reported) or after a full
 * table went out (with the snapshot's version — a SYNC carries the
 * whole table, so the peer is current as of it; the periodic SYNC
 * heals the entries a single oversized SYNC could not fit). A version
 * is never moved backwards: a stale snapshot's version is ignored. */
static void peer_set_known_version(const char *id, uint64_t version) {
    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < g_n_peer_versions; i++) {
        if (strcmp(g_peer_versions[i].id, id) == 0) {
            if (version > g_peer_versions[i].known_version)
                g_peer_versions[i].known_version = version;
            pthread_mutex_unlock(&g_lock);
            return;
        }
    }
    if (g_n_peer_versions >= MAX_PEER_VERSIONS) {
        /* Full: evict the oldest record to make room. The evicted
         * peer simply gets a full SYNC on its next digest round. */
        memmove(&g_peer_versions[0], &g_peer_versions[1],
                sizeof(peer_version_t) * (MAX_PEER_VERSIONS - 1));
        g_n_peer_versions--;
    }
    snprintf(g_peer_versions[g_n_peer_versions].id, PULSE_ID_LEN, "%s",
             id);
    g_peer_versions[g_n_peer_versions].known_version = version;
    g_n_peer_versions++;
    pthread_mutex_unlock(&g_lock);
}

/* Membership events for WATCH (0.29.0): record one event in the
 * node's event log. Call after the table lock is released, next to
 * the log line for the same transition. Event-log persistence
 * (0.38.0): the same event is appended to <data-dir>/events, so a
 * restart restores it. The append is best-effort — a data dir that
 * cannot be written just means the log does not survive the
 * restart, and a failed append must never stop the node — so a
 * failure is silent here; the log line for the transition is
 * already in the node's stdout either way.
 *
 * Suspicion duration (0.70.0): a REVIVED event for a peer that
 * was SUSPECT carries how long it sat SUSPECT — the refute
 * latency for this one recovery — passed in by the caller, which
 * is the only place that still knows the peer was SUSPECT: by
 * record time the table already says ALIVE. A DEAD peer coming
 * back is a restart, not a refutation, so the caller passes -1,
 * and every other event passes -1 too. */
static void record_event_phi_suspect(pulse_event_type_t type,
                                     const char *id,
                                     uint32_t incarnation, double phi,
                                     int64_t suspect_ms) {
    int64_t ts = (int64_t)time(NULL);
    /* Last-contact age in the event log (0.61.0): the ms since
     * this node last heard from the member, measured now — at
     * record time, not at replay time — so a replayed SUSPECTED
     * event still shows how quiet the peer was when it was
     * suspected, and a DEAD event shows a larger age than the
     * SUSPECTED event before it. Self has no age (its last_seen
     * is just startup time, so the age would be uptime) and a
     * member never heard from, or already gone from the table
     * when the event is recorded (a GC'd DROPPED, a LEAVE), has
     * none either: -1, never a fabricated zero. Every record
     * site runs after the table lock is released, so taking it
     * briefly here cannot deadlock. */
    int64_t last_seen_age = -1;
    /* Probe counts in the event log (0.67.0): how many direct
     * probes this node had sent the member and how many were
     * answered when the event was recorded — the same values JSON
     * reports as "probes_sent" / "probes_answered" and METRICS as
     * a PROBES line, frozen at the event instead of recomputed at
     * replay time, so a replayed SUSPECTED event still shows how
     * many probes went unanswered before the suspicion fired, and
     * a DEAD event shows a sent count that kept climbing while
     * the answered count stayed frozen. Self never probes itself
     * and a member never probed, or already gone from the table
     * when the event is recorded (a GC'd DROPPED, a LEAVE), has
     * none: -1, never a fabricated zero. */
    int64_t probes_sent = -1;
    int64_t probes_answered = -1;
    /* Recent probe success rate (0.68.0): the answered / sent
     * ratio over the last PULSE_PROBE_WINDOW direct probes to
     * the member when the event was recorded — the same value
     * JSON reports as "probe_rate", frozen at the event instead
     * of recomputed at replay time, so a replayed LOWRATE event
     * still shows how low the rate was when the threshold was
     * crossed, and a replayed RATEOK event (0.69.0) still shows
     * how far it had recovered when the episode ended. Carried
     * only on a LOWRATE or RATEOK event: every other
     * event has no rate to report. Self never probes itself
     * and a member never probed, or already gone from the table
     * when the event is recorded (a GC'd DROPPED, a LEAVE), has
     * none: -1, never a fabricated zero. */
    double probe_rate = -1;
    /* Mean probe RTT (0.73.0): the peer's mean direct-probe
     * round-trip time in ms when the event was recorded — the
     * same value JSON reports as "rtt_mean_ms", frozen at the
     * event instead of recomputed at replay time, so a replayed
     * SLOW event still shows how slow the peer was when the
     * threshold was crossed, and a replayed SLOWOK event still
     * shows how far the mean had recovered when the episode
     * ended. Carried only on a SLOW or SLOWOK event: every other
     * event has no mean to report. Self never probes itself
     * and a member never answered a probe, or already gone from
     * the table when the event was recorded, has none: -1, never
     * a fabricated zero. */
    int64_t rtt_mean_ms = -1;
    /* Flap count at record time (0.76.0): how many of the
     * member's completed flap cycles fall inside the recent
     * window when the event was recorded — the same value JSON
     * reports as "flaps_10m", frozen at the event instead of
     * recomputed at replay time, so a replayed FLAPRATE event
     * still shows how fast the peer was flapping when the
     * threshold was crossed, and a replayed FLAPOK event still
     * shows how far the count had fallen when the episode
     * ended. Carried only on a FLAPRATE or FLAPOK event: every
     * other event has no flap count to report. Self never
     * suspects itself and a member never flapped, or already
     * gone from the table when the event was recorded, has
     * none: -1, never a fabricated zero. */
    int64_t flaps_10m = -1;
    /* Lifetime flap count at record time (0.79.0): how many flap
     * cycles the member has completed in total when the event
     * was recorded — the same value JSON reports as
     * "flap_count", frozen at the event instead of recomputed
     * at replay time, so a replayed FLAP event still shows
     * which flap this was instead of only that one happened.
     * Carried only on a FLAP event, with the recent count
     * above: every other event has no completed cycle to
     * count. Self never suspects itself and a member never
     * flapped, or already gone from the table when the event
     * was recorded, has none: -1, never a fabricated zero. */
    int64_t flap_count = -1;
    if (strcmp(id, g_self_id) != 0) {
        int64_t now = pulse_now_ms();
        pthread_mutex_lock(&g_lock);
        member_t *m = member_find(&g_table, id);
        if (m) {
            last_seen_age = member_last_seen_age_ms(m, now);
            if (m->probes_sent > 0) {
                probes_sent = m->probes_sent;
                probes_answered = m->probes_answered;
            }
            if (type == PULSE_EVENT_LOWRATE ||
                type == PULSE_EVENT_RATEOK)
                probe_rate = member_probe_rate(m);
            if (type == PULSE_EVENT_SLOW ||
                type == PULSE_EVENT_SLOWOK)
                rtt_mean_ms = member_rtt_mean_ms(m);
            if (type == PULSE_EVENT_FLAPRATE ||
                type == PULSE_EVENT_FLAPOK)
                flaps_10m = member_flaps_recent(m, now);
            if (type == PULSE_EVENT_FLAP) {
                flap_count = m->flap_count;
                flaps_10m = member_flaps_recent(m, now);
            }
        }
        pthread_mutex_unlock(&g_lock);
    }
    uint64_t seq = pulse_event_record_flap_count(&g_event_log, type, id,
                                                incarnation, ts, phi,
                                                last_seen_age,
                                                probes_sent,
                                                probes_answered,
                                                probe_rate,
                                                suspect_ms,
                                                rtt_mean_ms,
                                                flaps_10m,
                                                flap_count);
    if (g_events_path[0] == '\0') return;
    pthread_mutex_lock(&g_events_file_lock);
    FILE *f = fopen(g_events_path, "a");
    if (f) {
        /* The persisted line mirrors the ring (0.38.0): a phi-mode
         * SUSPECTED event (0.60.0) carries its phi at the crossing
         * as a trailing " phi=<value>", and an event whose member
         * was heard from (0.61.0) carries its last-contact age as
         * a trailing " last_seen_ms=<ms>", after the phi when both
         * are present, and an event whose member was probed
         * (0.67.0) carries its probe counts as trailing
         * " probes_sent=<n> probes_answered=<n>", after the
         * last_seen_ms when it is present, and a LOWRATE event
         * (0.68.0) — or a RATEOK event (0.69.0) — carries its
         * recent probe success rate as a
         * trailing " probe_rate=<value>", after the probe counts
         * when they are present, and a REVIVED event for a peer
         * that was SUSPECT (0.70.0) carries how long it sat
         * SUSPECT as a trailing " suspect_ms=<ms>", after the
         * probe rate when it is present, and a SLOW event
         * (0.73.0) — or a SLOWOK event — carries its mean probe
         * RTT as a trailing " rtt_mean_ms=<ms>", after the
         * suspicion duration when it is present, and a FLAPRATE
         * event (0.76.0) — or a FLAPOK event — carries its flap
         * count in the recent window as a trailing
         * " flaps_10m=<n>", after the mean probe RTT when it is
         * present — and a FLAP event (0.79.0) carries its
         * lifetime flap count as a trailing " flap_count=<n>",
         * after the flaps_10m when it is present — so all eight values
         * survive a restart and a replay exactly as the WATCH
         * stream shows them; an event with none writes the plain
         * five fields. */
        fprintf(f, "%llu %s %s %u %lld",
                (unsigned long long)seq,
                pulse_event_type_name(type), id, incarnation,
                (long long)ts);
        if (phi >= 0) fprintf(f, " phi=%.2f", phi);
        if (last_seen_age >= 0)
            fprintf(f, " last_seen_ms=%lld",
                    (long long)last_seen_age);
        if (probes_sent >= 0)
            fprintf(f, " probes_sent=%lld probes_answered=%lld",
                    (long long)probes_sent,
                    (long long)probes_answered);
        if (probe_rate >= 0) fprintf(f, " probe_rate=%.2f", probe_rate);
        if (suspect_ms >= 0)
            fprintf(f, " suspect_ms=%lld", (long long)suspect_ms);
        if (rtt_mean_ms >= 0)
            fprintf(f, " rtt_mean_ms=%lld", (long long)rtt_mean_ms);
        if (flaps_10m >= 0)
            fprintf(f, " flaps_10m=%lld", (long long)flaps_10m);
        if (flap_count >= 0)
            fprintf(f, " flap_count=%lld", (long long)flap_count);
        fputc('\n', f);
        fclose(f);
    }
    pthread_mutex_unlock(&g_events_file_lock);
}

static void record_event_phi(pulse_event_type_t type, const char *id,
                             uint32_t incarnation, double phi) {
    record_event_phi_suspect(type, id, incarnation, phi, -1);
}

/* A peer coming back ALIVE (0.70.0): the REVIVED event carries how
 * long it sat SUSPECT — the refute latency for this one recovery —
 * when the caller measured it, or -1 when the peer was DEAD (a
 * restart, not a refutation). The caller is the only place that
 * still knows the peer was SUSPECT: by record time the table
 * already says ALIVE. */
static void record_event_revived(const char *id, uint32_t incarnation,
                                 int64_t suspect_ms) {
    record_event_phi_suspect(PULSE_EVENT_REVIVED, id, incarnation, -1,
                             suspect_ms);
}

static void record_event(pulse_event_type_t type, const char *id,
                         uint32_t incarnation) {
    record_event_phi(type, id, incarnation, -1);
}

/* Gossip traffic counters (0.80.0): map a parsed message to the
 * traffic kind it counts as. The switch is explicit instead of
 * casting the gmsg_type_t: the two enums agree today, but a new
 * wire verb must not silently land in the wrong bucket. -1 for
 * GMSG_UNKNOWN: an unparsed verb is not traffic with a peer. */
static int traffic_kind_for(gmsg_type_t type) {
    switch (type) {
        case GMSG_PING: return PULSE_TRAFFIC_PING;
        case GMSG_ACK: return PULSE_TRAFFIC_ACK;
        case GMSG_PING_REQ: return PULSE_TRAFFIC_PING_REQ;
        case GMSG_JOIN: return PULSE_TRAFFIC_JOIN;
        case GMSG_GOSSIP: return PULSE_TRAFFIC_GOSSIP;
        case GMSG_SYNC: return PULSE_TRAFFIC_SYNC;
        case GMSG_UNKNOWN: return -1;
    }
    return -1;
}

/* Count one message this node sent to a peer (0.80.0): the
 * traffic counters' send side. Takes the lock itself, so call
 * sites outside the table lock stay one line; call sites that
 * already hold the lock call member_note_traffic directly. An
 * unknown id is ignored — the peer may have been GC'd between
 * the send decision and the count — and a send to self never
 * happens. */
static void note_traffic_sent(const char *id, int kind) {
    pthread_mutex_lock(&g_lock);
    member_note_traffic(&g_table, id, kind, 1);
    pthread_mutex_unlock(&g_lock);
}

/* Direct contact with a member: it is alive. Logs JOIN for a first sighting
 * and ALIVE when a suspected/dead member comes back. The address stored is
 * the one the member advertises in its id, never the datagram source. */
static void note_contact(const gmsg_t *msg,
                         const struct sockaddr_storage *addr) {
    int is_new, revived;
    int64_t now = pulse_now_ms();
    pthread_mutex_lock(&g_lock);
    member_t *before = member_find(&g_table, msg->from_id);
    is_new = before == NULL;
    /* A suspicion is refuted when a peer we marked SUSPECT answers us
     * again: record how long it sat suspect. A DEAD peer coming back
     * is a restart, not a refutation, so only SUSPECT counts. */
    member_state_t prev = before ? before->state : MEMBER_ALIVE;
    int64_t since = before ? before->state_since_ms : 0;
    /* A flap completes here when a SUSPECT peer answers again:
     * the lifetime count moves under the lock, so snapshot it
     * before the mark and compare after — the count is the only
     * witness, since the armed flag lives inside member.c. */
    int64_t flaps_before = before ? before->flap_count : 0;
    revived = member_mark_alive(&g_table, msg->from_id, addr,
                                msg->from_inc, now);
    int64_t flaps_after = before ? before->flap_count : 0;
    if (is_new) metrics_note_join(&g_metrics);
    if (revived && prev == MEMBER_SUSPECT)
        metrics_note_suspect_refuted(&g_metrics, now - since);
    pthread_mutex_unlock(&g_lock);
    if (is_new) {
        log_line("JOIN %s", msg->from_id);
        record_event(PULSE_EVENT_JOINED, msg->from_id, msg->from_inc);
    } else if (revived) {
        /* revived means the state changed to ALIVE, so the peer
         * was SUSPECT or DEAD: a revival, not a routine heartbeat.
         * A SUSPECT peer coming back is a refutation — the REVIVED
         * event carries how long it sat SUSPECT (0.70.0), the
         * refute latency for this one recovery, in the log line
         * and on the WATCH stream; a DEAD peer coming back is a
         * restart, so it carries none. */
        int64_t suspect_ms =
            prev == MEMBER_SUSPECT ? now - since : -1;
        if (suspect_ms >= 0)
            log_line("ALIVE %s suspect_ms=%lld", msg->from_id,
                     (long long)suspect_ms);
        else
            log_line("ALIVE %s", msg->from_id);
        record_event_revived(msg->from_id, msg->from_inc, suspect_ms);
        /* A flap event on the WATCH stream (0.79.0): the
         * SUSPECT -> ALIVE leg completes a flap cycle the
         * suspicion started from ALIVE on this node, so the
         * stream carries one FLAP event right after the
         * REVIVED, with the lifetime and recent counts frozen
         * on it — a DEAD peer coming back is a restart, not a
         * flap, so its count never moves and it gets none. */
        if (flaps_after > flaps_before)
            record_event(PULSE_EVENT_FLAP, msg->from_id,
                         msg->from_inc);
    }
}

/* Merge a membership message into the table: a GOSSIP digest or a SYNC
 * full table — the entries merge the same way (higher incarnation wins;
 * DEAD > SUSPECT > ALIVE at equal incarnation). The sender is alive by
 * direct contact. A SYNC is answered with our own full table when the
 * exchange taught us something new, so one exchange converges both
 * sides; a GOSSIP gets no reply, the periodic digest round is one-way.
 * The answer is conditional on purpose: an unconditional answer would
 * ping-pong forever, each SYNC answered by a SYNC at network speed,
 * and the pair would burn a CPU core exchanging identical tables.
 * (That is what 0.13.0 did: every SYNC was answered, so the first
 * periodic SYNC started an endless reply loop. Answering only when
 * the merge added or changed a member, the sender was new or revived,
 * or we refuted a verdict about ourselves keeps the one-round-trip
 * convergence — the reply carries strictly newer information — and
 * lets the exchange go quiet once both sides agree.) */
static void handle_membership_msg(const gmsg_t *msg,
                                  const struct sockaddr_storage *from_addr,
                                  int reply_sync) {
    int refute = 0;
    /* The exchange taught us something new: a new or revived sender, a
     * refuted verdict about ourselves, or a merged entry that added or
     * changed a member. Only then is a SYNC answered with our own full
     * table; a SYNC that changes nothing gets no reply, which is what
     * stops two converged nodes from answering each other forever. */
    int learned = 0;
    char new_ids[8][PULSE_ID_LEN];
    uint32_t new_inc[8];
    int new_count = 0;
    /* State transitions learned from gossip (e.g. a peer's digest
     * says a member is DEAD, or a restarted member is ALIVE again
     * at a higher incarnation). Logged after unlock, like JOINs. */
    char changed_ids[8][PULSE_ID_LEN];
    member_state_t changed_states[8];
    uint32_t changed_inc[8];
    /* How long a gossip-learned revival sat SUSPECT (0.70.0):
     * -1 unless the entry went SUSPECT -> ALIVE, in which case
     * the ms from when this node marked it SUSPECT to the merge
     * that revived it — the same refute latency the direct-contact
     * path measures, frozen on the REVIVED event. */
    int64_t changed_suspect_ms[8];
    /* Whether a gossip-learned change completed a flap cycle
     * (0.79.0): the lifetime flap count after the merge, or -1
     * when the merge completed no cycle — a flap completes on
     * the SUSPECT -> ALIVE or SUSPECT -> DEAD leg, so the FLAP
     * event rides right after the REVIVED or DEAD event for the
     * same entry. */
    int64_t changed_flap_count[8];
    int changed_count = 0;
    pthread_mutex_lock(&g_lock);
    member_t *sender_before = member_find(&g_table, msg->from_id);
    int is_new = sender_before == NULL;
    /* Same refutation window as direct contact: the sender is alive
     * by direct contact too, so a SUSPECT sender coming back is a
     * locally measured refutation. */
    member_state_t sender_prev =
        sender_before ? sender_before->state : MEMBER_ALIVE;
    int64_t sender_since =
        sender_before ? sender_before->state_since_ms : 0;
    int64_t now = pulse_now_ms();
    /* A flap completes here when a SUSPECT sender answers again:
     * snapshot the lifetime count before the mark (see
     * note_contact) — the count is the only witness. */
    int64_t sender_flaps_before =
        sender_before ? sender_before->flap_count : 0;
    int revived = member_mark_alive(&g_table, msg->from_id, from_addr,
                                    msg->from_inc, now);
    int64_t sender_flaps_after =
        sender_before ? sender_before->flap_count : 0;
    if (is_new) metrics_note_join(&g_metrics);
    if (revived && sender_prev == MEMBER_SUSPECT)
        metrics_note_suspect_refuted(&g_metrics, now - sender_since);
    if (is_new || revived) learned = 1;
    for (int i = 0; i < msg->entry_count; i++) {
        const member_t *e = &msg->entries[i];
        member_t *before = member_find(&g_table, e->id);
        member_state_t prev = before ? before->state : MEMBER_ALIVE;
        int64_t since = before ? before->state_since_ms : 0;
        /* Snapshot the lifetime flap count before the merge
         * (0.79.0): the merge is the only place a gossip-learned
         * cycle completes, and the count moving is the witness. */
        int64_t flaps_before = before ? before->flap_count : 0;
        int r = member_merge(&g_table, g_self_id, e->id, &e->addr,
                             e->incarnation, e->state, pulse_now_ms());
        if ((r & MERGE_SELF_SUSPECTED) && !refute) {
            refute = 1;
            learned = 1;
            metrics_note_refute(&g_metrics);
        }
        if (r & MERGE_NEW) {
            learned = 1;
            metrics_note_join(&g_metrics);
        }
        if (r & MERGE_CHANGED) {
            learned = 1;
            /* A transition learned from gossip: count it, but
             * there is no local latency to measure. */
            member_t *after = member_find(&g_table, e->id);
            if (after && after->state != prev) {
                if (after->state == MEMBER_SUSPECT)
                    metrics_note_suspect(&g_metrics, -1);
                else if (after->state == MEMBER_DEAD)
                    metrics_note_dead(&g_metrics, -1);
                else if (after->state == MEMBER_ALIVE &&
                         prev == MEMBER_SUSPECT)
                    /* A refutation learned from gossip: the peer
                     * refuted the suspicion at a higher incarnation.
                     * Count it, but there is no local window to
                     * measure — the local entry may have been
                     * suspected by gossip itself. */
                    metrics_note_suspect_refuted(&g_metrics, -1);
            }
        }
        /* Annotations ride along with the entry: when the sender
         * carries some we did not have (or different ones), take
         * them — that is new information, so a SYNC answer is due.
         * An entry with no annotations says nothing about them. */
        if (e->meta_count > 0 &&
            member_merge_meta(&g_table, g_self_id, e->id, e->meta,
                              e->meta_count))
            learned = 1;
        if ((r & MERGE_NEW) && new_count < 8) {
            snprintf(new_ids[new_count], PULSE_ID_LEN, "%s", e->id);
            new_inc[new_count] = e->incarnation;
            new_count++;
        }
        if ((r & MERGE_CHANGED) && changed_count < 8) {
            member_t *after = member_find(&g_table, e->id);
            if (after && after->state != prev) {
                snprintf(changed_ids[changed_count], PULSE_ID_LEN,
                         "%s", e->id);
                changed_states[changed_count] = after->state;
                changed_inc[changed_count] = after->incarnation;
                changed_suspect_ms[changed_count] =
                    (prev == MEMBER_SUSPECT &&
                     after->state == MEMBER_ALIVE)
                        ? now - since
                        : -1;
                changed_flap_count[changed_count] =
                    (after->flap_count > flaps_before)
                        ? after->flap_count
                        : -1;
                changed_count++;
            }
        }
    }
    if (refute) {
        g_incarnation++;
        save_incarnation(g_cfg.data_dir, g_incarnation + 1);
        member_t *self = member_find(&g_table, g_self_id);
        if (self) {
            self->incarnation = g_incarnation;
            /* Our own incarnation is gossip-visible: stamp the change
             * so the next digest carries it as a delta entry. */
            member_note_changed(&g_table, self);
        }
    }
    pthread_mutex_unlock(&g_lock);
    if (is_new) {
        log_line("JOIN %s", msg->from_id);
        record_event(PULSE_EVENT_JOINED, msg->from_id, msg->from_inc);
    } else if (revived) {
        /* Same refutation window as direct contact: the sender is
         * alive by direct contact too, so a SUSPECT sender coming
         * back is a locally measured refutation — the REVIVED
         * event carries how long it sat SUSPECT (0.70.0); a DEAD
         * sender coming back is a restart, so it carries none. */
        int64_t suspect_ms =
            sender_prev == MEMBER_SUSPECT ? now - sender_since : -1;
        if (suspect_ms >= 0)
            log_line("ALIVE %s suspect_ms=%lld", msg->from_id,
                     (long long)suspect_ms);
        else
            log_line("ALIVE %s", msg->from_id);
        record_event_revived(msg->from_id, msg->from_inc, suspect_ms);
        /* A flap event on the WATCH stream (0.79.0): the sender
         * answering again completes a flap cycle exactly like a
         * direct-contact revival, so the FLAP event rides right
         * after the REVIVED — a DEAD sender coming back is a
         * restart, not a flap, so its count never moves and it
         * gets none. */
        if (sender_flaps_after > sender_flaps_before)
            record_event(PULSE_EVENT_FLAP, msg->from_id,
                         msg->from_inc);
    }
    for (int i = 0; i < new_count; i++) {
        log_line("JOIN %s", new_ids[i]);
        record_event(PULSE_EVENT_JOINED, new_ids[i], new_inc[i]);
    }
    for (int i = 0; i < changed_count; i++) {
        if (changed_states[i] == MEMBER_DEAD) {
            log_line("DEAD %s", changed_ids[i]);
            record_event(PULSE_EVENT_DEAD, changed_ids[i],
                         changed_inc[i]);
        } else if (changed_states[i] == MEMBER_ALIVE) {
            /* A gossip-learned revival: the REVIVED event carries
             * how long the peer sat SUSPECT on this node (0.70.0)
             * when it was SUSPECT — the same refute latency the
             * direct-contact path measures; a DEAD peer coming
             * back is a restart, so it carries none. */
            int64_t suspect_ms = changed_suspect_ms[i];
            if (suspect_ms >= 0)
                log_line("ALIVE %s suspect_ms=%lld", changed_ids[i],
                         (long long)suspect_ms);
            else
                log_line("ALIVE %s", changed_ids[i]);
            record_event_revived(changed_ids[i], changed_inc[i],
                                 suspect_ms);
        } else {
            log_line("SUSPECT %s", changed_ids[i]);
            record_event(PULSE_EVENT_SUSPECTED, changed_ids[i],
                         changed_inc[i]);
        }
        /* A flap event on the WATCH stream (0.79.0): the change
         * above completed a flap cycle — SUSPECT -> DEAD or
         * SUSPECT -> ALIVE — so the FLAP event rides right after
         * the DEAD or REVIVED for the same entry, with the
         * lifetime and recent counts frozen on it. A change that
         * completed no cycle carries -1 and gets none. */
        if (changed_flap_count[i] >= 0)
            record_event(PULSE_EVENT_FLAP, changed_ids[i],
                         changed_inc[i]);
    }
    if (refute) {
        log_line("REFUTE %s inc=%u", g_self_id, g_incarnation);
        record_event(PULSE_EVENT_REFUTED, g_self_id, g_incarnation);
    }
    if (reply_sync && learned) {
        /* Answer a full table with our own, so the exchange converges
         * both sides in one round trip — but only when the exchange
         * taught us something new (see above). Sent from a snapshot
         * taken under the lock, like the JOIN reply. The peer now
         * holds our whole table as of the snapshot, so record it as
         * current: its next digest can be a delta. */
        member_table_t copy;
        pthread_mutex_lock(&g_lock);
        copy = g_table;
        pthread_mutex_unlock(&g_lock);
        /* Gossip traffic counters (0.80.0): the reply SYNC counts
         * as a SYNC sent to the peer, on the attempt like every
         * other send count. */
        if (gossip_send_full_table(g_send_sock, from_addr, g_self_id,
                                   g_incarnation, &copy) == 0)
            peer_set_known_version(msg->from_id, copy.version);
        note_traffic_sent(msg->from_id, PULSE_TRAFFIC_SYNC);
    }
}

static void handle_message(const gmsg_t *msg) {
    /* Reply to the address the sender ADVERTISES in its id
     * (e.g. 127.0.0.1:17102), not the datagram's source address. A node's
     * datagrams come from an unbound sending socket, so their source port
     * is an ephemeral port nobody listens on; answering there (and
     * storing that address in the membership table) silently blackholes
     * every reply and every later probe. */
    struct sockaddr_storage from_addr;
    if (member_parse_addr(msg->from_id, &from_addr) != 0) return;
    switch (msg->type) {
        case GMSG_PING:
            note_contact(msg, &from_addr);
            gossip_send_ack(g_send_sock, &from_addr, msg->seq, g_self_id, g_incarnation);
            /* Gossip traffic counters (0.80.0): the ACK answering
             * the peer's PING. */
            note_traffic_sent(msg->from_id, PULSE_TRAFFIC_ACK);
            break;

        case GMSG_ACK: {
            struct sockaddr_storage requester;
            int has_requester = 0;
            /* Direct-probe round trip (0.21.0): the pending probe
             * records when its PING went out, so the ACK's arrival
             * measures the probe latency for this peer. An ACK that
             * arrived via an indirect probe still matches the
             * pending entry, so its sample includes the indirect
             * leg — an upper bound on the direct RTT, not a direct
             * measurement. A stale or duplicate ACK matches no
             * pending probe and adds no sample. */
            int64_t rtt_ms = -1;
            pthread_mutex_lock(&g_lock);
            for (int i = 0; i < MAX_PENDING; i++) {
                if (g_pending[i].active && g_pending[i].seq == msg->seq &&
                    strcmp(g_pending[i].target_id, msg->from_id) == 0) {
                    rtt_ms = pulse_now_ms() - g_pending[i].sent_ms;
                    g_pending[i].active = 0;
                    break;
                }
            }
            for (int i = 0; i < MAX_PENDING; i++) {
                if (g_indirect[i].active && g_indirect[i].seq == msg->seq &&
                    strcmp(g_indirect[i].target_id, msg->from_id) == 0) {
                    requester = g_indirect[i].requester;
                    has_requester = 1;
                    g_indirect[i].active = 0;
                    break;
                }
            }
            /* Slow-peer warning (0.22.0): member_note_rtt returns 2
             * when the peer's mean RTT first crosses above the
             * configured threshold, and 3 (0.73.0) when the mean
             * recovers to the threshold or below after a warned
             * episode. Log once per episode (the flag in the table
             * clears on recovery, so a re-crossing warns again),
             * and record a SLOW event on the crossing and a SLOWOK
             * event on the recovery, so the alert lifecycle is
             * visible on the WATCH stream instead of only in the
             * log — the mirror of LOWRATE/RATEOK and QUIET/QUIETOK.
             * The log and the event happen after the unlock and
             * after note_contact: the log lock is separate, but
             * there is no reason to hold the table lock across IO,
             * and the contact is what revives a SUSPECT peer to
             * ALIVE, so by record time the peer is ALIVE again and
             * the event's frozen last_seen_ms is the contact that
             * just arrived, not the age before it. */
            int64_t slow_mean_ms = -1;
            int64_t slowok_mean_ms = -1;
            if (rtt_ms >= 0) {
                /* Probe-RTT histogram (0.48.0): every answered
                 * direct probe adds one sample to the global RTT
                 * mean/max/histogram, next to the per-peer numbers
                 * member_note_rtt keeps — same sample, same rule
                 * (rtt_ms >= 0 only; a stale or duplicate ACK adds
                 * nothing to either). */
                metrics_note_rtt(&g_metrics, rtt_ms);
                int r = member_note_rtt(&g_table, msg->from_id, rtt_ms,
                                        g_cfg.slow_peer_rtt_ms);
                /* Recent-probe window (0.68.0): the ACK matches
                 * this probe's sequence number, so its slot in
                 * the window flips to answered — the same probe
                 * the RTT sample and the answered count above
                 * came from, so the window rate and the lifetime
                 * answered count move together. An ACK that
                 * matched no pending probe never reaches here
                 * (rtt_ms stays -1), so a stale or duplicate ACK
                 * flips nothing. */
                member_note_probe_answered(&g_table, msg->from_id,
                                           msg->seq);
                if (r == 2) {
                    member_t *slow = member_find(&g_table, msg->from_id);
                    if (slow) slow_mean_ms = member_rtt_mean_ms(slow);
                } else if (r == 3) {
                    member_t *slow = member_find(&g_table, msg->from_id);
                    if (slow) slowok_mean_ms = member_rtt_mean_ms(slow);
                }
            }
            pthread_mutex_unlock(&g_lock);
            note_contact(msg, &from_addr);
            if (slow_mean_ms >= 0) {
                log_line("SLOW %s mean=%lldms threshold=%lldms",
                         msg->from_id, (long long)slow_mean_ms,
                         (long long)g_cfg.slow_peer_rtt_ms);
                record_event(PULSE_EVENT_SLOW, msg->from_id,
                             msg->from_inc);
            }
            if (slowok_mean_ms >= 0) {
                log_line("SLOWOK %s mean=%lldms threshold=%lldms",
                         msg->from_id, (long long)slowok_mean_ms,
                         (long long)g_cfg.slow_peer_rtt_ms);
                record_event(PULSE_EVENT_SLOWOK, msg->from_id,
                             msg->from_inc);
            }
            if (has_requester) {
                /* Forward the ACK to the node that asked us to probe. */
                gossip_send_ack(g_send_sock, &requester, msg->seq,
                                msg->from_id, msg->from_inc);
                /* Gossip traffic counters (0.80.0): the forwarded
                 * ACK counts against the requester, not the target
                 * — the requester is the peer this datagram went
                 * to, and its id is the address it advertised when
                 * it sent the PING_REQ. */
                char requester_id[PULSE_ID_LEN];
                member_format_id(&requester, requester_id,
                                 sizeof requester_id);
                note_traffic_sent(requester_id, PULSE_TRAFFIC_ACK);
            }
            break;
        }

        case GMSG_PING_REQ: {
            if (!msg->has_target) break;
            uint32_t seq = msg->seq;
            int slot = -1;
            pthread_mutex_lock(&g_lock);
            for (int i = 0; i < MAX_PENDING; i++) {
                if (!g_indirect[i].active) {
                    slot = i;
                    break;
                }
            }
            if (slot >= 0) {
                g_indirect[slot].active = 1;
                g_indirect[slot].seq = seq;
                snprintf(g_indirect[slot].target_id,
                         sizeof g_indirect[slot].target_id, "%s",
                         msg->target_id);
                g_indirect[slot].requester = from_addr;
                g_indirect[slot].sent_ms = pulse_now_ms();
            }
            pthread_mutex_unlock(&g_lock);
            if (slot >= 0) {
                gossip_send_ping(g_send_sock, &msg->target_addr, seq, g_self_id,
                                 g_incarnation);
                /* Gossip traffic counters (0.80.0): the indirect
                 * PING counts against the target — the peer this
                 * datagram went to — not the requester. */
                note_traffic_sent(msg->target_id, PULSE_TRAFFIC_PING);
            }
            break;
        }

        case GMSG_JOIN: {
            note_contact(msg, &from_addr);
            member_table_t copy;
            pthread_mutex_lock(&g_lock);
            copy = g_table;
            pthread_mutex_unlock(&g_lock);
            /* Answer a JOIN with the whole table (SYNC), not the capped
             * 16-entry digest: a joiner that lands in a cluster larger
             * than the digest would otherwise wait for gossip rounds to
             * rotate the missing members through, while the seed
             * already holds the full picture. The joiner merges the
             * SYNC and answers with its own full table, so one
             * exchange converges both sides. The joiner now holds our
             * whole table as of the snapshot, so record it as current:
             * its next digest can be a delta. */
            if (gossip_send_full_table(g_send_sock, &from_addr, g_self_id,
                                       g_incarnation, &copy) == 0)
                peer_set_known_version(msg->from_id, copy.version);
            /* Gossip traffic counters (0.80.0): the JOIN reply is
             * a SYNC sent to the joiner. */
            note_traffic_sent(msg->from_id, PULSE_TRAFFIC_SYNC);
            break;
        }

        case GMSG_GOSSIP:
            handle_membership_msg(msg, &from_addr, 0);
            break;

        case GMSG_SYNC:
            handle_membership_msg(msg, &from_addr, 1);
            break;

        case GMSG_UNKNOWN:
            break;
    }
    /* Gossip traffic counters (0.80.0): the received message counts
     * against its sender, after handling — the handler may have
     * just added the sender to the table (JOIN, note_contact, the
     * membership merge), so counting here catches a first message
     * from a new member instead of dropping it for arriving before
     * the entry existed. A PING_REQ from a sender this node never
     * heard of has no entry — that case adds none — so that one
     * recv is missed, not invented. Unknown verbs count nothing. */
    {
        int kind = traffic_kind_for(msg->type);
        if (kind >= 0) {
            pthread_mutex_lock(&g_lock);
            member_note_traffic(&g_table, msg->from_id, kind, 0);
            pthread_mutex_unlock(&g_lock);
        }
    }
}

static void *receiver_thread(void *arg) {
    (void)arg;
    char buf[PULSE_MAX_MSG];
    while (!g_stop) {
        struct sockaddr_storage from; /* source address; only used for
                                         logging, never for replies (see
                                         handle_message) */
        socklen_t fromlen = sizeof(from);
        ssize_t n = recvfrom(g_sock, buf, sizeof(buf) - 1, 0,
                             (struct sockaddr *)&from, &fromlen);
        if (n < 0) {
            if (g_stop) break;
            if (errno == EINTR) continue;
            continue;
        }
        gmsg_t msg;
        /* Shared-secret gate: with a secret configured, a datagram
         * whose tag is missing or wrong is dropped here, before it can
         * touch the membership table. Without a secret, everything is
         * accepted, as before. */
        if (g_cfg.secret[0] != '\0' &&
            !pulse_secret_check(g_cfg.secret, (const unsigned char *)buf,
                                (size_t)n))
            continue;
        if (gossip_parse(buf, (size_t)n, &msg) != 0) continue;
        handle_message(&msg);
    }
    return NULL;
}

/* Save the membership table every 50 probe cycles (every ~5 s at the
 * default 100 ms probe interval) so a crash loses at most a few
 * seconds of membership knowledge. The save runs on a snapshot copy
 * taken under the lock; the file write itself happens outside it. */
#define PERSIST_EVERY_CYCLES 50

/* Live peer-file reload (0.24.0): re-read --peer-file and merge the
 * new list into the seed list.
 *
 * A peer newly listed in the file gets exactly the --seed treatment:
 * it joins the seed list (so the ~5 s rejoin retry covers it), is
 * added to the membership table, and gets a JOIN at once.
 *
 * A peer removed from the file is dropped from the seed list — but
 * from the membership table only if it is currently unreachable. A
 * peer that is still ALIVE stays a member: the file says who to seed
 * from, the cluster says who is alive, and a file edit alone must
 * never evict a live peer. It stays in the table until the normal
 * failure path suspects, deads and GCs it, and it gets no more
 * rejoin retries in the meantime — but it is remembered in a kept
 * list, so a later reload drops it the moment it stops answering
 * instead of forgetting it the moment the file edit lands. A peer
 * that is SUSPECT, DEAD, or absent from the table is force-removed
 * (with a tombstone, so gossip cannot resurrect it), and a peer that
 * is also a --seed flag is never dropped at all — the flag outranks
 * the file. Self is never dropped either.
 *
 * A reload that cannot read the file keeps the old list: a deleted
 * or typo'd file must not drop the node to a solo start. Bad lines
 * are logged and skipped, exactly as at startup. Runs on the prober
 * thread (SIGHUP) or the control thread (RELOAD, 0.25.0), serialized
 * by g_reload_lock: the file read and the JOIN sends happen outside
 * g_lock, the seed-list and table updates under it (the rejoin block
 * in probe_cycle reads the seed list under the same lock). The
 * outcome is reported back in *r, so the control plane can answer
 * the operator; the SIGHUP path passes NULL and just gets the logs. */
static void reload_peer_file_locked(pulse_reload_result_t *r) {
    if (g_cfg.peer_file[0] == '\0') {
        r->have_file = 0;
        return; /* no file, nothing to do */
    }
    r->have_file = 1;
    char peers[16][PULSE_ID_LEN];
    int n_peers = 0, n_bad = 0;
    if (pulse_peerfile_load(g_cfg.peer_file, peers, 16, &n_peers,
                            &n_bad) != 0) {
        int e = errno;
        r->read_ok = 0;
        snprintf(r->error, sizeof r->error,
                 "cannot read peer file %s: %s", g_cfg.peer_file,
                 strerror(e));
        log_line("PEERFILE reload %s: cannot read: %s", g_cfg.peer_file,
                 strerror(e));
        return;
    }
    r->read_ok = 1;
    /* Auto-reload watch (0.26.0): this successful read is the new
     * baseline — a SIGHUP/RELOAD reload must not leave the watch
     * thinking the file changed and re-running the merge. */
    struct timespec ts;
    if (peer_file_mtime(&ts)) {
        g_peer_file_mtime = ts;
        g_peer_file_mtime_known = 1;
    }
    /* Peers already named with --seed flags stay seeds whatever the
     * file says, so they take no part in the diff. */
    char wanted[16][PULSE_ID_LEN];
    int n_wanted = 0;
    for (int i = 0; i < n_peers; i++) {
        int is_flag = 0;
        for (int j = 0; j < g_cfg.seed_flag_count; j++) {
            if (strcmp(peers[i], g_cfg.seeds[j]) == 0) { is_flag = 1; break; }
        }
        if (!is_flag)
            snprintf(wanted[n_wanted++], PULSE_ID_LEN, "%s", peers[i]);
    }
    char added[16][PULSE_ID_LEN], removed[16][PULSE_ID_LEN];
    int n_added = 0, n_removed = 0;
    pulse_peerfile_diff(g_cfg.peer_file_seeds, g_cfg.peer_file_seed_count,
                        wanted, n_wanted, added, &n_added, removed,
                        &n_removed);

    typedef struct {
        char id[PULSE_ID_LEN];
        struct sockaddr_storage addr;
    } join_action_t;
    join_action_t joins[16];
    int n_joins = 0;
    char dropped[16][PULSE_ID_LEN];
    uint32_t dropped_inc[16]; /* incarnation of each dropped peer */
    int n_dropped = 0;
    char kept[16][PULSE_ID_LEN];
    int n_kept = 0;
    int n_full = 0; /* added peers with no room left in the seed list */
    int64_t now = pulse_now_ms();
    pthread_mutex_lock(&g_lock);
    /* Removals first: they free seed-list room for the additions. */
    for (int i = 0; i < n_removed; i++) {
        for (int j = 0; j < g_cfg.peer_file_seed_count; j++) {
            if (strcmp(g_cfg.peer_file_seeds[j], removed[i]) == 0) {
                memmove(&g_cfg.peer_file_seeds[j],
                        &g_cfg.peer_file_seeds[j + 1],
                        sizeof(g_cfg.peer_file_seeds[0]) *
                            (g_cfg.peer_file_seed_count - j - 1));
                g_cfg.peer_file_seed_count--;
                break;
            }
        }
        if (strcmp(removed[i], g_self_id) == 0) continue;
        member_t *m = member_find(&g_table, removed[i]);
        if (m && m->state == MEMBER_ALIVE) {
            /* Remember it: a later reload drops it once it stops
             * answering, instead of forgetting it the moment the
             * file edit lands. */
            if (g_cfg.kept_peer_count < 16) {
                copy_id(g_cfg.kept_peers[g_cfg.kept_peer_count++],
                        removed[i]);
                copy_id(kept[n_kept++], removed[i]);
            }
        } else {
            if (m) member_force_remove(&g_table, removed[i], now);
            dropped_inc[n_dropped] = m ? m->incarnation : 0;
            copy_id(dropped[n_dropped++], removed[i]);
        }
    }
    for (int i = 0; i < n_added; i++) {
        if (g_cfg.seed_flag_count + g_cfg.peer_file_seed_count >= 16) {
            n_full++;
            continue;
        }
        copy_id(g_cfg.peer_file_seeds[g_cfg.peer_file_seed_count++],
                added[i]);
        /* Back in the file after being kept: it is a seed again, so
         * forget the kept record — the diff already put it in the
         * added list, since it was never in peer_file_seeds. */
        for (int j = 0; j < g_cfg.kept_peer_count; j++) {
            if (strcmp(g_cfg.kept_peers[j], added[i]) == 0) {
                list_swap_remove(g_cfg.kept_peers,
                                 &g_cfg.kept_peer_count, j);
                break;
            }
        }
        struct sockaddr_storage addr;
        if (member_parse_addr(added[i], &addr) != 0) continue;
        if (!member_find(&g_table, added[i])) {
            member_t *m = member_add(&g_table, added[i], &addr, 0);
            if (m) m->state_since_ms = now;
        }
        if (addr.ss_family == g_family && n_joins < 16) {
            copy_id(joins[n_joins].id, added[i]);
            joins[n_joins].addr = addr;
            n_joins++;
        }
    }
    /* Re-examine the kept peers: a peer removed from the file while
     * ALIVE stays a member, but only while it keeps answering. One
     * that has since gone SUSPECT or DEAD is dropped now; one that
     * already left the table (GC'd on its own) just leaves the kept
     * list. A kept peer back in the file was re-added above, so it
     * is gone from this list already and is not examined here. */
    for (int i = 0; i < g_cfg.kept_peer_count;) {
        int in_file = 0;
        for (int j = 0; j < n_wanted; j++) {
            if (strcmp(g_cfg.kept_peers[i], wanted[j]) == 0) {
                in_file = 1;
                break;
            }
        }
        member_t *m = in_file
                          ? NULL
                          : member_find(&g_table, g_cfg.kept_peers[i]);
        if (!in_file && m && m->state != MEMBER_ALIVE) {
            member_force_remove(&g_table, g_cfg.kept_peers[i], now);
            dropped_inc[n_dropped] = m->incarnation;
            copy_id(dropped[n_dropped++], g_cfg.kept_peers[i]);
            list_swap_remove(g_cfg.kept_peers,
                             &g_cfg.kept_peer_count, i);
        } else if (!in_file && !m) {
            list_swap_remove(g_cfg.kept_peers,
                             &g_cfg.kept_peer_count, i);
        } else {
            i++;
        }
    }
    /* Rebuild the seed list as flags plus the file's current entries. */
    g_cfg.seed_count = g_cfg.seed_flag_count;
    for (int i = 0; i < g_cfg.peer_file_seed_count; i++) {
        snprintf(g_cfg.seeds[g_cfg.seed_count++], PULSE_ID_LEN, "%s",
                 g_cfg.peer_file_seeds[i]);
    }
    pthread_mutex_unlock(&g_lock);

    /* A reload JOIN is a one-shot merge message like a re-JOIN, so
     * it goes through the same one-retry send: the node has been
     * running and sending for a while, so a stale ICMP
     * port-unreachable from a dead first seed can be pending on the
     * send socket and fail the first attempt even though the
     * datagram would go out fine a moment later (see
     * gossip_send_join_retry). Without the retry the peer still
     * merges — it is in the seed list, so the rejoin retry covers
     * it — but the backoff may have pushed that retry seconds away.
     */
    for (int i = 0; i < n_joins; i++) {
        gossip_send_join_retry(g_send_sock, &joins[i].addr, g_self_id,
                               g_incarnation);
        /* Gossip traffic counters (0.80.0): a reload JOIN counts
         * as a JOIN sent to the newly listed peer. */
        note_traffic_sent(joins[i].id, PULSE_TRAFFIC_JOIN);
    }
    for (int i = 0; i < n_added; i++) {
        /* An added peer with no room never joined the seed list; the
         * summary line counts it as skipped, not added. */
        if (i < n_added - n_full) {
            log_line("PEERFILE ADD %s", added[i]);
            /* New in the table, incarnation unknown until it
             * answers: 0 is honest, not a guess. */
            record_event(PULSE_EVENT_JOINED, added[i], 0);
        }
    }
    for (int i = 0; i < n_dropped; i++) {
        log_line("PEERFILE DROP %s", dropped[i]);
        record_event(PULSE_EVENT_DROPPED, dropped[i], dropped_inc[i]);
    }
    for (int i = 0; i < n_kept; i++)
        log_line("PEERFILE KEEP %s (still ALIVE)", kept[i]);
    for (int i = 0; i < n_full; i++)
        log_line("PEERFILE %s skipped: seed list full (16)",
                 added[n_added - n_full + i]);
    /* kept= is the peers still held in the kept list after this
     * reload — removed from the file, still ALIVE, dropped by a
     * later reload once they stop answering. */
    if (n_bad > 0)
        log_line("PEERFILE RELOAD %s peers=%d added=%d dropped=%d kept=%d (%d bad lines skipped)",
                 g_cfg.peer_file, n_peers, n_added - n_full, n_dropped,
                 g_cfg.kept_peer_count, n_bad);
    else
        log_line("PEERFILE RELOAD %s peers=%d added=%d dropped=%d kept=%d",
                 g_cfg.peer_file, n_peers, n_added - n_full, n_dropped,
                 g_cfg.kept_peer_count);
    /* Report the outcome (same numbers as the summary line) so the
     * control plane can answer a RELOAD with it. */
    r->peers = n_peers;
    r->added = n_added - n_full;
    r->dropped = n_dropped;
    r->kept = g_cfg.kept_peer_count;
    r->bad = n_bad;
    /* Peer-file state for METRICS and JSON (0.28.0): this load is
     * now the last successful one. A failed load returns early
     * above and leaves the previous state untouched. */
    pthread_mutex_lock(&g_lock);
    g_peerfile_state.have_file = 1;
    snprintf(g_peerfile_state.path, sizeof g_peerfile_state.path,
             "%s", g_cfg.peer_file);
    g_peerfile_state.last_reload_unix = (int64_t)time(NULL);
    g_peerfile_state.last_mtime_unix = g_peer_file_mtime_known
                                           ? (int64_t)g_peer_file_mtime.tv_sec
                                           : 0;
    g_peerfile_state.peers = r->peers;
    g_peerfile_state.added = r->added;
    g_peerfile_state.dropped = r->dropped;
    g_peerfile_state.kept = r->kept;
    g_peerfile_state.bad = r->bad;
    pthread_mutex_unlock(&g_lock);
}

/* Entry point for both reload triggers: SIGHUP (via the prober
 * thread, which passes NULL and just gets the logs) and the RELOAD
 * control-plane command (which gets the outcome back). The lock
 * serializes the two, so they can never interleave. */
static void reload_peer_file(pulse_reload_result_t *out) {
    pulse_reload_result_t r;
    memset(&r, 0, sizeof r);
    pthread_mutex_lock(&g_reload_lock);
    reload_peer_file_locked(&r);
    pthread_mutex_unlock(&g_reload_lock);
    if (out) *out = r;
}

/* One probe cycle: ping a random peer, escalate timed-out probes to
 * indirect probes, suspect peers whose indirect probes also fail, sweep
 * stale suspects to dead, gossip a digest, and log a STATUS line.
 * jitter_state is the prober thread's xorshift32 state (see
 * prober_thread): probe_cycle draws from it for the seed-rejoin
 * backoff, so the backoff jitter shares the node's one draw
 * sequence instead of running a second PRNG. */
static void probe_cycle(int cycle, uint32_t *jitter_state) {
    int64_t now = pulse_now_ms();

    /* 0. Live peer-file reload (0.24.0): SIGHUP sets a flag (the
     * handler itself does no I/O); the prober picks it up here, on
     * its own thread, so the reload runs under the normal lock
     * discipline instead of inside a signal handler. */
    if (g_reload_peer_file) {
        g_reload_peer_file = 0;
        reload_peer_file(NULL);
    }

    /* 0b. Auto-reload watch (0.26.0): once a second, stat the peer
     * file; when its mtime moved since the last load or reload, run
     * the same reload merge — no signal, no control-plane call. The
     * check is wall-clock, so it is unaffected by the adaptive probe
     * interval. A deleted file is skipped quietly (a manual reload
     * still reports it); a file that reappears with a new mtime is
     * picked up on the next check. */
    if (g_cfg.peer_file[0] != '\0' &&
        now - g_peer_file_last_check_ms >= PEER_FILE_WATCH_INTERVAL_MS) {
        g_peer_file_last_check_ms = now;
        struct timespec ts;
        if (peer_file_mtime(&ts)) {
            if (g_peer_file_mtime_known &&
                (ts.tv_sec != g_peer_file_mtime.tv_sec ||
                 ts.tv_nsec != g_peer_file_mtime.tv_nsec)) {
                log_line("PEERFILE %s changed on disk, auto-reloading",
                         g_cfg.peer_file);
                reload_peer_file(NULL);
            } else if (!g_peer_file_mtime_known) {
                g_peer_file_mtime = ts;
                g_peer_file_mtime_known = 1;
            }
        }
    }

    /* 1. Probe a random peer. In phi-accrual mode the PING still goes
     * out (it is the stimulus that produces heartbeats), and since
     * 0.49.0 a pending entry is created too — marked rtt_only, so it
     * exists only to match the ACK and measure the probe RTT. Before
     * 0.49.0 no entry was created, so ACKs matched nothing and the
     * per-peer RTT and the RTT histogram stayed empty in phi mode.
     * The entry is never escalated: step 2's phi branch only sweeps
     * stale rtt_only entries, it never suspects on them, so the
     * failure-detection path is unchanged. */
    char peer_id[PULSE_ID_LEN];
    struct sockaddr_storage peer_addr;
    int have_peer = 0;
    pthread_mutex_lock(&g_lock);
    member_t *peer = member_random_peer(&g_table, g_self_id);
    if (peer) {
        snprintf(peer_id, sizeof peer_id, "%s", peer->id);
        peer_addr = peer->addr;
        have_peer = 1;
    }
    pthread_mutex_unlock(&g_lock);
    if (have_peer) {
        uint32_t seq;
        int slot = -1;
        int send = 0;
        pthread_mutex_lock(&g_lock);
        seq = g_seq++;
        for (int i = 0; i < MAX_PENDING; i++) {
            if (!g_pending[i].active) {
                slot = i;
                break;
            }
        }
        if (slot >= 0) {
            g_pending[slot].active = 1;
            g_pending[slot].seq = seq;
            snprintf(g_pending[slot].target_id,
                     sizeof g_pending[slot].target_id, "%s", peer_id);
            g_pending[slot].target_addr = peer_addr;
            g_pending[slot].sent_ms = now;
            g_pending[slot].indirect_tried = 0;
            g_pending[slot].rtt_only = g_cfg.phi_accrual ? 1 : 0;
            send = 1;
        } else if (g_cfg.phi_accrual) {
            /* No free slot: still send the PING — in phi mode it is
             * the heartbeat stimulus, and losing it would stall
             * failure detection. Only the RTT sample is lost. */
            send = 1;
        }
        pthread_mutex_unlock(&g_lock);
        if (send) {
            gossip_send_ping(g_send_sock, &peer_addr, seq, g_self_id,
                             g_incarnation);
            /* Per-member probe counts (0.66.0): the direct PING
             * just went out, so the peer's sent count moves — the
             * same probe an answered ACK later turns into an RTT
             * sample and an answered count. The send is counted
             * on the attempt: a failed send still went out as far
             * as the prober knows, and the ACK that never comes
             * back is exactly what the success rate measures. The
             * probe's sequence number (0.68.0) goes into the
             * recent-probe window as an unanswered slot, so the
             * ACK can flip exactly this probe's outcome. */
            pthread_mutex_lock(&g_lock);
            member_note_probe_sent(&g_table, peer_id, seq);
            /* Gossip traffic counters (0.80.0): the direct PING
             * counts as a PING sent to the peer, next to the probe
             * count it already moves. */
            member_note_traffic(&g_table, peer_id, PULSE_TRAFFIC_PING, 1);
            pthread_mutex_unlock(&g_lock);
        }
    }

    /* 1b. Re-JOIN seeds that are not ALIVE. A node that starts while its
     * seed is down sends JOIN once at startup and would otherwise stay
     * isolated forever: it suspects, deads and GCs the seed, and the
     * seed, once up, has no reason to contact it. The first retry
     * fires on the ~5 s tick, as before — then the retry backs off
     * (0.56.0): each consecutive unanswered re-JOIN doubles the wait
     * (10 s, 20 s, capped at 30 s, jittered with the probe jitter),
     * so a permanently dead seed is probed less and less often
     * instead of every 5 s forever, while a seed that comes back is
     * still re-joined within 30 s at worst — and promptly when the
     * outage is fresh, since the backoff resets the moment the seed
     * is ALIVE again. The seed is deliberately NOT re-added to the
     * table here: it comes back through the normal path (the seed
     * answers the JOIN with a full-table SYNC, and direct contact
     * re-adds it), so a seed that never answers — wrong secret,
     * wrong family, genuinely dead — stays GC'd instead of cycling
     * back into the table. A seed of the other address family is
     * skipped: it can never be reached from this socket. The due
     * time lives on the seed's metrics entry, in the monotonic
     * clock: a seed never re-joined has no entry and fires on the
     * legacy ~5 s tick; a seed whose backoff was reset (it was
     * ALIVE since the last failure) is due at once, so a fresh
     * outage is re-joined promptly; a seed mid-backoff waits out
     * its scheduled delay. */
    typedef struct {
        char id[PULSE_ID_LEN];
        struct sockaddr_storage addr;
        int was_absent; /* it had been GC'd from the table */
    } rejoin_action_t;
    rejoin_action_t rejoins[16];
    int n_rejoins = 0;
    {
        int retry_every = 5000 / g_cfg.probe_interval_ms;
        if (retry_every < 1) retry_every = 1;
        pthread_mutex_lock(&g_lock);
        for (int i = 0; i < g_cfg.seed_count && n_rejoins < 16; i++) {
            member_t *m = member_find(&g_table, g_cfg.seeds[i]);
            if (m && m->state == MEMBER_ALIVE) {
                /* Seed is up: its backoff resets, so the next
                 * outage starts again at the base interval instead
                 * of resuming at the cap — and a seed that was
                 * never re-joined has no entry, so the reset is
                 * a no-op for it. */
                metrics_reset_seed_backoff(&g_metrics, g_cfg.seeds[i]);
                continue;
            }
            struct sockaddr_storage saddr;
            if (member_parse_addr(g_cfg.seeds[i], &saddr) != 0) continue;
            if (saddr.ss_family != g_family) continue;
            const pulse_seed_rejoin_entry_t *e =
                metrics_seed_rejoin_entry(&g_metrics, g_cfg.seeds[i]);
            if (e == NULL) {
                /* Never re-joined: the first retry fires on the
                 * legacy ~5 s tick, as before. */
                if (cycle % retry_every != 0) continue;
            } else if (e->failures > 0 && now < e->next_due_ms) {
                /* Mid-backoff: not due yet. A reset entry
                 * (failures == 0, the seed was ALIVE since the
                 * last failure) is due at once — see above. */
                continue;
            }
            int was_absent = (m == NULL);
            snprintf(rejoins[n_rejoins].id, PULSE_ID_LEN, "%s",
                     g_cfg.seeds[i]);
            rejoins[n_rejoins].addr = saddr;
            rejoins[n_rejoins].was_absent = was_absent;
            n_rejoins++;
            /* Seed-rejoin count (0.53.0, per-seed 0.54.0,
             * last rejoin 0.55.0, backoff 0.56.0): counted here,
             * under the lock, because the send loop below sends
             * exactly this list — one count per re-JOIN that goes
             * out, whether the seed was SUSPECT/DEAD in the table
             * or already GC'd — and the per-seed breakdown takes
             * the seed's id, so the total and the breakdown always
             * agree. The attempt's wall-clock time (unix ms) is
             * recorded too, so the breakdown also says when each
             * seed was last retried. The next retry's delay is
             * the backoff for the failure count this attempt
             * leaves behind — doubled from the last, capped at
             * 30 s — jittered with the probe jitter, so nodes
             * waiting on the same dead seed do not retry in
             * lockstep; the scheduled delay and its due time are
             * recorded on the entry, so the current backoff state
             * is visible in METRICS / JSON / PROMETHEUS. */
            int failures_after = e ? (int)e->failures + 1 : 1;
            int delay_ms = pulse_jittered_interval_ms(
                pulse_rejoin_backoff_ms(failures_after),
                g_cfg.probe_jitter_frac, jitter_state);
            metrics_note_seed_rejoin(&g_metrics, g_cfg.seeds[i],
                                     (int64_t)time(NULL) * 1000,
                                     delay_ms, now + delay_ms);
        }
        pthread_mutex_unlock(&g_lock);
    }

    /* 2. Decide who to suspect this cycle. Fixed-timeout mode
     * escalates timed-out probes to indirect probes and suspects on a
     * second timeout. Phi-accrual mode suspects on phi alone: no
     * indirect probes, no fixed timeouts — the phi value is the
     * judgement. A peer with no heartbeat history at all is suspected
     * after PHI_NO_HISTORY_INTERVALS of silence (a seed that never
     * answers cannot produce a phi, so without this guard it would
     * sit ALIVE forever). */
    typedef struct {
        uint32_t seq;
        char target_id[PULSE_ID_LEN];
        struct sockaddr_storage target_addr;
        struct sockaddr_storage inter[PING_REQ_FANOUT];
        int n_inter;
    } indirect_action_t;
    indirect_action_t actions[8];
    int n_actions = 0;
    char suspect_ids[8][PULSE_ID_LEN];
    double suspect_phi[8]; /* phi at suspicion, or -1 (fixed-timeout path) */
    int n_suspect = 0;
    for (int i = 0; i < 8; i++) suspect_phi[i] = -1;
    pthread_mutex_lock(&g_lock);
    /* Probe-rate history (0.71.0): sample the windowed probe
     * success rate every probe cycle for ALIVE and SUSPECT peers,
     * so JSON can show how a peer's rate fell instead of only its
     * live value — the same cadence phi_history samples at, but
     * not gated on phi-accrual: the rate exists whether phi mode
     * is on or not. A DEAD peer's history is frozen at suspicion:
     * it is gone, its window no longer changes, and sampling it
     * would only push the interesting fall out of the bounded
     * ring. */
    for (int i = 0; i < g_table.count; i++) {
        member_t *m = &g_table.members[i];
        if (strcmp(m->id, g_self_id) == 0) continue;
        if (m->state == MEMBER_ALIVE || m->state == MEMBER_SUSPECT)
            member_sample_probe_rate(m, now);
    }
    if (g_cfg.phi_accrual) {
        for (int i = 0; i < g_table.count; i++) {
            member_t *m = &g_table.members[i];
            if (strcmp(m->id, g_self_id) == 0) continue;
            /* Phi history (0.47.0): sample the live phi every probe
             * cycle for ALIVE and SUSPECT peers, so JSON can show how
             * suspicion evolved instead of only the current value. A
             * DEAD peer's history is frozen at suspicion: it is gone,
             * its phi no longer changes, and sampling it would only
             * push the interesting rise out of the bounded ring. */
            if (m->state == MEMBER_ALIVE || m->state == MEMBER_SUSPECT)
                phi_sample(&m->phi, now);
            if (m->state != MEMBER_ALIVE) continue;
            double phi = phi_value(&m->phi, now);
            int no_history = !m->phi.have_last;
            int64_t silent_for = now - m->state_since_ms;
            if (phi >= g_cfg.phi_threshold ||
                (no_history &&
                 silent_for >=
                     (int64_t)PHI_NO_HISTORY_INTERVALS * g_cfg.probe_interval_ms)) {
                if (n_suspect < 8) {
                    snprintf(suspect_ids[n_suspect], PULSE_ID_LEN, "%s",
                             m->id);
                    suspect_phi[n_suspect] = phi;
                    n_suspect++;
                }
            }
        }
        /* Sweep stale rtt_only pending entries (0.49.0): an
         * unanswered phi-mode probe leaves an entry that no ACK will
         * ever close, and without a sweep the 32 slots would fill
         * and later probes would lose their RTT sample. The window
         * is the same 2 x ack timeout the fixed path keeps a pending
         * entry for a late ACK, so an ACK inside it still counts;
         * expiring one adds no sample, starts no indirect probe and
         * suspects nobody — the failure-detection path is untouched. */
        for (int i = 0; i < MAX_PENDING; i++) {
            if (g_pending[i].active && g_pending[i].rtt_only &&
                now - g_pending[i].sent_ms >= 2 * g_cfg.ack_timeout_ms)
                g_pending[i].active = 0;
        }
    } else {
        for (int i = 0; i < MAX_PENDING; i++) {
            if (!g_pending[i].active) continue;
            int64_t age = now - g_pending[i].sent_ms;
            if (!g_pending[i].indirect_tried && age >= g_cfg.ack_timeout_ms) {
                g_pending[i].indirect_tried = 1;
                if (n_actions < 8) {
                    actions[n_actions].seq = g_pending[i].seq;
                    snprintf(actions[n_actions].target_id,
                             sizeof actions[n_actions].target_id, "%s",
                             g_pending[i].target_id);
                    actions[n_actions].target_addr = g_pending[i].target_addr;
                    actions[n_actions].n_inter = 0;
                    n_actions++;
                }
            } else if (age >= 2 * g_cfg.ack_timeout_ms) {
                if (n_suspect < 8) {
                    snprintf(suspect_ids[n_suspect],
                             sizeof suspect_ids[n_suspect], "%s",
                             g_pending[i].target_id);
                    n_suspect++;
                }
                g_pending[i].active = 0;
            }
        }
        /* Pick up to PING_REQ_FANOUT intermediaries per action. */
        for (int a = 0; a < n_actions; a++) {
            for (int tries = 0; tries < 8 && actions[a].n_inter < PING_REQ_FANOUT;
                 tries++) {
                member_t *m = member_random_peer(&g_table, g_self_id);
                if (!m) break;
                if (strcmp(m->id, actions[a].target_id) == 0) continue;
                int dup = 0;
                for (int j = 0; j < actions[a].n_inter; j++) {
                    if (pulse_addr_equal(&actions[a].inter[j], &m->addr)) {
                        dup = 1;
                        break;
                    }
                }
                if (dup) continue;
                actions[a].inter[actions[a].n_inter++] = m->addr;
            }
        }
    }
    /* Mark suspects now, while holding the lock. Detection latency is
     * measured from the peer's last direct contact to now; a gossip
     * suspicion has no local heartbeat to measure against, so the
     * gossip path records -1 instead (see the GOSSIP handler). */
    int changed_idx[8];
    uint32_t changed_inc[8]; /* incarnation of each newly suspected peer */
    int n_changed = 0;
    for (int i = 0; i < n_suspect; i++) {
        member_t *m = member_find(&g_table, suspect_ids[i]);
        int64_t latency = m ? now - m->last_seen_ms : -1;
        if (member_mark_suspect(&g_table, suspect_ids[i], now)) {
            changed_idx[n_changed] = i;
            changed_inc[n_changed] = m ? m->incarnation : 0;
            n_changed++;
            metrics_note_suspect(&g_metrics, latency);
        }
    }
    /* 3. Sweep stale suspects to dead. */
    char newly_dead[8][PULSE_ID_LEN];
    uint32_t newly_dead_inc[8];
    /* Whether the sweep completed a flap cycle for each newly
     * dead peer (0.79.0): the lifetime flap count after the
     * mark, or -1 when the death completed no cycle — a peer
     * that died without ever being suspected on this node has
     * no cycle to complete, so the FLAP event rides only after
     * the DEAD events that did. */
    int64_t newly_dead_flap[8];
    int n_dead = 0;
    for (int i = 0; i < g_table.count; i++) {
        member_t *m = &g_table.members[i];
        if (m->state == MEMBER_SUSPECT &&
            now - m->state_since_ms >= g_cfg.suspect_timeout_ms) {
            int64_t duration = now - m->state_since_ms;
            /* Snapshot the lifetime flap count before the mark
             * (0.79.0): the SUSPECT -> DEAD leg completes a flap
             * cycle the suspicion started from ALIVE on this
             * node, and the count moving is the witness. */
            int64_t flaps_before = m->flap_count;
            if (member_mark_dead(&g_table, m->id, now)) {
                metrics_note_dead(&g_metrics, duration);
                if (n_dead < 8) {
                    snprintf(newly_dead[n_dead], PULSE_ID_LEN, "%s", m->id);
                    newly_dead_inc[n_dead] = m->incarnation;
                    newly_dead_flap[n_dead] =
                        (m->flap_count > flaps_before)
                            ? m->flap_count
                            : -1;
                    n_dead++;
                }
            }
        }
    }
    /* 3a. Quiet-peer early warning (0.62.0): an ALIVE peer whose
     * last-contact age crossed the configured threshold gets one
     * QUIET log line and one QUIET event per episode. The check
     * runs here, after the suspect-to-dead sweep, so a peer that
     * just went DEAD this cycle is never warned as quiet — the
     * DEAD line already says it is gone. The warning is once per
     * crossing: quiet_warned is set here and cleared by the
     * QUIETOK check below (0.72.0) when the peer is heard from
     * again, so a persistently quiet peer warns once instead of
     * once per probe cycle, and a peer that answers again and
     * goes quiet again warns again. A peer
     * never heard from has no age (-1), so it never warns: a seed
     * that never answered is not a peer going quiet. The age is
     * captured for the log line; the event re-measures it at
     * record time (record_event), the same value within a
     * millisecond. */
    char quiet_ids[8][PULSE_ID_LEN];
    uint32_t quiet_inc[8];
    int64_t quiet_age[8];
    int n_quiet = 0;
    if (g_cfg.quiet_peer_ms > 0) {
        for (int i = 0; i < g_table.count && n_quiet < 8; i++) {
            member_t *m = &g_table.members[i];
            if (strcmp(m->id, g_self_id) == 0) continue;
            if (m->state != MEMBER_ALIVE) continue;
            if (m->quiet_warned) continue;
            int64_t age = member_last_seen_age_ms(m, now);
            if (age < 0 || age <= g_cfg.quiet_peer_ms) continue;
            m->quiet_warned = 1;
            snprintf(quiet_ids[n_quiet], PULSE_ID_LEN, "%s", m->id);
            quiet_inc[n_quiet] = m->incarnation;
            quiet_age[n_quiet] = age;
            n_quiet++;
        }
    }
    /* 3a-b. Quiet-peer recovery (0.72.0): an ALIVE peer that was
     * flagged quiet and has been heard from again — its
     * last-contact age back at or below the threshold — gets one
     * QUIETOK log line and one QUIETOK event per episode. The
     * check runs here, right after the QUIET check, so the two
     * are decided on the same age in the same cycle: a peer
     * whose age is still above the threshold stays flagged and
     * warns nothing, and a peer whose age dropped below it
     * recovers instead of warning again. The flag is cleared
     * here, not on direct contact (member_mark_alive): clearing
     * it there would lose the recovery, since the cycle would
     * never see a flagged peer that answered again. A peer that
     * went SUSPECT while quiet keeps its flag — it is not ALIVE,
     * so no QUIETOK fires for it here — and gets its QUIETOK
     * only once it revives to ALIVE, so the recovery marks an
     * episode that ended with the peer back, not a flicker on
     * the way to DEAD. The age is captured for the log line;
     * the event re-measures it at record time (record_event),
     * the same value within a millisecond. A full batch (8)
     * leaves the flag set, so the recovery is collected on the
     * next cycle instead of lost. */
    char quietok_ids[8][PULSE_ID_LEN];
    uint32_t quietok_inc[8];
    int64_t quietok_age[8];
    int n_quietok = 0;
    if (g_cfg.quiet_peer_ms > 0) {
        for (int i = 0; i < g_table.count && n_quietok < 8; i++) {
            member_t *m = &g_table.members[i];
            if (strcmp(m->id, g_self_id) == 0) continue;
            if (m->state != MEMBER_ALIVE) continue;
            if (!m->quiet_warned) continue;
            int64_t age = member_last_seen_age_ms(m, now);
            if (age < 0 || age > g_cfg.quiet_peer_ms) continue;
            m->quiet_warned = 0;
            snprintf(quietok_ids[n_quietok], PULSE_ID_LEN, "%s",
                     m->id);
            quietok_inc[n_quietok] = m->incarnation;
            quietok_age[n_quietok] = age;
            n_quietok++;
        }
    }
    /* 3a-c. Low-probe-rate warning (0.68.0): an ALIVE peer whose
     * recent probe success rate fell below the configured
     * fraction gets one LOWRATE log line and one LOWRATE event
     * per episode. The check runs here, after the suspect-to-dead
     * sweep, so a peer that just went DEAD this cycle is never
     * warned as low-rate — the DEAD line already says it is
     * gone. The warning is once per crossing: low_rate_warned
     * is set here and cleared when the windowed rate recovers
     * to the threshold or above, so a persistently low peer warns
     * once instead of once per probe cycle, and a peer that
     * recovers and drops again warns again. The re-arm is on
     * recovery, like quiet_warned since 0.72.0: one answered
     * probe barely moves a 20-probe window, so
     * clearing on contact would re-fire almost at once for a
     * peer answering one probe in ten. A peer with too few
     * probes in its window is never warned and never re-armed:
     * its rate is not judged yet. The rate is captured for the
     * log line; the event re-measures it at record time
     * (record_event), the same value within a millisecond.
     * Recovery (0.69.0): the cycle that clears the flag also
     * logs one RATEOK line and records one RATEOK event for
     * the peer — the mirror of LOWRATE, so a consumer watching
     * the alert lifecycle sees the episode end instead of the
     * flag silently clearing — once per episode, re-armed by
     * the next LOWRATE. The RATEOK event carries the recovered
     * rate frozen as its probe_rate, like LOWRATE carries the
     * rate at the crossing. */
    char lowrate_ids[8][PULSE_ID_LEN];
    uint32_t lowrate_inc[8];
    double lowrate_rate[8];
    int n_lowrate = 0;
    char rateok_ids[8][PULSE_ID_LEN];
    uint32_t rateok_inc[8];
    double rateok_rate[8];
    int n_rateok = 0;
    if (g_cfg.low_probe_rate_frac > 0) {
        for (int i = 0; i < g_table.count && n_lowrate < 8; i++) {
            member_t *m = &g_table.members[i];
            if (strcmp(m->id, g_self_id) == 0) continue;
            if (m->state != MEMBER_ALIVE) continue;
            if (member_is_low_probe_rate(m,
                                         g_cfg.low_probe_rate_frac)) {
                if (m->low_rate_warned) continue;
                m->low_rate_warned = 1;
                snprintf(lowrate_ids[n_lowrate], PULSE_ID_LEN, "%s",
                         m->id);
                lowrate_inc[n_lowrate] = m->incarnation;
                lowrate_rate[n_lowrate] = member_probe_rate(m);
                n_lowrate++;
            } else if (m->low_rate_warned) {
                /* Not low any more: re-arm only on a real
                 * recovery — the windowed rate back at or above
                 * the threshold. A peer whose window is still
                 * too small to judge keeps its flag: it was
                 * warned on a full window, and a window that
                 * small cannot have recovered yet. Clearing the
                 * flag is the recovery: the RATEOK line and
                 * event below announce it, so the episode ends
                 * in the log and on the WATCH stream, not
                 * silently. A full rateok batch (8) leaves the
                 * flag set, so the recovery is collected on the
                 * next cycle instead of lost. */
                double rate = member_probe_rate(m);
                if (rate >= 0 &&
                    rate >= g_cfg.low_probe_rate_frac) {
                    m->low_rate_warned = 0;
                    if (n_rateok < 8) {
                        snprintf(rateok_ids[n_rateok], PULSE_ID_LEN,
                                 "%s", m->id);
                        rateok_inc[n_rateok] = m->incarnation;
                        rateok_rate[n_rateok] = rate;
                        n_rateok++;
                    } else {
                        /* Batch full: re-set the flag so the
                         * recovery is not lost — the next cycle
                         * collects it once the batch has been
                         * emitted. */
                        m->low_rate_warned = 1;
                    }
                }
            }
        }
    }
    /* 3a-d. Flap-rate warning (0.76.0): an ALIVE peer whose
     * completed flap cycles in the last 10 minutes reached the
     * configured threshold gets one FLAPRATE log line and one
     * FLAPRATE event per episode. The check runs here, after
     * the suspect-to-dead sweep, so a peer that just went DEAD
     * this cycle is never warned as flap-rate — the DEAD line
     * already says it is gone — and a peer that just revived
     * from SUSPECT this cycle, completing a flap, is ALIVE
     * again and is judged on its new count. The warning is
     * once per crossing: flap_rate_warned is set here and
     * cleared when the recent count falls back below the
     * threshold, so a persistently flapping peer warns once
     * instead of once per probe cycle, and a peer that
     * recovers and flaps again warns again. The re-arm is on
     * recovery, like quiet_warned and low_rate_warned: the
     * count only falls as flaps age out of the 10-minute
     * window, so clearing on contact would re-fire almost at
     * once for a peer still flapping. A peer that goes SUSPECT
     * while warned keeps its flag until it revives to ALIVE,
     * so its FLAPOK fires only once the recovery stuck — the
     * same rule as quiet_warned. The count is captured for
     * the log line; the event re-measures it at record time
     * (record_event), the same value within a millisecond.
     * Recovery: the cycle that clears the flag also logs one
     * FLAPOK line and records one FLAPOK event for the peer —
     * the mirror of FLAPRATE, so a consumer watching the alert
     * lifecycle sees the episode end instead of the flag
     * silently clearing — once per episode, re-armed by the
     * next FLAPRATE. The FLAPOK event carries the fallen count
     * frozen as its flaps_10m, like FLAPRATE carries the count
     * at the crossing. A warned peer whose recent count is
     * still at or above the threshold stays warned with no
     * new line; a peer below the threshold that was never
     * warned stays quiet. */
    char flaprate_ids[8][PULSE_ID_LEN];
    uint32_t flaprate_inc[8];
    int flaprate_flaps[8];
    int n_flaprate = 0;
    char flapok_ids[8][PULSE_ID_LEN];
    uint32_t flapok_inc[8];
    int flapok_flaps[8];
    int n_flapok = 0;
    if (g_cfg.flap_rate_threshold > 0) {
        for (int i = 0; i < g_table.count; i++) {
            member_t *m = &g_table.members[i];
            if (strcmp(m->id, g_self_id) == 0) continue;
            if (m->state != MEMBER_ALIVE) continue;
            int recent = member_flaps_recent(m, now);
            if (member_is_flap_rate(m, g_cfg.flap_rate_threshold,
                                    now)) {
                if (m->flap_rate_warned) continue;
                m->flap_rate_warned = 1;
                if (n_flaprate < 8) {
                    snprintf(flaprate_ids[n_flaprate], PULSE_ID_LEN,
                             "%s", m->id);
                    flaprate_inc[n_flaprate] = m->incarnation;
                    flaprate_flaps[n_flaprate] = recent;
                    n_flaprate++;
                } else {
                    /* Batch full: leave the flag set but the
                     * episode unannounced — the next cycle
                     * collects it once the batch has been
                     * emitted. The flag is already set, so the
                     * next cycle would skip it: clear it again
                     * so the warning is not lost. */
                    m->flap_rate_warned = 0;
                }
            } else if (m->flap_rate_warned) {
                /* Not flap-rate any more: the recent count fell
                 * back below the threshold as flaps aged out of
                 * the 10-minute window. Clearing the flag is
                 * the recovery: the FLAPOK line and event below
                 * announce it, so the episode ends in the log
                 * and on the WATCH stream, not silently. A full
                 * flapok batch (8) leaves the flag set, so the
                 * recovery is collected on the next cycle
                 * instead of lost. */
                m->flap_rate_warned = 0;
                if (n_flapok < 8) {
                    snprintf(flapok_ids[n_flapok], PULSE_ID_LEN,
                             "%s", m->id);
                    flapok_inc[n_flapok] = m->incarnation;
                    flapok_flaps[n_flapok] = recent;
                    n_flapok++;
                } else {
                    /* Batch full: re-set the flag so the
                     * recovery is not lost — the next cycle
                     * collects it once the batch has been
                     * emitted. */
                    m->flap_rate_warned = 1;
                }
            }
        }
    }
    /* 3b. Garbage-collect DEAD entries older than the retention period.
     * Tombstones live twice as long: a peer that learned the DEAD entry
     * later than we did still has up to one retention period to GC it
     * itself, and the tombstone stops its gossip from resurrecting the
     * entry here in the meantime. member_gc returns only ids, so the
     * incarnations for the DROPPED events are captured first: a DEAD
     * entry past retention is exactly what GC removes. */
    char gc_cand_ids[8][PULSE_ID_LEN];
    uint32_t gc_cand_inc[8];
    int n_gc_cand = 0;
    for (int i = 0; i < g_table.count && n_gc_cand < 8; i++) {
        member_t *m = &g_table.members[i];
        if (m->state == MEMBER_DEAD &&
            now - m->state_since_ms >= g_cfg.dead_retention_ms) {
            snprintf(gc_cand_ids[n_gc_cand], PULSE_ID_LEN, "%s", m->id);
            gc_cand_inc[n_gc_cand] = m->incarnation;
            n_gc_cand++;
        }
    }
    char gc_ids[8][PULSE_ID_LEN];
    int n_gc = member_gc(&g_table, now, g_cfg.dead_retention_ms,
                         2 * g_cfg.dead_retention_ms, gc_ids, 8);
    /* Match each removed id to its pre-GC incarnation (0 when the
     * entry was somehow not among the candidates — it cannot
     * happen, since GC removes exactly the entries past retention,
     * but a 0 incarnation beats a wrong one). */
    uint32_t gc_inc[8];
    for (int i = 0; i < n_gc; i++) {
        gc_inc[i] = 0;
        for (int j = 0; j < n_gc_cand; j++) {
            if (strcmp(gc_ids[i], gc_cand_ids[j]) == 0) {
                gc_inc[i] = gc_cand_inc[j];
                break;
            }
        }
    }
    /* 4. Gossip a digest to a few random peers every other cycle. The
     * fanout grows with log2(cluster size) — 1 peer for a 2-node
     * cluster, up to 4 for 8+ peers — so news still spreads in
     * O(log n) rounds as the cluster grows, without flooding a large
     * cluster. Only ALIVE/SUSPECT peers are gossip targets; each
     * peer's digest is built from the same snapshot copy, but the
     * digest itself is per-peer (a delta against what that peer is
     * believed to already know — see the send loop below). */
    member_table_t digest_copy;
    int have_digest = 0;
    struct sockaddr_storage digest_to[4];
    char digest_ids[4][PULSE_ID_LEN];
    int n_digest = 0;
    if (cycle % 2 == 0) {
        int n_peers = 0;
        for (int i = 0; i < g_table.count; i++) {
            if (strcmp(g_table.members[i].id, g_self_id) == 0) continue;
            if (g_table.members[i].state == MEMBER_DEAD) continue;
            n_peers++;
        }
        int fanout = member_gossip_fanout(n_peers);
        for (int tries = 0; tries < 8 && n_digest < fanout; tries++) {
            member_t *d = member_random_peer(&g_table, g_self_id);
            if (!d) break;
            int dup = 0;
            for (int j = 0; j < n_digest; j++) {
                if (pulse_addr_equal(&digest_to[j], &d->addr)) {
                    dup = 1;
                    break;
                }
            }
            if (dup) continue;
            digest_to[n_digest] = d->addr;
            snprintf(digest_ids[n_digest], PULSE_ID_LEN, "%s", d->id);
            n_digest++;
        }
        if (n_digest > 0) {
            digest_copy = g_table;
            have_digest = 1;
        }
    }
    /* 4b. Anti-entropy: every sync_interval, exchange full tables with
     * one random peer, so a node that missed many digests converges
     * instead of waiting for the random window to rotate the missing
     * entries through. The peer answers with its own full table, so
     * one exchange converges both sides. */
    member_table_t sync_copy;
    struct sockaddr_storage sync_to;
    char sync_id[PULSE_ID_LEN];
    int have_sync = 0;
    {
        int sync_every = g_cfg.sync_interval_ms / g_cfg.probe_interval_ms;
        if (sync_every < 1) sync_every = 1;
        if (cycle % sync_every == 0) {
            member_t *s = member_random_peer(&g_table, g_self_id);
            if (s) {
                sync_to = s->addr;
                snprintf(sync_id, sizeof sync_id, "%s", s->id);
                sync_copy = g_table;
                have_sync = 1;
            }
        }
    }
    /* 5. Periodic STATUS line. */
    int st_alive = 0, st_suspect = 0, st_dead = 0;
    if (cycle % 10 == 0) {
        st_alive = member_count_state(&g_table, MEMBER_ALIVE);
        st_suspect = member_count_state(&g_table, MEMBER_SUSPECT);
        st_dead = member_count_state(&g_table, MEMBER_DEAD);
    }
    /* 6. Periodic membership save (see PERSIST_EVERY_CYCLES). */
    member_table_t persist_copy;
    int do_persist = (cycle % PERSIST_EVERY_CYCLES == 0);
    if (do_persist) persist_copy = g_table;
    pthread_mutex_unlock(&g_lock);

    for (int i = 0; i < n_actions; i++) {
        for (int j = 0; j < actions[i].n_inter; j++) {
            gossip_send_ping_req(g_send_sock, &actions[i].inter[j], actions[i].seq,
                                 actions[i].target_id, g_self_id);
            /* Gossip traffic counters (0.80.0): the PING_REQ counts
             * against the intermediary it went to — its id is the
             * address the probe cycle picked from the table. */
            char inter_id[PULSE_ID_LEN];
            member_format_id(&actions[i].inter[j], inter_id,
                             sizeof inter_id);
            note_traffic_sent(inter_id, PULSE_TRAFFIC_PING_REQ);
        }
    }
    for (int i = 0; i < n_rejoins; i++) {
        /* A re-JOIN is the one message whose delivery matters for
         * merging, so it goes through the one-retry send: the send
         * socket is connect()ed to the first seed, and a stale ICMP
         * port-unreachable from a dead seed can fail the first
         * attempt even though the datagram would go out fine a
         * moment later (see gossip_send_join_retry). The count and
         * the backoff schedule stay honest: the re-JOIN did go out,
         * just on the second syscall. */
        gossip_send_join_retry(g_send_sock, &rejoins[i].addr, g_self_id,
                               g_incarnation);
        /* Gossip traffic counters (0.80.0): the re-JOIN counts as
         * a JOIN sent to the seed. */
        note_traffic_sent(rejoins[i].id, PULSE_TRAFFIC_JOIN);
        /* Logged only when the seed had been GC'd: a re-JOIN to a
         * SUSPECT/DEAD seed still in the table is routine. */
        if (rejoins[i].was_absent)
            log_line("REJOIN %s", rejoins[i].id);
    }
    for (int i = 0; i < n_changed; i++) {
        int idx = changed_idx[i];
        if (suspect_phi[idx] >= 0)
            log_line("SUSPECT %s phi=%.2f", suspect_ids[idx],
                     suspect_phi[idx]);
        else
            log_line("SUSPECT %s", suspect_ids[idx]);
        /* Phi at threshold crossing (0.60.0): the SUSPECTED event
         * carries the phi the probe cycle measured when it crossed
         * the threshold, so the event log — and the WATCH stream
         * reading it — records why this suspicion fired instead
         * of only that it did. Only a real crossing carries a
         * phi: a peer suspected by the no-history guard (never
         * answered, so its phi is 0.00) or by the fixed timeout
         * carries -1, and a gossip-learned suspicion records its
         * event on the gossip path with no phi either, since this
         * node never measured it. */
        record_event_phi(PULSE_EVENT_SUSPECTED, suspect_ids[idx],
                         changed_inc[i],
                         suspect_phi[idx] >= g_cfg.phi_threshold
                             ? suspect_phi[idx]
                             : -1);
    }
    for (int i = 0; i < n_dead; i++) {
        log_line("DEAD %s", newly_dead[i]);
        record_event(PULSE_EVENT_DEAD, newly_dead[i], newly_dead_inc[i]);
        /* A flap event on the WATCH stream (0.79.0): the
         * SUSPECT -> DEAD leg completes a flap cycle, so the
         * FLAP event rides right after the DEAD, with the
         * lifetime and recent counts frozen on it — a death
         * that completed no cycle gets none. */
        if (newly_dead_flap[i] >= 0)
            record_event(PULSE_EVENT_FLAP, newly_dead[i],
                         newly_dead_inc[i]);
    }
    /* Quiet-peer warning (0.62.0): the QUIET line says how quiet
     * the peer was when the threshold was crossed, and the QUIET
     * event carries the same age frozen as its last_seen_ms, so a
     * WATCH replay shows it too. The peer is still ALIVE at this
     * point — that is the point of the warning: the suspicion has
     * not fired yet. */
    for (int i = 0; i < n_quiet; i++) {
        log_line("QUIET %s age=%lldms threshold=%lldms", quiet_ids[i],
                 (long long)quiet_age[i],
                 (long long)g_cfg.quiet_peer_ms);
        record_event(PULSE_EVENT_QUIET, quiet_ids[i], quiet_inc[i]);
    }
    /* Quiet-peer recovery (0.72.0): the QUIETOK line says how
     * quiet the peer was when it was heard from again — its age
     * back at or below the threshold — and the QUIETOK event
     * carries the same age frozen as its last_seen_ms, so a
     * WATCH replay shows it too. The peer is still ALIVE at
     * this point — that is the point of the recovery: the flag
     * cleared because the peer is answering again, not because
     * it was suspected. */
    for (int i = 0; i < n_quietok; i++) {
        log_line("QUIETOK %s age=%lldms threshold=%lldms",
                 quietok_ids[i], (long long)quietok_age[i],
                 (long long)g_cfg.quiet_peer_ms);
        record_event(PULSE_EVENT_QUIETOK, quietok_ids[i],
                     quietok_inc[i]);
    }
    /* Low-probe-rate warning (0.68.0): the LOWRATE line says how
     * low the peer's recent probe success rate was when the
     * threshold was crossed, and the LOWRATE event carries the
     * same rate frozen as its probe_rate, so a WATCH replay shows
     * it too. The peer is still ALIVE at this point — that is
     * the point of the warning: the suspicion has not fired yet,
     * and a peer answering one probe in ten is worth knowing
     * about before it is suspected. */
    for (int i = 0; i < n_lowrate; i++) {
        log_line("LOWRATE %s rate=%.2f threshold=%.2f",
                 lowrate_ids[i], lowrate_rate[i],
                 g_cfg.low_probe_rate_frac);
        record_event(PULSE_EVENT_LOWRATE, lowrate_ids[i],
                     lowrate_inc[i]);
    }
    /* Low-probe-rate recovery (0.69.0): the RATEOK line says how
     * far the peer's recent probe success rate had recovered
     * when the episode ended, and the RATEOK event carries the
     * same rate frozen as its probe_rate, so a WATCH replay shows
     * it too. The peer is still ALIVE at this point — that is
     * the point of the recovery: the flag cleared because the
     * peer is answering again, not because it was suspected. */
    for (int i = 0; i < n_rateok; i++) {
        log_line("RATEOK %s rate=%.2f threshold=%.2f",
                 rateok_ids[i], rateok_rate[i],
                 g_cfg.low_probe_rate_frac);
        record_event(PULSE_EVENT_RATEOK, rateok_ids[i],
                     rateok_inc[i]);
    }
    /* Flap-rate warning (0.76.0): the FLAPRATE line says how
     * many flap cycles the peer completed in the last 10 minutes
     * when the threshold was crossed, and the FLAPRATE event
     * carries the same count frozen as its flaps_10m, so a WATCH
     * replay shows it too. The peer is still ALIVE at this point
     * — that is the point of the warning: the peer keeps dying
     * and coming back, and the count says how fast, instead of
     * only the latest death saying it died. */
    for (int i = 0; i < n_flaprate; i++) {
        log_line("FLAPRATE %s flaps=%d threshold=%lld",
                 flaprate_ids[i], flaprate_flaps[i],
                 (long long)g_cfg.flap_rate_threshold);
        record_event(PULSE_EVENT_FLAPRATE, flaprate_ids[i],
                     flaprate_inc[i]);
    }
    /* Flap-rate recovery (0.76.0): the FLAPOK line says how far
     * the peer's recent flap count had fallen when the episode
     * ended — flaps ageing out of the 10-minute window — and the
     * FLAPOK event carries the same count frozen as its
     * flaps_10m, so a WATCH replay shows it too. The peer is
     * still ALIVE at this point — that is the point of the
     * recovery: the flag cleared because the peer stopped
     * flapping fast, not because it was suspected. */
    for (int i = 0; i < n_flapok; i++) {
        log_line("FLAPOK %s flaps=%d threshold=%lld",
                 flapok_ids[i], flapok_flaps[i],
                 (long long)g_cfg.flap_rate_threshold);
        record_event(PULSE_EVENT_FLAPOK, flapok_ids[i],
                     flapok_inc[i]);
    }
    /* gc_ids holds at most 8 ids even when more entries were GC'd
     * in one cycle (member_gc returns the total removed), so the
     * loop is bounded at 8: reading past the array would log
     * garbage. */
    for (int i = 0; i < n_gc && i < 8; i++) {
        log_line("GC %s", gc_ids[i]);
        record_event(PULSE_EVENT_DROPPED, gc_ids[i], gc_inc[i]);
    }
    if (have_digest) {
        /* One digest per peer, each a delta against what that peer is
         * believed to already know. A peer with no record — never sent
         * to, or evicted — or one more than a digest's worth of changes
         * behind gets a full SYNC instead: a delta cannot carry that
         * much, and the SYNC converges it in one exchange. A peer that
         * is current gets no datagram at all until something changes.
         * The record advances only to what actually went out (the
         * digest reports the highest changed version it carried), so
         * a digest that could not fit every change leaves the rest
         * for the next round instead of pretending the peer knows it. */
        for (int i = 0; i < n_digest; i++) {
            int found = 0;
            uint64_t known = peer_known_version(digest_ids[i], &found);
            int changed = member_changed_count(&digest_copy, known);
            if (!found || changed > gossip_digest_size_hook(NULL)) {
                if (gossip_send_full_table(g_send_sock, &digest_to[i],
                                           g_self_id, g_incarnation,
                                           &digest_copy) == 0)
                    peer_set_known_version(digest_ids[i],
                                           digest_copy.version);
                /* Gossip traffic counters (0.80.0): a peer too far
                 * behind for a delta gets a full SYNC instead. */
                note_traffic_sent(digest_ids[i], PULSE_TRAFFIC_SYNC);
            } else {
                uint64_t through = known;
                if (gossip_send_digest(g_send_sock, &digest_to[i],
                                       g_self_id, g_incarnation,
                                       &digest_copy, known,
                                       &through,
                                       gossip_digest_size_hook(NULL)) == 0)
                    peer_set_known_version(digest_ids[i], through);
                /* Gossip traffic counters (0.80.0): the digest is
                 * a GOSSIP sent to the peer. */
                note_traffic_sent(digest_ids[i], PULSE_TRAFFIC_GOSSIP);
            }
        }
    }
    if (have_sync) {
        if (gossip_send_full_table(g_send_sock, &sync_to, g_self_id,
                                   g_incarnation, &sync_copy) == 0)
            peer_set_known_version(sync_id, sync_copy.version);
        /* Gossip traffic counters (0.80.0): the periodic
         * anti-entropy SYNC counts as a SYNC sent to the peer. */
        note_traffic_sent(sync_id, PULSE_TRAFFIC_SYNC);
        log_line("SYNC %s", sync_id);
    }
    if (cycle % 10 == 0) {
        log_line("STATUS alive=%d suspect=%d dead=%d total=%d", st_alive,
                 st_suspect, st_dead, st_alive + st_suspect + st_dead);
    }
    if (do_persist) {
        pulse_members_save(g_cfg.data_dir, &persist_copy, g_self_id,
                           pulse_now_ms());
    }
}

/* Control-plane hooks: the control server knows nothing about the
 * node's threads; it reads state through these. */
static void snapshot_hook(void *ctx, member_table_t *out) {
    (void)ctx;
    pthread_mutex_lock(&g_lock);
    *out = g_table;
    pthread_mutex_unlock(&g_lock);
}

static void metrics_hook(void *ctx, pulse_metrics_t *out) {
    (void)ctx;
    pthread_mutex_lock(&g_lock);
    *out = g_metrics;
    pthread_mutex_unlock(&g_lock);
}

static int phi_enabled_hook(void *ctx) {
    (void)ctx;
    return g_cfg.phi_accrual;
}

/* PEERS control-plane command (0.27.0): where a member came from.
 * Precedence: self, then a --seed flag (the flag outranks the file,
 * exactly as the reload does), then the --peer-file roster — or the
 * kept list, a peer removed from the file but kept because it was
 * still ALIVE — then anything else is discovered: learned from
 * gossip or a JOIN, never seeded. Returns a string literal; the
 * caller must not free it. The seed lists are read under g_lock,
 * since a reload can rebuild them. */
static const char *peer_source_hook(void *ctx, const char *id) {
    (void)ctx;
    if (strcmp(id, g_self_id) == 0) return "self";
    pthread_mutex_lock(&g_lock);
    const char *source = "discovered";
    for (int i = 0; i < g_cfg.seed_flag_count; i++) {
        if (strcmp(g_cfg.seeds[i], id) == 0) {
            source = "seed";
            break;
        }
    }
    if (strcmp(source, "discovered") == 0) {
        for (int i = 0; i < g_cfg.peer_file_seed_count; i++) {
            if (strcmp(g_cfg.peer_file_seeds[i], id) == 0) {
                source = "peer-file";
                break;
            }
        }
    }
    if (strcmp(source, "discovered") == 0) {
        for (int i = 0; i < g_cfg.kept_peer_count; i++) {
            if (strcmp(g_cfg.kept_peers[i], id) == 0) {
                source = "peer-file";
                break;
            }
        }
    }
    pthread_mutex_unlock(&g_lock);
    return source;
}

static int force_leave_hook(void *ctx, const char *id) {
    (void)ctx;
    if (strcmp(id, g_self_id) == 0) return -1;
    int removed = 0;
    uint32_t inc = 0;
    pthread_mutex_lock(&g_lock);
    member_t *m = member_find(&g_table, id);
    if (m) inc = m->incarnation;
    removed = member_force_remove(&g_table, id, pulse_now_ms());
    pthread_mutex_unlock(&g_lock);
    if (removed) {
        log_line("LEAVE %s", id);
        record_event(PULSE_EVENT_DROPPED, id, inc);
    }
    return removed;
}

/* RELOAD control-plane command (0.25.0): run the same peer-file
 * reload SIGHUP triggers, from the control thread, and report the
 * outcome to the operator. */
static void reload_peer_file_hook(void *ctx, pulse_reload_result_t *out) {
    (void)ctx;
    reload_peer_file(out);
}

/* METRICS / JSON peer-file state (0.28.0): the last successful
 * load or reload of --peer-file. */
static void peer_file_state_hook(void *ctx, pulse_peer_file_state_t *out) {
    (void)ctx;
    pthread_mutex_lock(&g_lock);
    *out = g_peerfile_state;
    pthread_mutex_unlock(&g_lock);
}

/* WATCH control-plane command (0.29.0): the next membership event
 * after a sequence number, and the latest sequence number. */
static int watch_next_hook(void *ctx, uint64_t last_seq,
                           pulse_event_t *out, int timeout_ms) {
    (void)ctx;
    return pulse_event_wait(&g_event_log, last_seq, out, timeout_ms);
}

static uint64_t watch_latest_seq_hook(void *ctx) {
    (void)ctx;
    return pulse_event_latest_seq(&g_event_log);
}

/* WATCH replay (0.31.0): the oldest sequence still held in the
 * ring, so a FROM below it can be answered with a GAP line and a
 * replay from here instead of a dropped connection. */
static uint64_t watch_oldest_seq_hook(void *ctx) {
    (void)ctx;
    return pulse_event_oldest_seq(&g_event_log);
}

/* WATCH event ring capacity (0.36.0): how many events the log
 * holds, so METRICS / JSON / PROMETHEUS can report it. */
static int watch_event_log_cap_hook(void *ctx) {
    (void)ctx;
    return pulse_event_log_cap(&g_event_log);
}

/* Gossip digest size (0.41.0): how many members one gossip round's
 * digest covers — the --gossip-digest-size the node started with,
 * 16 by default — so METRICS / JSON / PROMETHEUS can report it. */
static int gossip_digest_size_hook(void *ctx) {
    (void)ctx;
    return g_cfg.gossip_digest_size > 0 ? g_cfg.gossip_digest_size
                                        : PULSE_GOSSIP_ENTRIES;
}

/* Phi suspicion threshold (0.43.0): the phi value at which a peer
 * is suspected — the --phi-threshold the node started with, 8.0
 * by default — so PROMETHEUS can report it as
 * pulse_phi_threshold. */
static double phi_threshold_hook(void *ctx) {
    (void)ctx;
    return g_cfg.phi_threshold;
}

/* Set one annotation on this node itself (control plane META): update
 * the self entry, stamp it changed so the next digest gossips it, and
 * persist the set to the data dir so it survives a restart. Returns
 * 0 on success, -1 on an invalid key/value, -2 when the node already
 * carries the maximum number of annotations. */
static int set_meta_hook(void *ctx, const char *key, const char *value) {
    (void)ctx;
    int r;
    meta_kv_t saved[PULSE_MAX_META];
    int n_saved = 0;
    pthread_mutex_lock(&g_lock);
    member_t *self = member_find(&g_table, g_self_id);
    if (!self) {
        pthread_mutex_unlock(&g_lock);
        return -1;
    }
    r = member_meta_set(self, key, value);
    if (r == 0) {
        member_note_changed(&g_table, self);
        n_saved = self->meta_count;
        for (int i = 0; i < n_saved; i++) saved[i] = self->meta[i];
    }
    pthread_mutex_unlock(&g_lock);
    if (r == 0) {
        pulse_meta_save(g_cfg.data_dir, saved, n_saved);
        log_line("META %s=%s", key, value);
    }
    return r;
}

static void *control_thread(void *arg) {
    (void)arg;
    control_serve(g_control_sock, &g_hooks);
    return NULL;
}

static void *prober_thread(void *arg) {
    (void)arg;
    int cycle = 0;
    /* The interval the loop is currently sleeping: log only when it
     * changes, so a stable cluster does not spam the log every cycle.
     * Each iteration re-counts suspects and re-derives the interval
     * from the base and fast settings (see adaptive.h): while any peer
     * is SUSPECT the cycle shortens so direct probes, indirect
     * PING_REQ, the suspect-to-dead sweep and gossip rounds all run
     * faster, and it goes back to base the moment the table is
     * suspect-free. The cycle-based cadences inside probe_cycle (the
     * anti-entropy SYNC, the gossip round every other cycle) are
     * counted in cycles, so they speed up in wall time along with
     * the loop while the cluster is unstable — that is the point.
     * The seed rejoin is wall-clock based since 0.56.0 (only its
     * first retry still fires on the ~5 s cycle tick), so its
     * backoff delays stay in real milliseconds whatever the
     * probe interval is doing. */
    int current_interval_ms = g_cfg.probe_interval_ms;
    /* Jitter state (0.50.0): seeded once from time, pid and port, so
     * nodes that start together still draw different sequences and
     * drift out of lockstep within a few cycles. The state is local
     * to this thread: nothing else draws from it — probe_cycle
     * takes a pointer to it for the seed-rejoin backoff draws
     * (0.56.0), since it runs on this thread too. */
    uint32_t jitter_state =
        (uint32_t)(time(NULL) ^ ((uint32_t)getpid() << 16) ^
                   (uint32_t)g_cfg.port);
    while (!g_stop) {
        int n_suspect;
        pthread_mutex_lock(&g_lock);
        n_suspect = member_count_state(&g_table, MEMBER_SUSPECT);
        pthread_mutex_unlock(&g_lock);
        int interval = pulse_adaptive_interval_ms(g_cfg.probe_interval_ms,
                                                  g_cfg.probe_interval_fast_ms,
                                                  n_suspect);
        if (interval != current_interval_ms) {
            if (interval < g_cfg.probe_interval_ms)
                log_line("ADAPTIVE fast probe=%dms suspects=%d", interval,
                         n_suspect);
            else
                log_line("ADAPTIVE probe=%dms stable", interval);
            current_interval_ms = interval;
        }
        if (interval < g_cfg.probe_interval_ms) {
            pthread_mutex_lock(&g_lock);
            metrics_note_adaptive_fast(&g_metrics);
            pthread_mutex_unlock(&g_lock);
        }
        /* Jitter (0.50.0) applies to the sleep only: the ADAPTIVE log
         * and the fast-cycle count above still see the unjittered
         * interval, so a jittered cycle never looks like a switch
         * and never spams the log. The cycle-based cadences inside
         * probe_cycle inherit the jitter, which is the point — the
         * whole cycle drifts, not just the sleep. */
        int sleep_ms = pulse_jittered_interval_ms(
            interval, g_cfg.probe_jitter_frac, &jitter_state);
        struct timespec ts;
        ts.tv_sec = sleep_ms / 1000;
        ts.tv_nsec = (long)(sleep_ms % 1000) * 1000000L;
        nanosleep(&ts, NULL);
        if (g_stop) break;
        probe_cycle(++cycle, &jitter_state);
    }
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --port PORT [--id ID] [--seed HOST:PORT]...\n"
            "       [--peer-file FILE]\n"
            "       [--data-dir DIR] [--probe-interval-ms N]\n"
            "       [--probe-interval-fast-ms N]\n"
            "       [--probe-jitter-frac N]\n"
            "       [--ack-timeout-ms N] [--suspect-timeout-ms N]\n"
            "       [--slow-peer-rtt-ms N]\n"
            "       [--quiet-peer-ms N]\n"
            "       [--low-probe-rate-frac N]\n"
            "       [--flap-rate-threshold N]\n"
            "       [--dead-retention SECONDS]\n"
            "       [--phi-accrual] [--phi-threshold N]\n"
            "       [--sync-interval-ms N]\n"
            "       [--control-port PORT]\n"
            "       [--event-log-cap N]\n"
            "       [--gossip-digest-size N]\n"
            "       [--secret SECRET | --shared-secret SECRET]\n"
            "       [--meta KEY=VALUE]... (repeatable, at most 8)\n"
            "       [--ipv6] [--bind HOST]\n",
            prog);
}

int main(int argc, char **argv) {
    memset(&g_cfg, 0, sizeof g_cfg);
    g_cfg.port = -1;
    snprintf(g_cfg.data_dir, sizeof g_cfg.data_dir, ".");
    g_cfg.probe_interval_ms = 100;
    g_cfg.probe_interval_fast_ms = 0; /* default: half the base interval */
    g_cfg.probe_jitter_frac = 0.1;    /* default: ±10% probe jitter */
    g_cfg.ack_timeout_ms = 100;
    g_cfg.suspect_timeout_ms = 300;
    g_cfg.dead_retention_ms = 60000; /* 60 s */
    g_cfg.phi_accrual = 0;
    g_cfg.phi_threshold = PULSE_PHI_THRESHOLD_DEFAULT;
    g_cfg.sync_interval_ms = 10000; /* anti-entropy full-table exchange */
    g_cfg.slow_peer_rtt_ms = 0; /* slow-peer warning disabled */
    g_cfg.quiet_peer_ms = 0; /* quiet-peer warning disabled */
    g_cfg.low_probe_rate_frac = 0; /* low-probe-rate warning disabled */
    g_cfg.flap_rate_threshold = 0; /* flap-rate warning disabled */
    g_cfg.event_log_cap = 0; /* default WATCH event ring (128 events) */
    g_cfg.gossip_digest_size = 0; /* default gossip digest (16 entries) */
    g_cfg.control_port = -1; /* disabled unless --control-port is given */

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            g_cfg.port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            snprintf(g_cfg.id, sizeof g_cfg.id, "%s", argv[++i]);
            g_cfg.has_id = 1;
        } else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            if (g_cfg.seed_count < 16) {
                snprintf(g_cfg.seeds[g_cfg.seed_count++], PULSE_ID_LEN, "%s",
                         argv[++i]);
            } else {
                i++;
            }
        } else if (strcmp(argv[i], "--peer-file") == 0 && i + 1 < argc) {
            snprintf(g_cfg.peer_file, sizeof g_cfg.peer_file, "%s",
                     argv[++i]);
            if (g_cfg.peer_file[0] == '\0') {
                fprintf(stderr, "--peer-file must not be empty\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            snprintf(g_cfg.data_dir, sizeof g_cfg.data_dir, "%s", argv[++i]);
        } else if (strcmp(argv[i], "--probe-interval-ms") == 0 && i + 1 < argc) {
            g_cfg.probe_interval_ms = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--probe-interval-fast-ms") == 0 &&
                   i + 1 < argc) {
            g_cfg.probe_interval_fast_ms = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--probe-jitter-frac") == 0 &&
                   i + 1 < argc) {
            g_cfg.probe_jitter_frac = atof(argv[++i]);
            if (g_cfg.probe_jitter_frac < 0.0)
                g_cfg.probe_jitter_frac = 0.0;
            if (g_cfg.probe_jitter_frac > 0.9)
                g_cfg.probe_jitter_frac = 0.9;
        } else if (strcmp(argv[i], "--ack-timeout-ms") == 0 && i + 1 < argc) {
            g_cfg.ack_timeout_ms = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--suspect-timeout-ms") == 0 && i + 1 < argc) {
            g_cfg.suspect_timeout_ms = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--dead-retention") == 0 && i + 1 < argc) {
            int secs = atoi(argv[++i]);
            if (secs <= 0) {
                fprintf(stderr, "--dead-retention must be a positive number of seconds\n");
                return 2;
            }
            g_cfg.dead_retention_ms = (int64_t)secs * 1000;
        } else if (strcmp(argv[i], "--phi-accrual") == 0) {
            g_cfg.phi_accrual = 1;
        } else if (strcmp(argv[i], "--phi-threshold") == 0 && i + 1 < argc) {
            g_cfg.phi_threshold = atof(argv[++i]);
            if (g_cfg.phi_threshold <= 0) {
                fprintf(stderr, "--phi-threshold must be a positive number\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--sync-interval-ms") == 0 && i + 1 < argc) {
            g_cfg.sync_interval_ms = atoi(argv[++i]);
            if (g_cfg.sync_interval_ms <= 0) {
                fprintf(stderr, "--sync-interval-ms must be a positive number of milliseconds\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--slow-peer-rtt-ms") == 0 && i + 1 < argc) {
            g_cfg.slow_peer_rtt_ms = atoi(argv[++i]);
            if (g_cfg.slow_peer_rtt_ms < 0) {
                fprintf(stderr, "--slow-peer-rtt-ms must be 0 (disabled) or a positive number of milliseconds\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--quiet-peer-ms") == 0 && i + 1 < argc) {
            /* Quiet-peer early warning (0.62.0): warn when an ALIVE
             * peer's last-contact age crosses this many ms, so a
             * peer going quiet is visible before the suspicion
             * fires. 0 disables it, like --slow-peer-rtt-ms: the
             * right threshold depends on the probe interval and
             * the ack timeout, so there is no safe universal
             * default — it must sit above a healthy peer's normal
             * silence (a few probe intervals) and below the
             * suspicion delay (two ack timeouts in fixed-timeout
             * mode), or it never fires while the peer is ALIVE. */
            g_cfg.quiet_peer_ms = atoi(argv[++i]);
            if (g_cfg.quiet_peer_ms < 0) {
                fprintf(stderr, "--quiet-peer-ms must be 0 (disabled) or a positive number of milliseconds\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--low-probe-rate-frac") == 0 &&
                   i + 1 < argc) {
            /* Low-probe-rate warning (0.68.0): warn when an ALIVE
             * peer's recent probe success rate — answered / sent
             * over the last PULSE_PROBE_WINDOW direct probes —
             * falls below this fraction, so a peer answering one
             * probe in ten is visible while it is still ALIVE
             * instead of only after it is suspected. 0 disables
             * it, like --quiet-peer-ms: the right threshold
             * depends on how lossy the network is allowed to be,
             * so there is no safe universal default — it must
             * sit below a healthy peer's normal rate (near 1 on
             * a good link) and above 0, or it never fires. A
             * value above 1 would flag every peer that ever
             * missed a probe, so it is a clean startup error. */
            g_cfg.low_probe_rate_frac = atof(argv[++i]);
            if (g_cfg.low_probe_rate_frac < 0 ||
                g_cfg.low_probe_rate_frac > 1) {
                fprintf(stderr, "--low-probe-rate-frac must be 0 (disabled) or a fraction between 0 and 1\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--flap-rate-threshold") == 0 &&
                   i + 1 < argc) {
            /* Flap-rate warning (0.76.0): warn when an ALIVE
             * peer has completed at least this many flap cycles
             * in the last 10 minutes, so a peer dying and coming
             * back over and over is visible while it is still
             * ALIVE instead of only in the log after each death.
             * 0 disables it, like --low-probe-rate-frac: the
             * right threshold depends on how flappy the cluster
             * is allowed to be, so there is no safe universal
             * default — a peer that dies once an hour is an
             * incident, one that dies three times in ten minutes
             * is unstable. The count is read off a ring of the
             * last 16 flap times, so it saturates at 16: a
             * threshold above 16 could never fire, so it is a
             * clean startup error instead of a silent never. */
            g_cfg.flap_rate_threshold = atoi(argv[++i]);
            if (g_cfg.flap_rate_threshold < 0 ||
                g_cfg.flap_rate_threshold > PULSE_FLAP_HISTORY) {
                fprintf(stderr, "--flap-rate-threshold must be 0 (disabled) or a count between 1 and %d\n",
                        PULSE_FLAP_HISTORY);
                return 2;
            }
        } else if (strcmp(argv[i], "--control-port") == 0 && i + 1 < argc) {
            g_cfg.control_port = atoi(argv[++i]);
            if (g_cfg.control_port <= 0 || g_cfg.control_port > 65535) {
                fprintf(stderr, "--control-port must be a port number\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--event-log-cap") == 0 && i + 1 < argc) {
            /* Configurable WATCH event ring (0.36.0): how many
             * membership events the log holds for replay. A busy
             * cluster wants a bigger ring so a reconnecting
             * watcher can catch up further back; a small node can
             * hold less. 1 is the floor (a ring that holds nothing
             * can never replay), 100000 the ceiling (about 9 MB
             * of ring, at 88 bytes per event). */
            g_cfg.event_log_cap = atoi(argv[++i]);
            if (g_cfg.event_log_cap < 1 || g_cfg.event_log_cap > 100000) {
                fprintf(stderr,
                        "--event-log-cap must be between 1 and 100000\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--gossip-digest-size") == 0 &&
                   i + 1 < argc) {
            /* Configurable gossip digest size (0.41.0): how many
             * members one gossip round's digest covers. A large
             * cluster trades convergence speed for bandwidth by
             * shrinking it; a small one can grow it to the whole
             * table. 1 is the floor (a digest of 0 would gossip
             * nothing at all), 64 the ceiling (the table holds at
             * most 64 members, so a bigger digest buys nothing). */
            g_cfg.gossip_digest_size = atoi(argv[++i]);
            if (g_cfg.gossip_digest_size < 1 ||
                g_cfg.gossip_digest_size > PULSE_GOSSIP_ENTRIES_MAX) {
                fprintf(stderr,
                        "--gossip-digest-size must be between 1 and %d\n",
                        PULSE_GOSSIP_ENTRIES_MAX);
                return 2;
            }
        } else if ((strcmp(argv[i], "--secret") == 0 ||
                    strcmp(argv[i], "--shared-secret") == 0) &&
                   i + 1 < argc) {
            snprintf(g_cfg.secret, sizeof g_cfg.secret, "%s", argv[++i]);
            if (g_cfg.secret[0] == '\0') {
                fprintf(stderr, "--secret must not be empty\n");
                return 2;
            }
        } else if (strcmp(argv[i], "--meta") == 0 && i + 1 < argc) {
            char key[PULSE_META_KEY_LEN + 1];
            char value[PULSE_META_VAL_LEN + 1];
            if (member_meta_parse(argv[++i], key, sizeof key, value,
                                  sizeof value) != 0) {
                fprintf(stderr,
                        "--meta must be key=value (keys/values "
                        "[A-Za-z0-9._-], \"key=value\" at most %d chars)\n",
                        PULSE_META_MAX_LEN);
                return 2;
            }
            /* A repeated key updates the value instead of adding a
             * second pair. */
            int slot = -1;
            for (int j = 0; j < g_cfg.meta_count; j++) {
                if (strcmp(g_cfg.meta[j].key, key) == 0) {
                    slot = j;
                    break;
                }
            }
            if (slot < 0) {
                if (g_cfg.meta_count >= PULSE_MAX_META) {
                    fprintf(stderr, "--meta: at most %d annotations\n",
                            PULSE_MAX_META);
                    return 2;
                }
                slot = g_cfg.meta_count++;
            }
            snprintf(g_cfg.meta[slot].key,
                     sizeof g_cfg.meta[slot].key, "%s", key);
            snprintf(g_cfg.meta[slot].value,
                     sizeof g_cfg.meta[slot].value, "%s", value);
        } else if (strcmp(argv[i], "--ipv6") == 0) {
            g_cfg.ipv6 = 1;
        } else if (strcmp(argv[i], "--bind") == 0 && i + 1 < argc) {
            snprintf(g_cfg.bind_host, sizeof g_cfg.bind_host, "%s",
                     argv[++i]);
            if (g_cfg.bind_host[0] == '\0') {
                fprintf(stderr, "--bind must not be empty\n");
                return 2;
            }
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (g_cfg.port <= 0 || g_cfg.port > 65535) {
        usage(argv[0]);
        return 2;
    }

    /* Seedless bootstrap (0.23.0): peers listed in --peer-file join
     * the seed list, so they get exactly the --seed treatment — a
     * JOIN at startup, direct probing, and the ~5 s rejoin retry
     * while they are not ALIVE. A missing or unreadable file is a
     * clean startup error, not an empty peer list: silently starting
     * alone would hide a typo'd path and leave the node isolated. */
    g_cfg.seed_flag_count = g_cfg.seed_count;
    int n_peers = 0, n_peer_bad = 0, n_peer_dropped = 0;
    if (g_cfg.peer_file[0] != '\0') {
        char peers[16][PULSE_ID_LEN];
        if (pulse_peerfile_load(g_cfg.peer_file, peers, 16,
                                &n_peers, &n_peer_bad) != 0) {
            fprintf(stderr, "cannot read peer file %s: %s\n",
                    g_cfg.peer_file, strerror(errno));
            return 2;
        }
        for (int i = 0; i < n_peers; i++) {
            /* A peer already named with --seed stays a seed on the
             * strength of the flag: the file must not duplicate it
             * in the seed list, and a later reload must not drop it
             * when the file stops listing it. Record the file's own
             * seeds separately so the reload can diff against them. */
            int is_flag = 0;
            for (int j = 0; j < g_cfg.seed_flag_count; j++) {
                if (strcmp(peers[i], g_cfg.seeds[j]) == 0) {
                    is_flag = 1;
                    break;
                }
            }
            if (is_flag) continue;
            if (g_cfg.seed_count < 16) {
                copy_id(g_cfg.peer_file_seeds[g_cfg.peer_file_seed_count++],
                        peers[i]);
                copy_id(g_cfg.seeds[g_cfg.seed_count++], peers[i]);
            } else {
                n_peer_dropped++;
            }
        }
        if (n_peer_dropped > 0)
            fprintf(stderr,
                    "peer file %s: %d peers dropped, seed list full (16)\n",
                    g_cfg.peer_file, n_peer_dropped);
        /* Auto-reload watch (0.26.0): record the file's mtime now so
         * the prober only reloads when the file actually changes. */
        struct timespec mtime_ts;
        if (peer_file_mtime(&mtime_ts)) {
            g_peer_file_mtime = mtime_ts;
            g_peer_file_mtime_known = 1;
        }
        /* Peer-file state for METRICS and JSON (0.28.0): the initial
         * load counts as the first reload — added is the peers the
         * file contributed to the seed list (flag duplicates and
         * overflow-dropped peers never joined it). */
        pthread_mutex_lock(&g_lock);
        g_peerfile_state.have_file = 1;
        snprintf(g_peerfile_state.path, sizeof g_peerfile_state.path,
                 "%s", g_cfg.peer_file);
        g_peerfile_state.last_reload_unix = (int64_t)time(NULL);
        g_peerfile_state.last_mtime_unix = g_peer_file_mtime_known
                                               ? (int64_t)g_peer_file_mtime.tv_sec
                                               : 0;
        g_peerfile_state.peers = n_peers;
        g_peerfile_state.added = g_cfg.peer_file_seed_count;
        g_peerfile_state.dropped = 0;
        g_peerfile_state.kept = 0;
        g_peerfile_state.bad = n_peer_bad;
        pthread_mutex_unlock(&g_lock);
    }

    g_start_ms = pulse_now_ms();
    srand((unsigned)(time(NULL) ^ (long)getpid()));
    g_incarnation = load_incarnation(g_cfg.data_dir);
    gossip_set_secret(g_cfg.secret);

    /* Address family: an explicit --bind host decides it; otherwise
     * --ipv6 selects IPv6 and the default is IPv4. A node is one family
     * or the other — there is no dual-stack mode, so a v4 node and a v6
     * node cannot accidentally merge (see README, "IPv6"). */
    int family = AF_INET;
    struct sockaddr_storage bind_addr;
    memset(&bind_addr, 0, sizeof bind_addr);
    if (g_cfg.bind_host[0] != '\0') {
        if (member_parse_host(g_cfg.bind_host, &bind_addr) != 0) {
            fprintf(stderr, "bad --bind address: %s\n", g_cfg.bind_host);
            return 2;
        }
        family = bind_addr.ss_family;
    } else if (g_cfg.ipv6) {
        family = AF_INET6;
    }
    g_family = family;
    if (family == AF_INET6) {
        struct sockaddr_in6 *b6 = (struct sockaddr_in6 *)&bind_addr;
        b6->sin6_family = AF_INET6;
        b6->sin6_port = htons((uint16_t)g_cfg.port);
        /* bind_addr is otherwise all zeros (in6addr_any) unless --bind
         * set a specific host. */
    } else {
        struct sockaddr_in *b4 = (struct sockaddr_in *)&bind_addr;
        b4->sin_family = AF_INET;
        b4->sin_port = htons((uint16_t)g_cfg.port);
    }

    if (g_cfg.has_id) {
        snprintf(g_self_id, sizeof g_self_id, "%s", g_cfg.id);
    } else if (g_cfg.bind_host[0] != '\0') {
        /* Default id is the bind address: "[2001:db8::5]:17101". */
        member_format_id(&bind_addr, g_self_id, sizeof g_self_id);
    } else if (family == AF_INET6) {
        snprintf(g_self_id, sizeof g_self_id, "[::1]:%d", g_cfg.port);
    } else {
        snprintf(g_self_id, sizeof g_self_id, "127.0.0.1:%d", g_cfg.port);
    }

    g_sock = socket(family, SOCK_DGRAM, 0);
    if (g_sock < 0) {
        perror("socket");
        return 1;
    }
    int one = 1;
    setsockopt(g_sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (family == AF_INET6) {
        /* Pure IPv6: do not also accept IPv4-mapped traffic. A v6 node
         * is v6-only, so a v4 node cannot reach it through its IPv4
         * address and the two families stay separate clusters. */
        setsockopt(g_sock, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof one);
    }
    if (bind(g_sock, (struct sockaddr *)&bind_addr,
             pulse_addr_len(&bind_addr)) < 0) {
        perror("bind");
        return 1;
    }

    /* A dedicated sending socket. Some sandboxed environments refuse
     * sendto() on a UDP socket that has never been connect()ed, while a
     * connected socket may sendto() any destination. Connecting once
     * here (UDP connect only records a default destination; it sends
     * nothing and does not filter sendto) keeps gossip flowing there
     * and is a no-op on a normal host. The receiving socket stays
     * unconnected so it keeps accepting datagrams from any peer. */
    g_send_sock = socket(family, SOCK_DGRAM, 0);
    if (g_send_sock >= 0) {
        if (family == AF_INET6) {
            /* Same as the receiving socket: a v6 node is v6-only.
             * Without this, the send socket is dual-stack by default
             * and a v6 node could still reach a v4 peer (Linux maps
             * the IPv4 destination to ::ffff:a.b.c.d), so the two
             * families would half-merge: the v4 side would hear the
             * v6 node while the v6 side could never hear it back. */
            setsockopt(g_send_sock, IPPROTO_IPV6, IPV6_V6ONLY, &one,
                       sizeof one);
        }
        /* Connect to the first seed of our own family, if there is one;
         * a seed of the other family can never be reached from this
         * socket, so it is skipped, not connected to. With no usable
         * seed, connect to loopback (port 1, nothing listens there). */
        struct sockaddr_storage dst;
        memset(&dst, 0, sizeof dst);
        int have_dst = 0;
        for (int i = 0; i < g_cfg.seed_count; i++) {
            struct sockaddr_storage s;
            if (member_parse_addr(g_cfg.seeds[i], &s) == 0 &&
                s.ss_family == family) {
                dst = s;
                have_dst = 1;
                break;
            }
        }
        if (!have_dst) {
            if (family == AF_INET6) {
                struct sockaddr_in6 *d6 = (struct sockaddr_in6 *)&dst;
                d6->sin6_family = AF_INET6;
                d6->sin6_port = htons(1);
                inet_pton(AF_INET6, "::1", &d6->sin6_addr);
            } else {
                struct sockaddr_in *d4 = (struct sockaddr_in *)&dst;
                d4->sin_family = AF_INET;
                d4->sin_port = htons(1);
                inet_pton(AF_INET, "127.0.0.1", &d4->sin_addr);
            }
        }
        if (connect(g_send_sock, (struct sockaddr *)&dst,
                    pulse_addr_len(&dst)) < 0) {
            /* Non-fatal: plain sendto works on a normal host. */
            fprintf(stderr, "warning: send-socket connect failed: %s\n",
                    strerror(errno));
        }
    } else {
        /* Fall back to sending on the receiving socket. */
        g_send_sock = g_sock;
    }

    member_table_init(&g_table);
    metrics_init(&g_metrics);
    pulse_event_log_init(&g_event_log, g_cfg.event_log_cap);
    /* Event-log persistence (0.38.0): a restarted node loads the
     * events it recorded before its last shutdown back into the
     * ring, so sequence numbers continue instead of resetting to 1
     * and a WATCH FROM <seq> issued after a restart can still
     * replay what happened before it. The load is followed by a
     * compacting save, so the file holds only the last <cap>
     * events from here on; a missing file is the first-start case
     * (zero events), and an unreadable file is a warning, never a
     * failed start — the node just starts at sequence 1. */
    snprintf(g_events_path, sizeof g_events_path, "%s/events",
             g_cfg.data_dir);
    int n_events = pulse_event_log_load(&g_event_log, g_events_path);
    if (n_events > 0) {
        log_line("EVENTS %d restored", n_events);
        pulse_event_log_save(&g_event_log, g_events_path);
    } else if (n_events < 0) {
        fprintf(stderr, "warning: cannot read event log %s\n",
                g_events_path);
    }
    /* The address stored for self is the one the id advertises: peers
     * already reply to the advertised id, so the table entry should
     * agree with it. (An unparseable --id falls back to loopback.) */
    struct sockaddr_storage self_addr;
    if (member_parse_addr(g_self_id, &self_addr) != 0) {
        memset(&self_addr, 0, sizeof self_addr);
        if (family == AF_INET6) {
            struct sockaddr_in6 *s6 = (struct sockaddr_in6 *)&self_addr;
            s6->sin6_family = AF_INET6;
            s6->sin6_port = htons((uint16_t)g_cfg.port);
            inet_pton(AF_INET6, "::1", &s6->sin6_addr);
        } else {
            struct sockaddr_in *s4 = (struct sockaddr_in *)&self_addr;
            s4->sin_family = AF_INET;
            s4->sin_port = htons((uint16_t)g_cfg.port);
            inet_pton(AF_INET, "127.0.0.1", &s4->sin_addr);
        }
    }
    member_add(&g_table, g_self_id, &self_addr, g_incarnation);
    member_t *self = member_find(&g_table, g_self_id);
    self->last_seen_ms = g_start_ms;
    self->state_since_ms = g_start_ms;

    /* Member metadata: --meta flags win; without flags, restore the
     * annotations saved from the last run, so a restart keeps them
     * even when the operator forgets the flags. The effective set is
     * saved back, so a node started with flags persists them for the
     * next restart too. */
    if (g_cfg.meta_count == 0) {
        int n_saved = 0;
        if (pulse_meta_load(g_cfg.data_dir, g_cfg.meta, PULSE_MAX_META,
                            &n_saved) >= 0)
            g_cfg.meta_count = n_saved;
    }
    for (int i = 0; i < g_cfg.meta_count; i++)
        member_meta_set(self, g_cfg.meta[i].key, g_cfg.meta[i].value);
    if (g_cfg.meta_count > 0)
        pulse_meta_save(g_cfg.data_dir, g_cfg.meta, g_cfg.meta_count);

    /* Membership persistence: reload the peers this node knew before
     * its last shutdown, so a restarted node re-probes its old peers
     * instead of starting from only the seed list. Restored entries
     * are re-probed, never trusted (see persist.h); a restart with no
     * --seed still rejoins the cluster. Loaded before the seeds so a
     * seed that is also a restored peer keeps its restored entry. */
    int n_restored =
        pulse_members_load(g_cfg.data_dir, &g_table, g_self_id,
                           pulse_now_ms());
    if (n_restored > 0)
        log_line("RESTORE %d members", n_restored);

    for (int i = 0; i < g_cfg.seed_count; i++) {
        struct sockaddr_storage seed_addr;
        if (member_parse_addr(g_cfg.seeds[i], &seed_addr) != 0) {
            fprintf(stderr, "bad seed address: %s\n", g_cfg.seeds[i]);
            continue;
        }
        member_add(&g_table, g_cfg.seeds[i], &seed_addr, 0);
        /* A seed that never answers has no heartbeat history; the
         * phi-mode no-history guard measures silence from here. */
        member_t *seed = member_find(&g_table, g_cfg.seeds[i]);
        if (seed) seed->state_since_ms = g_start_ms;
        /* A startup JOIN is a one-shot merge message like a re-JOIN,
         * so it goes through the same one-retry send: the socket is
         * already connect()ed to the first seed, so when that seed is
         * down its ICMP port-unreachable is pending by the time the
         * loop reaches the second seed, and the second seed's JOIN
         * would be lost to the stale error (see
         * gossip_send_join_retry). The first seed's own JOIN cannot
         * fail that way — no send has happened yet, so no ICMP is
         * pending — but the retry is harmless there: it only runs
         * when the first send failed. */
        gossip_send_join_retry(g_send_sock, &seed_addr, g_self_id,
                               g_incarnation);
        /* Gossip traffic counters (0.80.0): the startup JOIN
         * counts as a JOIN sent to the seed. */
        note_traffic_sent(g_cfg.seeds[i], PULSE_TRAFFIC_JOIN);
    }

    /* The secret itself is never logged; only that a secret is on. */
    const char *sec = g_cfg.secret[0] != '\0' ? " secret=on" : "";
    const char *v6 = family == AF_INET6 ? " ipv6=on" : "";
    char slowbuf[32];
    if (g_cfg.slow_peer_rtt_ms > 0)
        snprintf(slowbuf, sizeof slowbuf, " slow-rtt=%lldms",
                 (long long)g_cfg.slow_peer_rtt_ms);
    else
        slowbuf[0] = '\0';
    char quietbuf[32];
    if (g_cfg.quiet_peer_ms > 0)
        snprintf(quietbuf, sizeof quietbuf, " quiet=%lldms",
                 (long long)g_cfg.quiet_peer_ms);
    else
        quietbuf[0] = '\0';
    char lowratebuf[32];
    if (g_cfg.low_probe_rate_frac > 0)
        snprintf(lowratebuf, sizeof lowratebuf, " low-rate=%.2f",
                 g_cfg.low_probe_rate_frac);
    else
        lowratebuf[0] = '\0';
    char flapratebuf[32];
    if (g_cfg.flap_rate_threshold > 0)
        snprintf(flapratebuf, sizeof flapratebuf, " flap-rate=%lld",
                 (long long)g_cfg.flap_rate_threshold);
    else
        flapratebuf[0] = '\0';
    if (g_cfg.phi_accrual) {
        log_line("START %s inc=%u port=%d phi=on threshold=%.1f%s%s%s%s%s%s", g_self_id,
                 g_incarnation, g_cfg.port, g_cfg.phi_threshold, sec, v6,
                 slowbuf, quietbuf, lowratebuf, flapratebuf);
    } else {
        log_line("START %s inc=%u port=%d%s%s%s%s%s%s", g_self_id, g_incarnation,
                 g_cfg.port, sec, v6, slowbuf, quietbuf, lowratebuf,
                 flapratebuf);
    }
    if (g_cfg.meta_count > 0) {
        char metabuf[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
        if (member_meta_format(self, metabuf, sizeof metabuf) < 0)
            metabuf[0] = '\0';
        log_line("META %s", metabuf);
    }
    if (g_cfg.peer_file[0] != '\0') {
        if (n_peer_bad > 0)
            log_line("PEERS %d from %s (%d bad lines skipped)", n_peers,
                     g_cfg.peer_file, n_peer_bad);
        else
            log_line("PEERS %d from %s", n_peers, g_cfg.peer_file);
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    /* Live peer-file reload (0.24.0): SIGHUP must survive repeated
     * delivery, so it needs sigaction, not signal. With this file's
     * strict _POSIX_C_SOURCE, glibc's signal() has System V
     * semantics: the handler resets to SIG_DFL after the first
     * delivery, and the second SIGHUP would kill the node. (SIGINT /
     * SIGTERM above are one-shot shutdowns, so the reset is harmless
     * there.) No SA_RESTART: the receiver already retries on EINTR,
     * and the prober's nanosleep returning early just runs a cycle. */
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_reload;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGHUP, &sa, NULL);

    pthread_t receiver, prober;
    if (pthread_create(&receiver, NULL, receiver_thread, NULL) != 0) {
        perror("pthread_create receiver");
        return 1;
    }
    if (pthread_create(&prober, NULL, prober_thread, NULL) != 0) {
        perror("pthread_create prober");
        return 1;
    }
    /* TCP control plane, if enabled: a separate thread serving the
     * hooks on the control port. The listen socket is shut down by
     * on_signal, which wakes the accept loop so the thread can join. */
    pthread_t control = 0;
    int have_control = 0;
    if (g_cfg.control_port > 0) {
        g_control_sock = control_listen(g_cfg.control_port, family);
        if (g_control_sock < 0) {
            fprintf(stderr, "control plane: listen on port %d failed: %s\n",
                    g_cfg.control_port, strerror(errno));
            return 1;
        }
        g_hooks.ctx = NULL;
        g_hooks.snapshot = snapshot_hook;
        g_hooks.metrics = metrics_hook;
        g_hooks.force_leave = force_leave_hook;
        g_hooks.reload_peer_file = reload_peer_file_hook;
        g_hooks.peer_file_state = peer_file_state_hook;
        g_hooks.watch_next = watch_next_hook;
        g_hooks.watch_latest_seq = watch_latest_seq_hook;
        g_hooks.watch_oldest_seq = watch_oldest_seq_hook;
        g_hooks.event_log_cap = watch_event_log_cap_hook;
        g_hooks.gossip_digest_size = gossip_digest_size_hook;
        g_hooks.phi_threshold = phi_threshold_hook;
        g_hooks.set_meta = set_meta_hook;
        g_hooks.phi_enabled = phi_enabled_hook;
        g_hooks.peer_source = peer_source_hook;
        g_hooks.slow_rtt_threshold_ms = g_cfg.slow_peer_rtt_ms;
        g_hooks.quiet_threshold_ms = g_cfg.quiet_peer_ms;
        g_hooks.low_probe_rate_frac = g_cfg.low_probe_rate_frac;
        g_hooks.flap_rate_threshold = g_cfg.flap_rate_threshold;
        g_hooks.self_id = g_self_id;
        g_hooks.start_ms = g_start_ms;
        if (pthread_create(&control, NULL, control_thread, NULL) != 0) {
            perror("pthread_create control");
            return 1;
        }
        have_control = 1;
        log_line("CONTROL %s:%d",
                 family == AF_INET6 ? "[::1]" : "127.0.0.1",
                 g_cfg.control_port);
    }
    while (!g_stop) pause();
    pthread_join(receiver, NULL);
    pthread_join(prober, NULL);
    if (have_control) pthread_join(control, NULL);
    /* Graceful leave: on a clean shutdown, tell every known peer we are
     * leaving, so they mark us DEAD at once instead of waiting ~500 ms
     * for the probe timeout to suspect us and the suspect timeout to
     * declare us dead. The announcement is a normal GOSSIP digest of
     * the table with our own entry marked DEAD at our current
     * incarnation: DEAD beats ALIVE at equal incarnation, so peers
     * merge it immediately (and log DEAD without a SUSPECT first). A
     * crash (SIGKILL, power loss) sends nothing, so the probe path
     * still covers unclean deaths; a restart rejoins at a higher
     * incarnation, which refutes the DEAD entry. */
    pthread_mutex_lock(&g_lock);
    member_table_t leave_copy = g_table;
    pthread_mutex_unlock(&g_lock);
    member_t *leave_self = member_find(&leave_copy, g_self_id);
    int n_notified = 0;
    if (leave_self) {
        leave_self->state = MEMBER_DEAD;
        leave_self->state_since_ms = pulse_now_ms();
        for (int i = 0; i < leave_copy.count; i++) {
            member_t *m = &leave_copy.members[i];
            if (strcmp(m->id, g_self_id) == 0) continue;
            if (m->state == MEMBER_DEAD) continue;
            if (gossip_send_digest(g_send_sock, &m->addr, g_self_id,
                                   g_incarnation, &leave_copy,
                                   0, NULL,
                                   gossip_digest_size_hook(NULL)) == 0)
                n_notified++;
            /* Gossip traffic counters (0.80.0): the leave
             * announcement is a GOSSIP sent to the peer — the
             * last datagram this process sends, counted on the
             * attempt like every other send. */
            note_traffic_sent(m->id, PULSE_TRAFFIC_GOSSIP);
        }
    }
    if (n_notified > 0)
        log_line("LEAVE %s", g_self_id);
    /* Clean shutdown: save the membership one last time, so the next
     * start restores the peers known at the moment of shutdown. */
    pthread_mutex_lock(&g_lock);
    member_table_t final_copy = g_table;
    pthread_mutex_unlock(&g_lock);
    pulse_members_save(g_cfg.data_dir, &final_copy, g_self_id,
                       pulse_now_ms());
    /* Clean shutdown: compact the persisted event log too, so the
     * file holds only the last <cap> events for the next start
     * instead of every event this run appended. */
    pulse_event_log_save(&g_event_log, g_events_path);
    log_line("STOP %s", g_self_id);
    close(g_sock);
    if (g_send_sock != g_sock) close(g_send_sock);
    if (g_control_sock >= 0) close(g_control_sock);
    return 0;
}
