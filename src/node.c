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
    revived = member_mark_alive(&g_table, msg->from_id, addr,
                                msg->from_inc, now);
    if (is_new) metrics_note_join(&g_metrics);
    if (revived && prev == MEMBER_SUSPECT)
        metrics_note_suspect_refuted(&g_metrics, now - since);
    pthread_mutex_unlock(&g_lock);
    if (is_new) {
        log_line("JOIN %s", msg->from_id);
    } else if (revived) {
        log_line("ALIVE %s", msg->from_id);
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
    int new_count = 0;
    /* State transitions learned from gossip (e.g. a peer's digest
     * says a member is DEAD, or a restarted member is ALIVE again
     * at a higher incarnation). Logged after unlock, like JOINs. */
    char changed_ids[8][PULSE_ID_LEN];
    member_state_t changed_states[8];
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
    int revived = member_mark_alive(&g_table, msg->from_id, from_addr,
                                    msg->from_inc, now);
    if (is_new) metrics_note_join(&g_metrics);
    if (revived && sender_prev == MEMBER_SUSPECT)
        metrics_note_suspect_refuted(&g_metrics, now - sender_since);
    if (is_new || revived) learned = 1;
    for (int i = 0; i < msg->entry_count; i++) {
        const member_t *e = &msg->entries[i];
        member_t *before = member_find(&g_table, e->id);
        member_state_t prev = before ? before->state : MEMBER_ALIVE;
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
            new_count++;
        }
        if ((r & MERGE_CHANGED) && changed_count < 8) {
            member_t *after = member_find(&g_table, e->id);
            if (after && after->state != prev) {
                snprintf(changed_ids[changed_count], PULSE_ID_LEN,
                         "%s", e->id);
                changed_states[changed_count] = after->state;
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
    } else if (revived) {
        log_line("ALIVE %s", msg->from_id);
    }
    for (int i = 0; i < new_count; i++)
        log_line("JOIN %s", new_ids[i]);
    for (int i = 0; i < changed_count; i++) {
        if (changed_states[i] == MEMBER_DEAD)
            log_line("DEAD %s", changed_ids[i]);
        else if (changed_states[i] == MEMBER_ALIVE)
            log_line("ALIVE %s", changed_ids[i]);
        else
            log_line("SUSPECT %s", changed_ids[i]);
    }
    if (refute)
        log_line("REFUTE %s inc=%u", g_self_id, g_incarnation);
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
        if (gossip_send_full_table(g_send_sock, from_addr, g_self_id,
                                   g_incarnation, &copy) == 0)
            peer_set_known_version(msg->from_id, copy.version);
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
             * configured threshold. Log once per episode (the flag
             * in the table clears on recovery, so a re-crossing
             * warns again). The log happens after the unlock: the
             * log lock is separate, but there is no reason to hold
             * the table lock across IO. */
            int64_t slow_mean_ms = -1;
            if (rtt_ms >= 0) {
                int r = member_note_rtt(&g_table, msg->from_id, rtt_ms,
                                        g_cfg.slow_peer_rtt_ms);
                if (r == 2) {
                    member_t *slow = member_find(&g_table, msg->from_id);
                    if (slow) slow_mean_ms = member_rtt_mean_ms(slow);
                }
            }
            pthread_mutex_unlock(&g_lock);
            if (slow_mean_ms >= 0)
                log_line("SLOW %s mean=%lldms threshold=%lldms",
                         msg->from_id, (long long)slow_mean_ms,
                         (long long)g_cfg.slow_peer_rtt_ms);
            note_contact(msg, &from_addr);
            if (has_requester) {
                /* Forward the ACK to the node that asked us to probe. */
                gossip_send_ack(g_send_sock, &requester, msg->seq,
                                msg->from_id, msg->from_inc);
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

    for (int i = 0; i < n_joins; i++)
        gossip_send_join(g_send_sock, &joins[i].addr, g_self_id,
                         g_incarnation);
    for (int i = 0; i < n_added; i++) {
        /* An added peer with no room never joined the seed list; the
         * summary line counts it as skipped, not added. */
        if (i < n_added - n_full)
            log_line("PEERFILE ADD %s", added[i]);
    }
    for (int i = 0; i < n_dropped; i++)
        log_line("PEERFILE DROP %s", dropped[i]);
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
 * stale suspects to dead, gossip a digest, and log a STATUS line. */
static void probe_cycle(int cycle) {
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
     * out (it is the stimulus that produces heartbeats), but no pending
     * entry is created: suspicion comes from phi, not from a probe
     * timeout, so there is nothing to escalate. */
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
        if (g_cfg.phi_accrual) {
            send = 1;
        } else {
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
                send = 1;
            }
        }
        pthread_mutex_unlock(&g_lock);
        if (send)
            gossip_send_ping(g_send_sock, &peer_addr, seq, g_self_id,
                             g_incarnation);
    }

    /* 1b. Re-JOIN seeds that are not ALIVE. A node that starts while its
     * seed is down sends JOIN once at startup and would otherwise stay
     * isolated forever: it suspects, deads and GCs the seed, and the
     * seed, once up, has no reason to contact it. Every ~5 s, re-send
     * JOIN to each seed that is not currently ALIVE, so the cluster
     * heals when the seed comes up. The seed is deliberately NOT
     * re-added to the table here: it comes back through the normal
     * path (the seed answers the JOIN with a full-table SYNC, and direct
     * contact re-adds it), so a seed that never answers — wrong
     * secret, wrong family, genuinely dead — stays GC'd instead of
     * cycling back into the table. A seed of the other address
     * family is skipped: it can never be reached from this socket. */
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
        if (cycle % retry_every == 0 && g_cfg.seed_count > 0) {
            pthread_mutex_lock(&g_lock);
            for (int i = 0; i < g_cfg.seed_count && n_rejoins < 16; i++) {
                member_t *m = member_find(&g_table, g_cfg.seeds[i]);
                if (m && m->state == MEMBER_ALIVE) continue;
                struct sockaddr_storage saddr;
                if (member_parse_addr(g_cfg.seeds[i], &saddr) != 0) continue;
                if (saddr.ss_family != g_family) continue;
                int was_absent = (m == NULL);
                snprintf(rejoins[n_rejoins].id, PULSE_ID_LEN, "%s",
                         g_cfg.seeds[i]);
                rejoins[n_rejoins].addr = saddr;
                rejoins[n_rejoins].was_absent = was_absent;
                n_rejoins++;
            }
            pthread_mutex_unlock(&g_lock);
        }
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
    if (g_cfg.phi_accrual) {
        for (int i = 0; i < g_table.count; i++) {
            member_t *m = &g_table.members[i];
            if (strcmp(m->id, g_self_id) == 0) continue;
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
    int n_changed = 0;
    for (int i = 0; i < n_suspect; i++) {
        member_t *m = member_find(&g_table, suspect_ids[i]);
        int64_t latency = m ? now - m->last_seen_ms : -1;
        if (member_mark_suspect(&g_table, suspect_ids[i], now)) {
            changed_idx[n_changed++] = i;
            metrics_note_suspect(&g_metrics, latency);
        }
    }
    /* 3. Sweep stale suspects to dead. */
    char newly_dead[8][PULSE_ID_LEN];
    int n_dead = 0;
    for (int i = 0; i < g_table.count; i++) {
        member_t *m = &g_table.members[i];
        if (m->state == MEMBER_SUSPECT &&
            now - m->state_since_ms >= g_cfg.suspect_timeout_ms) {
            int64_t duration = now - m->state_since_ms;
            if (member_mark_dead(&g_table, m->id, now)) {
                metrics_note_dead(&g_metrics, duration);
                if (n_dead < 8) {
                    snprintf(newly_dead[n_dead], PULSE_ID_LEN, "%s", m->id);
                    n_dead++;
                }
            }
        }
    }
    /* 3b. Garbage-collect DEAD entries older than the retention period.
     * Tombstones live twice as long: a peer that learned the DEAD entry
     * later than we did still has up to one retention period to GC it
     * itself, and the tombstone stops its gossip from resurrecting the
     * entry here in the meantime. */
    char gc_ids[8][PULSE_ID_LEN];
    int n_gc = member_gc(&g_table, now, g_cfg.dead_retention_ms,
                         2 * g_cfg.dead_retention_ms, gc_ids, 8);
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
        }
    }
    for (int i = 0; i < n_rejoins; i++) {
        gossip_send_join(g_send_sock, &rejoins[i].addr, g_self_id,
                         g_incarnation);
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
    }
    for (int i = 0; i < n_dead; i++)
        log_line("DEAD %s", newly_dead[i]);
    for (int i = 0; i < n_gc; i++)
        log_line("GC %s", gc_ids[i]);
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
            if (!found || changed > PULSE_GOSSIP_ENTRIES) {
                if (gossip_send_full_table(g_send_sock, &digest_to[i],
                                           g_self_id, g_incarnation,
                                           &digest_copy) == 0)
                    peer_set_known_version(digest_ids[i],
                                           digest_copy.version);
            } else {
                uint64_t through = known;
                if (gossip_send_digest(g_send_sock, &digest_to[i],
                                       g_self_id, g_incarnation,
                                       &digest_copy, known,
                                       &through) == 0)
                    peer_set_known_version(digest_ids[i], through);
            }
        }
    }
    if (have_sync) {
        if (gossip_send_full_table(g_send_sock, &sync_to, g_self_id,
                                   g_incarnation, &sync_copy) == 0)
            peer_set_known_version(sync_id, sync_copy.version);
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
    pthread_mutex_lock(&g_lock);
    removed = member_force_remove(&g_table, id, pulse_now_ms());
    pthread_mutex_unlock(&g_lock);
    if (removed) log_line("LEAVE %s", id);
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
     * ~5 s seed rejoin, the anti-entropy SYNC, the gossip round every
     * other cycle) are counted in cycles, so they speed up in wall
     * time along with the loop while the cluster is unstable — that
     * is the point. */
    int current_interval_ms = g_cfg.probe_interval_ms;
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
        struct timespec ts;
        ts.tv_sec = interval / 1000;
        ts.tv_nsec = (long)(interval % 1000) * 1000000L;
        nanosleep(&ts, NULL);
        if (g_stop) break;
        probe_cycle(++cycle);
    }
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --port PORT [--id ID] [--seed HOST:PORT]...\n"
            "       [--peer-file FILE]\n"
            "       [--data-dir DIR] [--probe-interval-ms N]\n"
            "       [--probe-interval-fast-ms N]\n"
            "       [--ack-timeout-ms N] [--suspect-timeout-ms N]\n"
            "       [--slow-peer-rtt-ms N]\n"
            "       [--dead-retention SECONDS]\n"
            "       [--phi-accrual] [--phi-threshold N]\n"
            "       [--sync-interval-ms N]\n"
            "       [--control-port PORT]\n"
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
    g_cfg.ack_timeout_ms = 100;
    g_cfg.suspect_timeout_ms = 300;
    g_cfg.dead_retention_ms = 60000; /* 60 s */
    g_cfg.phi_accrual = 0;
    g_cfg.phi_threshold = 8.0;
    g_cfg.sync_interval_ms = 10000; /* anti-entropy full-table exchange */
    g_cfg.slow_peer_rtt_ms = 0; /* slow-peer warning disabled */
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
        } else if (strcmp(argv[i], "--control-port") == 0 && i + 1 < argc) {
            g_cfg.control_port = atoi(argv[++i]);
            if (g_cfg.control_port <= 0 || g_cfg.control_port > 65535) {
                fprintf(stderr, "--control-port must be a port number\n");
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
        gossip_send_join(g_send_sock, &seed_addr, g_self_id, g_incarnation);
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
    if (g_cfg.phi_accrual) {
        log_line("START %s inc=%u port=%d phi=on threshold=%.1f%s%s%s", g_self_id,
                 g_incarnation, g_cfg.port, g_cfg.phi_threshold, sec, v6,
                 slowbuf);
    } else {
        log_line("START %s inc=%u port=%d%s%s%s", g_self_id, g_incarnation,
                 g_cfg.port, sec, v6, slowbuf);
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
        g_hooks.set_meta = set_meta_hook;
        g_hooks.phi_enabled = phi_enabled_hook;
        g_hooks.peer_source = peer_source_hook;
        g_hooks.slow_rtt_threshold_ms = g_cfg.slow_peer_rtt_ms;
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
                                   0, NULL) == 0)
                n_notified++;
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
    log_line("STOP %s", g_self_id);
    close(g_sock);
    if (g_send_sock != g_sock) close(g_send_sock);
    if (g_control_sock >= 0) close(g_control_sock);
    return 0;
}
