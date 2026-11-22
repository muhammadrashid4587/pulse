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
#include <time.h>
#include <unistd.h>

#include "gossip.h"
#include "member.h"
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
    int probe_interval_ms;
    int ack_timeout_ms;
    int suspect_timeout_ms;
    int64_t dead_retention_ms; /* how long a DEAD entry stays in the table */
    int phi_accrual;           /* suspect on phi instead of fixed timeouts */
    double phi_threshold;      /* phi at which a peer is suspected */
    int control_port;          /* TCP control plane port, -1 = disabled */
    char secret[PULSE_SECRET_MAX]; /* shared secret for joins, "" = none */
    int ipv6;                  /* use IPv6 (bind ::, ids are "[::1]:port") */
    char bind_host[128];       /* bind address override, "" = wildcard */
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
static control_hooks_t g_hooks;
static int64_t g_start_ms = 0;
static volatile sig_atomic_t g_stop = 0;
static uint32_t g_seq = 1;
static pending_probe_t g_pending[MAX_PENDING];
static pending_indirect_t g_indirect[MAX_PENDING];

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

static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
    if (g_sock >= 0) shutdown(g_sock, SHUT_RDWR);
    if (g_control_sock >= 0) shutdown(g_control_sock, SHUT_RDWR);
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

/* Direct contact with a member: it is alive. Logs JOIN for a first sighting
 * and ALIVE when a suspected/dead member comes back. The address stored is
 * the one the member advertises in its id, never the datagram source. */
static void note_contact(const gmsg_t *msg,
                         const struct sockaddr_storage *addr) {
    int is_new, revived;
    pthread_mutex_lock(&g_lock);
    is_new = member_find(&g_table, msg->from_id) == NULL;
    revived = member_mark_alive(&g_table, msg->from_id, addr,
                                msg->from_inc, pulse_now_ms());
    pthread_mutex_unlock(&g_lock);
    if (is_new) {
        log_line("JOIN %s", msg->from_id);
    } else if (revived) {
        log_line("ALIVE %s", msg->from_id);
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
            pthread_mutex_lock(&g_lock);
            for (int i = 0; i < MAX_PENDING; i++) {
                if (g_pending[i].active && g_pending[i].seq == msg->seq &&
                    strcmp(g_pending[i].target_id, msg->from_id) == 0) {
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
            pthread_mutex_unlock(&g_lock);
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
            gossip_send_digest(g_send_sock, &from_addr, g_self_id, g_incarnation, &copy);
            break;
        }

        case GMSG_GOSSIP: {
            int refute = 0;
            char new_ids[8][PULSE_ID_LEN];
            int new_count = 0;
            /* State transitions learned from gossip (e.g. a peer's digest
             * says a member is DEAD, or a restarted member is ALIVE again
             * at a higher incarnation). Logged after unlock, like JOINs. */
            char changed_ids[8][PULSE_ID_LEN];
            member_state_t changed_states[8];
            int changed_count = 0;
            pthread_mutex_lock(&g_lock);
            int is_new = member_find(&g_table, msg->from_id) == NULL;
            int revived = member_mark_alive(&g_table, msg->from_id, &from_addr,
                                            msg->from_inc, pulse_now_ms());
            for (int i = 0; i < msg->entry_count; i++) {
                const member_t *e = &msg->entries[i];
                member_t *before = member_find(&g_table, e->id);
                member_state_t prev = before ? before->state : MEMBER_ALIVE;
                int r = member_merge(&g_table, g_self_id, e->id, &e->addr,
                                     e->incarnation, e->state, pulse_now_ms());
                if (r & MERGE_SELF_SUSPECTED) refute = 1;
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
                if (self) self->incarnation = g_incarnation;
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
            break;
        }

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

/* One probe cycle: ping a random peer, escalate timed-out probes to
 * indirect probes, suspect peers whose indirect probes also fail, sweep
 * stale suspects to dead, gossip a digest, and log a STATUS line. */
static void probe_cycle(int cycle) {
    int64_t now = pulse_now_ms();

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
    /* Mark suspects now, while holding the lock. */
    int changed_idx[8];
    int n_changed = 0;
    for (int i = 0; i < n_suspect; i++) {
        if (member_mark_suspect(&g_table, suspect_ids[i], now)) {
            changed_idx[n_changed++] = i;
        }
    }
    /* 3. Sweep stale suspects to dead. */
    char newly_dead[8][PULSE_ID_LEN];
    int n_dead = 0;
    for (int i = 0; i < g_table.count; i++) {
        member_t *m = &g_table.members[i];
        if (m->state == MEMBER_SUSPECT &&
            now - m->state_since_ms >= g_cfg.suspect_timeout_ms) {
            if (member_mark_dead(&g_table, m->id, now) && n_dead < 8) {
                snprintf(newly_dead[n_dead], PULSE_ID_LEN, "%s", m->id);
                n_dead++;
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
    /* 4. Gossip a digest to a random peer every other cycle. */
    member_table_t digest_copy;
    int have_digest = 0;
    struct sockaddr_storage digest_to;
    if (cycle % 2 == 0) {
        member_t *d = member_random_peer(&g_table, g_self_id);
        if (d) {
            digest_to = d->addr;
            digest_copy = g_table;
            have_digest = 1;
        }
    }
    /* 5. Periodic STATUS line. */
    int st_alive = 0, st_suspect = 0, st_dead = 0;
    if (cycle % 10 == 0) {
        st_alive = member_count_state(&g_table, MEMBER_ALIVE);
        st_suspect = member_count_state(&g_table, MEMBER_SUSPECT);
        st_dead = member_count_state(&g_table, MEMBER_DEAD);
    }
    pthread_mutex_unlock(&g_lock);

    for (int i = 0; i < n_actions; i++) {
        for (int j = 0; j < actions[i].n_inter; j++) {
            gossip_send_ping_req(g_send_sock, &actions[i].inter[j], actions[i].seq,
                                 actions[i].target_id, g_self_id);
        }
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
    if (have_digest)
        gossip_send_digest(g_send_sock, &digest_to, g_self_id, g_incarnation,
                           &digest_copy);
    if (cycle % 10 == 0) {
        log_line("STATUS alive=%d suspect=%d dead=%d total=%d", st_alive,
                 st_suspect, st_dead, st_alive + st_suspect + st_dead);
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

static int phi_enabled_hook(void *ctx) {
    (void)ctx;
    return g_cfg.phi_accrual;
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

static void *control_thread(void *arg) {
    (void)arg;
    control_serve(g_control_sock, &g_hooks);
    return NULL;
}

static void *prober_thread(void *arg) {
    (void)arg;
    int cycle = 0;
    while (!g_stop) {
        struct timespec ts;
        ts.tv_sec = g_cfg.probe_interval_ms / 1000;
        ts.tv_nsec = (long)(g_cfg.probe_interval_ms % 1000) * 1000000L;
        nanosleep(&ts, NULL);
        if (g_stop) break;
        probe_cycle(++cycle);
    }
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --port PORT [--id ID] [--seed HOST:PORT]...\n"
            "       [--data-dir DIR] [--probe-interval-ms N]\n"
            "       [--ack-timeout-ms N] [--suspect-timeout-ms N]\n"
            "       [--dead-retention SECONDS]\n"
            "       [--phi-accrual] [--phi-threshold N]\n"
            "       [--control-port PORT]\n"
            "       [--secret SECRET | --shared-secret SECRET]\n"
            "       [--ipv6] [--bind HOST]\n",
            prog);
}

int main(int argc, char **argv) {
    memset(&g_cfg, 0, sizeof g_cfg);
    g_cfg.port = -1;
    snprintf(g_cfg.data_dir, sizeof g_cfg.data_dir, ".");
    g_cfg.probe_interval_ms = 100;
    g_cfg.ack_timeout_ms = 100;
    g_cfg.suspect_timeout_ms = 300;
    g_cfg.dead_retention_ms = 60000; /* 60 s */
    g_cfg.phi_accrual = 0;
    g_cfg.phi_threshold = 8.0;
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
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            snprintf(g_cfg.data_dir, sizeof g_cfg.data_dir, "%s", argv[++i]);
        } else if (strcmp(argv[i], "--probe-interval-ms") == 0 && i + 1 < argc) {
            g_cfg.probe_interval_ms = atoi(argv[++i]);
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
    if (g_cfg.phi_accrual) {
        log_line("START %s inc=%u port=%d phi=on threshold=%.1f%s%s", g_self_id,
                 g_incarnation, g_cfg.port, g_cfg.phi_threshold, sec, v6);
    } else {
        log_line("START %s inc=%u port=%d%s%s", g_self_id, g_incarnation,
                 g_cfg.port, sec, v6);
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

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
        g_hooks.force_leave = force_leave_hook;
        g_hooks.phi_enabled = phi_enabled_hook;
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
    log_line("STOP %s", g_self_id);
    close(g_sock);
    if (g_send_sock != g_sock) close(g_send_sock);
    if (g_control_sock >= 0) close(g_control_sock);
    return 0;
}
