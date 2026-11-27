#ifndef PULSE_MEMBER_H
#define PULSE_MEMBER_H

#include <netinet/in.h>
#include <stdint.h>

#include "phi.h"

#define PULSE_MAX_MEMBERS 64
#define PULSE_ID_LEN 64
#define PULSE_MAX_TOMBSTONES 16

/* Recent-probe window (0.68.0): how many of the last direct
 * probes to a peer make up its recent success rate, and how many
 * of them must be in the window before the rate is judged at all.
 * Five is enough to separate "one probe missed" from "answering
 * one in five" without waiting for the whole window to fill. */
#define PULSE_PROBE_WINDOW 20
#define PULSE_PROBE_WINDOW_MIN 5

/* Per-member probe-rate history (0.71.0): how many of the last
 * sampled windowed probe success rates to keep per peer — the
 * same 16-sample ring phi_history uses, so a JSON consumer can
 * see a peer's rate fall over time instead of only its live
 * value. */

#define PULSE_PROBE_RATE_HISTORY 16

/* Member metadata/annotations (0.20.0): a small set of key=value pairs
 * a node carries about itself — role=worker, zone=us-east-1,
 * version=1.2.3 — gossiped with its membership entry, shown in the
 * control plane, and persisted across restarts. At most
 * PULSE_MAX_META annotations per member, each "key=value" at most
 * PULSE_META_MAX_LEN chars long. Keys and values use only
 * [A-Za-z0-9._-]: no spaces, commas, semicolons, colons or '=' inside
 * a key or value, so an annotation survives the wire format (entries
 * are space-separated, meta pairs comma-separated after a ';') and
 * needs no escaping in JSON. */
#define PULSE_MAX_META 8
#define PULSE_META_KEY_LEN 32
#define PULSE_META_VAL_LEN 32
#define PULSE_META_MAX_LEN 64 /* "key=value" at most this long */

typedef struct {
    char key[PULSE_META_KEY_LEN + 1];
    char value[PULSE_META_VAL_LEN + 1];
} meta_kv_t;

typedef enum {
    MEMBER_ALIVE = 0,
    MEMBER_SUSPECT = 1,
    MEMBER_DEAD = 2
} member_state_t;

typedef struct {
    char id[PULSE_ID_LEN]; /* "host:port" */
    uint32_t incarnation;  /* incarnation the member had when removed */
    int64_t removed_ms;    /* monotonic ms when the tombstone was recorded */
    /* Table version at which this tombstone was recorded: a digest to a
     * peer that already knows everything up to version V only carries
     * tombstones with changed_version > V. See member_table_t.version. */
    uint64_t changed_version;
} tombstone_t;

typedef struct {
    char id[PULSE_ID_LEN]; /* "host:port", IPv6 hosts bracketed: "[::1]:port" */
    struct sockaddr_storage addr;
    uint32_t incarnation;
    member_state_t state;
    int64_t last_seen_ms;   /* monotonic ms of last direct contact */
    int64_t state_since_ms; /* when the current state started */
    /* Table version at which this entry last changed (added, or a new
     * incarnation or state): a digest to a peer that already knows
     * everything up to version V only carries entries with
     * changed_version > V. See member_table_t.version. */
    uint64_t changed_version;
    /* Phi-accrual history for this peer: fed by every direct contact
     * (member_mark_alive). Gossip about a peer is not a heartbeat —
     * only a packet from the peer itself proves it is alive. */
    phi_accrual_t phi;
    /* Probe round-trip time for this peer (0.21.0): the last measured
     * direct-probe PING->ACK round trip in ms, and the running mean
     * over all answered probes. Local measurement only — never
     * gossiped, never persisted, and not a table-version change (like
     * last_seen_ms, it is not gossip content). rtt_ms is -1 until a
     * probe has been answered; self never probes itself, so self
     * stays -1. A peer answered only via an indirect probe still gets
     * a sample: the clock runs from our PING send to the ACK's
     * arrival, including the indirect leg, so it is an upper bound
     * on the direct RTT, not a direct measurement. */
    int64_t rtt_ms;
    int64_t rtt_sum_ms;
    int64_t rtt_samples;
    /* Worst probe round-trip time for this peer (0.64.0): the
     * largest single direct-probe PING->ACK round trip in ms over
     * all answered probes — the mean can hide one 60 ms probe
     * among sub-millisecond ones, the max cannot. -1 until a probe
     * has been answered, like rtt_ms. Local measurement only —
     * never gossiped, never persisted. */
    int64_t rtt_max_ms;
    /* Per-member probe counts (0.66.0): how many direct probes
     * this node sent to the peer (probes_sent) and how many of
     * them were answered (probes_answered) — the probe success
     * rate, answered / sent. Sent counts only direct PINGs from
     * the probe cycle, the same probes the RTT samples come from;
     * an indirect probe (a PING this node sends because another
     * node asked it to with PING_REQ) is not counted — its ACK
     * goes back to the requester and adds no RTT sample here
     * either, so counting it as sent would drag the rate down
     * for a probe this node never judged. Answered increments
     * with the RTT sample, in member_note_rtt, so it always
     * equals rtt_samples: a probe that was never answered adds
     * to sent and to nothing else. A peer that never answers —
     * a seed that never came up — shows sent climbing with
     * answered stuck at 0, which is the point: the rate says
     * "probed, never answered" instead of "never probed". Local
     * measurement only — never gossiped, never persisted, and
     * not a table-version change, like the RTT itself. */
    int64_t probes_sent;
    int64_t probes_answered;
    /* Recent-probe window (0.68.0): the outcomes of the last
     * PULSE_PROBE_WINDOW direct probes to this peer — the recent
     * probe success rate, answered / sent over the window,
     * instead of the lifetime rate probes_sent/probes_answered
     * gives. Each slot holds one probe's sequence number and
     * whether it was answered; a slot starts unanswered when
     * its probe goes out (member_note_probe_sent) and flips
     * when its ACK arrives (member_note_probe_answered, matched
     * by sequence number — probes to one peer overlap, so the
     * most recent unanswered slot would be the wrong one). A
     * probe whose ACK arrives after its slot slid out of the
     * window flips nothing: a very late ACK is not recent
     * evidence. The window is a ring: probe_window_head is the
     * next slot to write, probe_window_count how many slots are
     * filled (at most PULSE_PROBE_WINDOW). Local measurement
     * only — never gossiped, never persisted, and not a
     * table-version change, like the lifetime counts. */
    uint32_t probe_window_seq[PULSE_PROBE_WINDOW];
    int probe_window_answered[PULSE_PROBE_WINDOW];
    int probe_window_head;
    int probe_window_count;
    /* Probe-rate history (0.71.0): the last
     * PULSE_PROBE_RATE_HISTORY sampled windowed probe success
     * rates for this peer — the recent rate (answered / sent
     * over the last PULSE_PROBE_WINDOW direct probes) as it
     * stood at each probe cycle — as [[age_ms, rate], ...]
     * pairs in JSON, oldest first, age_ms the ms since this
     * node started. The ring mirrors phi's history: the
     * prober thread samples once per probe cycle, never a
     * control-plane read, so polling JSON never moves the
     * history it is reading. A sample is only taken while the
     * peer has at least one probe in its window — a peer never
     * probed has no rate to sample — and a DEAD peer's history
     * is frozen at the value it reached at suspicion, like
     * its phi history. Local measurement only — never
     * gossiped, never persisted, and not a table-version
     * change, like the window it samples. */
    double probe_rate_hist[PULSE_PROBE_RATE_HISTORY];
    int64_t probe_rate_hist_ms[PULSE_PROBE_RATE_HISTORY];
    int probe_rate_hist_count;
    int probe_rate_hist_head;
    /* Low-probe-rate warning (0.68.0): whether a LOWRATE log
     * line and event have already been emitted for the current
     * low-rate episode — an ALIVE peer whose windowed probe
     * success rate fell below the configured threshold. Set
     * when the rate first crosses below the threshold, cleared
     * when the rate recovers to the threshold or above, so a
     * persistently low peer warns once per episode instead of
     * once per probe cycle, and a peer that recovers and drops
     * again warns again. Clearing the flag is the recovery:
     * the probe cycle logs a RATEOK line and records a RATEOK
     * event for the peer (0.69.0), so the episode ends in the
     * log and on the WATCH stream instead of silently. Like
     * quiet_warned since 0.72.0 it is NOT cleared
     * on direct contact: one answered probe barely moves a
     * 20-probe window, so clearing on contact would re-fire
     * almost at once for a peer answering one probe in ten. */
    int low_rate_warned;
    /* Slow-peer warning (0.22.0): whether a SLOW log line has already
     * been emitted for the current slow episode — a peer whose mean
     * RTT is above the configured threshold. Set when the mean first
     * crosses the threshold, cleared when it drops back below, so a
     * persistently slow peer logs once per episode instead of once
     * per probe, and a peer that recovers and slows again warns
     * again. */
    int rtt_slow_warned;
    /* Quiet-peer warning (0.62.0): whether a QUIET log line and
     * event have already been emitted for the current quiet
     * episode — an ALIVE peer whose last-contact age crossed the
     * configured threshold. Set when the age first crosses the
     * threshold, cleared by the probe cycle when the peer is
     * heard from again — its age back at or below the threshold
     * (see node.c) — so a persistently quiet peer warns once
     * per episode instead of once per probe cycle, and a peer
     * that answers again and goes quiet again warns again.
     * Clearing the flag is the recovery: the probe cycle logs a
     * QUIETOK line and records a QUIETOK event for the peer
     * (0.72.0), so the episode ends in the log and on the WATCH
     * stream instead of silently — the mirror of RATEOK for the
     * low-probe-rate warning. Unlike low_rate_warned it is
     * cleared on contact age, not on a windowed rate: one
     * answered probe drops the age at once, so the recovery
     * fires on the next probe cycle after the peer answers. A
     * peer that goes SUSPECT while quiet keeps its flag until
     * it revives to ALIVE, so its QUIETOK fires only once the
     * recovery stuck. */
    int quiet_warned;
    /* Metadata the member carries about itself (role, zone, version,
     * ...): set at startup with --meta, changeable at runtime with
     * the control plane's META command, gossiped with the membership
     * entry, and persisted across restarts. meta_count 0 means the
     * member carries no annotations — which is also what an entry
     * learned from a node that predates annotations looks like, so a
     * merge never wipes stored annotations on the strength of an
     * entry that carries none (see member_merge_meta). */
    meta_kv_t meta[PULSE_MAX_META];
    int meta_count;
} member_t;

typedef struct {
    member_t members[PULSE_MAX_MEMBERS];
    int count;
    /* Membership-table version: bumped on every change to what gossip
     * would send (a member added, removed, or changing incarnation or
     * state; a tombstone recorded). Each entry records the version at
     * which it last changed, so a digest to a peer can carry only the
     * entries that changed since the version that peer last saw
     * (delta encoding, 0.16.0) instead of the whole table every round.
     * The version is local and monotonic; it is never sent on the wire
     * and never persists — a restarted node starts at 0 and its peers
     * fall back to a full SYNC, exactly as for a peer never seen. */
    uint64_t version;
    /* Tombstones: members garbage-collected while DEAD. Gossip about a
     * tombstoned id at an incarnation <= the recorded one is ignored, so
     * a peer that still holds the DEAD entry cannot resurrect it here.
     * A higher incarnation clears the tombstone (the member rejoined).
     * Tombstones expire after their retention so the table stays bounded;
     * by then every peer has had time to GC the entry itself. */
    tombstone_t tombstones[PULSE_MAX_TOMBSTONES];
    int tombstone_count;
} member_table_t;

int64_t pulse_now_ms(void);
void member_table_init(member_table_t *t);
/* Mark one entry as changed at the current table version: the entry's
 * gossip content changed outside the member_* mutation functions (the
 * refutation path in node.c bumps our own incarnation directly, and
 * the graceful-leave announcement marks our own entry DEAD on a
 * throwaway copy). Bumps the table version and stamps the entry. */
void member_note_changed(member_table_t *t, member_t *m);
/* How many entries (members plus tombstones) changed after
 * since_version: the size of the delta a digest to a peer at that
 * version would carry. The caller uses it to decide between a delta
 * digest and a full SYNC. */
int member_changed_count(const member_table_t *t, uint64_t since_version);
member_t *member_find(member_table_t *t, const char *id);
member_t *member_add(member_table_t *t, const char *id,
                     const struct sockaddr_storage *addr,
                     uint32_t incarnation);

/* Parse a "host:port" id into a sockaddr_storage. IPv4 ids are bare
 * ("127.0.0.1:17101"); IPv6 ids are bracketed ("[::1]:17101"). A bare
 * IPv6 literal ("::1:17101") also parses: the port is the part after
 * the last colon. Returns 0 on success. */
int member_parse_addr(const char *id, struct sockaddr_storage *addr);
/* Parse a bare host literal (no port) into a sockaddr_storage: an IPv4
 * or IPv6 address, port left 0. Returns 0 on success. */
int member_parse_host(const char *host, struct sockaddr_storage *addr);
/* Format an address back to an id: "127.0.0.1:17101" or "[::1]:17101". */
void member_format_id(const struct sockaddr_storage *addr, char *id,
                      size_t len);
/* sizeof(struct sockaddr_in) or sizeof(struct sockaddr_in6) for the
 * address's family; 0 for an unknown family. Use as the length argument
 * to sendto/connect/bind. */
socklen_t pulse_addr_len(const struct sockaddr_storage *addr);
/* 1 when two addresses are the same family, port and address bytes. */
int pulse_addr_equal(const struct sockaddr_storage *a,
                     const struct sockaddr_storage *b);

/* Merge results (bit flags). */
#define MERGE_NONE 0
#define MERGE_NEW 1
#define MERGE_CHANGED 2
#define MERGE_SELF_SUSPECTED 4

/* Merge one remote membership entry into the table, SWIM-style:
 * higher incarnation wins; at equal incarnation DEAD > SUSPECT > ALIVE.
 * Entries about self are never applied; if one claims self is SUSPECT or
 * DEAD at an incarnation >= our own, MERGE_SELF_SUSPECTED is returned so
 * the caller can refute by bumping its incarnation. */
int member_merge(member_table_t *t, const char *self_id, const char *id,
                 const struct sockaddr_storage *addr, uint32_t incarnation,
                 member_state_t state, int64_t now);
/* Merge metadata learned from gossip into an existing entry: when the
 * incoming annotations (meta_count > 0) differ from the stored ones,
 * replace them and stamp the entry changed, so the change re-gossips
 * onward; returns 1 when anything changed, 0 otherwise. An entry
 * that carries no annotations never wipes stored ones — a peer that
 * predates annotations, or one relaying an entry it learned without
 * them, simply has nothing to say about them — so a node that drops
 * all its annotations cannot propagate the removal; it can only
 * replace them. Entries about self are ignored: our own annotations
 * are ours to set. The caller passes the entry's id, not self's. */
int member_merge_meta(member_table_t *t, const char *self_id, const char *id,
                      const meta_kv_t *meta, int meta_count);

/* Metadata helpers. member_meta_parse splits "key=value" into the two
 * buffers and validates both (1..PULSE_META_KEY_LEN / VAL_LEN chars,
 * [A-Za-z0-9._-] only, "key=value" at most PULSE_META_MAX_LEN chars);
 * returns 0 on success, -1 on bad input. member_meta_set adds or
 * updates one pair on an entry: 0 ok, -1 invalid, -2 full (already
 * PULSE_MAX_META distinct keys). member_meta_format writes the
 * comma-separated "k=v,k=v" list ("" when there are none) and returns
 * the length, or -1 when the buffer is too small. member_meta_parse_list
 * parses such a list into out (at most max pairs): 0 ok, -1 on any bad
 * item. member_meta_equal is 1 when two entries carry the same set of
 * pairs, regardless of order. member_meta_json writes the JSON object
 * {"k":"v",...} ({} when there are none) — keys and values are
 * restricted to JSON-safe characters, so no escaping is needed. */
int member_meta_parse(const char *s, char *key, size_t klen, char *value,
                      size_t vlen);
int member_meta_set(member_t *m, const char *key, const char *value);
int member_meta_format(const member_t *m, char *buf, size_t len);
int member_meta_parse_list(const char *s, meta_kv_t *out, int max,
                           int *out_count);
int member_meta_equal(const member_t *a, const member_t *b);
void member_meta_json(const member_t *m, char *buf, size_t len);

int member_mark_suspect(member_table_t *t, const char *id, int64_t now);
int member_mark_dead(member_table_t *t, const char *id, int64_t now);
/* Direct contact: the member is alive. Returns 1 if the member was new or
 * its state changed, 0 if it was already ALIVE. */
int member_mark_alive(member_table_t *t, const char *id,
                      const struct sockaddr_storage *addr,
                      uint32_t incarnation, int64_t now);

/* Record that a direct probe was sent to a peer (0.66.0): the
 * probe cycle's PING — the same probe an answered ACK later
 * turns into an RTT sample. Returns 1 when the member exists
 * and the count was bumped, 0 otherwise. Does not bump the
 * table version: probe counts are local measurement, not gossip
 * content. The probe's sequence number (0.68.0) is recorded in
 * the recent-probe window as an unanswered slot, so the ACK can
 * flip exactly this probe's outcome when it arrives. */
void member_note_probe_sent(member_table_t *t, const char *id,
                            uint32_t seq);
/* Record that a direct probe was answered (0.68.0): the ACK for
 * the probe with this sequence number arrived, so its slot in
 * the recent-probe window flips to answered. A sequence with no
 * slot — the probe never went out through this node, or its slot
 * already slid out of the window — is ignored: a very late ACK
 * is not recent evidence. Unknown ids are ignored too. */
void member_note_probe_answered(member_table_t *t, const char *id,
                                uint32_t seq);
/* The recent probe success rate (0.68.0): answered / sent over
 * the last PULSE_PROBE_WINDOW direct probes to this peer, or -1
 * when no probe to the peer is in the window yet — an absent
 * value, never a fabricated zero: a peer never probed has no
 * rate to report. The window counts only direct probes, like
 * probes_sent / probes_answered: an indirect probe's ACK goes
 * to the requester and adds no sample here either. */
double member_probe_rate(const member_t *m);
/* Sample the peer's current windowed probe success rate into
 * its probe-rate history (0.71.0): one sample per probe cycle,
 * taken by the prober thread. Nothing is recorded when the peer
 * has no probe in its window yet (member_probe_rate < 0) — a
 * peer never probed has no rate to sample — and a second sample
 * in the same millisecond is skipped, so the ring holds
 * distinct times, not duplicates (the same guard phi_sample
 * uses). */
void member_sample_probe_rate(member_t *m, int64_t now);
/* The probe-rate history as JSON (0.71.0): the last
 * PULSE_PROBE_RATE_HISTORY sampled rates as [[age_ms, rate],
 * ...] pairs, oldest first, age_ms the ms since this node
 * started, rates to two decimals like probe_rate itself.
 * Returns the bytes written, or -1 when the buffer is too
 * small. An empty history writes "[]": the peer has been
 * probed but no probe cycle has sampled it yet. */
int member_probe_rate_history_json(const member_t *m, int64_t start_ms,
                                   char *buf, size_t len);
/* Latest and minimum of the sampled probe-rate history, and how
 * many samples the ring holds (0.74.0) — the same shape as
 * phi_history_stats, but the minimum instead of the maximum: a
 * probe rate fails downward, so the worst sample is the lowest
 * one, not the highest. The latest is the most recent
 * probe-cycle sample — not the live rate recomputed at read
 * time — and the minimum is over the whole bounded window, so a
 * reader can see both where the rate is now and how far it fell
 * while the peer went quiet. All three are 0 / 0 / 0 when no
 * probe cycle has sampled the peer yet (or for a NULL member). */
void member_probe_rate_history_stats(const member_t *m, double *latest,
                                     double *min, int *count);
/* Probe-rate-history histogram (0.74.0): bucket bounds for the
 * per-member probe-rate-history histogram in PROMETHEUS — a
 * healthy peer sits at 1.00 and the low-probe-rate warning fires
 * below --low-probe-rate-frac, so the buckets split the fall:
 * under 0.5, 0.75, 0.9, 0.95, then 1.00 itself. The last bucket
 * is +Inf, so the total bucket count is NBOUNDS + 1. */
#define PULSE_PROBE_RATE_HIST_BOUNDS { 0.5, 0.75, 0.9, 0.95, 1.0 }
#define PULSE_PROBE_RATE_HIST_NBOUNDS 5
/* Cumulative bucket counts over the sampled probe-rate history:
 * counts[b] is how many samples are <= bounds[b] (the last
 * entry, counts[n_bounds], is the +Inf bucket — every sample),
 * *sum is the sum of the sampled rates, and *count is how many
 * samples the ring holds (0 when no probe cycle has sampled it
 * yet). A sample exactly at a bound lands in that bucket, like
 * the detection-latency and probe-RTT histograms. All outputs
 * are 0 for a NULL member. */
void member_probe_rate_history_hist(const member_t *m, const double *bounds,
                                    int n_bounds, int *counts, double *sum,
                                    int *count);
/* 1 when the peer counts as low-probe-rate (0.68.0): the
 * low-probe-rate warning is enabled (threshold > 0), the peer
 * is ALIVE, at least PULSE_PROBE_WINDOW_MIN probes are in its
 * window, and its recent success rate is below the threshold.
 * The threshold is a strict less-than: a rate exactly at the
 * threshold is not low. Self never probes itself, so self has
 * no window and is never low; a peer never probed has no rate
 * and is never low. SUSPECT and DEAD peers are not low either
 * — a suspicion already says the peer is silent, so the warning
 * is only for a peer still counted ALIVE. */
int member_is_low_probe_rate(const member_t *m, double threshold_frac);
/* How many members in the table currently count as low-probe-rate
 * (see member_is_low_probe_rate), excluding self (self_id): self
 * never probes itself, so it has no window to judge. */
int member_count_low_probe_rate(const member_table_t *t,
                                const char *self_id,
                                double threshold_frac);
/* Record a probe round-trip time for a peer: the ms between sending a
 * PING (direct, or via PING_REQ) and receiving its ACK. Updates the
 * last RTT, the running mean and the worst RTT (0.64.0). slow_threshold_ms is the slow-peer
 * threshold in ms (0 = disabled): when the peer's mean RTT first
 * crosses above it, returns 2 so the caller can warn once for the
 * episode (see rtt_slow_warned); the warning clears when the mean
 * drops back to or below the threshold, so a recovery followed by a
 * new crossing warns again — the sample that clears a warned episode
 * returns 3, so the caller can log the recovery once too (0.73.0).
 * Returns 1 when the member exists and the sample was recorded
 * without a new crossing or a recovery, 0 otherwise. Does not
 * bump the table version: RTT is local measurement, not gossip
 * content. A negative sample is ignored. */
int member_note_rtt(member_table_t *t, const char *id, int64_t rtt_ms,
                    int64_t slow_threshold_ms);
/* Mean probe RTT in ms over all answered probes, or -1 when the peer
 * has answered none. */
int64_t member_rtt_mean_ms(const member_t *m);
/* Worst probe RTT in ms over all answered probes (0.64.0), or -1
 * when the peer has answered none. */
int64_t member_rtt_max_ms(const member_t *m);
/* 1 when the peer counts as slow: the slow-peer warning is enabled
 * (threshold > 0), the peer has answered at least one probe, and its
 * mean RTT is above the threshold. Self never probes itself, so self
 * is never slow. */
int member_is_slow(const member_t *m, int64_t slow_threshold_ms);
/* How many members in the table currently count as slow (see
 * member_is_slow). */
int member_count_slow(const member_table_t *t, int64_t slow_threshold_ms);
/* 1 when the peer counts as quiet (0.62.0): the quiet-peer warning
 * is enabled (threshold > 0), the peer is ALIVE, and its
 * last-contact age is above the threshold — this node has not heard
 * from it directly for longer than the configured quiet window. A
 * peer never heard from has no age (-1), so it is never quiet: a
 * seed that never answered is a different problem, and a zero would
 * claim nothing. SUSPECT and DEAD peers are not quiet either — a
 * suspicion already says the peer is silent, so the warning is
 * only for a peer still counted ALIVE. Self is excluded by the
 * caller: self's last_seen is startup time, so its age would be
 * the node's uptime, not a contact age (see member_last_seen_age_ms). */
int member_is_quiet(const member_t *m, int64_t quiet_threshold_ms,
                    int64_t now);
/* How many members in the table currently count as quiet (see
 * member_is_quiet), excluding self (self_id): self's last-contact
 * age is the node's uptime, so counting it would report a quiet
 * peer on every node that has been up longer than the threshold. */
int member_count_quiet(const member_table_t *t, const char *self_id,
                       int64_t quiet_threshold_ms, int64_t now);
/* How long a member has been SUSPECT, in ms (0.58.0): now minus the
 * time its current state started. -1 when the member is not
 * SUSPECT — an ALIVE member has no suspicion to age, and a DEAD
 * member's suspicion is over, so its age is not a suspicion age.
 * The value is local measurement: for a suspicion learned from
 * gossip it is the time since this node learned of it, not since
 * the detecting node suspected it. */
int64_t member_suspect_age_ms(const member_t *m, int64_t now);
/* How long since this node last heard from a member, in ms
 * (0.59.0): now minus the member's last_seen_ms — the last direct
 * contact (a probe ACK or a gossip message carrying the member).
 * -1 when the member was never heard from (last_seen_ms 0): an
 * absent value, not a zero — a zero would claim contact just now.
 * Self is excluded by the caller: self never probes itself, so
 * its last_seen is just startup time and the age would be the
 * node's uptime, not a contact age. A DEAD member keeps its last
 * contact time until GC, so its age keeps growing — how long it
 * has been silent, which is the point. */
int64_t member_last_seen_age_ms(const member_t *m, int64_t now);

int member_count_state(const member_table_t *t, member_state_t state);
/* Force-remove a member (operator action, e.g. the control plane's
 * LEAVE): drop the entry from the table and record a tombstone so
 * gossip cannot resurrect it at the recorded incarnation or below.
 * A live member that hears about its own removal refutes by bumping
 * its incarnation and comes back; a dead one stays gone. Returns 1
 * if the member was present, 0 if unknown. */
int member_force_remove(member_table_t *t, const char *id, int64_t now);
/* Garbage-collect DEAD members whose state is older than
 * dead_retention_ms. Each removal records a tombstone so gossip cannot
 * resurrect the entry, and tombstones older than
 * tombstone_retention_ms are dropped. Removed ids are written into
 * `removed` (up to max_removed); returns the number removed. */
int member_gc(member_table_t *t, int64_t now, int64_t dead_retention_ms,
              int64_t tombstone_retention_ms,
              char removed[][PULSE_ID_LEN], int max_removed);
/* Random ALIVE or SUSPECT peer other than self, or NULL if there is none.
 * The returned pointer is only valid while the caller holds the table lock. */
member_t *member_random_peer(member_table_t *t, const char *self_id);
/* Gossip fanout for a cluster with peer_count gossipable peers
 * (other members, excluding self and DEAD entries): 1 + floor(log2(n)),
 * capped at 4. See member.c for the reasoning. */
int member_gossip_fanout(int peer_count);

const char *member_state_name(member_state_t s);
char member_state_code(member_state_t s);
member_state_t member_state_from_code(char c);

#endif
