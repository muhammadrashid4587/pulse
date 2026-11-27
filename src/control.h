#ifndef PULSE_CONTROL_H
#define PULSE_CONTROL_H

#include <stdint.h>

#include "events.h"
#include "member.h"
#include "metrics.h"

/* TCP control plane for pulse-node: a tiny line-based protocol on a
 * separate TCP port, one command per connection, plain text in and out
 * (netcat-friendly). Commands:
 *
 *   MEMBERS
 *       One line per member, then END:
 *       MEMBER <id> <incarnation> <ALIVE|SUSPECT|DEAD> <phi|'-'>
 *           <annotations|'-'>
 *       phi is the peer's current phi value (two decimals) when
 *       phi-accrual is on and the peer has heartbeat history, '-'
 *       otherwise (including self, which has no phi history).
 *       Annotations are the member's comma-separated "k=v,k=v" list,
 *       '-' when it carries none.
 *   PEERS
 *       One line per member, then END:
 *       PEER <id> <incarnation> <ALIVE|SUSPECT|DEAD> <last-seen-ms|'-'>
 *           <seed|peer-file|discovered|self>
 *       last-seen-ms is how many ms since this node last heard
 *       directly from the peer; '-' for self (it never probes
 *       itself) and for a peer never heard from (a seed that never
 *       answered). The source says where the peer came from: a
 *       --seed flag, the --peer-file roster, a peer removed from
 *       the file but kept because it was still ALIVE, or gossip /
 *       a JOIN from a peer the node was never told about
 *       (discovered). A peer named with both --seed and --peer-file
 *       reports as seed: the flag outranks the file.
 *   META <key=value>
 *       Set one annotation on this node itself (at most 8): OK
 *       <key=value>. The change is gossiped to the cluster with the
 *       next digest and persisted to the data dir, so it survives a
 *       restart. Bad syntax: ERR invalid meta <arg>; a ninth distinct
 *       key: ERR meta full (max 8); missing argument:
 *       ERR usage: META <key=value>.
 *   RELOAD
 *       Re-read --peer-file and merge it into the seed list — the
 *       same reload SIGHUP triggers, for an operator who cannot send
 *       the node a signal: OK peers=N added=A dropped=D kept=K
 *       bad=B. peers is how many peers the file lists; added/dropped/
 *       kept are the merge outcome (a still-ALIVE peer removed from
 *       the file is kept, never dropped on the strength of a file
 *       edit alone); bad is how many lines were skipped as malformed.
 *       No --peer-file: ERR no peer file. Unreadable file:
 *       ERR cannot read peer file <path>: <reason>, and the old list
 *       is kept. See "Live peer-file reload".
 *   STATUS
 *       STATUS id=<self-id> uptime=<seconds> alive=N suspect=N dead=N
 *       total=N, then END.
 *   LEAVE <member-id>
 *       Force-remove a member from the table: OK <id>. The removal is
 *       recorded as a tombstone and gossiped as DEAD, so peers mark the
 *       member DEAD and GC it on their own retention timer; a live
 *       member refutes by bumping its incarnation and comes back.
 *       Unknown member: ERR unknown member <id>. Self: ERR cannot
 *       leave self. Missing id: ERR usage: LEAVE <member-id>.
 *   METRICS
 *       One line of failure-detector metrics, then END:
 *       METRICS suspected=N dead=N refutes=N joins=N
 *           adaptive_fast_cycles=N
 *           detect_latency_ms mean=X max=Y samples=N
 *           suspect_to_dead_ms mean=X max=Y samples=N
 *           slow_peers=N quiet_peers=N low_probe_rate_peers=N
 *           peer_file=<path|'-'> last_reload=<unix|'-'>
 *           last_mtime=<unix|'-'> pf_peers=<N|'-'> pf_added=<N|'-'>
 *           pf_dropped=<N|'-'> pf_kept=<N|'-'> pf_bad=<N|'-'>
 *           watch_subscribers=<N> watch_sessions=<N>
 *           watch_delivered=<N> watch_filtered=<N> watch_dropped=<N>
 *           watch_event_log_cap=<N> gossip_digest_size=<N>
 *           phi_threshold=<N.NN>
 *       detect_latency_ms is last-direct-contact -> SUSPECT for
 *       suspicions this node detected itself (mean/max in ms, "-" when
 *       there are no samples); suspect_to_dead_ms is SUSPECT -> DEAD.
 *       adaptive_fast_cycles counts probe cycles that ran at the fast
 *       interval because a peer was SUSPECT (see "Adaptive probe
 *       interval").
 *       Transitions learned from gossip bump the counts but add no
 *       latency sample, so samples can be smaller than the counts.
 *       slow_peers is how many members currently count as slow: mean
 *       probe RTT above --slow-peer-rtt-ms (0 when the warning is
 *       disabled; see "Slow-peer warning").
 *       quiet_peers (0.62.0) is how many members currently count as
 *       quiet: ALIVE with a last-contact age above --quiet-peer-ms
 *       (0 when the warning is disabled; see "Quiet-peer early
 *       warning"). low_probe_rate_peers (0.68.0) is how many
 *       members currently count as low-probe-rate: ALIVE with a
 *       recent probe success rate below --low-probe-rate-frac (0
 *       when the warning is disabled; see "Low-probe-rate
 *       alert").
 *       The peer-file fields (0.28.0) are the --peer-file state: the
 *       path, the wall-clock time of the last successful load or
 *       reload, the file's mtime at that load, and the counts from
 *       that load (the same numbers as the PEERFILE RELOAD log line).
 *       All of them are "-" when the node runs without --peer-file.
 *       watch_subscribers (0.33.0) is how many WATCH streams are
 *       connected right now, and watch_sessions is how many WATCH
 *       sessions the node has served since startup — so an operator
 *       can see how many consumers are attached to the event
 *       stream, and whether any ever were. A client that went away
 *       drops out of the current count within about a second; the
 *       session count only ever grows.
 *       Delivery / drop counts (0.34.0): watch_delivered is how
 *       many EVENT lines WATCH streams have sent in total,
 *       watch_filtered is how many events streams consumed but
 *       skipped because their type/id filter excluded them, and
 *       watch_dropped is how many events streams lost because
 *       they had already scrolled out of the bounded event ring
 *       before the stream could consume them — a replay GAP, or a
 *       mid-stream gap that ends the stream with an ERR. All three
 *       are lifetime totals, including what live streams have
 *       done so far; JSON also carries the same three counts per
 *       currently-connected stream, so an operator can tell which
 *       consumer is falling behind instead of only that some
 *       consumer is. A stream that filters heavily is not a
 *       stream in trouble: filtered events were excluded on
 *       purpose, while a growing dropped count means a consumer
 *       is not keeping up with the cluster.
 *       watch_paused (0.37.0) is how many WATCH streams are
 *       paused right now: a paused stream is still connected and
 *       still counts as a subscriber, it just is not consuming
 *       events — see "WATCH PAUSE / WATCH RESUME" below.
 *       watch_event_log_cap (0.36.0) is how many events the WATCH
 *       event ring holds — the --event-log-cap the node started
 *       with, 128 by default — so an operator can see how far
 *       back a replay can reach before a GAP, and whether the
 *       node is running a bigger or smaller ring than the
 *       default. JSON carries the same cap, and PROMETHEUS as
 *       pulse_watch_event_log_cap.
 *       gossip_digest_size (0.41.0) is how many members one gossip
 *       round's digest covers — the --gossip-digest-size the node
 *       started with, 16 by default — so an operator can see
 *       whether the node is trading convergence speed for
 *       bandwidth. JSON carries the same size as a top-level
 *       "gossip_digest_size", and PROMETHEUS as
 *       pulse_gossip_digest_size.
 *       phi_threshold (0.45.0) is the phi value at which a peer is
 *       suspected — the --phi-threshold the node started with,
 *       8.0 by default, two decimals like PROMETHEUS's
 *       pulse_phi_threshold gauge — so a text-protocol consumer
 *       can judge a rising PHI line against the threshold that
 *       actually fires the suspicion instead of hard-coding 8.0.
 *       It is emitted always, like the other config gauges,
 *       whether or not --phi-accrual is on.
 *       Per-seed seed-rejoin breakdown (0.54.0): the seed_rejoins
 *       total rides with "seed_rejoins_by_seed=<id>=N,<id>=N"
 *       right after it — one entry per seed that has been
 *       re-joined, in first-rejoin order, "-" when no seed has
 *       been re-joined yet — so an operator polling the text
 *       protocol can see which seed a node keeps rejoining
 *       instead of only how often the retry fires; the counts
 *       sum to seed_rejoins. Per-seed last rejoin (0.55.0):
 *       "seed_last_rejoin_ms_by_seed=<id>=N,<id>=N" rides right
 *       after the breakdown — the wall-clock time of each seed's
 *       most recent re-JOIN, as unix milliseconds, "-" under the
 *       same rule — so an operator can also tell when each seed
 *       was last retried; a seed that was never re-joined has no
 *       entry in either, never a fabricated zero. Per-seed
 *       rejoin backoff (0.56.0):
 *       "seed_rejoin_backoff_ms_by_seed=<id>=N,<id>=N" rides
 *       right after the last-rejoin timestamps — the delay in ms
 *       the node will wait before the next re-JOIN to each seed,
 *       "-" under the same rule; 0 while the seed is ALIVE (no
 *       backoff pending), the scheduled delay while it is down
 *       and being backed off.
 *       Per-member phi (0.44.0): after the METRICS line, one
 *       "PHI <id> <value>" line per member that has a phi — phi
 *       mode on, not self, heartbeat history with at least
 *       PULSE_PHI_MIN_SAMPLES intervals — the live phi at reply
 *       time, two decimals like JSON's "phi" and PROMETHEUS's
 *       pulse_member_phi, so the phi trio is complete: an
 *       operator polling the text protocol can watch a peer's
 *       phi rise without parsing JSON or scraping Prometheus.
 *       A node without --phi-accrual emits no PHI line at all,
 *       and a peer never heard from has none either; a dead
 *       member keeps its last-known phi until GC.
 *       Suspicion age per member (0.58.0): after the PHI lines,
 *       one "SUSPECT <id> <ms>" line per SUSPECT member — how
 *       long the member has been suspected, in ms, the same
 *       value JSON reports as "suspect_ms" and PROMETHEUS as
 *       pulse_member_suspect_ms. The age grows while the member
 *       sits SUSPECT and the member drops off the list the
 *       moment it revives or goes DEAD; ALIVE and DEAD members
 *       have no line, so an absent line says "not suspected".
 *       Time since last contact per member (0.59.0): after the
 *       SUSPECT lines, one "LASTSEEN <id> <ms>" line per member
 *       heard from — how long since this node last heard from
 *       the member, in ms, the same value JSON reports as
 *       "last_seen_ms" and PROMETHEUS as
 *       pulse_member_last_seen_ms. Self has no line (its
 *       last_seen is just startup time) and a peer never heard
 *       from has none either; a DEAD member keeps its line
 *       until GC, the age growing — how long it has been silent.
 *       Per-member probe RTT (0.63.0): after the LASTSEEN
 *       lines, one "RTT <id> <last_ms> <mean_ms>" line per
 *       member that has answered a probe — the last and mean
 *       direct-probe round-trip time in ms, the same values
 *       JSON reports as "rtt_ms" / "rtt_mean_ms" and
 *       PROMETHEUS as pulse_member_rtt_ms /
 *       pulse_member_rtt_mean_ms, so the RTT trio is complete:
 *       an operator polling the text protocol can watch a
 *       peer's RTT climb without parsing JSON or scraping
 *       Prometheus. Self and a peer never heard from have no
 *       line (rtt_ms is -1 until a probe is answered); a 0 is
 *       a real sub-millisecond probe, not a missing one; a
 *       DEAD member keeps its last RTT until GC.
 *       Per-member probe counts (0.66.0): after the RTT lines,
 *       one "PROBES <id> <sent> <answered>" line per member
 *       that has been probed — how many direct probes this node
 *       sent to the peer and how many were answered, the probe
 *       success rate, the same values JSON reports as
 *       "probes_sent" / "probes_answered" and PROMETHEUS as
 *       pulse_member_probes_sent_total /
 *       pulse_member_probes_answered_total. A peer probed but
 *       never answered still has a line (sent climbing,
 *       answered 0); self and a peer never probed have none;
 *       a DEAD member keeps its counts until GC.
 *       Low-probe-rate warning (0.68.0): after the PROBES lines,
 *       one "LOWRATE <id> <rate>" line per member that counts
 *       as low-probe-rate — ALIVE, at least
 *       PULSE_PROBE_WINDOW_MIN direct probes in its window, and
 *       a recent success rate below --low-probe-rate-frac — the
 *       rate itself, two decimals, the same value JSON reports
 *       as "probe_rate" and PROMETHEUS as
 *       pulse_member_probe_rate. A peer whose rate is at or
 *       above the threshold, a peer never probed, and a SUSPECT
 *       or DEAD peer have no line; self never probes itself and
 *       has none either.
 *   JSON
 *       STATUS, MEMBERS and METRICS combined into one machine-readable
 *       JSON object on a single line, then END. Member phi is a number
 *       when phi-accrual is on and the peer has heartbeat history,
 *       null otherwise (including self). Latency means are null when
 *       there are no samples; max is 0 then. Each member also carries
 *       "meta": its annotations as a JSON object ({} when it has
 *       none), "rtt_ms" / "rtt_mean_ms" (null until a probe has been
 *       answered), "slow" (true when the peer's mean probe RTT is
 *       above --slow-peer-rtt-ms, false otherwise), "suspect_ms"
 *       (0.58.0: how long the member has been SUSPECT, in ms — null
 *       unless the member is SUSPECT — so a consumer can judge how
 *       close it is to the suspect timeout), "last_seen_ms"
 *       (0.59.0: how long since this node last heard from the
 *       member, in ms — null for self and for a peer never heard
 *       from — so a consumer can see a peer going quiet before
 *       the suspicion fires, and how long a DEAD member has been
 *       silent), "source" (where
 *       the peer came from: "seed", "peer-file", "discovered" or
 *       "self" — the same provenance PEERS reports, so a peer-file
 *       peer is distinguishable from a discovered one),
 *       "probes_sent" / "probes_answered" (0.66.0: how many
 *       direct probes this node sent to the peer and how many
 *       were answered — the probe success rate — always numbers,
 *       never null: 0 sent for self and for a peer never probed,
 *       sent climbing with answered 0 for a peer probed but
 *       never answered; answered always equals the number of
 *       RTT samples), "low_probe_rate" (0.68.0: true when the
 *       peer is ALIVE and its recent probe success rate is
 *       below --low-probe-rate-frac, false otherwise — self and
 *       a peer never probed are always false) and "probe_rate"
 *       (0.68.0: the recent success rate itself, answered /
 *       sent over the last PULSE_PROBE_WINDOW direct probes,
 *       two decimals — null until the first probe to the peer
 *       goes out, and always null for self — so a consumer can
 *       see how far below the threshold a flagged peer sits,
 *       and a DEAD member keeps its last window rate until GC),
 *       and the
 *       metrics object carries "slow_peers" (how many members are
 *       slow) and "quiet_peers" (0.62.0: how many members are
 *       quiet) and "low_probe_rate_peers" (0.68.0: how many
 *       members count as low-probe-rate).
 *       (0.54.0): the per-seed breakdown of "seed_rejoins" as an
 *       object — {"127.0.0.1:17101":2} — or {} when no seed has
 *       been re-joined yet, so a JSON consumer can see which seed
 *       a node keeps rejoining. The metrics object also carries
 *       "seed_last_rejoin_ms" (0.55.0): the same shape, but the
 *       wall-clock time of each seed's most recent re-JOIN as unix
 *       milliseconds — {"127.0.0.1:17101":1700000100000} — or {}
 *       when no seed has been re-joined yet, so a JSON consumer
 *       can tell when each seed was last retried; a seed that was
 *       never re-joined has no key in either. The metrics object
 *       also carries "seed_rejoin_backoff_ms" (0.56.0): the same
 *       shape, but the delay in ms the node will wait before the
 *       next re-JOIN to each seed — {"127.0.0.1:17101":10000} —
 *       or {} when no seed has been re-joined yet; 0 while the
 *       seed is ALIVE, the scheduled delay while it is down. A top-level "peer_file" object carries the --peer-file
 *       state (path, last_reload / last_mtime as unix seconds, and
 *       the counts from the last load), or null when the node runs
 *       without --peer-file. A top-level "watch" object (0.33.0)
 *       carries "subscribers" (how many WATCH streams are connected
 *       right now) and "sessions" (how many WATCH sessions the node
 *       has served since startup), plus the delivery / drop counts
 *       (0.34.0): "delivered" (EVENT lines sent in total),
 *       "filtered" (events skipped by a stream's filter) and
 *       "dropped" (events lost to a gap in the event ring), and
 *       "paused" (0.37.0, how many WATCH streams are paused right
 *       now — a paused stream is still connected, it just is not
 *       consuming events), and
 *       "event_log_cap" (0.36.0, how many events the WATCH ring
 *       holds — the --event-log-cap the node started with, 128 by
 *       default), and a "streams" array with the same three
 *       counts per currently-connected stream, keyed by its
 *       "session" number, plus "paused" per stream. A top-level
 *       "gossip_digest_size" (0.41.0) carries how many members
 *       one gossip round's digest covers — the
 *       --gossip-digest-size the node started with, 16 by
 *       default. A top-level "phi_threshold" (0.46.0) carries
 *       the phi value at which a peer is suspected — the
 *       --phi-threshold the node started with, 8.0 by default,
 *       two decimals like METRICS and PROMETHEUS — emitted
 *       always, whether or not --phi-accrual is on, so a JSON
 *       consumer can judge a rising per-member "phi" against
 *       the threshold that actually fires the suspicion.
 *   PROMETHEUS
 *       The same values METRICS and JSON report, as Prometheus text
 *       exposition format, then END. Each metric family carries a
 *       "# HELP" and "# TYPE" line: membership counts, slow peers,
 *       uptime and current watch subscribers as gauges; the
 *       lifetime transition totals (suspected, dead, refutes,
 *       joins, adaptive fast cycles, suspect refuted, watch
 *       sessions and the watch delivery / drop counts) as counters;
 *       the seed-rejoin counter (0.53.0, per-seed 0.54.0) rides
 *       as "pulse_seed_rejoin_total" with one labelled sample per
 *       seed that has been re-joined
 *       ("pulse_seed_rejoin_total{seed=\"<id>\"} N") — a seed never
 *       re-joined has no line, and a node that re-joined nothing
 *       emits the HELP/TYPE pair with no samples — so a scraper
 *       can alert on a single seed that keeps being rejoined;
 *       the wall-clock time of each seed's most recent re-JOIN
 *       rides as "pulse_seed_last_rejoin_ms" (0.55.0), one
 *       labelled gauge sample per re-joined seed, as unix
 *       milliseconds — a seed never re-joined has no line, never
 *       a fabricated zero — so a scraper can tell a retry that is
 *       still firing from one that stopped when the cluster
 *       merged; the delay before the next re-JOIN to each seed
 *       rides as "pulse_seed_rejoin_backoff_ms" (0.56.0), one
 *       labelled gauge sample per re-joined seed, in ms — 0
 *       while the seed is ALIVE, the scheduled delay while it is
 *       down — so a scraper can tell a seed still at the 5 s base
 *       from one backed off to the 30 s cap; the event ring capacity (pulse_watch_event_log_cap, 0.36.0)
 *       as a gauge; the paused-stream count (pulse_watch_paused,
 *       0.37.0) as a gauge; the gossip digest size
 *       (pulse_gossip_digest_size, 0.41.0) as a gauge; the phi
 *       suspicion threshold (pulse_phi_threshold, 0.43.0) as a
 *       gauge — the --phi-threshold the node started with, 8.0
 *       by default, so a scraper can alert on a rising
 *       pulse_member_phi against the threshold that actually
 *       fires the suspicion; the three
 *       latency groups as mean/max/samples
 *       and max are 0 with no samples — Prometheus has no null —
 *       and the samples gauge says whether the mean is real); the
 *       detection latency also as a real histogram (the same five
 *       buckets METRICS reports as hist, cumulative, with _sum and
 *       _count). Each member rides as a "pulse_member_info" gauge
 *       (1, labelled by member, state and source). The last and
 *       mean probe RTT per member ride as "pulse_member_rtt_ms"
 *       and "pulse_member_rtt_mean_ms" gauges (0.40.0), one line
 *       per member that has answered a probe. How many direct
 *       probes each member was sent and answered rides as
 *       "pulse_member_probes_sent_total" and
 *       "pulse_member_probes_answered_total" counters (0.66.0),
 *       one labelled sample per member that has been probed — a
 *       peer probed but never answered still has a sample (sent
 *       climbing, answered 0); self and a peer never probed have
 *       none; a DEAD member keeps its counters until GC. How long each
 *       SUSPECT member has been suspected rides as a
 *       "pulse_member_suspect_ms" gauge (0.58.0), one line per
 *       SUSPECT member — the same value JSON reports as
 *       "suspect_ms" and METRICS as a SUSPECT line — so a scraper
 *       can alert on a suspicion nearing the suspect timeout
 *       instead of only on the DEAD transition; ALIVE and DEAD
 *       members have no line. How long since this node last heard
 *       from each member rides as a "pulse_member_last_seen_ms"
 *       gauge (0.59.0), one line per member heard from — the same
 *       value JSON reports as "last_seen_ms" and METRICS as a
 *       LASTSEEN line — so a scraper can alert on a peer going
 *       quiet before the suspicion fires; self and a peer never
 *       heard from have no line, and a DEAD member keeps its
 *       gauge until GC, the value growing. How many members
 *       count as low-probe-rate rides as a "pulse_low_probe_rate_peers"
 *       gauge (0.68.0) — ALIVE members whose recent probe success
 *       rate is below --low-probe-rate-frac, 0 when the warning
 *       is disabled — and the rate itself rides as a
 *       "pulse_member_probe_rate" gauge, one line per member with
 *       at least one direct probe in its window, two decimals
 *       like JSON's "probe_rate" — so a scraper can alert on a
 *       peer answering one probe in ten instead of only on the
 *       count; self and a peer never probed have no line, and a
 *       DEAD member keeps its last window rate until GC. The phi
 *       failure-detector value per member rides as a
 *       "pulse_member_phi" gauge (0.42.0), one line per member
 *       that has a phi — phi mode on, not self, heartbeat history
 *       with at least PULSE_PHI_MIN_SAMPLES intervals — two
 *       decimals like JSON; a node without --phi-accrual emits no
 *       such line. The peer-file
 *       gauges (peers / added / dropped / kept / bad from the last
 *       load, last_reload / last_mtime as unix seconds) are only
 *       emitted when the node runs with --peer-file: an absent
 *       metric is how Prometheus says "not applicable". This is
 *       the exposition format, not an HTTP endpoint — scrape it
 *       with pulse-ctl, or point a scraper at a tiny exporter
 *       that forwards the command.
 *   WATCH [types] [ids] [FROM <seq>]
 *       A long-lived stream of membership events, for tooling that
 *       watches a cluster instead of polling MEMBERS or JSON once a
 *       second. The reply starts with "OK watching", then one
 *       SNAPSHOT line per member — id, incarnation, state,
 *       last-seen-ms (or '-') and source, the PEERS shape — then
 *       END, then one EVENT line per membership event as it happens:
 *       EVENT <seq> <JOINED|SUSPECTED|DEAD|REVIVED|DROPPED|REFUTED|QUIET|LOWRATE>
 *       <id> <incarnation> <unix-seconds>. JOINED is a peer
 *       appearing in the table, SUSPECTED is ALIVE -> SUSPECT, DEAD
 *       is SUSPECT -> DEAD, REVIVED is a SUSPECT/DEAD peer coming
 *       back ALIVE, DROPPED is a peer leaving the table (GC after
 *       its retention, a LEAVE, or a peer-file reload drop),
 *       REFUTED is this node hearing itself SUSPECT/DEAD and bumping
 *       its incarnation, QUIET (0.62.0) is an ALIVE peer whose
 *       last-contact age crossed the quiet-peer threshold — the
 *       peer is still alive, it has just gone quiet — and LOWRATE
 *       (0.68.0) is an ALIVE peer whose recent probe success rate
 *       fell below the low-probe-rate threshold — the peer is
 *       still alive, it has just started answering too few of its
 *       probes. The connection
 *       stays open until the client closes it (or falls too far
 *       behind: the event log is a bounded ring, so a slow consumer
 *       gets "ERR gap: events lost, reconnect" and must re-read the
 *       snapshot). Sequence numbers
 *       are 1-based and monotonic for the node's life; a jump in
 *       them means events were missed. The snapshot is taken after
 *       the latest sequence is read, so no event is ever lost
 *       between the two — an event in that window may appear both
 *       in the snapshot and in the stream, and a client dedupes by
 *       sequence number. WATCH runs on its own thread, so other
 *       control-plane commands keep answering while a client
 *       watches. No watch hooks: ERR watch not supported.
 *       A SUSPECTED event this node raised in phi-accrual mode
 *       carries a trailing " phi=<value>" (0.60.0) — the phi that
 *       crossed the threshold, two decimals. An event whose member
 *       was heard from carries a trailing " last_seen_ms=<ms>"
 *       (0.61.0) — the ms since this node last heard from the
 *       member when the event was recorded, the same value JSON
 *       reports as "last_seen_ms", frozen at the event so a
 *       replay still shows how quiet the peer was when the
 *       transition happened — after the phi field when both are
 *       present. Self, a member never heard from, and a member
 *       already gone from the table when the event was recorded
 *       carry no field: an absent value, never a fabricated
 *       zero. An event whose member was probed carries trailing
 *       " probes_sent=<n> probes_answered=<n>" (0.67.0) — how
 *       many direct probes this node had sent the member and how
 *       many were answered when the event was recorded, the same
 *       values JSON reports as "probes_sent" / "probes_answered"
 *       and METRICS as a PROBES line, frozen at the event so a
 *       replay still shows how many probes went unanswered before
 *       a SUSPECTED fired — after the last_seen_ms field when it
 *       is present. Self, a member never probed, and a member
 *       already gone from the table when the event was recorded
 *       carry no probe fields, under the same rule. A LOWRATE
 *       event carries a trailing " probe_rate=<value>" (0.68.0)
 *       — the recent probe success rate when the threshold was
 *       crossed, two decimals, the same value JSON reports as
 *       "probe_rate", frozen at the event so a replay still shows
 *       how low the rate was — after the probe counts when they
 *       are present; no other event carries one. An event with
 *       no trailing field writes the plain five fields, so
 *       existing consumers see byte-identical lines.
 *       A filter (0.30.0) narrows the stream server-side: an
 *       optional type list, an id list, or both, in any order —
 *       "WATCH JOINED,DEAD" (only those event types),
 *       "WATCH 127.0.0.1:17102" (only that peer), or
 *       "WATCH JOINED,DEAD 127.0.0.1:17102" (both). A token with a
 *       ':' is an id list (comma-separated host:port, IPv6
 *       bracketed); a token without one is a type list
 *       (comma-separated EVENT words). The snapshot is narrowed by
 *       the id list too, so a watcher subscribed to one peer sees
 *       only that peer; a type filter does not narrow the snapshot,
 *       which has no event type. A filtered-out event still
 *       consumes its sequence number, so a filtered stream's
 *       numbers jump by design — a jump means filtered-out events,
 *       not lost ones. A bad filter is refused up front:
 *       ERR invalid watch filter <reason>.
 *       A state filter (0.39.0) narrows the stream by the member
 *       state an event leaves its member in: "WATCH STATE
 *       SUSPECT,DEAD" carries only events that land on SUSPECT
 *       or DEAD. JOINED, REVIVED, REFUTED and QUIET land on ALIVE,
 *       SUSPECTED on SUSPECT, DEAD on DEAD, and DROPPED lands on
 *       no state — the member left the table — so it never
 *       matches a state filter; a watcher that wants removals
 *       subscribes to the DROPPED type instead. The state filter
 *       narrows the snapshot too, by the member's current state,
 *       and composes with the type/id filters and FROM, in any
 *       order. A second STATE is refused, like a second FROM,
 *       and a bad state is refused up front with "ERR invalid
 *       watch filter unknown member state <name>".
 *       Replay / resume (0.31.0): "WATCH FROM <seq>" replays the
 *       buffered events from that sequence number — in order,
 *       after the snapshot and END, before live events — so a
 *       client that reconnects can catch up instead of starting
 *       from now. FROM 0 replays from the oldest buffered event.
 *       A FROM past the latest sequence starts live (nothing to
 *       replay yet). A FROM below the oldest buffered event — the
 *       requested event has been evicted from the event ring
 *       (whose capacity is --event-log-cap, 128 by default)
 *       — replays from the oldest buffered event instead, after
 *       a "GAP <requested> <oldest>" line that says how much was
 *       lost. FROM composes with the type/id filters, in any
 *       order ("WATCH FROM 5 JOINED,DEAD"); a bad FROM is refused
 *       up front: ERR invalid watch filter <reason>.
 *       Heartbeat (0.32.0): a stream that has sent nothing for 5 s
 *       sends "HEARTBEAT <unix-seconds>", so a client on a quiet
 *       cluster can tell "nothing happened" from "the connection
 *       died silently". The heartbeat carries no sequence number
 *       and is sent regardless of the filter; any line sent —
 *       EVENT, GAP, or HEARTBEAT — resets the 5 s clock, so a
 *       busy cluster never sees one.
 *       Backpressure / pause (0.37.0): a slow consumer can stop
 *       the flow without closing the connection — "WATCH PAUSE
 *       <session>" on a separate connection pauses the stream
 *       with that session number (the number JSON reports per
 *       stream), and "WATCH RESUME <session>" resumes it. A
 *       paused stream stops consuming events: its thread waits
 *       instead of reading the ring, so the node keeps recording
 *       into the same bounded ring and never grows memory for a
 *       stream that is not reading. On resume the stream picks
 *       up from the last sequence it consumed, replaying what
 *       the ring still holds, in order; when the ring has
 *       scrolled past the resume point the same gap signal
 *       covers it — the stream ends with "ERR gap: events lost,
 *       reconnect" and the lost events count as dropped. Both
 *       commands are idempotent (pausing an already-paused
 *       stream answers "OK paused <session>" again) and answer
 *       "OK paused <session>" / "OK resumed <session>"; a
 *       session that is not a live stream answers "ERR unknown
 *       watch session <n>", a session that is not a number
 *       answers "ERR invalid watch session <arg>", and a missing
 *       session answers "ERR usage: WATCH PAUSE <session>".
 *       A paused stream still sends its HEARTBEAT when 5 s pass
 *       with nothing sent — the stream is alive, it is just not
 *       consuming — and a paused stream whose client goes away
 *       ends instead of lingering. METRICS reports how many
 *       streams are paused ("watch_paused"), JSON carries the
 *       count in the "watch" object ("paused") and per stream
 *       ("paused":true/false), and PROMETHEUS carries
 *       "pulse_watch_paused" as a gauge.
 *       Subscriber count (0.33.0): METRICS and JSON report how many
 *       WATCH streams are connected right now ("watch_subscribers"
 *       / "watch"."subscribers") and how many sessions the node has
 *       served since startup ("watch_sessions" / "watch"."sessions"),
 *       so an operator can see how many consumers are attached to
 *       the event stream without counting connections by hand.
 *       Delivery / drop counts (0.34.0): each stream counts the
 *       EVENT lines it sent ("delivered"), the events it consumed
 *       but skipped because its type/id filter excluded them
 *       ("filtered"), and the events it lost because they had
 *       already scrolled out of the bounded event ring before the
 *       stream could consume them ("dropped" — a replay GAP at
 *       stream start, a replay GAP on the first wait, or a
 *       mid-stream gap that ends the stream with an ERR). METRICS
 *       reports the three as lifetime totals
 *       ("watch_delivered" / "watch_filtered" / "watch_dropped",
 *       including what live streams have done so far); JSON
 *       carries the same three totals in the "watch" object, plus
 *       a "streams" array with the same three counts per
 *       currently-connected stream, keyed by its "session"
 *       number. A send that fails ends the stream and is counted
 *       neither way. At most 64 concurrent streams are listed
 *       per-stream in JSON; a further stream still runs and still
 *       feeds the totals, it just has no slot to list.
 *   anything else
 *       ERR unknown command
 *
 * The server is deliberately dumb: it knows nothing about the node's
 * threads or locks. All state access goes through the hooks, which the
 * node implements (snapshot copies the table under the node's lock;
 * force_leave removes under the lock). One-shot commands are served
 * one connection at a time; the server is a control interface, not
 * a data path. WATCH is the exception: it is a long-lived stream,
 * so it runs on its own thread and the accept loop keeps serving
 * other commands while a client watches.
 */

/* RELOAD control-plane command (0.25.0): the outcome of re-reading
 * --peer-file, filled in by the node's reload hook. have_file is 0
 * when the node was started without --peer-file; read_ok is 0 when
 * the file could not be read, and error then holds the reason
 * ("cannot read peer file <path>: <strerror>"). peers is how many
 * peers the file lists; added/dropped/kept are the merge outcome
 * (the same numbers as the PEERFILE RELOAD log line); bad is how many
 * lines were skipped as malformed. */
typedef struct {
    int have_file;
    int read_ok;
    int peers;
    int added;
    int dropped;
    int kept;
    int bad;
    char error[640];
} pulse_reload_result_t;

/* Peer-file state (0.28.0): what --peer-file the node is using and
 * what the last successful load or reload of it did, for METRICS
 * and JSON. have_file is 0 when the node was started without
 * --peer-file; last_reload_unix is the wall-clock time (seconds
 * since the epoch) of the last successful load or reload — the
 * initial load at startup counts as the first one, with added =
 * the peers the file contributed to the seed list — and
 * last_mtime_unix is the file's mtime (seconds) at that load.
 * peers / added / dropped / kept / bad are the counts from that
 * load: the same numbers as the PEERFILE RELOAD log line (kept =
 * peers still held in the kept list after the reload). A reload
 * that cannot read the file leaves the state at the last successful
 * load, so the numbers always describe a roster the node actually
 * holds. */
typedef struct {
    int have_file;
    char path[512];
    int64_t last_reload_unix; /* 0 = never loaded */
    int64_t last_mtime_unix;  /* 0 = unknown */
    int peers;
    int added;
    int dropped;
    int kept;
    int bad;
} pulse_peer_file_state_t;

typedef struct {
    void *ctx;
    /* Copy the membership table into out (hook locks as needed). */
    void (*snapshot)(void *ctx, member_table_t *out);
    /* Force-remove a member. Returns 1 if removed, 0 if the member is
     * unknown, -1 if the id is this node itself. */
    int (*force_leave)(void *ctx, const char *id);
    /* Re-read --peer-file and merge it into the seed list — the same
     * reload SIGHUP triggers — filling out with the outcome. NULL
     * when the node does not support it: RELOAD then replies ERR. */
    void (*reload_peer_file)(void *ctx, pulse_reload_result_t *out);
    /* Copy the --peer-file state (path, last reload time/mtime,
     * counts from the last successful load) into out. NULL when the
     * node does not track it: METRICS and JSON then report no peer
     * file. */
    void (*peer_file_state)(void *ctx, pulse_peer_file_state_t *out);
    /* Copy the failure-detector metrics into out (hook locks as needed). */
    void (*metrics)(void *ctx, pulse_metrics_t *out);
    /* Set one annotation on this node itself. Returns 0 on success,
     * -1 on invalid key/value, -2 when the node already carries the
     * maximum number of annotations. NULL when the node does not
     * support it: META then replies ERR. */
    int (*set_meta)(void *ctx, const char *key, const char *value);
    /* 1 when phi-accrual suspicion is on (phi values are meaningful). */
    int (*phi_enabled)(void *ctx);
    /* Where a member came from, for the PEERS command: "seed" (a
     * --seed flag), "peer-file" (the --peer-file roster, or a peer
     * removed from it but kept because it was still ALIVE),
     * "discovered" (learned from gossip or a JOIN, never seeded),
     * or "self". NULL when the node does not track provenance:
     * PEERS then reports every non-self member as "discovered". */
    const char *(*peer_source)(void *ctx, const char *id);
    /* Slow-peer RTT threshold in ms (0 = warning disabled): a peer
     * whose mean probe RTT is above this counts as slow, and is
     * reported as "slow" per member in JSON and as slow_peers in
     * METRICS and JSON. */
    int64_t slow_rtt_threshold_ms;
    /* Quiet-peer last-contact threshold in ms (0 = warning
     * disabled, 0.62.0): an ALIVE peer whose last-contact age is
     * above this counts as quiet, and is reported as "quiet" per
     * member in JSON and as quiet_peers in METRICS and JSON. */
    int64_t quiet_threshold_ms;
    /* Low-probe-rate threshold as a fraction (0 = warning
     * disabled, 0.68.0): an ALIVE peer whose recent probe success
     * rate — answered / sent over the last PULSE_PROBE_WINDOW
     * direct probes — is below this counts as low-probe-rate,
     * and is reported as "low_probe_rate" per member in JSON
     * and as low_probe_rate_peers in METRICS and JSON. */
    double low_probe_rate_frac;
    /* Membership events for the WATCH command (0.29.0): the next
     * event after last_seq, or a wait for one. Returns 1 with *out
     * filled, 0 when no new event arrived within timeout_ms, -1
     * when the event after last_seq has already scrolled out of
     * the node's bounded event log (the consumer fell too far
     * behind and must reconnect and re-read the snapshot). NULL
     * when the node does not support watching: WATCH then replies
     * ERR watch not supported. */
    int (*watch_next)(void *ctx, uint64_t last_seq, pulse_event_t *out,
                      int timeout_ms);
    /* The latest event sequence number (0 when no event has been
     * recorded yet): a WATCH stream starts live from here, after
     * its snapshot. NULL when the node does not support watching. */
    uint64_t (*watch_latest_seq)(void *ctx);
    /* The oldest event sequence number still held in the log (0
     * when no event has been recorded yet): a WATCH FROM below
     * this one has been evicted, so the stream replays from here
     * after a GAP line. NULL when the node does not support
     * watching. */
    uint64_t (*watch_oldest_seq)(void *ctx);
    /* How many events the WATCH event ring holds (0.36.0): the
     * --event-log-cap the node started with, 128 by default.
     * METRICS / JSON / PROMETHEUS report it so an operator can
     * see how far back a replay can reach. NULL when the node
     * does not support watching. */
    int (*event_log_cap)(void *ctx);
    /* How many members one gossip round's digest covers (0.41.0):
     * the --gossip-digest-size the node started with, 16 by
     * default. METRICS / JSON / PROMETHEUS report it so an
     * operator can see whether the node is trading convergence
     * speed for bandwidth. NULL reports the default 16. */
    int (*gossip_digest_size)(void *ctx);
    /* The phi value at which a peer is suspected (0.43.0): the
     * --phi-threshold the node started with, 8.0 by default.
     * PROMETHEUS reports it so a scraper can alert on a rising
     * pulse_member_phi against the threshold that actually fires
     * the suspicion, instead of hard-coding 8.0. NULL reports
     * the default 8.0. */
    double (*phi_threshold)(void *ctx);
    const char *self_id;
    int64_t start_ms; /* monotonic ms when the node started */
} control_hooks_t;

/* Create, bind and listen on a TCP socket for the control plane, on the
 * loopback address of the given family (AF_INET or AF_INET6). The
 * control plane follows the node's family: a v6 node answers on ::1,
 * a v4 node on 127.0.0.1. Returns the listen fd, or -1 on error (with
 * errno set). */
int control_listen(int port, int family);

/* Serve connections on fd until accept() fails (e.g. the fd is shut
 * down from another thread). Returns when the server stops. */
void control_serve(int fd, const control_hooks_t *hooks);

#endif
