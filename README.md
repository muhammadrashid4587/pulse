# pulse

A SWIM-style gossip failure detector in C. Nodes join a cluster over UDP,
gossip membership lists, probe each other directly, and mark a peer dead
within a configurable sub-second timeout when it stops responding.

Built for a concrete use case: fast failure detection between ward devices
in a hospital — a monitor, an infusion pump, and a bedside gateway that
need to notice within a second when one of them drops off the network, so
another device can take over alerting. It is a distributed-systems
building block, not a production service mesh.

## What it is

- C11, no dependencies beyond libc and pthreads.
- One binary, `pulse-node`: one process per cluster member.
- SWIM failure detection: direct PING/ACK probes, indirect probes
  (PING_REQ) through two random peers when a direct probe goes unanswered,
  SUSPECT then DEAD with incarnation numbers and refutation.
- Gossip: every node periodically exchanges a digest of its membership
  table with a few random peers — the fanout grows with log2(cluster
  size), capped at 4 — so news of a dead node spreads in O(log n)
  rounds even as the cluster grows. Recently-changed SUSPECT/DEAD
  entries lead each digest, so failure news is never stuck behind
  the random window when the table is larger than the digest.
- Incarnation numbers persist in `<data-dir>/incarnation`, so a restarted
  node rejoins with a higher incarnation and can refute stale
  SUSPECT/DEAD gossip about itself.
- Garbage collection: DEAD entries are dropped after a configurable
  retention (default 60 s), with tombstones that stop gossip from
  resurrecting them until every peer has GC'd them too.
- Phi-accrual failure detection (opt-in, `--phi-accrual`): each peer
  keeps a window of heartbeat inter-arrival intervals, and a peer is
  suspected when its phi — -log10 of the probability of a gap this
  long — crosses a configurable threshold (default 8.0), instead of
  after a fixed ack timeout. The fixed-timeout path stays the default.
- TCP control plane (opt-in, `--control-port`): a line-based protocol
  for asking a running node for its membership (`MEMBERS`), its status
  (`STATUS`), its failure-detector metrics (`METRICS`), the whole
  picture as one machine-readable JSON object (`JSON`), each peer with
  its last-seen time and where it came from (`PEERS`), setting an
  annotation on the node (`META <key=value>`), triggering a live
  peer-file reload (`RELOAD`), force-removing a member
  (`LEAVE <id>`), or watching membership events as they happen
  (`WATCH`). See "Control plane" below. Query it with the bundled
  `pulse-ctl` client or plain netcat.
- Optional shared secret (`--secret`): nodes started with the same
  secret only accept traffic from peers presenting it, so two clusters
  on the same network cannot accidentally merge. See "Shared secret"
  below. Nodes without a secret behave exactly as before.
- IPv6 (`--ipv6`): a node can run entirely over IPv6, with bracketed
  member ids (`[::1]:17101`), the control plane on `::1`, and
  `pulse-ctl` accepting `[::1]:port`. See "IPv6" below. IPv4 remains
  the default and is unchanged.
- Seed rejoin: a node that starts while its seed is down re-sends
  `JOIN` to the seed (logged as `REJOIN`) until the seed
  answers, so a late-starting seed still merges the cluster instead
  of leaving the node isolated forever. The first retry fires on
  the ~5 s tick, then the retry backs off (0.56.0) — each
  consecutive unanswered re-JOIN doubles the wait, up to a 30 s
  cap. The retry is counted as
  `seed_rejoins` in `METRICS` and `JSON` and as
  `pulse_seed_rejoin_total` in `PROMETHEUS` (0.53.0), broken down
  per seed as `seed_rejoins_by_seed` in `METRICS` and `JSON` and
  with a `seed` label on the Prometheus counter (0.54.0), so an
  operator can see which seed a node keeps rejoining, and with the
  wall-clock time of each seed's most recent re-JOIN as
  `seed_last_rejoin_ms_by_seed` in `METRICS`,
  `"seed_last_rejoin_ms"` in `JSON`, and a labelled
  `pulse_seed_last_rejoin_ms` gauge in `PROMETHEUS` (0.55.0), so
  an operator can see when each seed was last retried — a seed
  that was never re-joined has no entry anywhere, never a
  fabricated zero. The retry backs off (0.56.0): each consecutive
  unanswered re-JOIN doubles the wait — 10 s, 20 s, capped at
  30 s, jittered — so a permanently dead seed is probed less and
  less often, and the current wait rides per seed as
  `seed_rejoin_backoff_ms_by_seed` in `METRICS`,
  `"seed_rejoin_backoff_ms"` in `JSON`, and a labelled
  `pulse_seed_rejoin_backoff_ms` gauge in `PROMETHEUS`, 0 while
  the seed is ALIVE. A failed `JOIN` send is retried once,
  immediately (0.56.0) — the send socket is `connect()`ed to the
  first seed, so a stale ICMP port-unreachable from a dead seed
  can fail a send that would have gone out fine a moment later —
  and the same one-retry send covers the startup `JOIN` and a
  peer-file reload's `JOIN`s too (0.57.0), so a one-shot merge
  message is never lost to that stale error. See "Seed rejoin"
  below.
- Membership persistence: a node saves the peers it knows to
  `<data-dir>/members` every ~5 s and on clean shutdown, and reloads
  them on startup, so a restarted node re-discovers its old peers
  even with no `--seed` — and re-probes every restored entry instead
  of trusting it. See "Membership persistence" below.
- Graceful leave: a node that stops cleanly (SIGINT/SIGTERM)
  announces its departure to its peers before exiting, so they mark
  it DEAD at once instead of waiting ~500 ms for the probe timeout
  to suspect it. A crash sends nothing, so the probe path still
  covers unclean deaths. See "Graceful leave" below.
- Anti-entropy: every ~10 s a node exchanges its whole membership
  table with a random peer (`SYNC`), and the peer answers with its own
  full table when the exchange taught it something new, so a node that
  missed many incremental digests converges in one exchange instead of
  waiting for the random window to rotate the missing entries through.
  A `SYNC` that changes nothing gets no reply: an unconditional answer
  would ping-pong forever, each `SYNC` answered by a `SYNC` at network
  speed. See "Anti-entropy" below. A `JOIN` is answered with a `SYNC`
  for the same reason: a joiner gets the seed's whole table at once
  instead of waiting for gossip rounds to rotate the missing members
  through.
- Gossip delta encoding: every membership-table change bumps a table
  version, and each entry records the version at which it last changed,
  so a digest to a peer carries only the entries that changed since the
  version that peer last saw — oldest change first — instead of the
  whole digest window every round. A converged cluster sends no
  gossip bytes at all until something actually changes. A peer with no
  recorded version, or one more than a digest's worth of changes
  behind, gets a full `SYNC` instead of a delta. Delivery is UDP, so a
  lost delta can leave a peer behind its recorded version; the periodic
  anti-entropy `SYNC` heals that within one interval, and failure
  detection never depends on gossip (each node probes its peers
  directly). See "Gossip delta encoding" below.
- Adaptive probe interval: while any peer is SUSPECT the cluster is
  mid-incident, so the probe cycle shortens from the base interval to
  a fast interval (default half the base, `--probe-interval-fast-ms`
  to set it, never below 25 ms) — direct probes, indirect PING_REQ,
  the suspect-to-dead sweep and gossip rounds all run faster until
  the table is suspect-free again, then the interval returns to base.
  The speedup is counted as `adaptive_fast_cycles` in `METRICS` and
  `JSON`, and each switch is logged as an `ADAPTIVE` line. See
  "Adaptive probe interval" below.
- Probe-interval jitter: every probe cycle sleeps the adaptive
  interval times (1 + u) with u uniform in [-j, +j] (default j = 0.1,
  `--probe-jitter-frac` to set it), so nodes that start together
  drift out of lockstep within a few cycles instead of probing,
  gossiping, and syncing in bursts — each node's mean interval stays
  the base, and failure detection semantics are untouched (suspicion
  still comes from the fixed ack timeout or from phi alone). See
  "Probe-interval jitter" below.
- Suspicion age per member: each member in `JSON` carries
  `suspect_ms` — how long it has been SUSPECT, in ms, `null`
  unless it is SUSPECT — and the same value rides as one
  `SUSPECT <id> <ms>` line per SUSPECT member in `METRICS` and
  as one `pulse_member_suspect_ms` gauge sample per SUSPECT
  member in `PROMETHEUS`, so an operator can see how close a
  suspicion is to the suspect timeout instead of only learning
  about it when the member goes DEAD. See "Suspicion age per
  member" below.
- Time since last contact per member: each member in `JSON`
  carries `last_seen_ms` — how long since this node last heard
  from the member, in ms, `null` for self and for a peer never
  heard from — and the same value rides as one `LASTSEEN <id>
  <ms>` line per member heard from in `METRICS` and as one
  `pulse_member_last_seen_ms` gauge sample per member heard from
  in `PROMETHEUS`, so an operator can see a peer going quiet
  before the suspicion even fires, and how long a DEAD member
  has been silent. See "Time since last contact per member"
  below.
- Phi threshold crossing in the event log: a SUSPECTED event this
  node raises itself in phi-accrual mode carries the phi that
  crossed the threshold — a trailing ` phi=<value>` on the
  `WATCH` `EVENT` line and in the persisted `<data-dir>/events`
  file — so the record of a suspicion says why it fired, not
  only that it did. Only a real crossing carries a phi. See "Phi
  threshold crossing in the event log" below.
- Last-contact age in the event log: every `WATCH` `EVENT` line
  for a member this node heard from carries a trailing
  ` last_seen_ms=<ms>` — how long since this node last heard from
  the member when the event was recorded, the same value `JSON`
  reports as `last_seen_ms`, frozen at the event instead of
  recomputed at replay time — so a replayed SUSPECTED event still
  shows how quiet the peer was when it was suspected, and a DEAD
  event shows a larger age than the SUSPECTED event before it.
  Self, a member never heard from, and a member already gone from
  the table carry no field. See "Last-contact age in the event
  log" below.
- Suspicion-refutation window: when a peer marked SUSPECT comes back
  ALIVE — a slow peer, a dropped probe, a transient partition — the
  suspicion is counted as refuted, and the time the peer sat SUSPECT
  is recorded as a refute latency (mean/max/samples) in `METRICS`
  and `JSON` as `suspect_refuted` / `refute_latency_ms`, so an
  operator can see how long false suspicions stay visible to the
  cluster. A DEAD peer coming back is a restart, not a refutation,
  and is not counted. See "Suspicion-refutation window" below.
- Member metadata: each node carries a small set of `key=value`
  annotations about itself (`role=worker`, `zone=us-east-1`,
  `version=1.2.3`), set with `--meta` or the control plane's `META`
  command, gossiped with its membership entry, visible in `MEMBERS`
  and `JSON`, and persisted across restarts. See "Member metadata"
  below.
- Probe RTT: each node measures the round-trip time of its direct
  probes (PING to ACK) per peer — the last RTT and the mean over all
  answered probes — and reports them per member in `JSON` as
  `rtt_ms` / `rtt_mean_ms`, so an operator can see which peers are
  slow before they ever get suspected. See "Probe RTT" below.
- Probe RTT histogram: every answered direct probe is also bucketed
  into a global RTT histogram (<1, 1–4, 5–9, 10–49, ≥50 ms) with
  mean/max/samples, exposed as `probe_rtt_ms` in `METRICS` and
  `JSON` and as a real histogram in `PROMETHEUS`, so the mean RTT
  cannot hide a slow tail — a peer that usually answers in under a
  millisecond but sometimes takes 60 ms. See "Probe RTT histogram"
  below.
- Per-member probe RTT in `METRICS`: the last, mean and worst
  direct-probe RTT `JSON` reports as `rtt_ms` / `rtt_mean_ms` /
  `rtt_max_ms` and `PROMETHEUS` as `pulse_member_rtt_ms` /
  `pulse_member_rtt_mean_ms` / `pulse_member_rtt_max_ms` now also
  ride as one `RTT <id> <last_ms> <mean_ms> <max_ms>` line per
  member that has answered a probe in the `METRICS` reply, after
  the `LASTSEEN` lines, so a text-protocol consumer can watch a
  peer's RTT climb — and spot the one slow probe the mean hides —
  without parsing `JSON` or scraping Prometheus. The worst RTT
  (0.65.0) is appended as a fourth field: a consumer reading the
  first three fields by position still gets the same last and
  mean, but a strict parser expecting exactly three numbers sees
  a format change. See "Per-member probe RTT in METRICS" below.
- Worst probe RTT per member: the largest single direct-probe
  round trip over all answered probes per peer rides as
  `rtt_max_ms` in `JSON` and as a `pulse_member_rtt_max_ms`
  gauge in `PROMETHEUS`, so the mean cannot hide one 60 ms probe
  among sub-millisecond ones — a peer that usually answers fast
  but occasionally stalls is visible while it is still ALIVE.
  See "Worst probe RTT per member" below.
- Per-member probe counts: each node counts, per peer, how many
  direct probes it sent and how many were answered — the probe
  success rate — as `probes_sent` / `probes_answered` in `JSON`,
  one `PROBES <id> <sent> <answered>` line per probed member in
  `METRICS`, and `pulse_member_probes_sent_total` /
  `pulse_member_probes_answered_total` counters in `PROMETHEUS`,
  so a peer that is probed but never answers (sent climbing,
  answered 0) is distinguishable from a peer never probed at
  all. See "Per-member probe counts" below.
- Probe counts in the event log: every `WATCH` `EVENT` line for a
  member this node has probed carries trailing
  ` probes_sent=<n> probes_answered=<n>` — how many direct probes
  this node had sent the member and how many were answered when
  the event was recorded, the same values `JSON` reports as
  `probes_sent` / `probes_answered`, frozen at the event instead
  of recomputed at replay time — so a replayed SUSPECTED event
  still shows how many probes went unanswered before the
  suspicion fired, and a DEAD event shows a sent count that kept
  climbing while the answered count stayed frozen. Self, a member
  never probed, and a member already gone from the table carry
  no fields. See "Probe counts in the event log" below.
- Slow-peer warning: when a peer's mean probe RTT rises above a
  configurable threshold (`--slow-peer-rtt-ms`), the node logs a
  `SLOW` line once per episode and reports the peer as `"slow":true`
  in `JSON` (with a `slow_peers` count in `METRICS` and `JSON`), so
  a congested link or a struggling device is visible while the peer
  is still ALIVE, instead of only after it is suspected. See
  "Slow-peer warning" below.
- Quiet-peer early warning: when an ALIVE peer's last-contact age
  rises above a configurable threshold (`--quiet-peer-ms`), the
  node logs a `QUIET` line once per episode, records a `QUIET`
  event on the `WATCH` stream, and reports the peer as
  `"quiet":true` in `JSON` (with a `quiet_peers` count in `METRICS`
  and `JSON` and a `pulse_quiet_peers` gauge in `PROMETHEUS`), so
  a peer going quiet is visible before the suspicion even fires,
  instead of only when it is suspected. See "Quiet-peer early
  warning" below.
- Low-probe-rate alert: when an ALIVE peer's recent probe success
  rate — answered / sent over its last 20 direct probes — falls
  below a configurable fraction (`--low-probe-rate-frac`), the
  node logs a `LOWRATE` line once per episode, records a `LOWRATE`
  event on the `WATCH` stream, and reports the peer as
  `"low_probe_rate":true` with its `"probe_rate"` in `JSON` (with
  a `low_probe_rate_peers` count in `METRICS` and `JSON`, a
  `LOWRATE` line in `METRICS`, and a `pulse_low_probe_rate_peers`
  gauge plus a `pulse_member_probe_rate` gauge in `PROMETHEUS`),
  so a peer answering one probe in ten is visible while it is
  still ALIVE, instead of only when it is suspected. See
  "Low-probe-rate alert" below.
- Seedless bootstrap: a node reads its initial peers from a file
  (`--peer-file`, one `host:port` per line, comments and blank lines
  ignored) and joins the cluster without a `--seed` flag — bad lines
  are logged and skipped, a missing file is a clean startup error.
  See "Seedless bootstrap" below.
- Live peer-file reload: while a `--peer-file` is in use, SIGHUP
  re-reads the file — newly listed peers join the cluster, peers
  removed from the file are dropped once they stop answering, and a
  peer that is still ALIVE is never dropped on the strength of a
  file edit alone — or the control plane's `RELOAD` command does the
  same reload over TCP. The node also watches the file itself: once
  a second it checks the file's mtime, and an edit re-runs the same
  merge with no signal and no control-plane call. See "Live
  peer-file reload" below. The last load is visible without reading
  the log: `METRICS` and `JSON` report the peer-file path, the last
  reload time and the file's mtime at that load, and the counts from
  that load, and each member in `JSON` carries where it came from
  (`source`), so a peer-file peer is distinguishable from a
  discovered one. See "Peer-file state in METRICS and JSON" below.
- Prometheus exposition format: the control plane's `PROMETHEUS`
  command answers the same values `METRICS` and `JSON` report as
  Prometheus text format — membership counts as gauges, lifetime
  transition totals as counters, the detection latency as a real
  histogram, one `pulse_member_info` gauge per member labelled by
  member, state and source, the last, mean and worst probe RTT per member
  as gauges, the probe counts per member as counters, the phi failure-detector value per member as a gauge,
  the latest and max of each member's sampled phi history as
  gauges, and each member's phi history as a real histogram
  (0.52.0), and the peer-file gauges when the node runs with
  `--peer-file` — so a scraper can watch the cluster
  without parsing the text protocol. See "Prometheus exposition
  format (PROMETHEUS)" below.

## What it is not

- Not a service mesh, not a consensus protocol, not encrypted. Without
  a shared secret, any process on the network can join and any member
  can claim another is dead. It detects crashes on a trusted LAN; it
  does not defend against a malicious network. With a secret, only
  peers presenting the secret are accepted — but that is a join gate
  against accidental cross-talk, not authentication (see "Shared
  secret").
- Not a replacement for Raft or Paxos. It answers one question — "is that
  node still there?" — and leaves agreement to something else.
- Dead members are garbage-collected after a retention period (default
  60 s, `--dead-retention`): the entry is dropped, a tombstone stops
  gossip from resurrecting it, and a rejoining node comes back at a
  higher incarnation. Tombstones ride along in gossip digests as DEAD
  entries, so a removal spreads to peers (which mark the member DEAD
  and GC it on their own retention timer) instead of waiting for each
  peer to notice the crash itself. A live member that hears itself
  DEAD refutes by bumping its incarnation.

## Protocol

Messages are one text line per UDP datagram:

```
PING <seq> <from-id> <from-inc>
ACK <seq> <from-id> <from-inc>
PING_REQ <seq> <target-id> <from-id>
JOIN <from-id> <from-inc>
GOSSIP <from-id> <from-inc> <count> <id:inc:state> ...
SYNC <from-id> <from-inc> <count> <id:inc:state> ...
```

State codes in gossip entries: `A` alive, `S` suspect, `D` dead.
Member ids are `host:port`, so a gossip entry is self-contained.
IPv6 ids are bracketed (`[::1]:17101`), which keeps the `id:inc:state`
entries unambiguous even though the address itself contains colons.

When a shared secret is configured, every message carries a trailing
` <tag>` token: the first 8 bytes of HMAC-SHA256(secret, message),
hex-encoded (16 chars). A receiver with a secret drops any datagram
whose tag is missing or wrong, before it touches the membership
table; a receiver without a secret ignores the trailing token. The
tag needs no protocol change: the parser ignores the extra token.

Failure detection, per node, every probe interval (default 100 ms):

1. Pick a random peer, send PING, wait for ACK.
2. No ACK within the ack timeout (100 ms): send PING_REQ to two random
   peers, asking them to probe the target for you.
3. Still no ACK after a second ack timeout: mark the target SUSPECT and
   gossip that.
4. A SUSPECT older than the suspect timeout (300 ms) becomes DEAD.
   Default path from crash to DEAD is roughly 500 ms.

Merge rule for gossip, per entry: higher incarnation wins; at equal
incarnation, DEAD beats SUSPECT beats ALIVE. A node that hears gossip
calling itself SUSPECT or DEAD bumps its incarnation and gossips ALIVE
again — that is the refutation. SYNC entries merge by exactly the same
rule; the only difference from GOSSIP is how many entries arrive.

### Phi-accrual mode (`--phi-accrual`)

With `--phi-accrual`, suspicion is driven by phi instead of the fixed
ack timeout: every direct contact with a peer (PING, ACK, gossip,
JOIN) is a heartbeat, and each peer keeps the last 100 inter-arrival
intervals. phi is -log10 of the probability, under a normal model of
those intervals, of a gap as long as the one since the peer's last
heartbeat. A healthy peer's phi stays near 0; a silent peer's phi
grows without bound, and the peer is suspected at the threshold
(default 8.0, `--phi-threshold`). The suspect-to-dead sweep and
gossip are unchanged, and suspicion is logged as
`SUSPECT <id> phi=8.42`.

Two honest caveats. The normal model is an approximation — real
arrival intervals are closer to exponential — so phi is conservative
here (it rises a little faster than the "true" probability would
suggest). And the spread is floored at 100 ms (or 5% of the mean):
with perfectly regular arrivals the variance is ~0 and any tiny
scheduling delay would look infinitely suspicious. Detection latency
scales with cluster size, because arrivals come from random probing:
on a 3-node cluster a dead peer is suspected roughly 0.7 s after it
stops answering. A peer with no heartbeat history at all (a seed that
never answers) is suspected after 20 probe intervals of silence, since
it cannot produce a phi. In phi mode there are no indirect probes:
phi is the judgement.

## Quickstart

```sh
make
# Terminal 1: first node, no seeds
./pulse-node --port 17101 --data-dir /tmp/p1
# Terminal 2: join via the first node
./pulse-node --port 17102 --seed 127.0.0.1:17101 --data-dir /tmp/p2
# Terminal 3
./pulse-node --port 17103 --seed 127.0.0.1:17101 --data-dir /tmp/p3
```

Kill one node and watch the others log `SUSPECT` then `DEAD` within
about half a second. Restart it and watch `ALIVE` come back.

Or run the scripted version:

```sh
scripts/demo.sh
```

## Configuration

| Flag | Default | Meaning |
| ---- | ------- | ------- |
| `--port` | — (required) | UDP port to listen on |
| `--id` | `127.0.0.1:<port>` | Member id; set it to a reachable address on multi-host clusters |
| `--seed` | — (repeatable) | `host:port` of a known member to join through |
| `--peer-file` | — | File of initial peers, one `host:port` per line (see "Seedless bootstrap") |
| `--data-dir` | `.` | Directory holding the `incarnation` and `members` state files |
| `--probe-interval-ms` | 100 | How often to probe a random peer |
| `--probe-interval-fast-ms` | half of `--probe-interval-ms` | Probe interval while any peer is SUSPECT (adaptive probe interval; floored at 25 ms, never slower than the base) |
| `--probe-jitter-frac` | 0.1 | ±fraction of jitter on the probe interval each cycle (see "Probe-interval jitter"); 0 disables it, clamped to 0.9 |
| `--ack-timeout-ms` | 100 | How long to wait for an ACK before an indirect probe |
| `--slow-peer-rtt-ms` | 0 (disabled) | Warn when a peer's mean probe RTT is above this many ms (see "Slow-peer warning") |
| `--quiet-peer-ms` | 0 (disabled) | Warn when an ALIVE peer's last-contact age is above this many ms (see "Quiet-peer early warning") |
| `--low-probe-rate-frac` | 0 (disabled) | Warn when an ALIVE peer's recent probe success rate is below this fraction (see "Low-probe-rate alert") |
| `--suspect-timeout-ms` | 300 | How long a SUSPECT lasts before becoming DEAD |
| `--dead-retention` | 60 | Seconds a DEAD entry stays in the table before GC |
| `--phi-accrual` | off | Suspect on phi instead of the fixed ack timeout |
| `--phi-threshold` | 8.0 | Phi value at which a peer is suspected (phi mode only) |
| `--sync-interval-ms` | 10000 | How often to exchange full tables (anti-entropy SYNC) with a random peer |
| `--control-port` | — (disabled) | TCP port for the control plane (`MEMBERS`, `STATUS`, `PEERS`, `LEAVE`, `RELOAD`, `WATCH`) |
| `--event-log-cap` | 128 | How many membership events the `WATCH` event ring holds for replay (see "Control-plane event stream (WATCH)") |
| `--gossip-digest-size` | 16 | How many members one gossip round's digest covers (see "Configurable gossip digest size") |
| `--secret` / `--shared-secret` | — (none) | Shared secret for joins: only peers presenting the same secret are accepted |
| `--meta KEY=VALUE` | — (none) | An annotation this node carries about itself (repeatable, at most 8): gossiped with its membership entry, shown in `MEMBERS`/`JSON`, persisted across restarts |
| `--ipv6` | off | Run over IPv6: bind `::`, ids are `[::1]:port`, control plane on `::1` |
| `--bind HOST` | — (wildcard) | Bind address override, a literal IP: decides the family and the default id host (`--bind ::1` is IPv6, `--bind 127.0.0.1` is IPv4) |

Each node logs timestamped events to stdout: `JOIN`, `SUSPECT`, `DEAD`,
`GC` (a DEAD entry removed after its retention expired), `ALIVE`,
`REFUTE`, `LEAVE` (this node announced its clean shutdown to its
peers), `PEERS <n> from <file>` (initial peers loaded from a peer
list file — see "Seedless bootstrap"), `PEERFILE RELOAD <file>
peers=<n> added=<a> dropped=<d> kept=<k>` (a SIGHUP re-read the peer
file — see "Live peer-file reload"), `PEERFILE <file> changed on
disk, auto-reloading` (the 1 s mtime watch saw the file change and
re-ran the same merge with no signal — see "Live peer-file
reload"), `PEERFILE ADD <id>` /
`PEERFILE DROP <id>` / `PEERFILE KEEP <id> (still ALIVE)` (one line per
peer the reload added, dropped, or kept), `SLOW <id> mean=Nms threshold=Mms` (a peer's mean probe RTT
rose above `--slow-peer-rtt-ms`, once per slow episode — see
"Slow-peer warning"), `QUIET <id> age=Nms threshold=Mms` (an ALIVE
peer's last-contact age rose above `--quiet-peer-ms`, once per quiet
episode — see "Quiet-peer early warning"), `LOWRATE <id> rate=0.40 threshold=0.50` (an ALIVE
peer's recent probe success rate fell below `--low-probe-rate-frac`, once per low-rate
episode — see "Low-probe-rate alert"), `SYNC <id>` (a periodic full-table exchange was sent to that
peer), `ADAPTIVE fast probe=Nms suspects=N` when the probe interval
shortens while a peer is SUSPECT and `ADAPTIVE probe=Nms stable`
when it returns to base, and a `STATUS alive=N suspect=N dead=N total=N` line
every second.

Note on sending: `pulse-node` sends on a dedicated socket that is
`connect()`ed once at startup. Some sandboxed environments refuse
`sendto()` on a UDP socket that has never been `connect()`ed; the one-time
connect keeps gossip flowing there and is a no-op on a normal host. The
receiving socket stays unconnected so it keeps accepting datagrams from
any peer.

## Control plane

Start a node with `--control-port` and it listens on a TCP socket on
localhost for one line-based command per connection, replying in plain
text and closing the connection. No authentication — it is a local
debugging and operations interface, not a public API; do not expose
the port beyond localhost.

```sh
./pulse-node --port 17101 --peer-file peers --control-port 18401 --data-dir /tmp/p1

./pulse-ctl 127.0.0.1:18401 MEMBERS
# MEMBER 127.0.0.1:17101 1 ALIVE - role=worker,zone=us-east-1
# MEMBER 127.0.0.1:17102 1 ALIVE 0.42 -
# END

./pulse-ctl 127.0.0.1:18401 META version=1.2.3
# OK version=1.2.3

./pulse-ctl 127.0.0.1:18401 STATUS
# STATUS id=127.0.0.1:17101 uptime=42 alive=2 suspect=0 dead=0 total=2
# END

./pulse-ctl 127.0.0.1:18401 LEAVE 127.0.0.1:17102
# OK 127.0.0.1:17102

./pulse-ctl 127.0.0.1:18401 RELOAD
# OK peers=3 added=1 dropped=0 kept=0 bad=0

./pulse-ctl 127.0.0.1:18401 PEERS
# PEER 127.0.0.1:17101 1 ALIVE - self
# PEER 127.0.0.1:17102 1 ALIVE 12 peer-file
# PEER 127.0.0.1:17103 1 ALIVE 8 discovered
# END

./pulse-ctl 127.0.0.1:18401 METRICS
# METRICS suspected=1 dead=1 refutes=0 joins=2 seed_rejoins=0 seed_rejoins_by_seed=- adaptive_fast_cycles=3 suspect_refuted=0 detect_latency_ms mean=203 max=203 samples=1 hist=[0,0,1,0,0] suspect_to_dead_ms mean=301 max=301 samples=1 refute_latency_ms mean=- max=- samples=0 slow_peers=0 quiet_peers=0 peer_file=peers last_reload=1795450000 last_mtime=1795449900 pf_peers=3 pf_added=1 pf_dropped=0 pf_kept=0 pf_bad=0 watch_subscribers=0 watch_sessions=0 watch_delivered=0 watch_filtered=0 watch_dropped=0 watch_paused=0
# END

./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101","uptime":42,"alive":2,"suspect":0,"dead":0,"total":2,"members":[{"id":"127.0.0.1:17101","incarnation":1,"state":"ALIVE","phi":null,"phi_history":null,"meta":{"role":"worker","zone":"us-east-1"},"rtt_ms":null,"rtt_mean_ms":null,"slow":false,"quiet":false,"suspect_ms":null,"last_seen_ms":null,"source":"self"},{"id":"127.0.0.1:17102","incarnation":1,"state":"ALIVE","phi":0.42,"phi_history":[[1200,0.18],[1300,0.21],[1400,0.42]],"meta":{},"rtt_ms":1,"rtt_mean_ms":0,"rtt_max_ms":1,"slow":false,"quiet":false,"suspect_ms":null,"last_seen_ms":12,"source":"peer-file"}],"metrics":{"suspected":1,"dead":1,"refutes":0,"joins":2,"seed_rejoins":0,"seed_rejoins_by_seed":{},"adaptive_fast_cycles":3,"suspect_refuted":0,"slow_peers":0,"quiet_peers":0,"detect_latency_ms":{"mean":203,"max":203,"samples":1,"hist":[0,0,1,0,0]},"suspect_to_dead_ms":{"mean":301,"max":301,"samples":1},"refute_latency_ms":{"mean":null,"max":0,"samples":0},"probe_rtt_ms":{"mean":0,"max":1,"samples":11,"hist":[10,1,0,0,0]}},"peer_file":{"path":"peers","last_reload":1795450000,"last_mtime":1795449900,"peers":3,"added":1,"dropped":0,"kept":0,"bad":0},"watch":{"subscribers":0,"sessions":0,"delivered":0,"filtered":0,"dropped":0,"paused":0,"streams":[]}}
# END
./pulse-ctl 127.0.0.1:18401 WATCH
# OK watching
# SNAPSHOT 127.0.0.1:17101 1 ALIVE - self
# SNAPSHOT 127.0.0.1:17102 1 ALIVE 12 peer-file
# END
# EVENT 1 JOINED 127.0.0.1:17102 1 1795450000
# EVENT 2 SUSPECTED 127.0.0.1:17103 1 1795450001
```

Or with netcat, if it is installed:

```sh
printf 'MEMBERS\n' | nc 127.0.0.1 18401
```

Commands:

| Command | Reply |
| ------- | ----- |
| `MEMBERS` | One `MEMBER <id> <incarnation> <ALIVE\|SUSPECT\|DEAD> <phi\|-> <annotations\|->` line per member, then `END`. Phi is the peer's current phi (two decimals) when phi-accrual is on and the peer has heartbeat history; `-` otherwise, including self. Annotations are the member's comma-separated `key=value` list, `-` when it carries none. |
| `PEERS` | One `PEER <id> <incarnation> <ALIVE\|SUSPECT\|DEAD> <last-seen-ms\|-> <seed\|peer-file\|discovered\|self>` line per member, then `END`. Last-seen is the ms since this node last heard directly from the peer (`-` for self and for a peer never heard from, e.g. a seed that never answered); the source says where the peer came from — a `--seed` flag, the `--peer-file` roster (or a peer removed from it but kept because it was still ALIVE), or gossip/a JOIN from a peer the node was never told about. A peer named with both `--seed` and `--peer-file` reports as `seed`: the flag outranks the file. See "Peer provenance" below. |
| `STATUS` | One `STATUS id=<self> uptime=<seconds> alive=N suspect=N dead=N total=N` line, then `END`. |
| `META <key=value>` | Set one annotation on this node itself: `OK <key=value>`. The change is gossiped with the next digest and persisted to the data dir, so it survives a restart. Bad syntax: `ERR invalid meta <arg>`; a ninth distinct key: `ERR meta full (max 8)`; missing argument: `ERR usage: META <key=value>`. |
| `LEAVE <member-id>` | `OK <id>`: the member is dropped from the table and a tombstone is recorded, which is gossiped as DEAD so peers mark it DEAD and GC it on their own retention. A live member comes back — direct contact is proof of life, and gossip refutation bumps its incarnation — so `LEAVE` cannot declare a healthy node dead. Unknown member: `ERR unknown member <id>`; self: `ERR cannot leave self`; missing id: `ERR usage: LEAVE <member-id>`. |
| `RELOAD` | Re-read `--peer-file` and merge it into the seed list — the same reload SIGHUP triggers, for an operator who cannot send the node a signal: `OK peers=N added=A dropped=D kept=K bad=B`. `peers` is how many peers the file lists; `added`/`dropped`/`kept` are the merge outcome (a still-ALIVE peer removed from the file is kept, never dropped on a file edit alone); `bad` is how many lines were skipped as malformed. No `--peer-file`: `ERR no peer file`. Unreadable file: `ERR cannot read peer file <path>: <reason>`, and the old list is kept. |
| `METRICS` | One `METRICS suspected=N dead=N refutes=N joins=N seed_rejoins=N seed_rejoins_by_seed=<id>=N,...|-> seed_last_rejoin_ms_by_seed=<id>=N,...|-> seed_rejoin_backoff_ms_by_seed=<id>=N,...|-> adaptive_fast_cycles=N suspect_refuted=N detect_latency_ms mean=X max=Y samples=N hist=[a,b,c,d,e] suspect_to_dead_ms mean=X max=Y samples=N refute_latency_ms mean=X max=Y samples=N probe_rtt_ms mean=X max=Y samples=N hist=[a,b,c,d,e] slow_peers=N peer_file=<path\|-> last_reload=<unix\|-> last_mtime=<unix\|-> pf_peers=<N\|-> pf_added=<N\|-> pf_dropped=<N\|-> pf_kept=<N\|-> pf_bad=<N\|-> watch_subscribers=<N> watch_sessions=<N> watch_delivered=<N> watch_filtered=<N> watch_dropped=<N> watch_event_log_cap=<N> gossip_digest_size=<N> phi_threshold=<N.NN>` line, then `END`. `detect_latency_ms` is last-direct-contact to SUSPECT for suspicions this node detected itself; `suspect_to_dead_ms` is SUSPECT to DEAD. Mean/max are `-` when there are no samples. `hist` is the detection-latency histogram: counts of locally measured detection latencies in the buckets <100 ms, 100–249, 250–499, 500–999, ≥1000 ms, so the mean cannot hide a slow tail. Transitions learned from gossip bump the counts but add no latency sample, so samples can be smaller than the counts. `suspect_refuted` is SUSPECT to ALIVE transitions — suspicions that turned out wrong — and `refute_latency_ms` is how long those peers sat SUSPECT before coming back (mean/max/samples, `-` with no samples); a refutation learned from gossip bumps the count without a sample, and a DEAD peer coming back is a restart, not a refutation. `seed_rejoins` (0.53.0) is how many re-JOINs this node has sent to seeds that were not ALIVE — the ~5 s retry while a seed is down, SUSPECT/DEAD in the table, or already GC'd; see "Seed-rejoin count" below. `seed_rejoins_by_seed` (0.54.0) is the same count broken down per seed — `127.0.0.1:17101=2,127.0.0.1:17103=1`, in first-rejoin order, `-` when no seed has been re-joined yet — so an operator can see which seed a node keeps rejoining instead of only how often the retry fires; the counts sum to `seed_rejoins`. `seed_last_rejoin_ms_by_seed` (0.55.0) is the wall-clock time of each seed's most recent re-JOIN, as unix milliseconds, in the same per-seed shape and order — `127.0.0.1:17101=1700000100000`, `-` when no seed has been re-joined yet — so an operator can tell when each seed was last retried, and whether a climbing count is still firing or stopped when the cluster merged; a seed that was never re-joined has no entry in either field, never a fabricated zero. `seed_rejoin_backoff_ms_by_seed` (0.56.0) is the delay in ms the node will wait before the next re-JOIN to each seed, in the same per-seed shape and order — `127.0.0.1:17101=10000`, `-` when no seed has been re-joined yet — so an operator can tell a seed still at the 5 s base from one whose retry has backed off to the 30 s cap; 0 while the seed is ALIVE (no backoff pending, the retry resets when the seed comes back), the scheduled delay while it is down and being backed off. `probe_rtt_ms` (0.48.0) is the round-trip time of every answered direct probe — mean/max/samples plus `hist`, counts in the buckets <1 ms, 1–4, 5–9, 10–49, ≥50 ms — so the mean cannot hide a slow tail; a probe never answered adds no sample, like the per-peer RTT it feeds; see "Probe RTT histogram" below. `slow_peers` is how many members currently count as slow — mean probe RTT above `--slow-peer-rtt-ms` (see "Slow-peer warning"); 0 when the warning is disabled. `quiet_peers` (0.62.0) is how many members currently count as quiet — ALIVE with a last-contact age above `--quiet-peer-ms` (see "Quiet-peer early warning"); 0 when the warning is disabled, and self is never counted: self's last-contact age is the node's uptime, not a contact age. `low_probe_rate_peers` (0.68.0) is how many members currently count as low-probe-rate — ALIVE with a recent probe success rate below `--low-probe-rate-frac` (see "Low-probe-rate alert"); 0 when the warning is disabled, and self is never counted: self never probes itself, so it has no window to judge. The peer-file fields (0.28.0) are the `--peer-file` state — the path, the wall-clock time of the last successful load or reload, the file's mtime at that load, and the counts from that load (the same numbers as the `PEERFILE RELOAD` log line) — and are all `-` when the node runs without `--peer-file`; see "Peer-file state in METRICS and JSON". `watch_subscribers` / `watch_sessions` (0.33.0) are how many `WATCH` streams are connected right now and how many sessions the node has served since startup; see "WATCH subscriber count in METRICS and JSON". `watch_delivered` / `watch_filtered` / `watch_dropped` (0.34.0) are the lifetime totals of EVENT lines streams sent, events streams consumed but skipped because their filter excluded them, and events streams lost to a gap in the bounded event ring; see "WATCH drop and delivery counts in METRICS and JSON". `watch_paused` (0.37.0) is how many `WATCH` streams are paused right now — a paused stream is still connected and still counts as a subscriber, it just is not consuming events; see "WATCH backpressure: pausing a stream". `watch_event_log_cap` (0.36.0) is how many events the `WATCH` event ring holds — the `--event-log-cap` the node started with, 128 by default — so an operator can see how far back a replay can reach. `gossip_digest_size` (0.41.0) is how many members one gossip round's digest covers — the `--gossip-digest-size` the node started with, 16 by default — so an operator can see whether the node is trading convergence speed for bandwidth. Per-member phi (0.44.0): after the `METRICS` line, one `PHI <id> <value>` line per member that has a phi — phi mode on, not self, heartbeat history with at least two intervals — the live phi at reply time, two decimals like `JSON` and `PROMETHEUS`, so the phi trio is complete; a node without `--phi-accrual` emits no `PHI` line at all, and a peer never heard from has none. `phi_threshold` (0.45.0) is the phi value at which a peer is suspected — the `--phi-threshold` the node started with, 8.0 by default, two decimals like `PROMETHEUS` — emitted always, whether or not `--phi-accrual` is on, so a text-protocol consumer can judge a rising `PHI` line against the threshold that actually fires the suspicion; see \"Phi suspicion threshold in METRICS\". Suspicion age per member (0.58.0): after the `METRICS` line, one `SUSPECT <id> <ms>` line per SUSPECT member — how long the member has been suspected, in ms, the same value `JSON` reports as `suspect_ms` and `PROMETHEUS` as `pulse_member_suspect_ms` — so a text-protocol consumer can see how close a suspicion is to the suspect timeout; the age grows while the member sits SUSPECT and the member drops off the list the moment it revives or goes DEAD, and ALIVE and DEAD members have no line. Time since last contact per member (0.59.0): after the `SUSPECT` lines, one `LASTSEEN <id> <ms>` line per member heard from — how long since this node last heard from the member, in ms, the same value `JSON` reports as `last_seen_ms` and `PROMETHEUS` as `pulse_member_last_seen_ms` — so a text-protocol consumer can see a peer going quiet before the suspicion fires; self has no line and a peer never heard from has none, and a DEAD member keeps its line until GC, the age growing. Per-member probe RTT (0.63.0): after the `LASTSEEN` lines, one `RTT <id> <last_ms> <mean_ms> <max_ms>` line per member that has answered a probe — the last, mean and worst direct-probe round-trip time in ms, the same values `JSON` reports as `rtt_ms` / `rtt_mean_ms` / `rtt_max_ms` and `PROMETHEUS` as `pulse_member_rtt_ms` / `pulse_member_rtt_mean_ms` / `pulse_member_rtt_max_ms` — so a text-protocol consumer can watch a peer's RTT climb without parsing `JSON` or scraping Prometheus; self and a peer never heard from have no line, a 0 is a real sub-millisecond probe rather than a missing one, and a DEAD member keeps its last RTT until GC. The worst RTT is the fourth field (0.65.0), appended after the last and mean: a consumer reading the first three fields by position still gets the same last and mean, but a strict parser expecting exactly three numbers sees a format change. Per-member probe counts (0.66.0): after the `RTT` lines, one `PROBES <id> <sent> <answered>` line per member that has been probed — how many direct probes this node sent the peer and how many were answered, the probe success rate, the same values `JSON` reports as `probes_sent` / `probes_answered` and `PROMETHEUS` as `pulse_member_probes_sent_total` / `pulse_member_probes_answered_total` — so a text-protocol consumer can tell a peer that is probed but never answers (sent climbing, answered 0) from one never probed at all; self and a peer never probed have no line, and a DEAD member keeps its counts until GC. Low-probe-rate alert (0.68.0): after the `PROBES` lines, one `LOWRATE <id> <rate>` line per member that counts as low-probe-rate — ALIVE, at least 5 direct probes in its window, and a recent success rate below `--low-probe-rate-frac` — the rate itself, two decimals, the same value `JSON` reports as `probe_rate` and `PROMETHEUS` as `pulse_member_probe_rate` — so a text-protocol consumer can see how far below the threshold a flagged peer sits; a peer at or above the threshold, a peer never probed, and a SUSPECT or DEAD peer have no line, and self never probes itself so it has none. |
| `JSON` | `STATUS`, `MEMBERS`, and `METRICS` combined into one machine-readable JSON object on a single line, then `END`. Member phi is a number when phi-accrual is on and the peer has heartbeat history, `null` otherwise (including self). Each member also carries a `phi_history` (0.47.0) — the last 16 sampled phi values as `[[age_ms, phi], ...]` pairs, oldest first, `age_ms` the ms since this node started, `null` under the same rule as `phi` itself (`[]` when the peer has history but no probe cycle has sampled it yet), so a JSON consumer can see how a peer's suspicion evolved instead of only its live phi; a DEAD peer's history is frozen at the value it reached at suspicion. Latency means are `null` when there are no samples; max is `0` then. The detection-latency histogram is `detect_latency_ms.hist`, an array of the same five bucket counts. The probe-RTT histogram (0.48.0) is `probe_rtt_ms` in the metrics object — `{"mean":...,"max":...,"samples":...,"hist":[...]}` over the buckets <1, 1–4, 5–9, 10–49, ≥50 ms, mean `null` with no samples; see "Probe RTT histogram" below. Each member also carries a `meta` object with its annotations (`{}` when it has none), and `rtt_ms` / `rtt_mean_ms` / `rtt_max_ms` with its probe round-trip times (see "Probe RTT" below and "Worst probe RTT per member" below; all three `null` until a probe has been answered, and always `null` for self), and `probes_sent` / `probes_answered` with how many direct probes this node sent the peer and how many were answered (see "Per-member probe counts" below; always numbers, never `null` — 0 sent for self and for a peer never probed, sent climbing with answered 0 for a peer probed but never answered), plus `slow` — `true` when the peer's mean RTT is above `--slow-peer-rtt-ms`, `false` otherwise (see "Slow-peer warning") — plus `quiet` (0.62.0) — `true` when the peer is ALIVE and its last-contact age is above `--quiet-peer-ms`, `false` otherwise (see "Quiet-peer early warning"; self is always `false`) — plus `low_probe_rate` (0.68.0) — `true` when the peer is ALIVE and its recent probe success rate is below `--low-probe-rate-frac`, `false` otherwise (see "Low-probe-rate alert"; self is always `false`) — plus `probe_rate` (0.68.0) — the rate itself, answered / sent over the last 20 direct probes, two decimals, `null` until the first probe to the peer goes out and always `null` for self — and `suspect_ms` (0.58.0) — how long the member has been SUSPECT, in ms, `null` unless it is SUSPECT (see "Suspicion age per member") — and `last_seen_ms` (0.59.0) — how long since this node last heard from the member, in ms, `null` for self and for a peer never heard from (see "Time since last contact per member") — and `source`, where the peer came from (`seed`, `peer-file`, `discovered`, or `self`, the same provenance `PEERS` reports). The metrics object also carries `slow_peers`, the count of currently slow members, and `quiet_peers` (0.62.0), the count of currently quiet members, and `low_probe_rate_peers` (0.68.0), the count of currently low-probe-rate members. The metrics object also carries `seed_rejoins` (0.53.0), how many re-JOINs this node has sent to seeds that were not ALIVE — the ~5 s seed-rejoin retry; see "Seed-rejoin count" below. The metrics object also carries `seed_rejoins_by_seed` (0.54.0), the same count broken down per seed as an object — `{"127.0.0.1:17101":2}` — or `{}` when no seed has been re-joined yet, so a JSON consumer can see which seed a node keeps rejoining; the values sum to `seed_rejoins`. The metrics object also carries `seed_last_rejoin_ms` (0.55.0), the same shape but the wall-clock time of each seed's most recent re-JOIN as unix milliseconds — `{"127.0.0.1:17101":1700000100000}` — or `{}` when no seed has been re-joined yet, so a JSON consumer can tell when each seed was last retried; a seed that was never re-joined has no key in either object. The metrics object also carries `seed_rejoin_backoff_ms` (0.56.0), the same shape but the delay in ms the node will wait before the next re-JOIN to each seed — `{"127.0.0.1:17101":10000}` — or `{}` when no seed has been re-joined yet, so a JSON consumer can tell a seed still at the 5 s base from one backed off to the 30 s cap; 0 while the seed is ALIVE (no backoff pending), the scheduled delay while it is down. A top-level `peer_file` object carries the `--peer-file` state (path, `last_reload` / `last_mtime` as unix seconds, and the counts from the last load), or `null` when the node runs without `--peer-file` (0.28.0). A top-level `watch` object carries `subscribers` (how many `WATCH` streams are connected right now) and `sessions` (how many `WATCH` sessions the node has served since startup) (0.33.0), plus the delivery / drop counts (0.34.0): `delivered` (EVENT lines sent in total), `filtered` (events skipped by a stream's filter), `dropped` (events lost to a gap in the event ring), `paused` (0.37.0, how many `WATCH` streams are paused right now), `event_log_cap` (0.36.0, how many events the `WATCH` ring holds — the `--event-log-cap` the node started with, 128 by default), and a `streams` array with the same three counts per currently-connected stream, keyed by its `session` number, plus `paused` per stream. A top-level `gossip_digest_size` (0.41.0) carries how many members one gossip round's digest covers — the `--gossip-digest-size` the node started with, 16 by default. A top-level `phi_threshold` (0.46.0) carries the phi value at which a peer is suspected — the `--phi-threshold` the node started with, 8.0 by default, two decimals like `METRICS` and `PROMETHEUS` — emitted always, whether or not `--phi-accrual` is on, so a JSON consumer can judge a rising per-member `phi` against the threshold that actually fires the suspicion; see "Phi suspicion threshold in JSON". See "WATCH subscriber count in METRICS and JSON", "WATCH drop and delivery counts in METRICS and JSON", and "WATCH backpressure: pausing a stream". Built for tooling that polls cluster state without scraping the text protocol. |
| `PROMETHEUS` | The same values `METRICS` and `JSON` report, as Prometheus text exposition format, then `END`. Each metric family carries a `# HELP` and a `# TYPE` line: membership counts (`pulse_members_alive`, `pulse_members_suspect`, `pulse_members_dead`, `pulse_members_total`), `pulse_slow_peers`, `pulse_quiet_peers` (0.62.0), `pulse_low_probe_rate_peers` (0.68.0), `pulse_uptime_seconds` and `pulse_watch_subscribers` as gauges; the lifetime transition totals (`pulse_suspected_total`, `pulse_dead_total`, `pulse_refutes_total`, `pulse_joins_total`, `pulse_seed_rejoin_total` — one labelled sample per seed that has been re-joined, `pulse_seed_rejoin_total{seed="<id>"} N` (0.54.0), a seed never re-joined has no line and a node that re-joined nothing emits the HELP/TYPE pair with no samples — and the wall-clock time of each seed's most recent re-JOIN as a labelled gauge, `pulse_seed_last_rejoin_ms{seed="<id>"} <unix ms>` (0.55.0), under the same rule — a seed never re-joined has no line, never a fabricated zero, and a node that re-joined nothing emits the HELP/TYPE pair with no samples — so a scraper can alert on a seed whose last retry is getting stale, or tell a retry that is still firing from one that stopped when the cluster merged — and the delay before the next re-JOIN to each seed as a labelled gauge, `pulse_seed_rejoin_backoff_ms{seed="<id>"} <ms>` (0.56.0), under the same rule, 0 while the seed is ALIVE — so a scraper can tell a seed still at the 5 s base from one backed off to the 30 s cap — `pulse_adaptive_fast_cycles_total`, `pulse_suspect_refuted_total`, `pulse_watch_sessions_total`, `pulse_watch_delivered_total`, `pulse_watch_filtered_total`, `pulse_watch_dropped_total`) as counters; the event ring capacity (`pulse_watch_event_log_cap`, 0.36.0) and the paused-stream count (`pulse_watch_paused`, 0.37.0) as gauges; the gossip digest size (`pulse_gossip_digest_size`, 0.41.0) as a gauge; the four latency groups as `mean` / `max` / `samples` gauges (`pulse_detect_latency_ms_mean`, `pulse_suspect_to_dead_ms_mean`, `pulse_refute_latency_ms_mean`, `pulse_probe_rtt_ms_mean`, and the matching `max` and `samples` — mean and max are 0 with no samples, since Prometheus has no null, and the `samples` gauge says whether the mean is real); the detection latency also as a real histogram (`pulse_detect_latency_ms_bucket{le="100"|"250"|"500"|"1000"|"+Inf"}`, plus `_sum` and `_count`, the same five buckets `METRICS` reports as `hist`), and the probe RTT also as a real histogram (`pulse_probe_rtt_ms_bucket{le="1"|"5"|"10"|"50"|"+Inf"}`, plus `_sum` and `_count`, the same five buckets `METRICS` reports as `probe_rtt_ms hist`) (0.48.0). Each member rides as a `pulse_member_info{member="<id>",state="<ALIVE\|SUSPECT\|DEAD>",source="<seed\|peer-file\|discovered\|self>"} 1` gauge, so a scraper can alert on a member's state without polling `JSON`. The last and mean direct-probe round-trip time per member ride as `pulse_member_rtt_ms{member="<id>"}` and `pulse_member_rtt_mean_ms{member="<id>"}` gauges (0.40.0), one line per member that has answered a probe — self and a never-heard-from peer have no line, and a dead member keeps its last RTT. The worst single direct-probe round-trip time per member rides as a `pulse_member_rtt_max_ms{member="<id>"}` gauge (0.64.0), under the same rule — one line per member that has answered a probe, a value of 0 a real worst case of sub-millisecond probes rather than a missing one, and a dead member keeps its worst RTT until GC — so a scraper can alert on a peer that usually answers fast but occasionally stalls, instead of trusting the mean alone. How many direct probes each member was sent and answered rides as `pulse_member_probes_sent_total` / `pulse_member_probes_answered_total` counters (0.66.0), one labelled sample per member that has been probed — a peer probed but never answered still has a sample (sent climbing, answered 0); self and a peer never probed have none; a DEAD member keeps its counters until GC. The recent probe success rate per member rides as a `pulse_member_probe_rate{member="<id>"}` gauge (0.68.0), one line per member with at least one direct probe in its window — two decimals, the same value `JSON` reports as `probe_rate` — so a scraper can alert on a peer answering one probe in ten instead of only on the lifetime rate; self and a peer never probed have no line, and a DEAD member keeps its last window rate until GC. How long each SUSPECT member has been suspected rides as a `pulse_member_suspect_ms{member="<id>"}` gauge (0.58.0), one line per SUSPECT member — the same value `JSON` reports as `suspect_ms` and `METRICS` as a `SUSPECT` line — so a scraper can alert on a suspicion nearing the suspect timeout instead of only on the DEAD transition; ALIVE and DEAD members have no line. How long since this node last heard from each member rides as a `pulse_member_last_seen_ms{member="<id>"}` gauge (0.59.0), one line per member heard from — the same value `JSON` reports as `last_seen_ms` and `METRICS` as a `LASTSEEN` line — so a scraper can alert on a peer going quiet before the suspicion fires; self and a peer never heard from have no line, and a DEAD member keeps its gauge until GC, the value growing. The phi failure-detector value per member rides as a `pulse_member_phi{member=\"<id>\"}` gauge (0.42.0), one line per member that has a phi — phi mode on, not self, heartbeat history with at least two intervals — two decimals like `JSON`, so a scraper can alert on a rising phi before the threshold suspicion fires; a node without `--phi-accrual` emits no such line, and a dead member keeps its last-known phi until GC. The last 16 sampled phi values `JSON` reports as `phi_history` ride as `pulse_member_phi_history_latest{member=\"<id>\"}`, `pulse_member_phi_history_max{member=\"<id>\"}` and `pulse_member_phi_history_samples{member=\"<id>\"}` gauges (0.51.0), one line per member that has a phi — the latest is the most recent probe-cycle sample, not the live value recomputed at scrape time, the max is over the whole window, and the samples gauge says how many of the 16 slots are filled; a node without `--phi-accrual` emits no such lines, and a dead member keeps its frozen history gauges until GC. The same 16 sampled values also ride as a real histogram per member, `pulse_member_phi_history` (0.52.0) — cumulative buckets at `le="0.5"`, `"1"`, `"2"`, `"4"`, `"8"` and `"+Inf"`, plus `_sum` and `_count` — under the same rule as the gauges, so a scraper can compute quantiles of a peer's phi history instead of trusting the latest and max alone, and a dead member keeps its frozen histogram until GC. The suspicion threshold itself rides as a `pulse_phi_threshold` gauge (0.43.0) — the `--phi-threshold` the node started with, 8.0 by default — emitted always, so a scraper can alert on a rising phi against the threshold that actually fires the suspicion instead of hard-coding 8.0. The peer-file gauges (`pulse_peer_file_peers`, `pulse_peer_file_added`, `pulse_peer_file_dropped`, `pulse_peer_file_kept`, `pulse_peer_file_bad`, `pulse_peer_file_last_reload_seconds`, `pulse_peer_file_last_mtime_seconds`) are only emitted when the node runs with `--peer-file`: an absent metric is how Prometheus says "not applicable". This is the exposition format, not an HTTP endpoint — scrape it with `pulse-ctl`, or point a scraper at a tiny exporter that forwards the command. See "Prometheus exposition format (PROMETHEUS)" below. |
| `WATCH [types] [ids] [STATE <list>] [FROM <seq>]` | A long-lived stream: `OK watching`, then one `SNAPSHOT <id> <incarnation> <ALIVE\|SUSPECT\|DEAD> <last-seen-ms\|-> <seed\|peer-file\|discovered\|self>` line per member (the `PEERS` shape), then `END`, then one `EVENT <seq> <JOINED\|SUSPECTED\|DEAD\|REVIVED\|DROPPED\|REFUTED\|QUIET\|LOWRATE> <id> <incarnation> <unix-seconds>` line per membership event as it happens, until the client closes the connection. A `QUIET` event (0.62.0) is an ALIVE peer whose last-contact age crossed `--quiet-peer-ms` — the peer is still alive, it has just gone quiet — and it carries the age as a trailing ` last_seen_ms=<ms>`, like every other event for a member heard from. A `LOWRATE` event (0.68.0) is an ALIVE peer whose recent probe success rate fell below `--low-probe-rate-frac` — the peer is still alive, it has just started answering too few of its probes — and it carries the rate as a trailing ` probe_rate=<value>`, frozen at the crossing. A SUSPECTED event this node raised in phi-accrual mode carries a trailing ` phi=<value>` (0.60.0) — the phi that crossed the threshold, two decimals like the `SUSPECT ... phi=` log line; every other event has no trailing field. An event whose member was heard from carries a trailing ` last_seen_ms=<ms>` (0.61.0) — the ms since this node last heard from the member when the event was recorded, the same value `JSON` reports as `last_seen_ms`, frozen at the event so a replay still shows how quiet the peer was when the transition happened — after the phi field when both are present; self, a member never heard from, and a member already gone from the table carry no field, and an event with neither trailing field writes the plain five fields. See "Phi threshold crossing in the event log" and "Last-contact age in the event log" below. Sequence numbers are 1-based and monotonic for the node's life; a slow consumer that falls more than the event log holds gets `ERR gap: events lost, reconnect` and must reconnect and re-read the snapshot. Runs on its own thread, so other commands keep answering while a client watches. An optional filter (0.30.0) narrows the stream server-side: a comma-separated type list (`WATCH JOINED,DEAD`), a comma-separated peer-id list (`WATCH 127.0.0.1:17102`), or both, in any order — the snapshot is narrowed by the id list too, a filtered-out event still consumes its sequence number so a filtered stream's numbers jump by design, and a bad filter is refused up front with `ERR invalid watch filter <reason>`. A state filter (0.39.0) narrows the stream by the member state an event leaves its member in (`WATCH STATE SUSPECT,DEAD`): JOINED, REVIVED, REFUTED, QUIET and LOWRATE land on ALIVE, SUSPECTED on SUSPECT, DEAD on DEAD, and DROPPED lands on no state at all so it never matches a state filter — the snapshot is narrowed by the member's current state too, and `STATE` composes with the type/id filters and `FROM`, in any order; a bad state is refused up front with `ERR invalid watch filter unknown member state <name>`. Replay / resume (0.31.0): `WATCH FROM <seq>` replays the buffered events from that seq, in order, after the snapshot and `END` and before live events, so a reconnecting client catches up instead of starting from now — `FROM 0` replays from the oldest buffered event, a `FROM` past the latest starts live, and a `FROM` below the oldest buffered event replays from the oldest instead, after a `GAP <requested> <oldest>` line; `FROM` composes with the type/id filters, in any order, and a bad `FROM` is refused up front. Heartbeat (0.32.0): a stream that has sent nothing for 5 s sends `HEARTBEAT <unix-seconds>`, so a client on a quiet cluster can tell "nothing happened" from "the connection died silently" — it carries no sequence number and is sent regardless of the filter, and any line sent resets the 5 s clock. Backpressure / pause (0.37.0): a slow consumer can stop the flow without closing the connection — `WATCH PAUSE <session>` on a separate connection pauses the stream with that session number (the number `JSON` reports per stream), and `WATCH RESUME <session>` resumes it; a paused stream stops consuming events, and on resume it replays what the ring still holds, or ends with `ERR gap` when the ring has scrolled past the resume point. Both are idempotent; a session that is not a live stream answers `ERR unknown watch session <n>`. Event-log persistence (0.38.0): the event log is saved to `<data-dir>/events` as events are recorded and loaded back on startup, so sequence numbers continue across a restart instead of resetting to 1 and a `WATCH FROM <seq>` after a restart can still replay pre-restart events. See "WATCH backpressure: pausing a stream" and "Event-log persistence across restarts" below. |
| anything else | `ERR unknown command` |

## Shared secret

Start every node in a cluster with the same `--secret` (or
`--shared-secret`) value and each node tags every UDP message it sends
with HMAC-SHA256(secret, message), truncated to 8 bytes, and drops any
incoming datagram whose tag is missing or wrong. A node without a
secret sends untagged messages and accepts anything, exactly as
before, so existing single-cluster setups need no change.

```sh
./pulse-node --port 17101 --secret ward-alpha --data-dir /tmp/p1
./pulse-node --port 17102 --secret ward-alpha --seed 127.0.0.1:17101 --data-dir /tmp/p2
```

What it is and is not:

- It is a join gate against accidental cross-talk: two wards, or a
  test rig and a production cluster on the same LAN, cannot merge into
  one membership table. A node with a different secret is simply
  ignored — it never appears in `MEMBERS` — and a node with no secret
  is ignored by secret nodes the same way.
- It is not authentication, not encryption, and not replay protection.
  The secret is passed on the command line (visible in `ps`), the
  messages are still plaintext, the tag is only 8 bytes, and a
  captured datagram can be replayed while its content is still valid.
  Do not use it to keep an attacker out; use it to keep two friendly
  clusters apart. The `START` line reports `secret=on` when a secret
  is configured; the secret value itself is never logged.
- Mixed clusters do not merge, but not symmetrically: a plain node
  accepts anything, so it *hears* a secret node's tagged traffic and
  adds it to its table, then suspects and GCs it when no untagged
  reply ever comes. The secret node never acknowledges the plain node
  at all. The result in both directions is two separate clusters, not
  one working one.

## IPv6

Start a node with `--ipv6` and it runs entirely over IPv6: the UDP
socket binds `::` (v6-only), the default id is `[::1]:<port>`, seeds
are bracketed (`--seed '[::1]:17101'`), and the control plane listens
on `::1` (`pulse-ctl '[::1]:18401' MEMBERS`). `--bind HOST` pins the
bind address to a literal IP instead of the wildcard — `--bind ::1`
for v6 loopback, `--bind 127.0.0.1` for v4 loopback — and the bind
address decides the family and the default id host. IPv4 stays the
default: a node with neither flag behaves exactly as before.

```sh
./pulse-node --ipv6 --port 17101 --data-dir /tmp/p1
./pulse-node --ipv6 --port 17102 --seed '[::1]:17101' --data-dir /tmp/p2
```

What it is and is not:

- A node is one family or the other. There is no dual-stack mode: a
  v6 node sets `IPV6_V6ONLY` on both its sockets, so it neither
  receives nor sends IPv4 traffic. A v4 node and a v6 node therefore
  cannot merge — a seed of the other family is simply unanswerable,
  and goes SUSPECT, DEAD, then GC'd like any seed that never answers.
  (The V6ONLY flag on the *send* socket matters: without it Linux
  maps an IPv4 destination to `::ffff:a.b.c.d` and a v6 node can
  still reach a v4 peer, so the v4 side would hear the v6 node while
  the v6 side could never hear it back — a half-merge. The test
  suite caught that and the flag is set on both sockets.)
- Member ids are `host:port` with IPv6 hosts bracketed, so `[::1]:17101`
  is the id and `::1:17101` also parses (the port is after the last
  colon) but always formats back bracketed. The wire protocol is
  unchanged: gossip entries split `id:inc:state` from the right, so
  bracketed ids need no escaping.
- The control plane follows the node's family. A v6 node's control
  port is on `::1` only, same as the v4 one is on `127.0.0.1` only.
- A v6 node bound to `::` listens on all IPv6 interfaces, like the
  v4 node bound to `0.0.0.0` listens on all IPv4 interfaces. It
  still advertises `[::1]` by default — pass `--id` with a reachable
  address for a multi-host cluster, as with IPv4.

## Seed rejoin

A node sends `JOIN` to each `--seed` once at startup. If the seed is
down at that moment, the node would otherwise stay isolated forever:
it probes the seed, suspects it, marks it DEAD, garbage-collects it,
and the seed — once it comes up — has no reason to contact the node
at all. Two 1-node clusters, permanently.

So the node re-sends `JOIN` to each seed that is not currently
ALIVE in its membership table (logged as `REJOIN <id>` when the
seed had already been garbage-collected). The first retry fires on
the ~5 s tick, as before — then the retry backs off (0.56.0): each
consecutive unanswered re-JOIN doubles the wait — 10 s, 20 s —
up to a 30 s cap, jittered ±10% with the probe jitter, so a
permanently dead seed is probed less and less often instead of
every 5 s forever, while a seed that comes back is still re-joined
within 30 s at worst, and within ~5 s when the outage is fresh:
the backoff resets the moment the seed is ALIVE again, so the next
outage starts again at the base interval instead of resuming at
the cap. A failed re-JOIN send is retried once, immediately
(0.56.0): the send socket is `connect()`ed to the first seed, so
when that seed is down the kernel reports the ICMP
port-unreachable as `ECONNREFUSED` on the next `sendto`, and the
call fails even though the datagram would have gone out fine a
moment later — the error is consumed by the failed call, so the
immediate retry succeeds. Without the retry, a re-JOIN lost to
that stale error would leave the cluster unmerged until the next
retry, which the backoff may have pushed seconds away. The same
one-retry send covers the other two one-shot `JOIN`s (0.57.0):
the startup `JOIN` to each seed — the socket is already
`connect()`ed to the first seed when the loop runs, so when that
seed is down its ICMP is pending by the time the loop reaches
the second seed, and the second seed's `JOIN` would be lost to
the stale error (the first seed's own `JOIN` cannot fail that
way: no send has happened yet, so no ICMP is pending) — and a
peer-file reload's `JOIN` to each newly listed peer, where the
hazard is worse: the node has been running and sending for a
while, so with a dead first seed a stale error is pending about
half the time, and a lost reload `JOIN` would leave the new peer
to the rejoin retry, which the backoff may have pushed up to
30 s away. The retry is one shared send now, not three copies
of the pattern: a `JOIN` still goes out exactly once on the
happy path — the retry only runs when the first send failed —
and a lost probe or gossip message is still just a lost probe,
retried by the next round instead of a second syscall. When the
seed comes up it answers the `JOIN` with a
full-table SYNC, both sides learn about each other, and the
cluster merges. The seed is deliberately not re-added to the
table by the retry itself: it comes back through the normal path
(direct contact re-adds it), so a seed that never answers — wrong
secret, wrong family, genuinely dead — stays garbage-collected
instead of cycling back into the table. A seed that never comes
up costs one UDP datagram per retry interval and nothing else; a
seed of the other address family is skipped, and a seed that is
already ALIVE is never re-joined.

Every re-JOIN the retry sends is counted as `seed_rejoins` in
`METRICS` and `JSON` and as `pulse_seed_rejoin_total` in `PROMETHEUS`
(0.53.0) — one count per re-JOIN, whether the seed was SUSPECT/DEAD
in the table or already garbage-collected — and broken down per
seed as `seed_rejoins_by_seed` in `METRICS` and `JSON` and with a
`seed` label on the Prometheus counter (0.54.0), so an operator can
see which seed a node keeps rejoining. Each seed also carries the
wall-clock time of its most recent re-JOIN, as unix milliseconds
(0.55.0) — `seed_last_rejoin_ms_by_seed` in `METRICS`,
`"seed_last_rejoin_ms"` in the `JSON` metrics object, and a
labelled `pulse_seed_last_rejoin_ms` gauge in `PROMETHEUS` — so
an operator can tell when each seed was last retried, and whether
a climbing count is still firing or stopped when the cluster
merged; a seed that was never re-joined has no entry anywhere,
never a fabricated zero. Each seed also carries the delay the node
will wait before its next re-JOIN, in milliseconds (0.56.0) —
`seed_rejoin_backoff_ms_by_seed` in `METRICS`,
`"seed_rejoin_backoff_ms"` in the `JSON` metrics object, and a
labelled `pulse_seed_rejoin_backoff_ms` gauge in `PROMETHEUS` —
so an operator can tell a seed still at the 5 s base from one
whose retry has backed off to the 30 s cap, and a seed that came
back (0, no backoff pending) from one still being retried. A node
whose seeds were up at startup reports 0, exactly like a node
with no seeds at all. See "Seed-rejoin count" below.

```sh
# Terminal 1: start node 2 first — its seed is not up yet
./pulse-node --port 17102 --seed 127.0.0.1:17101 --data-dir /tmp/p2
# Terminal 2: start node 1 a few seconds later
./pulse-node --port 17101 --data-dir /tmp/p1
# Node 2 logs REJOIN, node 1 logs JOIN, both report alive=2.
```

## Seedless bootstrap

A `--seed` flag names one peer to join through, which makes the
seed special: it has to be up first (or the rejoin retry has to
wait for it), and every node's command line has to name it. A
`--peer-file` file replaces that with a plain list — one
`host:port` per line, blank lines and `#` comment lines ignored —
that every node in the cluster can share:

```
# ward cluster peers
127.0.0.1:17101
127.0.0.1:17102
127.0.0.1:17103
```

```sh
./pulse-node --port 17101 --peer-file peers --data-dir /tmp/p1
./pulse-node --port 17102 --peer-file peers --data-dir /tmp/p2
./pulse-node --port 17103 --peer-file peers --data-dir /tmp/p3
# All three converge on alive=3, in any start order, with no --seed.
```

Each entry behaves exactly like a `--seed` flag: the node sends it
a `JOIN` at startup, probes it, and re-sends `JOIN` while it is
not ALIVE — first on the ~5 s tick, then backing off to 30 s
(0.56.0) — so a peer that starts late still merges.
The file can list every member including the node itself — joining
yourself is harmless — and a node can combine `--seed` flags with a
peer file; both feed the same seed list (at most 16 entries total).
The node logs `PEERS <n> from <file>` at startup.

Two deliberate strictnesses. A malformed line is logged
(`peer file <file> line <n>: skipping bad peer '<line>'`) and
skipped, never fatal: a hand-edited file with one typo must not
stop the node from joining through its good peers. But a missing
or unreadable file is a clean startup error — the node prints
`cannot read peer file <file>: <reason>` and exits nonzero before
starting — because silently starting alone would hide a typo'd
path and leave the node isolated with no sign anything was wrong.
An empty file is not an error: a node with an empty peer file
starts alone, exactly like one with no seeds.

## Live peer-file reload

The peer file is the cluster's roster, and rosters change: a ward
device is added, one is retired, one is renumbered. Restarting a
node to pick up the new roster would drop it out of the cluster and
force a rejoin, so while a `--peer-file` is in use the node re-reads
the file on SIGHUP — `kill -HUP <pid>` — and merges the new list
into its seed list without restarting:

```sh
# Terminal 1: 3-node cluster from one shared peer file
./pulse-node --port 17101 --peer-file peers --data-dir /tmp/p1
# Edit peers: remove 127.0.0.1:17103, add 127.0.0.1:17104
kill -HUP $(pgrep -f 'pulse-node --port 17101')
# Node 1 logs PEERFILE ADD 127.0.0.1:17104 once node 4 starts, and
# PEERFILE KEEP 127.0.0.1:17103 (still ALIVE) for the removed peer.
```

A peer newly listed in the file gets exactly the `--seed` treatment:
it joins the seed list (so the seed-rejoin retry covers it while it
is down), is added to the membership table, and gets a `JOIN` at
once, so it merges as soon as it starts answering. That `JOIN`
goes through the same one-retry send as a re-JOIN (0.57.0): the
node has been running and sending for a while, so a stale ICMP
port-unreachable from a dead first seed can be pending on the
send socket and fail the first attempt even though the datagram
would go out fine a moment later — the immediate retry succeeds,
so the new peer is not left to the rejoin retry, which the
backoff may have pushed seconds away.

A peer removed from the file is dropped from the seed list — but
from the membership table only if it is currently unreachable. A
peer that is still ALIVE stays a member: the file says who to seed
from, the cluster says who is alive, and a file edit alone must
never evict a live peer. It stays in the table until the normal
failure path suspects, deads and GCs it, and it gets no more rejoin
retries in the meantime — but the node remembers it in a kept list,
so a later SIGHUP drops it the moment it stops answering instead of
forgetting it the moment the file edit lands. A peer that is SUSPECT,
DEAD, or absent from the table is force-removed at once, with a
tombstone so gossip cannot resurrect it, and the drop is logged as
`PEERFILE DROP <id>`; a kept peer is logged as `PEERFILE KEEP <id>
(still ALIVE)`. Every reload ends with a summary line,
`PEERFILE RELOAD <file> peers=<n> added=<a> dropped=<d> kept=<k>`,
where `kept` is how many removed-but-live peers are still held.

Three deliberate strictnesses. A peer also named with a `--seed`
flag is never added or dropped by a reload: the flag outranks the
file, so a flag peer stays a seed whatever the file says. Self is
never dropped: a node that removes its own line from the file stays
a member of its own cluster. And a reload that cannot read the file
— deleted, permissions, a typo'd edit — keeps the old list and logs
`PEERFILE reload <file>: cannot read: <reason>`, instead of dropping
the node to a solo start; bad lines are logged and skipped, exactly
as at startup. SIGHUP on a node with no `--peer-file` is a no-op:
the node keeps running, logs nothing, and never restarts — SIGHUP
never restarts a node, with or without a peer file.

The reload runs on the prober thread, on its next cycle (within one
probe interval): the SIGHUP handler itself only sets a flag, so no
file I/O happens inside a signal handler. The handler is installed
with `sigaction`, not `signal`: with this program's strict POSIX
feature macros, `signal` has System V semantics and the handler
would reset to the default after the first delivery, so the second
SIGHUP would kill the node.

The same reload is available over the control plane as `RELOAD`
(0.25.0): `./pulse-ctl 127.0.0.1:18401 RELOAD` re-reads the file from
the control thread and answers `OK peers=N added=A dropped=D kept=K
bad=B` — the same numbers as the `PEERFILE RELOAD` log line — so an
operator who cannot signal the process (a container, a supervised
service) can still trigger a reload. A node with no `--peer-file`
answers `ERR no peer file`, and an unreadable file answers `ERR
cannot read peer file <path>: <reason>` while the old list is kept.
SIGHUP and RELOAD run the same code, serialized on one lock, so the
two triggers can never interleave their seed-list edits and JOIN
sends.

And since 0.26.0 the node watches the file itself: once a second the
prober thread stats `--peer-file`, and when its mtime moved since
the last load or reload it re-runs the same merge — no signal, no
control-plane call, no restart. The reload logs `PEERFILE <file>
changed on disk, auto-reloading` first, then the usual `PEERFILE
RELOAD` summary and `ADD`/`DROP`/`KEEP` lines, so an auto-reload is
visible in the log exactly like a manual one. A SIGHUP or RELOAD
reload refreshes the watch's baseline, so a manual reload never
leaves the watch thinking the file changed and re-running the merge
right after. The check is wall-clock (once a second), so it is
unaffected by the adaptive probe interval, and it costs one `stat`
per second and nothing else while the file is untouched.

Three honest caveats. The watch sees the mtime, not the content: an
edit that preserves the mtime (say, `cp -p`) is invisible to it, and
SIGHUP or `RELOAD` is still the way to force a reload then. A kept
peer — removed from the file while still ALIVE — that dies afterwards
is only dropped at the next file change, when the merge re-examines
it: the watch cannot know a peer died without a file edit to
trigger on, so until the next edit the peer leaves the table the
slow way, through the normal suspect-to-dead-to-GC path. And a
deleted file is skipped quietly (a manual reload still reports it);
when the file reappears with a new mtime the watch picks it up on
the next check.

## Peer-file state in METRICS and JSON

A reload is logged, but an operator polling the cluster should not
have to scrape the log to answer "which roster is this node running
from, and when did it last pick it up?" — so since 0.28.0 the
peer-file state is exposed on the control plane, in both forms:

```sh
./pulse-ctl 127.0.0.1:18401 METRICS
# METRICS ... slow_peers=0 peer_file=peers last_reload=1795450000 last_mtime=1795449900 pf_peers=3 pf_added=1 pf_dropped=0 pf_kept=0 pf_bad=0

./pulse-ctl 127.0.0.1:18401 JSON
# {...,"peer_file":{"path":"peers","last_reload":1795450000,"last_mtime":1795449900,"peers":3,"added":1,"dropped":0,"kept":0,"bad":0}}
```

`peer_file` is the `--peer-file` path as given on the command line,
`last_reload` is the wall-clock time (unix seconds) of the last
successful load or reload, and `last_mtime` is the file's mtime
(unix seconds) at that load — so a `last_mtime` older than the
file's current mtime means the node has not picked up the latest
edit yet, and a `last_reload` that never advances after an edit
means the reloads are not reaching the node at all. The counts are
the same numbers as the `PEERFILE RELOAD` log line for that load:
`peers` listed in the file, `added` / `dropped` / `kept` from the
merge, `bad` lines skipped as malformed. The initial load at
startup counts as the first reload, with `added` the peers the file
contributed to the seed list, so the fields are populated from the
moment the node starts — there is no "no reload yet" state to
special-case. A reload that cannot read the file leaves the state
at the last successful load instead of blanking it, so the numbers
always describe a roster the node actually holds; the failure is
still in the log (`PEERFILE reload ... cannot read`).

Each member in `JSON` also carries a `source` field — `seed`,
`peer-file`, `discovered`, or `self` — the same provenance `PEERS`
reports, so a peer-file peer is distinguishable from a discovered
one in the machine-readable view too, including a kept peer
(removed from the file but still ALIVE), which still reports
`peer-file` until a later reload drops it.

A node without `--peer-file` reports `-` for every peer-file field
in `METRICS` and `"peer_file":null` in `JSON`; the per-member
`source` still works there, since seeds and gossip exist without a
file. One honest caveat: the `METRICS` line is space-separated, so
a peer-file path containing a space breaks its field parsing —
`JSON` is the machine-readable form, and its path is escaped.

## Prometheus exposition format (`PROMETHEUS`)

`METRICS` is one line a human can read and `JSON` is one object a
program can parse, but neither is what a monitoring stack scrapes:
Prometheus wants `# HELP` / `# TYPE` lines and `name value` pairs,
with counters for things that only grow and gauges for current
state. Since 0.35.0 the control plane answers that directly:

```sh
./pulse-ctl 127.0.0.1:18401 PROMETHEUS
# # HELP pulse_members_alive Members currently ALIVE.
# # TYPE pulse_members_alive gauge
# pulse_members_alive 2
# # HELP pulse_suspected_total ALIVE to SUSPECT transitions seen.
# # TYPE pulse_suspected_total counter
# pulse_suspected_total 1
# # HELP pulse_detect_latency_ms Detection latency in ms, last direct contact to SUSPECT.
# # TYPE pulse_detect_latency_ms histogram
# pulse_detect_latency_ms_bucket{le="100"} 0
# pulse_detect_latency_ms_bucket{le="250"} 1
# pulse_detect_latency_ms_bucket{le="500"} 1
# pulse_detect_latency_ms_bucket{le="1000"} 1
# pulse_detect_latency_ms_bucket{le="+Inf"} 1
# pulse_detect_latency_ms_sum 203
# pulse_detect_latency_ms_count 1
# pulse_member_info{member="127.0.0.1:17101",state="ALIVE",source="self"} 1
# pulse_member_info{member="127.0.0.1:17102",state="ALIVE",source="peer-file"} 1
# pulse_member_rtt_ms{member="127.0.0.1:17102"} 1
# pulse_member_rtt_mean_ms{member="127.0.0.1:17102"} 0
# pulse_member_rtt_max_ms{member="127.0.0.1:17102"} 1
# pulse_peer_file_peers 3
# END
```

The mapping from `METRICS` / `JSON` is one-to-one, so nothing new
is measured — it is the same values in a scrapable shape. The
lifetime totals are counters (`_total`): suspected, dead, refutes,
joins, adaptive fast cycles, suspect refuted, watch sessions, and
the watch delivery / drop counts. Current state is gauges:
membership counts, slow peers, uptime, current watch subscribers,
and current paused watch streams (`pulse_watch_paused`, 0.37.0).
The three latency groups are `mean` / `max` / `samples` gauges;
mean and max are 0 with no samples, because Prometheus has no null
— the `samples` gauge is what says whether the mean is real, the
same way `JSON` uses `null`. The detection latency also rides as
a real histogram: the same five buckets `METRICS` reports as
`hist` (<100, 100–249, 250–499, 500–999, ≥1000 ms), cumulative,
with `_sum` and `_count`, so a scraper can compute quantiles
instead of trusting the mean. Each member is one
`pulse_member_info` gauge (always 1), labelled by member, state
and source — the Prometheus way of exposing per-member state
without a separate metric per peer — and the peer-file gauges
carry the last load's counts and times, emitted only when the node
runs with `--peer-file`.

Probe RTT per member (0.40.0): the last and mean direct-probe
round-trip time `JSON` already reports as `rtt_ms` /
`rtt_mean_ms` ride as `pulse_member_rtt_ms{member="<id>"}` and
`pulse_member_rtt_mean_ms{member="<id>"}` gauges, one line per
member that has answered a probe, so a scraper can alert on a
slow peer without polling `JSON` — the same threshold logic as
the slow-peer warning, but as a number instead of a boolean.
Self has no line — it never probes itself — and neither does a
peer never heard from: an absent series is how Prometheus says
"not applicable", the same as the peer-file gauges. A value of 0
is a real sub-millisecond probe, not a missing one. A member
that goes DEAD keeps its last RTT, the same as `JSON`, so the
gauge is the last-known RTT, not a live one, for a dead member.

Phi per member (0.42.0): the phi failure-detector value `JSON`
already reports as `phi` rides as
`pulse_member_phi{member="<id>"}` — one line per member that has
a phi, under the same rule as `JSON`: phi mode on, not self,
heartbeat history with at least two intervals — two decimals
like `JSON`, so a scraper can alert on a rising phi well before
the threshold suspicion fires, instead of only after the peer
is already SUSPECT. A node without `--phi-accrual` emits no
`pulse_member_phi` line at all, and a peer never heard from has
none either: an absent series is how Prometheus says "not
applicable", the same as the RTT gauges. A member that goes DEAD
keeps its last heartbeat, so its phi keeps growing until GC, the
same as `JSON` reports it — the gauge is the last-known phi, not
a live one, for a dead member.

Phi suspicion threshold (0.43.0): the threshold the per-member phi
is judged against rides as a `pulse_phi_threshold` gauge — the
`--phi-threshold` the node started with, 8.0 by default, two
decimals like the phi gauges — so a scraper can alert on a rising
phi against the threshold that actually fires the suspicion,
instead of hard-coding 8.0 and silently disagreeing with a node
started with `--phi-threshold 5.5`. It is emitted always, like
the other config gauges (`pulse_gossip_digest_size`,
`pulse_watch_event_log_cap`): the threshold is the node's
configured value whether or not `--phi-accrual` is on, so a
scraper watching a fixed-timeout node still sees the threshold
its phi mode would use.

Per-member phi in METRICS (0.44.0): the phi failure-detector
value `JSON` reports as `phi` per member and `PROMETHEUS` rides
as a `pulse_member_phi` gauge now also rides in the text
protocol — one `PHI <id> <value>` line after the `METRICS` line
for each member that has a phi, under the same rule as the other
two: phi mode on, not self, heartbeat history with at least two
intervals — two decimals like `JSON`, the live phi at reply
time, so it rises as a peer goes quiet. That completes the phi
trio: an operator polling the text protocol can watch a peer's
phi rise without parsing `JSON` or scraping Prometheus, and a
`pulse-ctl` one-liner shows the same number the other two views
show. A node without `--phi-accrual` emits no `PHI` line at all,
and a peer never heard from has none either; a member that goes
DEAD keeps its last-known phi until GC, the same as `JSON`
reports it. The lines ride after the `METRICS` line and before
the usual `END` trailer, so a parser that reads only the first
line sees exactly the `METRICS` line it always saw.

Phi suspicion threshold in METRICS (0.45.0): the threshold the
per-member phi is judged against now also rides on the
`METRICS` line itself — `phi_threshold=<N.NN>` at the end of the
line, after `gossip_digest_size` — the `--phi-threshold` the
node started with, 8.0 by default, two decimals like the
`pulse_phi_threshold` gauge. It is emitted always, like the
other config gauges, whether or not `--phi-accrual` is on, so a
text-protocol consumer can judge a rising `PHI` line against
the threshold that actually fires the suspicion instead of
hard-coding 8.0 — and a `pulse-ctl` one-liner shows the same
number `PROMETHEUS` reports, with no scraping at all.

Phi suspicion threshold in JSON (0.46.0): the same threshold
now also rides in `JSON` as a top-level `"phi_threshold":N.NN`
number, after `"gossip_digest_size"` — the `--phi-threshold`
the node started with, 8.0 by default, two decimals like the
other two views. It is emitted always, like the other config
fields, whether or not `--phi-accrual` is on, so a JSON
consumer polling cluster state can judge a rising per-member
`"phi"` against the threshold that actually fires the
suspicion instead of hard-coding 8.0 — and a `pulse-ctl`
one-liner shows the same number `METRICS` and `PROMETHEUS`
report, with no text parsing or scraping at all.

Phi history in JSON (0.47.0): the live per-member `"phi"` says
how suspicious a peer is right now, but not how it got there —
was it a slow climb over a second, or a sudden jump? Each member
in `JSON` now also carries `"phi_history"`: the last 16 phi
values sampled for that peer, as `[[age_ms, phi], ...]` pairs
oldest first — `age_ms` is the ms since this node started, `phi`
to two decimals like the live value — so a JSON consumer can see
the suspicion evolve: near 0 while the peer answers, climbing
past the `"phi_threshold"` as it goes quiet, frozen at the value
it reached when the peer went DEAD. The samples are taken once
per probe cycle (~100 ms by default, so 16 samples cover ~1.6 s),
for ALIVE and SUSPECT peers only: a DEAD peer's history is frozen
at suspicion, since its phi no longer changes, and sampling it
would only push the interesting rise out of the bounded ring.
The field follows the same rule as `"phi"` itself: `null` when
phi-accrual is off, for self, or for a peer with no heartbeat
history, and `[]` when the peer has history but no probe cycle
has sampled it yet. Polling `JSON` never moves the history it
reads — the samples come from the prober thread, not from a
control-plane read — and the full time series stays in `JSON`: a
history is a time series, and a gauge is a point in time. Since
0.51.0 `PROMETHEUS` also carries the history as summary gauges —
latest, max and sample count per member — so a scraper can alert
on the shape of the rise without polling `JSON`; see "Phi history
in PROMETHEUS" below.

Phi history in PROMETHEUS (0.51.0): the live phi gauge says how
suspicious a peer is right now, but a peer that climbed to 7.9
and fell back looks the same as one that never climbed at all —
the history is what tells them apart, and a scraper should not
have to poll `JSON` to see it. Each member that has a phi now
also carries three gauges over its last 16 sampled values:
`pulse_member_phi_history_latest{member="<id>"}` is the most
recent probe-cycle sample (not the live value recomputed at
scrape time, so it agrees with the last entry of `JSON`'s
`phi_history`), `pulse_member_phi_history_max{member="<id>"}`
is the largest sample in the window, and
`pulse_member_phi_history_samples{member="<id>"}` is how many
of the 16 slots are filled — the samples gauge says whether the
latest and max are real, the same convention as the latency
groups' `samples` gauges. The rule is the same as the live phi
gauge: phi mode on, not self, heartbeat history with at least
two intervals, and at least one sample taken — a peer with
history but no sample yet has no lines, and a node without
`--phi-accrual` emits none at all. A member that goes DEAD keeps
its frozen history gauges until GC, like the live phi gauge, so
a scraper can still see how far phi climbed at suspicion after
the peer is gone. The full time series — every sample with its
age — stays in `JSON` only: three gauges summarise a history,
they do not replace it.

Phi history histogram in PROMETHEUS (0.52.0): the latest and max
gauges say where a peer's phi is now and how far it climbed, but
not how it spent its window — a peer that sat near 0 with one
spike to 7.9 looks the same as one that climbed steadily past 4.
The same 16 sampled values now also ride as a real histogram per
member, `pulse_member_phi_history`: cumulative buckets at
`le="0.5"`, `"1"`, `"2"`, `"4"`, `"8"` and `"+Inf"`, plus
`pulse_member_phi_history_sum{member="<id>"}` (the sum of the
sampled values, two decimals like the gauges) and
`pulse_member_phi_history_count{member="<id>"}` (how many of the
16 slots are filled — always equal to the `+Inf` bucket and to
the `pulse_member_phi_history_samples` gauge for the same
member). The buckets split the climb a peer makes as it goes
quiet: under 0.5 while it answers, past 1 and 2 as suspicion
builds, past 4 as it nears the threshold, and the 8.0 bucket is
the threshold itself — a sample exactly at a bound lands in that
bucket, like the detection-latency and probe-RTT histograms. The
rule is the same as the history gauges: phi mode on, not self,
heartbeat history with at least two intervals, and at least one
sample taken — a peer with history but no sample yet has no
lines, and a node without `--phi-accrual` emits none at all. A
member that goes DEAD keeps its frozen histogram until GC, like
its gauges, so the shape of the rise at suspicion stays visible
after the peer is gone.

Probe RTT histogram (0.48.0): the same five-bucket shape as the
detection-latency histogram, but over answered direct probes —
`pulse_probe_rtt_ms` as a real histogram with cumulative buckets
at `le="1"`, `"5"`, `"10"`, `"50"` and `"+Inf"`, plus `_sum` and
`_count`, and `pulse_probe_rtt_ms_mean` / `max` / `samples` as
gauges alongside it. The buckets are <1, 1–4, 5–9, 10–49, ≥50 ms
(the loopback case is nearly all bucket 0), a sample exactly at a
bound lands in the higher bucket like the detection histogram, and
the gauges are 0 with no samples yet — a mean of 0 with 0 samples
means "no probe answered", the same convention as the other
latency groups. A scraper can compute RTT quantiles from the
histogram instead of trusting the mean, and can alert on the tail
— a peer that usually answers in under a millisecond but
sometimes takes 60 ms — before the peer is ever suspected.

Two honest caveats. This is the exposition *format*, not an HTTP
endpoint: the control plane is a line-based TCP protocol, so the
reply ends with the usual `END` trailer, which a Prometheus
scraper would not understand — scrape it with `pulse-ctl`, or
point a scraper at a tiny exporter that forwards the command and
strips the trailer. And the uptime gauge is seconds since this
node started, not since the process was built: a restarted node
resets it, which is exactly what an uptime gauge should do.

## Peer provenance

A membership table answers "who is in the cluster", but not "how did
this node hear about them" — and when a cluster is misbehaving that is
the first question an operator asks: is that peer here because the
roster says so, because a `--seed` flag named it, or because gossip
spread it from somewhere else? The control plane's `PEERS` command
(0.27.0) answers it, one line per member:

```sh
./pulse-ctl 127.0.0.1:18401 PEERS
# PEER 127.0.0.1:17101 1 ALIVE - self
# PEER 127.0.0.1:17102 1 ALIVE 12 peer-file
# PEER 127.0.0.1:17103 1 ALIVE 8 discovered
```

Each line carries the member id, its incarnation, its state, the ms
since this node last heard directly from it, and its source. The
last-seen is `-` for self — it never probes itself, so its stored
time is just startup — and for a peer never heard from, a seed that
never answered: no contact, no number. For a live peer it is the gap
since the last PING, ACK, gossip, or JOIN from that peer, so a peer
whose last-seen keeps climbing toward the ack timeout is a peer about
to be suspected, and the number is a local measurement — it is never
gossiped, and it says nothing about what any other node last saw.

The source is where *this* node got the peer from, and a `--seed`
flag outranks the file, exactly as the reload does: a peer named with
both reports as `seed`. A peer removed from the file but kept because
it was still ALIVE still reports as `peer-file` — the file is where
it came from, even though the file no longer lists it — until a later
reload drops it, and then it is gone from the table entirely. A peer
restored from `<data-dir>/members` after a restart reports as
`discovered`: persistence remembers that the peer existed, not how
the node first heard of it, and the peer is re-probed from scratch
either way.

One honest caveat. The source is this node's view, not the cluster's:
a peer that is `peer-file` here may be `discovered` on a node whose
file never listed it, and a peer that is `seed` here may be `peer-file`
on a node that seeds from the roster. That is the point — `PEERS`
shows why *this* node knows each member, so a roster typo, a stale
seed flag, or a peer that only gossip ever mentioned can be told
apart at a glance.

## Control-plane event stream (`WATCH`)

Polling `MEMBERS` or `JSON` once a second tells an operator what the
cluster looks like, but not what just changed — and a failure that
comes and goes between two polls is invisible entirely. Since 0.29.0
the control plane streams membership events instead: one long-lived
TCP connection that pushes an `EVENT` line for every transition as
it happens, so a dashboard, an alerter, or a ward gateway can watch
the cluster live instead of polling it.

```sh
./pulse-ctl 127.0.0.1:18401 WATCH
# OK watching
# SNAPSHOT 127.0.0.1:17101 1 ALIVE - self
# SNAPSHOT 127.0.0.1:17102 1 ALIVE 12 peer-file
# END
# EVENT 1 JOINED 127.0.0.1:17102 1 1795450000
# EVENT 2 SUSPECTED 127.0.0.1:17103 1 1795450001
# EVENT 3 DEAD 127.0.0.1:17103 1 1795450002
# ... the connection stays open until the client closes it
```

The stream starts with a snapshot, not with history: `OK watching`,
then one `SNAPSHOT` line per member in the `PEERS` shape (id,
incarnation, state, ms since last direct contact, source), then
`END`. A late subscriber therefore sees the cluster as it is the
moment it connects, without having to ask `MEMBERS` first — and
without replaying everything that happened before it arrived. The
events after the snapshot are the changes:

| Event | Meaning |
| ----- | ------- |
| `JOINED` | A peer appeared in the table — first direct contact, a gossip entry about a peer never seen, or a peer added by a peer-file reload. |
| `SUSPECTED` | A peer went ALIVE to SUSPECT — this node stopped hearing from it, or gossip said so. |
| `DEAD` | A peer went SUSPECT to DEAD — its suspect window ran out, or gossip said so. |
| `REVIVED` | A SUSPECT or DEAD peer came back ALIVE — it answered again. A DEAD peer coming back is a restart, not a refutation, but it is still a revival to a watcher. |
| `DROPPED` | A peer left the table — garbage-collected after its retention expired, force-removed by `LEAVE`, or dropped by a peer-file reload. |
| `REFUTED` | This node heard gossip calling itself SUSPECT or DEAD and bumped its incarnation to refute it. |

Each `EVENT` line carries a sequence number (1-based, monotonic for
the node's life), the member id, its incarnation at the event, and
the wall-clock time in unix seconds. A client that sees a jump in
the sequence numbers knows it missed events. An event may also
carry trailing fields, in this order: a phi-mode SUSPECTED carries
` phi=<value>` (see "Phi threshold crossing in the event log"), an
event whose member was heard from carries
` last_seen_ms=<ms>` (see "Last-contact age in the event log"),
and an event whose member was probed carries
` probes_sent=<n> probes_answered=<n>` (see "Probe counts in the
event log") — all frozen at the event, so a replay shows the values
as they were when the transition happened, not as they are now.

A watcher that only cares about some of the cluster can narrow the
stream instead of filtering client-side (0.30.0): `WATCH` takes an
optional type list, an id list, or both, in any order —

```sh
./pulse-ctl 127.0.0.1:18401 WATCH JOINED,DEAD 127.0.0.1:17102
# OK watching
# SNAPSHOT 127.0.0.1:17102 1 ALIVE 12 peer-file
# END
# EVENT 4 JOINED 127.0.0.1:17102 1 1795450003
# EVENT 9 DEAD 127.0.0.1:17102 1 1795450008
```

A token with a `:` is a peer-id list (comma-separated `host:port`,
IPv6 bracketed); a token without one is an event-type list
(comma-separated `EVENT` words). The filter is applied server-side
before sending, so a filtered-out event never crosses the wire —
and the snapshot is narrowed by the id list too, so a watcher
subscribed to one peer sees only that peer (a type filter does not
narrow the snapshot, which has no event type). A filtered-out event
still consumes its sequence number, so a filtered stream's numbers
jump by design: a jump means filtered-out events, not lost ones,
and only an `ERR gap` line means the client fell behind. A bad
filter is refused up front — `WATCH BOGUS` answers `ERR invalid
watch filter unknown event type BOGUS` and the connection closes —
instead of opening a stream that silently drops everything.

A watcher that only cares about trouble can filter by the member
state instead of the event type (0.39.0): `WATCH STATE
SUSPECT,DEAD` carries only the events that leave their member
SUSPECT or DEAD, so a pager or an alerter never sees joins or
revivals at all —

```sh
./pulse-ctl 127.0.0.1:18401 WATCH STATE SUSPECT,DEAD
# OK watching
# END
# EVENT 4 SUSPECTED 127.0.0.1:17103 1 1795450018
# EVENT 5 DEAD 127.0.0.1:17103 1 1795450019
```

Each event type lands on exactly one state: JOINED, REVIVED and
REFUTED land on ALIVE, SUSPECTED on SUSPECT, DEAD on DEAD. That is
why a state filter and a type filter are not the same subscription:
`WATCH DEAD` and `WATCH STATE DEAD` carry the same events, but
`WATCH STATE ALIVE` carries JOINED *and* REVIVED *and* REFUTED —
every event that leaves a member alive — where a type list would
have to name all three. DROPPED lands on no state at all: the
member left the table, so there is no state to match, and a state
filter never carries it — a watcher that wants removals subscribes
to the DROPPED type instead. The state filter narrows the snapshot
too, by the member's current state, so the stream above opens with
no `SNAPSHOT` lines on an all-ALIVE cluster, and `WATCH STATE DEAD`
opened after a kill snapshots only the dead member. It composes
with the type/id filters and `FROM`, in any order (`WATCH STATE
DEAD FROM 5`, `WATCH JOINED,DEAD STATE SUSPECT,DEAD`), and the
filter applies to replayed events exactly as to live ones. A bad
state is refused up front — `WATCH STATE BOGUS` answers `ERR
invalid watch filter unknown member state BOGUS`, `WATCH STATE`
with no list answers `ERR invalid watch filter STATE requires a
state list`, and a second `STATE` answers `ERR invalid watch
filter duplicate STATE` — and the connection closes.

A watcher that disconnects and reconnects can also resume instead
of starting from now (0.31.0): `WATCH FROM <seq>` replays the
buffered events from that sequence number, in order, after the
snapshot and `END` and before live events —

```sh
./pulse-ctl 127.0.0.1:18401 WATCH FROM 4
# OK watching
# SNAPSHOT 127.0.0.1:17101 1 ALIVE - self
# SNAPSHOT 127.0.0.1:17102 1 ALIVE 12 peer-file
# END
# EVENT 4 JOINED 127.0.0.1:17102 1 1795450003
# EVENT 9 DEAD 127.0.0.1:17102 1 1795450008
# ... live events continue after the replay
```

The snapshot still goes first: it is the current table, the replay
is the transitions the client missed, and a client dedupes by
sequence number when an event appears in both (an event that lands
between the snapshot and the replay is in both, exactly as with the
plain live stream). `FROM 0` replays from the oldest buffered
event — the whole ring — and a `FROM` past the latest sequence
starts live, with nothing to replay yet: the next event to be
recorded is the first one the client sees. `FROM` composes with
the type/id filters, in any order (`WATCH FROM 5 JOINED,DEAD`,
`WATCH JOINED,DEAD FROM 5`), and the filter applies to replayed
events exactly as to live ones — a filtered-out replayed event
still consumes its sequence number, so the replayed numbers jump
by design.

The ring only holds the last `--event-log-cap` events (128 by
default), so a resume point older than that has been evicted:
instead of dropping the connection, the stream signals the loss
and replays from the oldest event it still holds —

```sh
./pulse-ctl 127.0.0.1:18401 WATCH FROM 2
# OK watching
# SNAPSHOT 127.0.0.1:17101 1 ALIVE - self
# END
# GAP 2 7
# EVENT 7 JOINED 127.0.0.1:17102 1 1795450010
# ... the replay continues from seq 7, then live events
```

`GAP <requested> <oldest>` says the requested seq is gone and the
replay starts at `<oldest>` instead; the stream stays open, unlike
a consumer that falls behind mid-stream, which still gets
`ERR gap: events lost, reconnect` and must reconnect. A bad `FROM`
is refused up front — `WATCH FROM` answers `ERR invalid watch
filter FROM requires a sequence number`, `WATCH FROM xyz` answers
`ERR invalid watch filter invalid FROM seq xyz`, and a second
`FROM` answers `ERR invalid watch filter duplicate FROM` — and
the connection closes.

A quiet cluster is a problem for a watcher: a stream that sends
nothing for minutes looks exactly like a connection that died
silently — a half-open TCP connection, a crashed gateway, a
firewall that dropped the state — and the client has no way to
tell the two apart. So since 0.32.0 a stream that has sent nothing
for 5 s sends a heartbeat of its own —

```sh
./pulse-ctl 127.0.0.1:18401 WATCH
# OK watching
# SNAPSHOT 127.0.0.1:17101 1 ALIVE - self
# END
# HEARTBEAT 1795450010
# HEARTBEAT 1795450015
# EVENT 4 JOINED 127.0.0.1:17102 1 1795450018
```

`HEARTBEAT <unix-seconds>` carries the current wall-clock time
and no sequence number: it is not an event, it does not consume
one, and it is sent regardless of the type/id filter — a watcher
subscribed to one peer still hears the stream itself is alive.
Any line sent — `EVENT`, `GAP`, or `HEARTBEAT` — resets the 5 s
clock, so a busy cluster never sees a heartbeat at all, and a
client that only understands `EVENT` lines ignores anything else,
so the heartbeat is safe to add to an existing watcher.

Three honest caveats. The event log is a bounded ring (the last
`--event-log-cap` events, 128 by default): a consumer that falls
more than a ring behind — a client that stops reading for a long
time — gets `ERR gap: events lost, reconnect` and the connection
closes, instead of the node buffering unboundedly for it; the
client reconnects and re-reads the snapshot.
The snapshot is taken just after the latest sequence number is read,
so no event is ever lost between the two — but an event that lands
in that tiny window may appear both in the snapshot and in the
stream, and a client dedupes by sequence number. And the log only
holds what this node recorded: members restored from
`<data-dir>/members` are pre-existing state, not events, so a
`WATCH` opened after a restart sees them in the snapshot and never
as `JOINED` lines — same for seed and peer-file entries that were
already in the table before the stream opened (a peer-file peer
that was dead before it ever answered comes back as `REVIVED`, not
`JOINED`). Since 0.38.0 the events themselves survive a restart
(see "Event-log persistence across restarts"), so a replay after a
restart can still reach events recorded before it — only the ring's
bound still applies.

`WATCH` runs on its own thread, so the rest of the control plane
keeps answering while a client watches — `MEMBERS`, `STATUS`,
`LEAVE` and the rest are unaffected by an open stream. `pulse-ctl`
already prints until the server closes the connection, so it streams
a `WATCH` until interrupted (Ctrl-C); with its stdout to a file,
run it under `stdbuf -o0` or the lines sit in the buffer. Like the
rest of the control plane, `WATCH` has no authentication — it is a
local debugging and operations interface, not a public API.

## Configurable event-log ring size (`--event-log-cap`)

Since 0.36.0 the ring is not a fixed 128 events: a node started
with `--event-log-cap N` holds the last N membership events for
replay (1 to 100000, default 128). A busy cluster — many members
churning — wants a bigger ring, so a watcher that reconnects after
a minute can still catch up from its last sequence instead of
getting a `GAP` and re-reading the snapshot; a small node that
never replays can hold less. The cap is fixed at startup (the ring
is allocated once, when the node starts) and is reported on the
control plane so an operator can see it without reading the
command line: `METRICS` reports `watch_event_log_cap=N`, `JSON`
carries `"event_log_cap":N` in the `watch` object, and
`PROMETHEUS` carries `pulse_watch_event_log_cap N`. A bigger ring
costs memory — 88 bytes per event — and nothing else: the
ring is still bounded, a slow consumer still gets `ERR gap` when
it falls more than the ring behind, and replay still starts from
the oldest held event with a `GAP` line when the requested seq is
gone.

## Event-log persistence across restarts

The ring is bounded, but it used to be amnesiac too: a restarted
node started at sequence 1, so a watcher that reconnected after a
restart could not tell a replayed event from a new one — sequence 1
meant two different events before and after the restart — and a
`WATCH FROM <seq>` issued after a restart could only replay what
happened since the restart. Since 0.38.0 the node appends each
event to `<data-dir>/events` as it records it (one line per event:
`<seq> <TYPE> <id> <incarnation> <unix_ts>`), and on startup loads
the file back into the ring, so sequence numbers continue across a
restart and a replay after a restart reaches the events recorded
before it. The node logs `EVENTS <n> restored` when it loads any.

The file is compacted, not grown forever: on startup and on clean
shutdown it is rewritten with only the last `--event-log-cap`
events the ring holds, so it never holds more than one run's worth
of events past the ring. Between those compactions it grows by one
line per event — membership events are rare (one per transition),
so a long run adds little, and the next restart or clean shutdown
trims it back. A missing file loads zero events (the first-start
case), a malformed line is skipped, and a file that cannot be read
at all is a warning, never a failed start — the node just starts at
sequence 1. The append itself is best-effort for the same reason: a
data dir that cannot be written means the log does not survive the
restart, and a failed append never stops the node.

Two honest caveats. The persisted log is this node's view only: it
records what this node saw, so a watcher replaying after a restart
sees the events this node recorded, not the cluster's — a peer
another node suspected while this node was down never appears in
it. And restored members are still not events: a peer restored
from `<data-dir>/members` appears in the snapshot after a restart,
never as a replayed `JOINED`, so a replay that starts before a
restart still begins with the events, not with the restoration.

## WATCH subscriber count in METRICS and JSON

A `WATCH` stream is invisible to the rest of the control plane:
nothing in `MEMBERS` or `STATUS` says a dashboard, an alerter, or
a ward gateway is actually attached to the event stream — and a
watcher that went away silently leaves no trace either, until the
next event has nobody to deliver it to. Since 0.33.0 the node
counts its subscribers, in both forms:

```sh
./pulse-ctl 127.0.0.1:18401 METRICS
# METRICS ... watch_subscribers=2 watch_sessions=5

./pulse-ctl 127.0.0.1:18401 JSON
# {...,"watch":{"subscribers":2,"sessions":5}}
```

`watch_subscribers` is how many `WATCH` streams are connected
right now; `watch_sessions` is how many `WATCH` sessions the node
has served since startup. The two numbers answer different
questions: subscribers says whether anything is watching at this
moment (0 on a node nothing ever subscribed to), sessions says
whether anything ever watched at all — a node with 0 subscribers
and 5 sessions had watchers that all went away, a node with 0 and
0 never had one.

A session is counted when its stream thread starts, so a `WATCH`
refused up front — a bad filter, or a node built without watch
support — never becomes a session and counts in neither number.
The current count drops when the stream's thread ends, so a client
that went away is gone from it within about a second of the
disconnect being noticed (the stream's wait times out every second
and a closed socket is noticed on the next check). The session
count only ever grows: closing a stream never takes a session back.

Two honest caveats. The count is this node's view of its own
streams, not the cluster's: each node counts only the watchers
attached to it, so a dashboard connected to one node is invisible
on the others. And the count cannot tell a live watcher from a
half-open one that has not been noticed yet — the `HEARTBEAT` is
what proves a stream is alive, the count only proves it was
counted.

## WATCH drop and delivery counts in METRICS and JSON

Counting subscribers says how many consumers are attached, but
not whether they are keeping up: a watcher that falls behind the
event ring (128 events by default, `--event-log-cap` since 0.36.0)
is dropped with `ERR gap` and the events it missed are gone, and
nothing in `METRICS` said so — the subscriber count just fell by
one. Since 0.34.0 each stream counts what it does with every event
it consumes, in both forms:

```sh
./pulse-ctl 127.0.0.1:18401 METRICS
# METRICS ... watch_subscribers=2 watch_sessions=5 watch_delivered=41 watch_filtered=7 watch_dropped=3

./pulse-ctl 127.0.0.1:18401 JSON
# {...,"watch":{"subscribers":2,"sessions":5,"delivered":41,"filtered":7,"dropped":3,"streams":[{"session":4,"delivered":30,"filtered":0,"dropped":0},{"session":5,"delivered":11,"filtered":7,"dropped":3}]}}
```

Three counts per stream:

- `delivered` is how many `EVENT` lines the stream actually sent
  — live events and replayed ones alike. A send that fails ends
  the stream and is counted neither way: the client is gone, so
  there is no consumer left to report it to.
- `filtered` is how many events the stream consumed but skipped
  because its type/id filter excluded them. A stream that filters
  heavily is not a stream in trouble: those events were excluded
  on purpose, and they still consumed their sequence numbers by
  design.
- `dropped` is how many events the stream lost because they had
  already scrolled out of the bounded event ring before the
  stream could consume them: a replay `GAP` at stream start (the
  events between the requested seq and the oldest buffered one),
  a replay `GAP` on the first wait (a burst evicted the resume
  point between the snapshot and the wait), or a mid-stream gap
  (the events between the last consumed seq and the oldest
  buffered one, which ends the stream with `ERR gap`). A growing
  `dropped` count is the signal a consumer is not keeping up
  with the cluster — a dashboard that reconnects and replays
  from a stale seq every time, or a slow consumer on a busy
  cluster.

`METRICS` reports the three as lifetime totals — including what
live streams have done so far, not only what finished streams
did — so the totals only ever grow. `JSON` carries the same three
totals in the `watch` object, plus a `streams` array with the
same three counts per currently-connected stream, keyed by its
`session` number (the session count the stream was assigned when
it started), so an operator can tell which consumer is falling
behind instead of only that some consumer is. A stream that goes
away leaves the array; its counts stay in the totals.

Two honest caveats. At most 64 concurrent streams are listed
per-stream in `JSON`: a further stream still runs and still feeds
the totals, it just has no slot to list until one frees up — 64
is the same bound as the membership table, and a node with 64
concurrent watchers has stranger problems than the listing. And
the totals count what streams did, not what clients received: an
`EVENT` line the node sent successfully but the client's network
dropped afterwards still counts as delivered, because from the
node's side the send completed — the counts measure the stream,
not the wire.

## WATCH backpressure: pausing a stream

A slow consumer has two bad options with a plain `WATCH`: keep
reading and fall behind until the ring drops it with `ERR gap`,
or close the connection and lose the stream entirely. Since
0.37.0 it has a third — pause the stream, catch up, and resume:

```sh
./pulse-ctl 127.0.0.1:18401 WATCH
# (in another terminal, once the stream is open)
./pulse-ctl 127.0.0.1:18401 WATCH PAUSE 1
# OK paused 1
./pulse-ctl 127.0.0.1:18401 WATCH RESUME 1
# OK resumed 1
```

`WATCH PAUSE <session>` and `WATCH RESUME <session>` are
one-shot commands on a separate connection, addressed by the
session number — the number `JSON` reports per stream in the
`watch` object's `streams` array, assigned when the stream
opened. Pausing stops the stream consuming events: its thread
waits instead of reading the ring, so no `EVENT` line reaches
the client while it is paused, and the events it misses stay in
the bounded ring. On resume the stream picks up from the last
sequence it consumed and replays what the ring still holds, in
order, exactly as a live stream does — a paused stream is a
stream that stopped reading, not a stream that stopped existing,
so nothing about the replay rules changes.

When the ring has scrolled past the resume point — more events
happened while paused than the ring holds — the same gap signal
covers it: the stream ends with `ERR gap: events lost,
reconnect` and the lost events count as `dropped`, exactly as
with a mid-stream gap on a stream that was never paused. Pausing
is backpressure, not a bigger buffer: the ring is still bounded
(`--event-log-cap`, 128 by default), the node still records into
it at the same rate, and a paused stream that waits too long
still loses events — it just gets to choose when to stop and
start, instead of being dropped the moment it falls behind.

The paused state is visible without reading the log: `METRICS`
reports `watch_paused=N` (how many streams are paused right
now), `JSON` carries `"paused":N` in the `watch` object and
`"paused":true/false` per stream, and `PROMETHEUS` carries
`pulse_watch_paused N` as a gauge. A paused stream is still
connected and still counts as a subscriber — pausing does not
hide a consumer, it says the consumer is there and not reading.
Both commands are idempotent: pausing an already-paused stream
(or resuming a running one) answers `OK` again, since the state
is observable in `METRICS` / `JSON` and the reply does not need
to say whether anything changed. A session that is not a live
stream answers `ERR unknown watch session <n>`; a session that
is not a number answers `ERR invalid watch session <arg>`; a
missing session answers `ERR usage: WATCH PAUSE <session>`.

Two honest caveats. A paused stream still sends its `HEARTBEAT`
when 5 s pass with nothing sent — the stream is alive, it is
just not consuming — so a paused consumer still sees one line
every 5 s on a quiet cluster, and nothing else. And an event
already being sent when the pause lands still arrives: the pause
stops the flow from the moment it takes effect, it does not
recall a line already on the wire — so a client that pauses and
then asserts "nothing arrived after the pause" should allow for
the one event that was in flight.

## Membership persistence

A node saves the peers it knows to `<data-dir>/members` every ~5 s
and once more on clean shutdown (SIGINT/SIGTERM), one line per
member — `id incarnation state age-seconds` — written atomically
(tmp file + rename), and reloads the file on startup, logging
`RESTORE <n> members`. A restarted node therefore re-discovers its
old peers by probing them directly, even when it starts with no
`--seed` at all.

Nothing in the file is trusted. Every restored entry is re-probed
through the normal failure-detection path, and a restored entry that
stays silent goes SUSPECT then DEAD again like any other peer:

- ALIVE entries come back ALIVE, with `last_seen` aged by the
  recorded age, so the prober picks them up immediately.
- SUSPECT and DEAD entries come back SUSPECT with a fresh clock, so
  they get a full suspect window of probing before the sweep can
  declare them DEAD. A peer that was DEAD when the node shut down
  may have restarted since, and a peer that was SUSPECT may be fine
  now — neither verdict survives the restart.
- Entries the node never actually heard from (a seed that never
  answered) are not saved: there is nothing to remember about them,
  and the seed-rejoin retry already covers seeds. Self is never
  saved either — self is re-added at startup with a higher
  incarnation. A save with no peers writes an empty file, so a stale
  file can never resurrect a shrunken cluster.
- Malformed lines are skipped, not fatal: a hand-edited or truncated
  file never stops a node from starting. A missing file loads zero
  members, which is the first-start case.

```sh
# Terminal 1: 3-node cluster, then stop node 3 cleanly (Ctrl-C)
./pulse-node --port 17103 --seed 127.0.0.1:17101 --data-dir /tmp/p3
# Terminal 1 again: restart node 3 with NO seed, same data dir
./pulse-node --port 17103 --data-dir /tmp/p3
# Node 3 logs RESTORE 2 members and the cluster is back to alive=3.
```

## Graceful leave

A node that stops cleanly (SIGINT/SIGTERM) tells its peers it is
leaving before it exits: it sends each ALIVE/SUSPECT peer a GOSSIP
digest of its table with its own entry marked DEAD at its current
incarnation, and logs `LEAVE <id>`. DEAD beats ALIVE at equal
incarnation, so the peers merge it immediately and log `DEAD` without
a `SUSPECT` first — a planned stop (a ward device taken down for
maintenance) clears out of the cluster at once instead of taking the
~500 ms probe-timeout path.

A crash sends nothing: SIGKILL, power loss, or a segfault leave no
chance to announce, so the probe path still covers unclean deaths.
And a restart is unaffected: the node rejoins at a higher incarnation
(the incarnation file bumps on every start), which refutes the DEAD
entry, so a gracefully-left node comes straight back to ALIVE.

## Anti-entropy

A digest carries at most 16 entries, so a node that was partitioned
away — or that joined while failure news was spreading — can miss
updates for a long time: the random window only rotates the missing
entries through by chance. Every `--sync-interval-ms` (default 10 s)
each node therefore sends its whole membership table to one random
ALIVE/SUSPECT peer as a `SYNC` message, and the peer answers with its
own full table, so one exchange converges both sides. Entries merge
by the same rule as gossip, and tombstones ride along as DEAD, so a
SYNC also spreads removals.

One datagram holds at most 1400 bytes, so a full 64-member table of
long IPv6 ids does not fit in a single SYNC. The entries then start
at a rotating offset that advances by the number sent, so successive
SYNCs walk the whole table and a stale peer still converges within a
couple of exchanges instead of re-receiving the same head of the
table forever. A table that fits — every IPv4 table, and small IPv6
ones — converges in exactly one exchange.

A `JOIN` is answered with a `SYNC` for the same reason. Before that,
the seed answered with the capped 16-entry digest, so a joiner
landing in a cluster larger than the digest had to wait for gossip
rounds to rotate the missing members through — the seed already held
the full picture and the joiner had no reason to wait for it. The
joiner merges the `SYNC` and answers with its own full table, so one
exchange converges both sides, exactly as in the periodic case.

A `SYNC` is answered only when the exchange taught the receiver
something new — a new or revived sender, a refuted verdict about
itself, or a merged entry that added or changed a member. A `SYNC`
that changes nothing gets no reply. That condition matters: 0.13.0
answered every `SYNC` unconditionally, so the first periodic exchange
started an endless reply loop, each `SYNC` answered by a `SYNC` at
network speed, and a converged pair burned a CPU core exchanging
identical tables. Answering only on new information keeps the
one-round-trip convergence — the reply always carries strictly newer
information — and lets the exchange go quiet once both sides agree.

## Gossip fanout

Every other probe cycle (every ~200 ms at the default 100 ms probe
interval) a node sends its membership digest to a few random peers
instead of just one. The fanout is `1 + floor(log2(peers))`, capped
at 4: 1 peer for a 2-node cluster, 2 for 2–3 peers, 3 for 4–7, 4
beyond that. One peer per round is enough while the cluster is tiny,
but gossip only keeps its O(log n) spread as the cluster grows if
each round reaches O(log n) peers; the cap bounds the extra traffic
on a large cluster. Only ALIVE and SUSPECT peers are gossip targets,
and each peer's digest is built from the same table snapshot, so the
change costs no extra snapshots — but the digest itself is per-peer,
a delta against what that peer is believed to already know (see
"Gossip delta encoding" below), so two targets in the same round can
receive different digests.

## Digest prioritization

A digest carries at most the configured digest size (16 entries by
default, `--gossip-digest-size` — see "Configurable gossip digest
size" below), but the table holds up to 64
members, so a purely random window can miss a fresh SUSPECT for
several rounds and delay exactly the news that matters most. The
first up-to-4 slots of every digest are therefore reserved for the
members that most recently turned SUSPECT or DEAD — most recently
changed first, DEAD before SUSPECT on a tie — and the remaining
slots are the usual random window, skipping entries already sent as
urgent, so the rest of the table still rotates through instead of
starving. Tombstones still ride along after the table entries.

## Gossip delta encoding

Before this, every gossip round sent the same 16-entry digest to each
target peer whether anything had changed or not — a converged cluster
burned a digest per peer every 200 ms just to say nothing new. Every
membership-table change now bumps a table version, and each entry
records the version at which it last changed (added, or a new
incarnation or state; a routine heartbeat that only moves
`last_seen_ms` is not a change). Each node remembers, per peer, the
highest version it believes that peer has already received, and a
digest to that peer carries only the entries that changed after that
version — oldest change first — and nothing at all when nothing
changed. A converged cluster is therefore quiet on the gossip channel
until something actually happens.

Two details keep the bookkeeping honest. First, the digest reports
the highest changed version it actually carried, and the peer's
recorded version advances only that far: when more entries changed
than the digest holds, the rest go out on the next digest instead of
being recorded as known. Members and tombstones share one
version-ordered list, so a tombstone can never be squeezed out below
the reported version. Second, a peer with no recorded version — never
sent to, or evicted from the per-peer table — or one more than a
digest's worth of changes behind gets a full `SYNC` instead of a
delta: a delta cannot carry that much, and the `SYNC` converges it in
one exchange.

Delta encoding changes nothing on the wire: a `GOSSIP` datagram has
the same verb and the same `id:inc:state` entries, only fewer of them,
so a node running an older version merges a delta exactly as it
merged a full digest. The one thing it does not guarantee is delivery:
UDP can drop a delta, leaving a peer behind the version recorded for
it. That is bounded by the periodic anti-entropy `SYNC`, which still
exchanges full tables every ~10 s and heals any missed change within
one interval — and failure detection itself never depends on gossip,
since each node probes its peers directly and suspects them on its
own timeouts.

## Configurable gossip digest size (`--gossip-digest-size`)

Since 0.41.0 the digest is not a fixed 16 entries: a node started
with `--gossip-digest-size N` covers N members per gossip round's
digest (1 to 64, default 16). A large cluster — say 60 ward devices —
can trade convergence speed for bandwidth by shrinking it: a
4-entry digest is a quarter of the gossip bytes per round, and the
table still rotates through in a few rounds, with the urgent
SUSPECT/DEAD entries still leading (clamped to the digest, so a
digest of 1 still leads with the single most urgent entry). A small
cluster can grow it to the whole table, so every round carries
everything. The size is fixed at startup and is reported on the
control plane so an operator can see it without reading the
command line: `METRICS` reports `gossip_digest_size=N`, `JSON`
carries `"gossip_digest_size":N` top-level, and `PROMETHEUS`
carries `pulse_gossip_digest_size N`. The delta rule follows the
size: a peer more than one digest's worth of changes behind gets a
full `SYNC` instead of a delta, so the threshold is N changes, not
16. A bad size is a clean startup error: `--gossip-digest-size 0`
and `--gossip-digest-size 65` both exit 2 with an error message.
One honest caveat: the digest size is per node, not per cluster —
a node with a 4-entry digest still understands a 16-entry digest
from a peer, since the wire format is unchanged, so a mixed-size
cluster converges, just at the pace of its smallest digest.

## Adaptive probe interval

A stable cluster has nothing to prove in a hurry: it probes at the
configured `--probe-interval-ms` (default 100 ms) and keeps CPU and
gossip traffic low. But while any peer is SUSPECT the cluster is
mid-incident — a node may be dying, or may be about to refute — and
every part of the probe cycle matters more: the direct probe, the
indirect PING_REQ, the suspect-to-dead sweep, and the gossip round
all run once per cycle. So while suspects exist the cycle shortens
to the fast interval, and it goes back to base the moment the table
is suspect-free again.

The fast interval defaults to half the base (`--probe-interval-fast-ms`
sets it explicitly) and is clamped to 25 ms at the low end — below
that the loop would spin fast enough to matter on a ward device's
CPU for no detection gain — and to the base interval at the high
end, so a misconfigured fast value can never make detection slower
than the base. A base already below 25 ms has nothing to speed up
and stays at base. The switch is logged only when the interval
changes (`ADAPTIVE fast probe=50ms suspects=1`, then `ADAPTIVE
probe=100ms stable`), so a stable cluster does not spam the log, and
the fast cycles are counted as `adaptive_fast_cycles` in the
`METRICS` text line and in `JSON` — the only trace that the prober
ever sped up, since the current interval itself is not stored.

The cycle-based cadences inside the probe cycle — the anti-entropy
SYNC, the gossip round every other cycle — are counted in cycles,
so they speed up in wall time along with the
loop while the cluster is unstable. That is the point: a node that
missed many digests heals faster, and a suspect is swept to DEAD
sooner, exactly when the cluster needs it. The timeouts themselves
do not change — `--ack-timeout-ms` and `--suspect-timeout-ms` are
still wall-clock durations — so the fast interval only makes the
checks that enforce them run more often, it never redefines them.

## Probe-interval jitter

Nodes in a cluster usually start together — same deploy, same
restart — and with the same base interval they probe in lockstep:
every node picks its random peer, sends its PING, and gossips on
roughly the same schedule, so the cluster's traffic comes in bursts
and a slow peer gets probed by everyone at once. Jitter breaks the
lockstep: each probe cycle sleeps the adaptive interval times
(1 + u), with u drawn uniform in [-j, +j] and j the configured
fraction (`--probe-jitter-frac`, default 0.1, so a 100 ms base sleeps
90–110 ms per cycle). Two nodes with the same base drift apart
within a few cycles, while each node's mean interval stays the base
— jitter spreads the load, it does not speed the cluster up or slow
it down.

The jitter applies after the adaptive choice, so the base and fast
intervals are both jittered, and the `ADAPTIVE` log lines and the
`adaptive_fast_cycles` count still see the unjittered interval: a
jittered cycle never looks like a switch, and a stable cluster still
logs nothing. The cycle-based cadences inside the probe cycle — the
anti-entropy SYNC, the gossip round every other cycle — are counted
in cycles, so they inherit the jitter too:
the whole cycle drifts, not just the sleep. The timeouts are
untouched — `--ack-timeout-ms` and `--suspect-timeout-ms` are still
wall-clock durations — and suspicion still comes from the fixed ack
timeout or from phi alone, so a jittered probe cycle can never make
a healthy peer look dead or a dead one look alive.

The fraction clamps to [0, 0.9]: 0 disables jitter entirely, and
beyond 0.9 a "probe interval" stops meaning anything. The jittered
sleep never drops below the 25 ms floor when the base is at or above
it — the same floor as the adaptive interval, so jitter cannot make
the loop spin — and a base already below the floor floors at 1 ms
instead. The randomness is an explicit xorshift32 state seeded once
per node from time, pid, and port (not `rand()`), so nodes that start
together still draw different sequences; the same code with a fixed
seed replays the same sequence, which is what the unit tests use.

## Suspicion-refutation window

Not every suspicion is a death. A peer can go quiet for a probe
cycle or two — a scheduling stall, a burst of dropped packets, a
ward device busy with a firmware write — and come straight back.
The detector already handles that correctly (direct contact is
proof of life), but until now it left no trace: `suspected` counted
the scare and nothing counted the all-clear.

So a SUSPECT that comes back ALIVE is now counted as a refuted
suspicion (`suspect_refuted` in `METRICS` and `JSON`), and the time
it spent SUSPECT is recorded as a refute latency — mean, max, and
sample count — alongside the detection latency and the
suspect-to-dead duration. The three latencies together describe the
whole life of a suspicion: how fast it was raised, how long a false
one stayed visible, and how long a true one took to confirm.

The same local-vs-gossip split applies as for the other latencies:
a peer revived by direct contact (it answered a probe, or sent a
message) adds a measured sample, because the local table knows
exactly when the suspicion started; a refutation learned from
gossip — the peer refuted the verdict at a higher incarnation and
another node spread the news — bumps the count without a sample.
And a DEAD peer coming back is a restart, not a refutation: it was
declared dead and rejoined, so it is not counted here. On a healthy
cluster the refute latency stays short (a few hundred milliseconds
at most); a refute latency that keeps approaching the suspect
timeout means the cluster is flapping on the edge of declaring live
peers dead, and the timeouts or the network deserve a look.

## Suspicion age per member

The refute latency tells an operator how long a false suspicion
lasted after the fact. It says nothing while the suspicion is still
open: a peer sitting SUSPECT is either about to come back or about
to be declared DEAD, and the only number on the control plane was
the suspect count — no sense of how far along the suspicion is.

So each member now carries its suspicion age: `suspect_ms` in
`JSON` — the milliseconds since the member went SUSPECT, `null`
unless it is SUSPECT — one `SUSPECT <id> <ms>` line per SUSPECT
member after the `PHI` lines in `METRICS`, and one
`pulse_member_suspect_ms{member="<id>"}` gauge sample per SUSPECT
member in `PROMETHEUS`. The age is measured from when the member's
current state started on this node, so for a suspicion learned from
gossip it is the time since this node learned of it, not since the
detecting node raised it — the honest local value, the same
convention as the refute latency. Judge it against the suspect
timeout (`--suspect-timeout-ms`, 300 ms by default): a peer at 280
ms of a 300 ms timeout is one probe cycle from DEAD, while a peer
at 20 ms has barely started. The age grows while the member sits
SUSPECT and disappears the moment it revives or goes DEAD — an
ALIVE member has no suspicion to age, and a DEAD member's
suspicion is over, so both report `null` and have no line or
gauge sample. In phi-accrual mode the same age applies: the
suspicion was raised on phi, but the clock from SUSPECT to DEAD
is still the suspect timeout.

## Time since last contact per member

The suspicion age tells an operator how far along an open
suspicion is. It says nothing about a peer that is still ALIVE
but going quiet: a peer whose gossip stopped arriving a second
ago looks exactly like one heard from a millisecond ago, until
the probe timeout finally suspects it — and a DEAD member's
silence has no number at all, only the state.

So each member now carries its last-contact age: `last_seen_ms`
in `JSON` — the milliseconds since this node last heard from the
member, `null` for self and for a peer never heard from — one
`LASTSEEN <id> <ms>` line per member heard from after the
`SUSPECT` lines in `METRICS`, and one
`pulse_member_last_seen_ms{member="<id>"}` gauge sample per
member heard from in `PROMETHEUS`. The age is now minus the
member's `last_seen_ms`, the last direct contact — a probe ACK
or a gossip message carrying the member — so it is the honest
local value, the same convention as the suspicion age. Self is
excluded everywhere: self never probes itself, so its
`last_seen` is just startup time and the age would be uptime,
not a contact age. A peer never heard from reports `null` and
has no line or gauge sample — an absent value, not a zero,
because a zero would claim contact just now. A live peer's age
stays under a few probe intervals; a dying peer's climbs toward
the suspect timeout, so the gauge is the early warning the
suspicion age cannot give. A DEAD member keeps its line and
gauge sample until GC, the age growing — how long it has been
silent — where the suspicion age disappears the moment the
member goes DEAD.

## Per-member probe RTT in METRICS

The per-peer RTT lived in two of the three surfaces: `JSON`
reports `rtt_ms` / `rtt_mean_ms` per member and `PROMETHEUS`
carries `pulse_member_rtt_ms` / `pulse_member_rtt_mean_ms`
gauges, but the text protocol only had the global
`probe_rtt_ms` histogram — an operator polling `METRICS` could
see the cluster-wide RTT tail but not which peer was slow.

So `METRICS` now carries one `RTT <id> <last_ms> <mean_ms> <max_ms>`
line per member that has answered a probe, after the `LASTSEEN`
lines — the same last, mean and worst values `JSON` and `PROMETHEUS`
report, under the same rule: `rtt_ms` is -1 until a probe has
been answered, so self (which never probes itself) and a peer
never heard from have no line, and a 0 is a real
sub-millisecond probe, not a missing one. The worst RTT (0.65.0)
is the largest single direct-probe round trip over all answered
probes for that peer, the same value `JSON` reports as
`rtt_max_ms` and `PROMETHEUS` as `pulse_member_rtt_max_ms` — it
only ever grows, so a peer that usually answers in under a
millisecond but once took 60 ms shows it on this line while it
is still ALIVE. It is appended as a fourth field, after the last
and mean: a consumer that reads the first three fields by
position still gets the same last and mean, but a strict parser
expecting exactly three numbers sees a format change, and this
is the first time the `RTT` line has changed shape since 0.63.0.
A DEAD member keeps its last RTT until GC, so the line is the
last measured RTT, not a live one, for a dead member — the number
that says how slow the peer was just before it died, where the
global histogram only says the cluster once saw a slow probe.

## Phi threshold crossing in the event log

The `SUSPECT` log line already prints the phi at a suspicion,
and `METRICS` / `JSON` / `PROMETHEUS` already report the live
phi — but all three are a snapshot at poll time. By the time an
operator reads them, the peer is already SUSPECT and its phi
has moved on, or the peer has gone DEAD and the value that
fired the suspicion is gone. The event log is the record of
what happened; it should say why it happened.

So a SUSPECTED event this node raises itself in phi-accrual
mode now carries the phi the probe cycle measured when it
crossed the threshold: the `WATCH` stream sends it as a
trailing ` phi=<value>` on the `EVENT` line — two decimals, the
same value the `SUSPECT ... phi=` log line printed — and the
persisted `<data-dir>/events` file carries the same trailing
field, so a `WATCH FROM <seq>` replay after a restart restores
the crossing value too. Only a real crossing carries a phi: a
peer suspected by the no-history guard (never answered, so its
phi is 0.00) or by the fixed timeout has no phi on its event,
and a suspicion learned from gossip carries none either, since
this node never measured it — a fabricated value would be
worse than an absent one. Every other event writes the plain
five fields, so existing consumers see byte-identical lines.

## Last-contact age in the event log

The phi on a SUSPECTED event says why a suspicion fired, but
only in phi-accrual mode, and only for suspicions this node
raised itself. Every other event — a JOINED, a REVIVED, a
fixed-timeout SUSPECTED, a DEAD — says when it happened and
nothing about how quiet the peer was when it did: a watcher
replaying the log cannot tell a peer that answered a
millisecond before it was suspected from one that had been
silent for the whole escalation, and by the time the watcher
polls `JSON` for `last_seen_ms`, the value has moved on.

So every event whose member was heard from now carries its
last-contact age at record time: the `WATCH` stream sends it
as a trailing ` last_seen_ms=<ms>` on the `EVENT` line — the
ms since this node last heard from the member when the event
was recorded, the same value `JSON` reports as `last_seen_ms`
— and the persisted `<data-dir>/events` file carries the same
trailing field, so a `WATCH FROM <seq>` replay after a restart
restores the age as it was, not as it is now. The age is frozen
at the event: a JOINED carries ~0 ms, the peer was just heard
from; a SUSPECTED carries at least one ack timeout of silence
in fixed-timeout mode, the whole escalation; a DEAD carries a
larger age than the SUSPECTED event before it, the silence
having grown across the suspect timeout. It rides after the
phi field when both are present — a phi-mode SUSPECTED carries
both, the crossing and the silence behind it. Self, a member
never heard from, and a member already gone from the table
when the event was recorded (a GC'd DROPPED, a LEAVE) carry
no field: an absent value, never a fabricated zero, under the
same rule as `JSON`'s `last_seen_ms`. An event with neither
trailing field writes the plain five fields, so existing
consumers see byte-identical lines.

## Probe counts in the event log

The last-contact age on an event says how quiet the peer was
when the transition happened, but not how hard this node was
trying to reach it: a watcher replaying the log cannot tell a
peer that was probed twice and answered neither from one that
was probed a hundred times and answered ninety-nine — and by
the time the watcher polls `METRICS` for the `PROBES` line, the
counts have moved on.

So every event whose member was probed now carries its probe
counts at record time: the `WATCH` stream sends them as
trailing ` probes_sent=<n> probes_answered=<n>` on the `EVENT`
line — how many direct probes this node had sent the member and
how many were answered when the event was recorded, the same
values `JSON` reports as `probes_sent` / `probes_answered` and
`METRICS` as a `PROBES` line — and the persisted
`<data-dir>/events` file carries the same trailing fields, so a
`WATCH FROM <seq>` replay after a restart restores the counts as
they were, not as they are now. The counts are frozen at the
event: a SUSPECTED carries sent at least answered, answered at
least 1 for a peer that used to answer; a DEAD carries a sent
count at least as large and an answered count frozen at the same
value, the peer having stopped answering; a SUSPECTED for a peer
that never answered carries sent at least 1 with answered 0 —
probed but never answered, distinguishable from never probed at
all. They ride after the last_seen_ms field when it is present —
a SUSPECTED carries both, the silence and the unanswered probes
behind it. Self, a member never probed, and a member already
gone from the table when the event was recorded (a GC'd DROPPED,
a LEAVE) carry no fields: an absent value, never a fabricated
zero, under the same rule as `JSON`'s probe counts. An event
with no trailing field writes the plain five fields, so existing
consumers see byte-identical lines.

## Member metadata

A node can carry a small set of `key=value` annotations about
itself — what it is for, where it runs, which build it is — so an
operator looking at the cluster can tell the members apart without
memorizing ports:

```sh
./pulse-node --port 17101 --meta role=worker --meta zone=us-east-1 \
    --meta version=1.2.3 --data-dir /tmp/p1
./pulse-ctl 127.0.0.1:18401 MEMBERS
# MEMBER 127.0.0.1:17101 1 ALIVE - role=worker,zone=us-east-1,version=1.2.3
```

The rules are deliberately tight, because annotations ride in every
gossip entry and in the JSON output: at most 8 per node, each
`key=value` at most 64 chars, and keys and values use only
`[A-Za-z0-9._-]` — no spaces, commas, semicolons, colons, or `=`
inside a key or value, so an annotation survives the wire format and
needs no escaping in JSON. A repeated key updates the value instead
of adding a second pair.

Annotations travel with the membership entry: on the wire an entry is
`id:inc:state;k=v,k=v` (entries without the suffix — older nodes —
parse exactly as before), so a peer learns a member's annotations
from the same gossip that tells it the member exists, and a changed
annotation bumps the table version, so the next digest carries it as
a delta. The control plane shows them in `MEMBERS` (a `-` when a
member carries none) and as a `meta` object per member in `JSON`
(`{}` when there are none), and `META key=value` sets one on a
running node — the change is gossiped and persisted at once.

Persistence has two halves. A node's own annotations are saved to
`<data-dir>/meta`, one `key=value` per line: a node started with
`--meta` flags saves those, and a node started without flags loads
the saved ones, so a restart keeps its annotations even when the
flags are forgotten. What a node knows about its *peers* is saved in
`<data-dir>/members` as an optional fifth field on each line, so a
restarted node restores its peers' annotations until gossip
refreshes them.

Two honest caveats. An entry that carries no annotations never wipes
stored ones: a peer that predates annotations, or one relaying an
entry it learned without them, simply has nothing to say about them.
So a node that drops *all* its annotations cannot propagate the
removal — it can only replace them. And annotations are not
authenticated: like the membership entries themselves, they are
whoever claims them. With a shared secret only trusted peers can
inject them; without one, any process on the network can already
claim to be a member.

## Probe RTT

Every direct probe already times its own round trip: the prober
records when a PING goes out and the ACK's arrival closes the loop.
That measurement is now kept per peer — the last RTT in ms, the
mean over every probe that peer has answered, and the worst single
probe (0.64.0) — and reported per
member in `JSON` as `rtt_ms` / `rtt_mean_ms` / `rtt_max_ms`:

```sh
./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101",...,"members":[{"id":"127.0.0.1:17101",...,"rtt_ms":null,"rtt_mean_ms":null,"rtt_max_ms":null},{"id":"127.0.0.1:17102",...,"rtt_ms":1,"rtt_mean_ms":0,"rtt_max_ms":1}],...}
```

The point is early warning, not precision. A peer whose RTT is
climbing toward the ack timeout is a peer about to be suspected, and
on a ward network that climb is the first sign of a congested link
or a device struggling under load — visible here while the peer is
still ALIVE, instead of only in the detection-latency histogram
after it has already been suspected.

Three honest caveats. The clock has millisecond resolution, so on
loopback the RTT is usually 0 or 1 ms and the mean rounds down to 0;
the numbers only become interesting on a real network, or with a
slow peer. An ACK that arrives via an indirect probe still matches
the pending direct probe, so its sample runs from the PING send to
the forwarded ACK's arrival — an upper bound on the direct RTT,
not a direct measurement. And the RTT is local measurement, not
cluster state: it is never gossiped, never persisted, and does not
bump the table version, so a restarted node starts at `null` again
until its next probe is answered, and two nodes can legitimately
disagree about a third peer's RTT.

Since 0.49.0 this works in phi-accrual mode too: the phi-mode
probe records a pending entry marked `rtt_only`, so its ACK is
matched and timed exactly like a fixed-timeout probe's — see
"Probe RTT histogram" below.

## Probe RTT histogram

The per-peer mean RTT hides the tail: a peer that usually answers
in under a millisecond but sometimes takes 60 ms has a mean near 0
and is exactly the peer that is about to be suspected. So every
answered direct probe also adds one sample to a global RTT
histogram — mean, max and sample count, plus five buckets:

- <1 ms — the sub-millisecond loopback case (`rtt_ms == 0`)
- 1–4 ms
- 5–9 ms
- 10–49 ms
- ≥50 ms

```sh
./pulse-ctl 127.0.0.1:18401 METRICS
# METRICS ... refute_latency_ms mean=- max=- samples=0 probe_rtt_ms mean=0 max=1 samples=11 hist=[10,1,0,0,0] slow_peers=0 ...

./pulse-ctl 127.0.0.1:18401 JSON
# {...,"metrics":{...,"probe_rtt_ms":{"mean":0,"max":1,"samples":11,"hist":[10,1,0,0,0]},...}}

./pulse-ctl 127.0.0.1:18401 PROMETHEUS
# # TYPE pulse_probe_rtt_ms histogram
# pulse_probe_rtt_ms_bucket{le="1"} 10
# pulse_probe_rtt_ms_bucket{le="5"} 11
# pulse_probe_rtt_ms_bucket{le="10"} 11
# pulse_probe_rtt_ms_bucket{le="50"} 11
# pulse_probe_rtt_ms_bucket{le="+Inf"} 11
# pulse_probe_rtt_ms_sum 1
# pulse_probe_rtt_ms_count 11
```

The three views carry the same numbers: `probe_rtt_ms mean=X
max=Y samples=N hist=[a,b,c,d,e]` at the end of the `METRICS`
line, a `probe_rtt_ms` object (`mean`/`max`/`samples`/`hist`)
in the `JSON` metrics object, and `pulse_probe_rtt_ms_mean` /
`max` / `samples` gauges plus a real `pulse_probe_rtt_ms`
histogram (cumulative buckets, `_sum`, `_count`) in
`PROMETHEUS`. A sample exactly at a bound lands in the higher
bucket, the same rule as the detection-latency histogram, so a
sample is never counted as faster than it was. Mean is `-` in
`METRICS` and `null` in `JSON` (0 in `PROMETHEUS`, where there is
no null) until a probe has been answered.

The same three caveats as the per-peer RTT apply: the clock has
millisecond resolution, so on loopback the histogram is nearly
all bucket 0; an ACK that arrives via an indirect probe adds a
sample that is an upper bound on the direct RTT, not a direct
measurement; and the histogram is local measurement — never
gossiped, never persisted, and it does not bump the table version
— so a restarted node starts at zero samples again. In
phi-accrual mode the histogram used to stay empty: the prober sent
the PING but created no pending entry (suspicion comes from phi,
not from a probe timeout, so there was nothing to escalate), so an
ACK matched no pending probe and no RTT sample was ever recorded
— the per-member `rtt_ms` was `null` there for the same reason.
Since 0.49.0 the phi-mode probe creates a pending entry marked
`rtt_only` — it exists only to match the ACK and measure the RTT,
never to escalate or suspect — so the histogram and the per-member
RTT are populated in phi mode exactly as in the fixed-timeout mode,
and suspicion still comes from phi alone. (In phi mode there are
no indirect probes, so the indirect-ACK caveat above never applies
there: every phi-mode sample is a direct measurement.)

## Worst probe RTT per member

The global histogram says the cluster has a slow tail, but not
which peer it belongs to — and the per-peer mean hides it: a peer
that usually answers in under a millisecond but sometimes takes
60 ms has a mean near 0. So each member also carries its worst
single probe round trip — the largest direct-probe RTT over all
answered probes — as `rtt_max_ms` in `JSON`, as a
`pulse_member_rtt_max_ms` gauge in `PROMETHEUS`, and (0.65.0) as
the fourth field of the `METRICS` `RTT` line:

```sh
./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101",...,"members":[{"id":"127.0.0.1:17102",...,"rtt_ms":1,"rtt_mean_ms":0,"rtt_max_ms":60}],...}

./pulse-ctl 127.0.0.1:18401 PROMETHEUS
# # TYPE pulse_member_rtt_max_ms gauge
# pulse_member_rtt_max_ms{member="127.0.0.1:17102"} 60

./pulse-ctl 127.0.0.1:18401 METRICS
# RTT 127.0.0.1:17102 1 0 60
```

The worst RTT only ever grows — a fast probe never erases the
worst one — and it is `null` in `JSON` (no gauge line in
`PROMETHEUS`) until the peer has answered a probe, always `null`
for self. A DEAD member keeps its worst RTT until GC, like its
last and mean, so the gauge is the last-known worst, not a live
one, for a dead member. It is local measurement like the rest of
the RTT: never gossiped, never persisted, and it does not bump
the table version.

## Per-member probe counts

The RTT trio says how fast a peer answers, but not how often it
answers at all — a peer that answers one probe in ten has a fine
mean RTT and a terrible success rate, and a seed that never came
up looks identical to a peer this node never probed: both have
`rtt_ms` null and no `RTT` line. So each member also counts its
direct probes — how many this node sent, and how many were
answered — as `probes_sent` / `probes_answered` in `JSON`, one
`PROBES <id> <sent> <answered>` line per probed member after the
`RTT` lines in `METRICS`, and
`pulse_member_probes_sent_total` /
`pulse_member_probes_answered_total` counters in `PROMETHEUS`:

```sh
./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101",...,"members":[{"id":"127.0.0.1:17102",...,"probes_sent":142,"probes_answered":139}],...}

./pulse-ctl 127.0.0.1:18401 METRICS
# PROBES 127.0.0.1:17102 142 139

./pulse-ctl 127.0.0.1:18401 PROMETHEUS
# # TYPE pulse_member_probes_sent_total counter
# pulse_member_probes_sent_total{member="127.0.0.1:17102"} 142
# # TYPE pulse_member_probes_answered_total counter
# pulse_member_probes_answered_total{member="127.0.0.1:17102"} 139
```

Sent counts only direct PINGs from the probe cycle — the same
probes the RTT samples come from. An indirect probe (a PING this
node sends because another node asked it to with `PING_REQ`) is
not counted: its ACK goes back to the requester and adds no RTT
sample here either, so counting it as sent would drag the rate
down for a probe this node never judged. Answered increments
with the RTT sample, so it always equals the number of RTT
samples for the peer. In `JSON` both fields are always numbers,
never `null`: 0 sent for self (it never probes itself) and for a
peer never probed, and a peer probed but never answered — a seed
that never came up — shows sent climbing with answered 0, which
is the interesting case, not a missing value. In `METRICS` and
`PROMETHEUS` a member with 0 sent has no line or sample: an
absent line is how those surfaces say "never probed", the same
as the RTT line says "never answered". A DEAD member keeps its
counts until GC, like its RTT, so the counters are the
last-known counts, not live ones, for a dead member — how many
probes went unanswered just before it died. The counts are
local measurement like the RTT: never gossiped, never persisted,
and they do not bump the table version.

## Slow-peer warning

The RTT numbers above are only useful if somebody is watching them.
With `--slow-peer-rtt-ms N` a node watches them itself: a peer whose
*mean* probe RTT rises above N ms counts as slow, and the node says
so three ways — a `SLOW <id> mean=Nms threshold=Mms` log line, a
`"slow":true` flag on that member in `JSON`, and a `slow_peers`
count in `METRICS` and in the `JSON` metrics object. The default is
0, which disables the warning entirely: without a threshold the node
reports no slow peers and logs no `SLOW` lines, exactly as before.

```sh
./pulse-node --port 17101 --control-port 18401 --slow-peer-rtt-ms 50 --data-dir /tmp/p1
./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101",...,"members":[{...,"rtt_ms":61,"rtt_mean_ms":58,"slow":true},...],"metrics":{...,"slow_peers":1,...}}
```

The comparison is on the mean, not the last RTT: one slow probe is
noise — a scheduling stall, a single dropped packet — and the mean
only climbs when the peer is consistently slow. The threshold is a
strict greater-than, so a peer sitting exactly at the threshold is
not slow. A peer that has answered no probe yet has no mean and is
never slow, and self is never slow (a node never probes itself).

The log line fires once per slow episode, not once per probe: the
first sample that pushes the mean above the threshold logs `SLOW`,
later samples while the peer stays slow log nothing, and when the
mean recovers to the threshold or below the warning clears — so a
peer that recovers and slows again warns again. That is the whole
point of the warning: a peer whose RTT is climbing toward the ack
timeout is a peer about to be suspected, and on a ward network that
climb is the first sign of a congested link or a device struggling
under load — visible here while the peer is still ALIVE, instead of
only in the detection-latency histogram after it has already been
suspected.

Two honest caveats. The threshold is a single number for the whole
cluster, so it has to fit the slowest healthy link: set it just
below the ack timeout and a peer that is slow enough to be suspected
soon is exactly the peer that warns. And slowness is local
measurement, like the RTT itself: it is never gossiped, so two nodes
can disagree about whether a third peer is slow, and a restarted
node forgets which peers were slow until its probes measure them
again.

## Quiet-peer early warning

The last-contact age above is only useful if somebody is watching
it. With `--quiet-peer-ms N` a node watches it itself: an ALIVE
peer whose last-contact age rises above N ms counts as quiet, and
the node says so four ways — a `QUIET <id> age=Nms threshold=Mms`
log line, a `QUIET` event on the `WATCH` stream (carrying the age
as a trailing ` last_seen_ms=<ms>`, frozen at the crossing, and
persisted in `<data-dir>/events` like every other event), a
`"quiet":true` flag on that member in `JSON`, and a `quiet_peers`
count in `METRICS` and in the `JSON` metrics object (plus a
`pulse_quiet_peers` gauge in `PROMETHEUS`). The default is 0,
which disables the warning entirely: without a threshold the node
reports no quiet peers, logs no `QUIET` lines, and records no
`QUIET` events, exactly as before.

```sh
./pulse-node --port 17101 --control-port 18401 --quiet-peer-ms 300 --data-dir /tmp/p1
./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101",...,"members":[{...,"last_seen_ms":412,"quiet":true},...],"metrics":{...,"quiet_peers":1,...}}
```

The comparison is on the last-contact age, not on the probe
timeout: a peer that answers its probes stays under a few probe
intervals, so any age above the threshold means the peer has
stopped answering entirely — a dropped link, a wedged process, a
device that lost power — and the warning lands while the peer is
still ALIVE, before the fixed-timeout suspicion or the phi
threshold fires. The threshold is a strict greater-than, so a peer
sitting exactly at the threshold is not quiet. A peer never heard
from has no age and is never quiet, and self is never quiet (a
node never probes itself, and self's last-contact age would just
be the node's uptime).

The log line and the event fire once per quiet episode, not once
per probe cycle: the first probe cycle that sees the age above the
threshold logs `QUIET` and records the event, later cycles while
the peer stays quiet log nothing, and the next direct contact
re-arms the warning — so a peer that answers again and goes quiet
again warns again. The flag in `JSON` clears the moment the peer
is suspected: a SUSPECT peer is not quiet, the suspicion already
says it is silent, and the warning exists to say something before
the suspicion, not to repeat it.

Two honest caveats. The threshold has to sit above a healthy
peer's normal silence (a few probe intervals, more on a large
cluster where the random probe target rotates) and below the
suspicion delay (two ack timeouts in fixed-timeout mode), or it
never fires while the peer is ALIVE — a threshold above the
suspicion delay can only ever fire for a peer that is already
SUSPECT, which is to say never. And quietness is local
measurement, like the last-contact age itself: it is never
gossiped, so two nodes can disagree about whether a third peer is
quiet, and a restarted node forgets which peers were quiet until
their silence crosses the threshold again.

## Low-probe-rate alert

The lifetime probe counts above say how many probes a peer has
ever answered, which dilutes a fresh problem across an hour of
good behaviour: a peer that answered everything for an hour and
then started dropping half its probes still shows a lifetime rate
near 1. With `--low-probe-rate-frac F` a node watches the recent
rate itself: each peer keeps the outcomes of its last 20 direct
probes, and an ALIVE peer whose answered / sent ratio over that
window falls below F counts as low-probe-rate, and the node says
so five ways — a `LOWRATE <id> rate=0.40 threshold=0.50` log line,
a `LOWRATE` event on the `WATCH` stream (carrying the rate as a
trailing ` probe_rate=<value>`, frozen at the crossing, and
persisted in `<data-dir>/events` like every other event), a
`"low_probe_rate":true` flag and a `"probe_rate"` number on that
member in `JSON`, a `low_probe_rate_peers` count in `METRICS` and
in the `JSON` metrics object (plus a `LOWRATE <id> <rate>` line in
`METRICS`), and a `pulse_low_probe_rate_peers` gauge plus a
`pulse_member_probe_rate` gauge in `PROMETHEUS`. The default is 0,
which disables the warning entirely: without a threshold the node
reports no low-rate peers, logs no `LOWRATE` lines, and records no
`LOWRATE` events, exactly as before.

```sh
./pulse-node --port 17101 --control-port 18401 --low-probe-rate-frac 0.5 --data-dir /tmp/p1
./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101",...,"members":[{...,"low_probe_rate":true,"probe_rate":0.40},...],"metrics":{...,"low_probe_rate_peers":1,...}}
```

The window is the last 20 direct probes this node sent the peer,
and only direct probes count: an indirect probe (a PING this node
sends because another node asked it to with PING_REQ) is not
counted — its ACK goes back to the requester and adds no RTT
sample here either — so a peer is never flagged for a probe this
node never judged. The rate is judged only once at least 5 probes
are in the window, so one missed probe cannot flag a peer: a peer
that answered 4 of its first 5 probes shows a rate of 0.80 and no
flag at a 0.5 threshold. The threshold is a strict less-than, so a
rate exactly at the threshold is not low. A peer never probed has
no rate and is never low, and self is never low (a node never
probes itself, so it has no window to judge).

The log line and the event fire once per low-rate episode, not
once per probe cycle: the first probe cycle that sees the rate
below the threshold logs `LOWRATE` and records the event, later
cycles while the peer stays low log nothing, and the warning
re-arms only when the windowed rate recovers to the threshold or
above — so a peer that recovers and drops again warns again. The
re-arm is on recovery, not on direct contact the way the quiet
warning's is: one answered probe barely moves a 20-probe window,
so clearing on contact would re-fire almost at once for a peer
answering one probe in ten. The flag in `JSON` clears the moment
the peer is suspected: a SUSPECT peer is not low-rate, the
suspicion already says it is silent, and the warning exists to say
something before the suspicion, not to repeat it. The rate itself
stays measurable for a SUSPECT or DEAD peer — a DEAD member keeps
its last window rate until GC, like its RTT and probe counts — so
a consumer can still see how low the rate was when the peer died.

Two honest caveats. The threshold has to sit below a healthy
peer's normal rate (near 1 on a good link, lower on a lossy one)
and above 0, or it never fires — and on a cluster where the
random probe target rotates, a peer's window fills slowly, so a
peer can sit below the threshold for a while before its window
holds enough probes to judge it. And the rate is local
measurement, like the probe counts themselves: it is never
gossiped, so two nodes can disagree about whether a third peer is
low-rate, and a restarted node forgets every window until its
probes fill them again.

## Seed-rejoin count

The seed-rejoin retry is silent unless you read the log: a node
waiting on a down seed logs one `REJOIN` line per retry only when
the seed had already been garbage-collected, and nothing at all for
a retry to a seed still SUSPECT/DEAD in the table. The count makes
the retry visible without the log: every re-JOIN the seed-rejoin
retry sends bumps `seed_rejoins` in `METRICS` and in the `JSON` metrics
object, and `pulse_seed_rejoin_total` in `PROMETHEUS` — one count
per re-JOIN, whether the seed was SUSPECT/DEAD in the table or
already GC'd, so the count can run ahead of the `REJOIN` lines in
the log. A node whose seeds were up at startup, or with no seeds
at all, reports 0.

```sh
./pulse-node --port 17102 --seed 127.0.0.1:17101 --control-port 18402 --data-dir /tmp/p2
./pulse-ctl 127.0.0.1:18402 METRICS
# METRICS suspected=1 dead=1 refutes=0 joins=0 seed_rejoins=2 seed_rejoins_by_seed=127.0.0.1:17101=2 ...
./pulse-ctl 127.0.0.1:18402 JSON
# ..."seed_rejoins":2,"seed_rejoins_by_seed":{"127.0.0.1:17101":2}...
./pulse-ctl 127.0.0.1:18402 PROMETHEUS
# pulse_seed_rejoin_total{seed="127.0.0.1:17101"} 2
```

Per-seed breakdown (0.54.0): the total says how often the retry
fires, but not to which seed — a node with three seeds and a
climbing total cannot tell which one is still down. `METRICS`
carries `seed_rejoins_by_seed=<id>=N,<id>=N` right after the total
(`-` when no seed has been re-joined yet), `JSON` carries the same
breakdown as `"seed_rejoins_by_seed":{"<id>":N}` in the metrics
object (`{}` when empty), and `PROMETHEUS` carries one labelled
sample per re-joined seed — `pulse_seed_rejoin_total{seed="<id>"}
N` — so a scraper can alert on a single seed that keeps being
rejoined; summing the labels gives the total. A seed that was
never re-joined has no entry anywhere (its count is implicitly 0),
entries appear in first-rejoin order, and the breakdown is local
measurement like the total: never gossiped, never persisted, reset
on restart.

Per-seed last rejoin (0.55.0): the count says how often each seed
was retried, but not when — a count that stopped climbing could
have stopped a minute ago or an hour ago. `METRICS` carries
`seed_last_rejoin_ms_by_seed=<id>=N,<id>=N` right after the
breakdown (`-` when no seed has been re-joined yet), `JSON`
carries the same timestamps as
`"seed_last_rejoin_ms":{"<id>":N}` in the metrics object (`{}`
when empty), and `PROMETHEUS` carries one labelled gauge sample
per re-joined seed — `pulse_seed_last_rejoin_ms{seed="<id>"} N`
— where N is the wall-clock time of that seed's most recent
re-JOIN, as unix milliseconds. Unix ms, not seconds: the retry
fires every 5–30 s, and second resolution would hide whether the
last attempt was this interval or the one before. A seed that was
never re-joined has no entry anywhere — there is no time to
report, so none is fabricated — entries appear in first-rejoin
order, like the counts, and a later re-JOIN to the same seed
moves only its timestamp; the timestamps are local measurement
like the counts: never gossiped, never persisted, reset on
restart.

```sh
./pulse-ctl 127.0.0.1:18402 METRICS
# ... seed_rejoins=2 seed_rejoins_by_seed=127.0.0.1:17101=2 seed_last_rejoin_ms_by_seed=127.0.0.1:17101=1700000100000 ...
./pulse-ctl 127.0.0.1:18402 JSON
# ..."seed_rejoins":2,"seed_rejoins_by_seed":{"127.0.0.1:17101":2},"seed_last_rejoin_ms":{"127.0.0.1:17101":1700000100000}...
./pulse-ctl 127.0.0.1:18402 PROMETHEUS
# pulse_seed_last_rejoin_ms{seed="127.0.0.1:17101"} 1700000100000
```

Two honest caveats. The count is local measurement, like the other
transition totals: it is never gossiped, so two nodes waiting on
the same down seed each count their own retries, and a restarted
node starts at 0 again — it is a lifetime counter for this process,
not a cluster-wide one. And it counts retries sent, not retries
answered: a seed that never comes up keeps the count climbing by
one per retry interval, which is exactly the signal — a climbing
count with no `JOIN` from the seed is a seed that is still down,
while a count that stops climbing once the cluster merges is a
seed that came up.

Per-seed rejoin backoff (0.56.0): the count says how often each seed
was retried and the timestamp says when, but neither says how long
the node will wait before trying again — a seed retried every 5 s
looks the same as one retried every 30 s until the next attempt
lands. `METRICS` carries
`seed_rejoin_backoff_ms_by_seed=<id>=N,<id>=N` right after the
last-rejoin timestamps (`-` when no seed has been re-joined yet),
`JSON` carries the same delays as
`"seed_rejoin_backoff_ms":{"<id>":N}` in the metrics object (`{}`
when empty), and `PROMETHEUS` carries one labelled gauge sample
per re-joined seed — `pulse_seed_rejoin_backoff_ms{seed="<id>"} N`
— where N is the delay in milliseconds the node will wait before
the next re-JOIN to that seed: the backoff for its consecutive
failures — 10 s after the first unanswered attempt, 20 s after
the second, 30 s from the third on — jittered ±10%, so the value
is the delay actually scheduled, not the unjittered policy value.
0 while the seed is ALIVE: the backoff resets the moment the seed
comes back, so a seed that merged reads 0 instead of the delay it
was last scheduled at, and a later outage starts again at the
base. A seed that was never re-joined has no entry anywhere,
entries appear in first-rejoin order like the counts, and the
delays are local measurement like the counts: never gossiped,
never persisted, reset on restart.

```sh
./pulse-ctl 127.0.0.1:18402 METRICS
# ... seed_rejoins=2 seed_rejoins_by_seed=127.0.0.1:17101=2 seed_last_rejoin_ms_by_seed=127.0.0.1:17101=1700000100000 seed_rejoin_backoff_ms_by_seed=127.0.0.1:17101=10000 ...
./pulse-ctl 127.0.0.1:18402 JSON
# ..."seed_rejoins":2,"seed_rejoins_by_seed":{"127.0.0.1:17101":2},"seed_last_rejoin_ms":{"127.0.0.1:17101":1700000100000},"seed_rejoin_backoff_ms":{"127.0.0.1:17101":10000}...
./pulse-ctl 127.0.0.1:18402 PROMETHEUS
# pulse_seed_rejoin_backoff_ms{seed="127.0.0.1:17101"} 10000
```

Two honest caveats. The delay is the scheduled wait, not a promise
about the wall clock: the retry fires on the prober's cycle, so a
loaded node can slip a cycle past the due time, and the jitter
moves each scheduled delay by up to 10% either way — the value
says which backoff step the seed is on, not the exact millisecond
the next JOIN goes out. And a seed that comes back after a long
outage can take up to the 30 s cap to be noticed: the backoff
trades merge latency for a quieter retry, so a ward that needs a
returning device noticed within seconds should keep the seed's
outage short or restart the waiting node, which resets every
backoff to the base.

## Tests

```sh
make test
```

- `tests/test_member.c`: unit tests for the membership table — merge
  precedence, incarnation rules, refutation, state transitions, peer
  selection, and probe RTT recording (last/mean, unknown ids and
  negative samples ignored, recording an RTT is not a gossip-visible
  change). Slow-peer warning: a peer is slow only when its mean RTT
  is strictly above the threshold (mean == threshold is not slow, a
  threshold of 0 disables the warning, a peer with no samples is
  never slow), `member_note_rtt` returns 2 on the first crossing
  above the threshold and 1 while already warned, the warning clears
  when the mean recovers, and a second crossing warns again.
  Quiet-peer warning (0.62.0): a peer is quiet only when it is ALIVE
  and its last-contact age is strictly above the threshold (age ==
  threshold is not quiet, a threshold of 0 disables the warning, a
  peer never heard from has no age and is never quiet, SUSPECT/DEAD
  peers are not quiet), the count excludes self, and direct contact
  clears `quiet_warned` so a later silence warns again.
  Low-probe-rate alert (0.68.0): the rate is answered / sent over
  the last 20 direct probes, judged only once at least 5 are in
  the window (rate == threshold is not low, a threshold of 0
  disables the warning, a peer never probed has no rate and is
  never low, SUSPECT/DEAD peers are not low but their rate stays
  measurable), a slot slides out of the window at 20 so the rate
  tracks the recent probes, a very late ACK for an evicted slot
  flips nothing, and duplicate or unknown ACKs are ignored.
- `tests/test_cluster.sh`: integration test with real processes — 3 nodes
  converge, a killed node is marked DEAD by both survivors, the survivors
  agree on membership, and a restarted node (same data dir) is cleared
  back to ALIVE.
- `tests/test_gc.sh`: integration test for garbage collection — with a
  2 s `--dead-retention`, a killed node is GC'd by both survivors (the
  cluster agrees on 2 nodes, dead=0), and a restarted node at a higher
  incarnation is accepted back as ALIVE.
- `tests/test_phi.c`: unit tests for phi-accrual — phi is 0 with no
  history, stays low for regular heartbeats, rises monotonically with
  silence and crosses 8, follows a changed cadence as old samples age
  out, and is fed by `member_mark_alive`. The phi history (0.47.0)
  samples the live phi with its timestamp, oldest first, bounded at
  16, and a same-millisecond re-sample is skipped; the history stats
  (0.51.0) report the latest and max over the window; and the history
  histogram (0.52.0) buckets the samples cumulatively at 0.5, 1, 2, 4
  and 8 — a sample exactly at a bound lands in that bucket — with the
  `+Inf` bucket holding every sample and the sum of the sampled
  values.
- `tests/test_phi.sh`: integration test for phi-accrual — a 3-node
  cluster in `--phi-accrual` mode converges, a killed node is SUSPECTED
  with a phi value and then marked DEAD by both survivors, and a
  restarted node comes back ALIVE.
- `tests/test_phi_watch.sh`: integration test for the phi threshold
  crossing in the event log (0.60.0) — a 2-node cluster in
  `--phi-accrual` mode converges with a `WATCH` open on the survivor,
  a killed node is SUSPECTED and the stream carries the event with a
  trailing ` phi=<value>` at least the 8.0 threshold, the DEAD event
  that follows carries no phi, and the persisted `<data-dir>/events`
  file carries the same phi.
- `tests/test_last_seen_watch.sh`: integration test for the
  last-contact age in the event log (0.61.0) — a 2-node cluster
  converges from a `--peer-file` with a `WATCH` open on the
  survivor, a third node joins and the stream carries its JOINED
  event with a small ` last_seen_ms`, a killed node is SUSPECTED
  with a ` last_seen_ms` of at least one ack timeout and then DEAD
  with a larger one, the persisted `<data-dir>/events` file
  carries the same age on the SUSPECTED line as the stream did,
  and a `WATCH FROM 0` replay shows the never-started peer's
  SUSPECTED event with no trailing field at all.
- `tests/test_quiet_peer.sh`: integration test for the quiet-peer
  early warning (0.62.0) — a 2-node cluster converges from a
  `--peer-file` with `--quiet-peer-ms 300` and a long ack timeout
  so the suspicion fires well after the quiet threshold, a killed
  node is logged and streamed as exactly one `QUIET` line and one
  `QUIET` event with a ` last_seen_ms` at least the threshold and
  under the suspicion delay, its `JSON` flag reads `"quiet":true`
  while it is still ALIVE with `quiet_peers=1` in `METRICS` and
  `pulse_quiet_peers 1` in `PROMETHEUS`, and all three clear once
  it is SUSPECTED — with the persisted `<data-dir>/events` file
  and a `WATCH FROM 0` replay carrying the same `QUIET` event.
- `tests/test_low_probe_rate.sh`: integration test for the
  low-probe-rate alert (0.68.0) — a 2-node cluster converges from
  a `--peer-file` with `--low-probe-rate-frac 0.5` and a long ack
  timeout so the suspicion fires ~4 s after the kill, well after
  the ~1.1 s it takes the 20-probe window to slide below 0.5, a
  killed node is logged and streamed as exactly one `LOWRATE` line
  and one `LOWRATE` event with a ` probe_rate` below the
  threshold, its `JSON` flag reads `"low_probe_rate":true` with a
  `"probe_rate"` below the threshold while it is still ALIVE with
  `low_probe_rate_peers=1` and a `LOWRATE` line in `METRICS` and
  `pulse_low_probe_rate_peers 1` with a `pulse_member_probe_rate`
  below the threshold in `PROMETHEUS`, and the flag and count
  clear once it is SUSPECTED — with the persisted
  `<data-dir>/events` file and a `WATCH FROM 0` replay carrying
  the same `LOWRATE` event at the same rate. Before the kill the
  peer reads `"low_probe_rate":false` with `"probe_rate":1.0` and
  self reads `"probe_rate":null` with `"low_probe_rate":false`.
- `tests/test_metrics.c`: unit tests for the metrics module — a fresh
  struct formats with zero counts and `-` latencies and an all-zero
  histogram, locally detected suspicions feed mean/max detection latency
  and bucket into the histogram (<100, 100–249, 250–499, 500–999,
  ≥1000 ms, with boundary samples landing in the higher bucket),
  gossip-learned transitions bump the counts without adding a latency
  sample or a histogram count, and refutes/joins are plain counters.
  Answered probes feed a second histogram the same way (0.48.0) —
  mean/max probe RTT and buckets <1, 1–4, 5–9, 10–49, ≥50 ms, with
  boundary samples landing in the higher bucket and a `-1` adding
  nothing. Seed rejoins (0.53.0–0.55.0) count per re-JOIN sent and
  break down per seed with a last-rejoin timestamp, and the backoff
  (0.56.0) records the consecutive failures, the scheduled delay
  and its due time on each entry — the formatters render the delay
  as text and JSON (`-` / `{}` when nothing was ever re-joined) —
  and a reset clears the failures, delay and due time while leaving
  the lifetime count and timestamp untouched, never creating an
  entry for a seed that was never re-joined.
- `tests/test_adaptive.c`: unit tests for the adaptive probe interval
  — the default fast interval is half the base floored at 25 ms, no
  suspects means the base interval whatever the fast setting, suspects
  mean the configured fast interval (or the default when it is 0),
  the fast interval is clamped to the 25 ms floor and never slower
  than base, a base below the floor has nothing to speed up, and a
  nonsense base falls back to the floor instead of spinning.
  Probe-interval jitter (0.50.0): a zero fraction sleeps the exact
  base, 1000 draws at ±10% stay inside [90, 110] ms and land on both
  sides of the base, 10000 draws average within 2 ms of the base, the
  same seed replays the same sequence and a different seed draws a
  different one, the fraction clamps at 0.9, a base at the floor never
  sleeps below 25 ms, a nonsense base falls back to the floor, and a
  NULL state returns the base unchanged. Seed-rejoin backoff
  (0.56.0): no failures means the 5 s base, one failure 10 s, two
  20 s, three and beyond the 30 s cap, and a huge count never
  overflows past the cap.
- `tests/test_jitter.sh`: integration test for probe-interval jitter —
  a 2-node cluster converges with jitter on (one node at
  `--probe-jitter-frac 0.2`, the other at the 0.1 default), both
  members list the peer ALIVE, the members stay ALIVE after 3 s, and
  the same pair still converges with jitter off
  (`--probe-jitter-frac 0`).
- `tests/test_control.sh`: integration test for the TCP control plane —
  `MEMBERS` lists the node itself, `STATUS` reports id and totals,
  `METRICS` reports zero counts and no samples on a fresh node and
  the detected kill with measured latencies and a histogram after a
  peer dies, `JSON` returns one line of valid JSON with the node itself
  as the only member and zeroed metrics (including an all-zero
  histogram) on a fresh node — the member carries `"source":"self"`,
  and `"peer_file":null` since the node runs without `--peer-file` —
  and after a kill it lists
  the dead member with state `DEAD` and numeric latency means and a
  histogram,
  unknown commands and bad `LEAVE` usage get clean `ERR` lines, a
  second node appears in `MEMBERS` after joining, `LEAVE` removes it
  from the listing immediately, the live node comes back via direct
  contact, and `LEAVE` of a dead node removes it for good.
  Probe RTT: after the join, `JSON` reports a numeric `rtt_ms` /
  `rtt_mean_ms` for the peer and `null` for self, and after the kill
  the dead member still carries its measured RTT. Slow-peer warning:
  node 1 runs with `--slow-peer-rtt-ms 1` and loopback RTTs are 0-1 ms,
  so the peer is not slow — `JSON` reports `"slow":false` for it and
  `slow_peers` is 0 in `METRICS` and `JSON`, and a fresh node reports
  `slow_peers=0` too (a positive crossing is covered by the unit
  tests; loopback cannot be made deterministically slow).
- `tests/test_secret.c`: unit tests for the join gate — SHA-256
  against FIPS 180-4 vectors (including one million 'a's), HMAC-SHA256
  against the RFC 4231 test case 2 vector, tag shape and
  determinism, verify against a wrong secret / tampered message /
  tampered tag, and `pulse_secret_check` accept/drop decisions
  (untagged, wrong-secret, non-hex token, bare tag, trailing
  whitespace). Also checks a tagged message still parses.
- `tests/test_secret.sh`: integration test for the join gate — same
  secret converges, different secrets stay separate (the seeding node
  suspects, deads, and GCs the unanswering seed), no secret converges
  as before, and a mixed secret/plain pair does not merge. Also checks
  `START` reports `secret=on` only for secret nodes and that the
  secret value never appears in a log.
- `tests/test_gossip.c`: unit tests for digest prioritization over
  real UDP loopback sockets — a SUSPECT leads a small digest, the
  four most recently changed SUSPECT/DEAD entries lead a capped
  16-entry digest from a 20-member table in most-recent-first order,
  a same-instant tie puts DEAD before SUSPECT, a table with no
  urgent entries still caps at 16 distinct ALIVE entries, a
  tombstone still rides along as DEAD, and an empty table sends
  nothing. Anti-entropy: a SYNC carries the whole 20-member table,
  a stale node holding only 5 members converges to all 20 with the
  sender's states and incarnations after merging one SYNC, a
  tombstone rides along in a SYNC as DEAD, an empty table sends no
  SYNC, and a 64-member table of long IPv6 ids rotates so two
  consecutive SYNCs cover every member between them.
- `tests/test_ipv6.c`: unit tests for IPv6 — parse/format round-trips
  for IPv4, bracketed and bare IPv6 ids, rejects for malformed ids and
  ports, `pulse_addr_equal`/`pulse_addr_len`, bare-host parsing for
  `--bind`, gossip parsing of PING/JOIN/PING_REQ/GOSSIP messages
  carrying IPv6 ids, and membership-table operations on v6 ids.
- `tests/test_persist.c`: unit tests for membership persistence —
  save/load round-trips the id, incarnation, state, and age
  (including a bracketed IPv6 id), self and never-contacted entries
  are not saved, a DEAD entry loads back as SUSPECT (re-probed, not
  trusted), malformed lines are skipped, an empty save leaves an
  empty file, and ages clamp at one day. Annotations ride along as a
  fifth field in the members file and survive the round trip (with
  `-` for members that carry none, and a malformed annotations field
  dropped while the entry still loads), and the node's own
  annotations round-trip through the `meta` file (missing file loads
  zero, malformed lines are skipped, a duplicate key keeps its first
  value).
- `tests/test_persist.sh`: integration test for membership
  persistence — a 3-node cluster converges, node 3 stops cleanly and
  its peers land in the members file, node 3 restarts with no seed
  and re-discovers both peers (`RESTORE 2 members`, alive=3 again),
  and a node started from a hand-written members file converges
  with the real peer while the bogus peer is suspected, marked
  dead, and garbage-collected.
- `tests/test_rejoin.sh`: integration test for seed rejoin — a node
  starts with its seed down, GCs the unanswering seed, the seed comes
  up later, and the node re-sends JOIN (REJOIN log line) until both
  nodes converge on alive=2. The rejoin retry is visible in all
  three views: `METRICS` counts `seed_rejoins` and breaks it down
  per seed (`seed_rejoins_by_seed`), `JSON` carries the same
  breakdown, and `PROMETHEUS` labels the counter per seed; the
  per-seed last rejoin (0.55.0) is checked the same way in all
  three — `seed_last_rejoin_ms_by_seed` in `METRICS`,
  `"seed_last_rejoin_ms"` in `JSON`, and a labelled
  `pulse_seed_last_rejoin_ms` gauge in `PROMETHEUS`, each a
  13-digit unix-ms timestamp for the one seed that was re-joined.
  The backoff (0.56.0) is checked the same way too: once the seed
  is ALIVE its delay resets to 0 in all three views, and a third
  node whose seed never comes up at all retries on the doubling
  schedule — count 1 with a ~10 s delay, then count 2 with a
  ~20 s delay — in `METRICS`, `JSON` and `PROMETHEUS` alike.
- `tests/test_sync.sh`: integration test for anti-entropy — a 2-node
  cluster with `--sync-interval-ms 2000` converges, both nodes log a
  periodic `SYNC` line to each other within a few seconds, and both
  stay converged on alive=2 after the exchange.
- `tests/test_join_sync.sh`: integration test for JOIN answered with
  a full-table SYNC — a raw JOIN datagram sent to a lone node is
  answered with a SYNC carrying the node's table (not a GOSSIP
  digest), and a 4th node joining a converged 3-node cluster reports
  alive=4 within a few seconds, with the seed converging too. Also
  checks the SYNC reply loop is gone: once converged, both nodes
  stay near idle (under 15% of a core over 3 s) instead of burning
  CPU answering each other's SYNCs forever.
- `tests/test_leave.sh`: integration test for graceful leave — a
  3-node cluster converges, node 3 stops cleanly and both survivors
  mark it DEAD within 400 ms (the probe path alone needs ~500 ms, so
  a DEAD that fast can only come from the announcement), the leaving
  node logs its `LEAVE` announcement, the survivors never log a
  `SUSPECT` for it, and a restarted node comes back ALIVE. Crash
  detection itself is still covered by the other tests, which kill
  with SIGKILL so no announcement is sent.
- `tests/test_ipv6.sh`: integration test for IPv6 — a 3-node `--ipv6`
  cluster converges, a killed node is marked DEAD by both survivors,
  the survivors agree, a restarted node comes back ALIVE, the control
  plane answers over IPv6 (`pulse-ctl '[::1]:port'`), `--bind ::1`
  and `--bind 127.0.0.1` nodes join normally, and mixed families do
  not merge (each side GCs the unanswering cross-family seed and
  neither side ever learns the other's id).
- `tests/test_adaptive.sh`: integration test for the adaptive probe
  interval — a 3-node cluster converges, a killed node is suspected
  by both survivors, and while it is SUSPECT the survivors log an
  `ADAPTIVE fast` line and run probe cycles at the fast interval (a
  node started with `--probe-interval-fast-ms 40` logs probe=40ms, a
  default node logs probe=50ms), `METRICS` and `JSON` report
  `adaptive_fast_cycles` above zero, and once the dead peer is
  declared DEAD the survivors log `ADAPTIVE probe=100ms stable` — the
  speedup only lasts while the cluster is actually unstable. A stable
  cluster logs no `ADAPTIVE` line at all.

- `tests/test_refute.sh`: integration test for the
  suspicion-refutation window — a 2-node cluster converges, node 2
  is frozen with SIGSTOP until node 1 suspects it, then resumed with
  SIGCONT (with a stretched 2 s suspect timeout so the suspicion is
  still open); node 1 logs ALIVE, never DEAD, and `METRICS` reports
  `suspect_refuted=1` with one measured refute-latency sample, and
  `JSON` carries the same count and latency group.
- `tests/test_meta.c`: unit tests for member metadata — `key=value`
  parsing and validation (bad syntax, bad characters, the 32-char
  key/value and 64-char total limits), set/update and the 8-pair cap,
  format and list round-trips, order-insensitive equality, JSON
  rendering (`{"role":"worker"}` or `{}`), the gossip wire round-trip
  over real UDP loopback (annotations ride in the digest and parse
  back, a malformed suffix is dropped while the entry still merges,
  plain entries still parse), and merge propagation (learned
  annotations stamp the entry changed, the same set twice is a
  no-op, an entry with no annotations never wipes stored ones, self
  entries are untouched).
- `tests/test_meta.sh`: integration test for member metadata — a node
  started with `--meta` flags logs them and shows them in `MEMBERS`
  and as a `meta` object in `JSON`, a joiner with no annotations
  learns the seed's via gossip and shows `-` / `{}` itself, `META`
  at runtime is gossiped to peers within seconds, bad `META` usage
  gets clean `ERR` lines and the 8 cap is enforced (the ninth key is
  refused), and a clean restart with the same data dir and no
  `--meta` flags restores the node's own annotations from the saved
  meta file and its peers' annotations from the members file, with
  peers seeing them again after the rejoin.
- `tests/test_peerfile.c`: unit tests for the peer list file —
  comments (including indented ones) and blank lines ignored,
  valid IPv4 and bracketed IPv6 ids load, a bare IPv6 id
  canonicalizes to bracketed form, bad lines (garbage, bad port,
  missing port, unparseable host) are skipped and counted,
  duplicates are dropped silently, CRLF endings work, the cap
  counts overflow as bad, an empty file loads zero peers, and a
  missing file is an error. Live reload diff (0.24.0): the set
  difference between the old list and the new one — added is in new
  but not old, removed is in old but not new, identical lists diff
  to nothing, an empty side adds or removes everything, and
  duplicates inside one list collapse to a single entry.
- `tests/test_peerfile.sh`: integration test for seedless bootstrap
  — a missing peer file is a clean startup error (nonzero exit, no
  `START` line), three nodes started with `--peer-file` and no
  `--seed` converge on alive=3 from one shared file (with a comment,
  a blank line, and a bad line that is logged and skipped), and a
  node whose file lists a peer that never answers still joins the
  live cluster and GCs the dead entry.
- `tests/test_peerfile_reload.sh`: integration test for live
  peer-file reload on SIGHUP (0.24.0) — three nodes converge from
  one shared file; node 3 is removed from the file while ALIVE and
  a SIGHUP on node 1 logs `PEERFILE KEEP` while node 3 stays ALIVE
  in the membership; node 3 is then killed, and once node 1
  suspects it a second SIGHUP drops it at once (`PEERFILE DROP`,
  alive=2 within seconds instead of waiting out the 30 s dead
  retention, and gone from `JSON`); node 4 is added to the file and
  started, and a SIGHUP logs `PEERFILE ADD` with the cluster back
  to alive=3; a SIGHUP with an unchanged file reloads cleanly
  (added=0 dropped=0 kept=0), and a bad line added to the file is
  logged and skipped without disturbing the cluster; a SIGHUP on a
  node with no `--peer-file` is a no-op and the node keeps running;
  and no node restarts on SIGHUP (exactly one `START` line each).
- `tests/test_peerfile_watch.sh`: integration test for
  auto-reload on peer-file mtime change (0.26.0) — three nodes
  converge from one shared file; removing node 3 from the file while
  ALIVE makes node 1 log `PEERFILE KEEP` with no SIGHUP and no
  `RELOAD` ever sent; killing node 3 and then making a comment-only
  edit (no peer change, new mtime) drops the now-unreachable kept
  peer at once (`PEERFILE DROP`, alive=2); adding node 4 to the file
  and starting it logs `PEERFILE ADD` with the cluster back to
  alive=3; and with the file untouched the `PEERFILE RELOAD` count
  is stable across a 3 s idle window, so the watch does not spam
  reloads. No node restarts (exactly one `START` line each).
- `tests/test_control.c`: unit tests for the control-plane dispatch
  (0.25.0) — fake hooks serve canned state and the test drives the
  real `control_listen` / `control_serve` over loopback: `RELOAD`
  answers `OK peers=3 added=1 dropped=0 kept=0 bad=0` on success,
  `ERR no peer file` with no peer file, and `ERR cannot read peer
  file ...` for an unreadable file; a node with no reload hook
  answers `ERR reload not supported` instead of crashing on a NULL
  hook; and `MEMBERS` on an empty table is just `END`, with an
  unknown command still a clean `ERR unknown command`. `PEERS`
  (0.27.0) is covered the same way: a canned table of self, a seed
  peer, a peer-file peer and a never-heard-from discovered peer
  answers one `PEER` line each with the right id, incarnation,
  state and source — a numeric last-seen for peers heard from, `-`
  for self and for the never-heard-from peer — and `PEERS` on an
  empty table is just `END`; with no peer-source hook at all, every
  non-self member reports as `discovered` and self still as `self`.
  `METRICS` and `JSON` (0.28.0) are covered the same way: the exact
  `METRICS` line carries the peer-file path, last reload time /
  last mtime, and the counts from the last load, the `JSON` line
  carries the same state as a top-level `peer_file` object and a
  `source` on every member, and a node with no peer file — or no
  peer-file hook at all — reports `-` / `null` instead of crashing.
  `WATCH` (0.29.0) is covered two ways: the event log itself
  (record / wait order, timeout, wakeup on record from another
  thread, and the gap when the ring overflows past 128 events;
  a plain record carries no phi, a phi-mode SUSPECTED event
  recorded with a phi round-trips through save/load with it, and
  a pre-0.60.0 line without the phi field loads with no phi —
  0.60.0), and
  the stream over the control plane — `OK watching`, a `SNAPSHOT`
  of the canned table in the `PEERS` shape, then live `EVENT`
  lines for events recorded after connecting, with one-shot
  commands still answering while the stream is open, and a clean
  `ERR watch not supported` with no watch hooks. `WATCH` filtering
  (0.30.0) is covered the same way: a filtered stream's snapshot
  holds only the filtered peer, only matching events arrive, and a
  bad filter is refused up front. `WATCH` replay / resume
  (0.31.0) is covered the same way: `FROM <seq>` replays the
  buffered events from that seq after the snapshot and `END`,
  before live events; a `FROM` below the oldest buffered event
  replays from the oldest instead, after a `GAP` line; `FROM 0`
  replays from the oldest; a `FROM` past the latest starts live;
  and a bad `FROM` is refused up front. `WATCH` heartbeat
  (0.32.0) is covered the same way: a live `WATCH` on a quiet
  stream sends a `HEARTBEAT` line with a plausible unix timestamp
  once 5 s pass with nothing sent, and a second one about 5 s
  later, with no `EVENT` line in between. The `WATCH`
  events end to end are covered by `tests/test_watch.sh`, and the
  filter end to end by `tests/test_watch_filter.sh`, the
  replay end to end by `tests/test_watch_replay.sh`, and the
  heartbeat end to end by `tests/test_watch_heartbeat.sh`.
  `WATCH` subscriber count (0.33.0) is covered the same way:
  `METRICS` and `JSON` report the streams connected right now and
  the sessions served since startup — 1 subscriber / 2 sessions
  while one stream is open after an earlier one closed, 2 / 3 with
  a second open, back to 1 / 3 when the second closes, and 0 / 3
  when the first closes too — and a `WATCH` refused up front never
  becomes a session. `WATCH` drop and delivery counts (0.34.0)
  are covered the same way: a plain `WATCH` that receives two live
  events reports delivered=2 on its per-stream entry in `JSON`, a
  `WATCH DEAD` that sees two non-`DEAD` events reports filtered=2
  and delivered=0, and a `WATCH FROM 2` against a ring whose
  oldest held seq is 18 reports dropped=16 and replays the 128
  buffered events as delivered=128 — and the exact `METRICS` line
  carries `watch_delivered=0 watch_filtered=0 watch_dropped=0`
  before any `WATCH` connects. The lifetime totals in `METRICS`
  are not asserted exactly: a just-closed stream's thread lingers
  up to a second before it notices the close, and events recorded
  in that window are consumed by the lingering thread, so the
  totals depend on thread-exit timing. Low-probe-rate alert
  (0.68.0) is covered the same way: a snapshot hook fills the
  probe windows (5 sent / 4 answered for one peer, 5 / 1 for
  another, self and the never-heard-from peer 0 / 0) and the exact
  `METRICS` line carries `low_probe_rate_peers=0`, the `LOWRATE`
  event round-trips through the persisted event log with its
  ` probe_rate=` trailing field, and at a 0.5 threshold only the
  5 / 1 peer shows a `LOWRATE` line in `METRICS`, a
  `"low_probe_rate":true` flag with a `"probe_rate"` below the
  threshold in `JSON`, and a `pulse_low_probe_rate_peers 1` gauge
  with a `pulse_member_probe_rate` below the threshold in
  `PROMETHEUS` — with the warning disabled again all of those
  read 0 / false / absent.
- `tests/test_peerfile_metrics.sh`: integration test for
  METRICS/JSON exposure of peer-file state (0.28.0) — two nodes
  converge from one shared peer file (with a bad line) and `METRICS`
  reports the path, the counts from the initial load (`pf_peers=2
  pf_added=2 pf_bad=1`), a plausible `last_reload` and a `last_mtime`
  equal to the file's mtime, while `JSON` carries the same state as
  a `peer_file` object and `source` per member (`self` / `peer-file`);
  removing a live peer from the file and adding another, then
  SIGHUP, shows the new counts (`pf_peers=2 pf_kept=1 pf_bad=0`,
  `last_mtime` moved) with the kept peer still `peer-file` in
  `JSON`; a SIGHUP and a `RELOAD` on an unchanged file both refresh
  the state to the no-op reload (`added=0 dropped=0 kept=1`), and
  `RELOAD` advances `last_reload`; a node with no `--peer-file`
  reports `-` for every peer-file field in `METRICS` and
  `"peer_file":null` in `JSON`, with its seed peer's source still
  `seed`; and no node restarts (exactly one `START` line each).
- `tests/test_reload.sh`: integration test for the `RELOAD`
  control-plane command (0.25.0) — the same scenarios as the SIGHUP
  reload test, driven over TCP instead: three nodes converge from
  one shared file; removing node 3 from the file and sending `RELOAD`
  keeps it ALIVE (`OK peers=2 added=0 dropped=0 kept=1 bad=0`,
  `PEERFILE KEEP`); killing node 3 and sending `RELOAD` once it is
  suspected drops it at once (`OK peers=2 added=0 dropped=1 kept=0
  bad=0`, alive=2, gone from `JSON`); adding node 4 and sending
  `RELOAD` adds it (alive=3); an unchanged file is a clean no-op and
  a bad line is logged and skipped (`bad=1`); `RELOAD` on a node with
  no `--peer-file` is refused (`ERR no peer file`) and on a node
  whose file was deleted is refused (`ERR cannot read peer file ...`)
  with the old list kept; and no node restarts on `RELOAD` (exactly
  one `START` line each).
- `tests/test_peers.sh`: integration test for the `PEERS`
  control-plane command (0.27.0) — three nodes converge from one
  shared peer file and `PEERS` on node 1 lists the file peers as
  `peer-file` and itself as `self` (numeric last-seen for the live
  peers, `-` for self); a fourth node joining through `--seed` only
  lists the seed as `seed` and the peers it learned from the seed's
  SYNC as `discovered`; killing node 3 and waiting for node 1 to
  mark it `DEAD` still lists it as `DEAD` and `peer-file`; and the
  reply ends with `END`.
- `tests/test_watch.sh`: integration test for the `WATCH`
  control-plane command (0.29.0) — two nodes converge from one
  shared peer file and a `WATCH` on node 1 opens with `OK watching`,
  a `SNAPSHOT` line per member in the `PEERS` shape, and `END`, while
  `MEMBERS` still answers on another connection; a third node
  joining through `--seed` only streams `JOINED`, stopping it
  (SIGSTOP) streams `SUSPECTED` and resuming it (SIGCONT) streams
  `REVIVED`, killing it streams `DEAD`, and `LEAVE` over the
  control plane streams `DROPPED`; the `EVENT` lines arrive in
  lifecycle order with strictly increasing sequence numbers, and
  the connection stays open for the whole test. A node joining
  through `--seed` is used deliberately: a peer-file peer already
  sits in the table before it ever answers, so it would come back
  as `REVIVED`, not `JOINED`.
- `tests/test_watch_filter.sh`: integration test for `WATCH`
  filtering (0.30.0) — two nodes converge from one shared peer
  file and two filtered `WATCH`es open on node 1: one for types
  `JOINED,DEAD` and peer `127.0.0.1:17833`, which has not joined
  yet, opens with `OK watching` and `END` but no `SNAPSHOT` lines
  at all, and one for peer `127.0.0.1:17832` snapshots only that
  peer; a third node joining through `--seed` only streams `JOINED`
  on the first stream and nothing on the second, stopping it
  (SIGSTOP) streams `SUSPECTED` on neither — the type filter drops
  it on the first, the peer filter on the second — resuming it
  (SIGCONT) streams `REVIVED` on neither, and killing it streams
  `DEAD` on the first and nothing on the second; a bad filter is
  refused up front (`WATCH BOGUS` answers `ERR invalid watch
  filter unknown event type BOGUS`); the first stream's `EVENT`
  lines are exactly `JOINED` then `DEAD`, with strictly increasing
  sequence numbers, and both streams stay open for the whole test.
  Both nodes run with a 3 s suspect timeout so neither declares
  the stopped node `DEAD` while it is stopped — with its default
  timeout the second node would gossip a `DEAD` to node 1 first,
  and the first stream would correctly carry it before the kill.
- `tests/test_watch_state_filter.sh`: integration test for `WATCH`
  state filtering (0.39.0) — two nodes converge from one shared
  peer file and two state-filtered `WATCH`es open on node 1: a
  `STATE DEAD` stream opens with `OK watching` and `END` but no
  `SNAPSHOT` lines at all (both members are ALIVE), and a `STATE
  ALIVE` stream snapshots both members; a third node joining
  through `--seed` only streams `JOINED` on the ALIVE stream and
  nothing on the DEAD stream, stopping it (SIGSTOP) streams
  `SUSPECTED` on neither — it lands on SUSPECT, in neither filter
  — resuming it (SIGCONT) streams `REVIVED` on the ALIVE stream
  and nothing on the DEAD stream, and killing it streams `DEAD` on
  the DEAD stream and nothing on the ALIVE stream; after the kill
  a fresh `STATE DEAD` `WATCH` snapshots only the dead member and
  a fresh `STATE ALIVE` `WATCH` snapshots only the two live ones;
  bad `STATE` filters are refused up front (`WATCH STATE BOGUS`,
  `WATCH STATE` with no list, and a duplicate `STATE`); the DEAD
  stream's `EVENT` lines are exactly one `DEAD`, the ALIVE
  stream's exactly `JOINED` then `REVIVED`, with strictly
  increasing sequence numbers, and both streams stay open for the
  whole test. Both nodes run with a 3 s suspect timeout so neither
  declares the stopped node `DEAD` while it is stopped.
- `tests/test_watch_replay.sh`: integration test for `WATCH`
  replay / resume (0.31.0) — two nodes converge from one shared
  peer file and a live `WATCH` on node 1 records the sequence
  numbers of a third node's lifecycle (JOINED when it joins
  through `--seed`, SUSPECTED then DEAD when it is killed); a
  second `WATCH` opened after the kill with `FROM <join-seq>`
  replays exactly JOINED, SUSPECTED, DEAD with the same sequence
  numbers and no `GAP` line, since nothing was evicted; restarting
  node 3 streams REVIVED live on the resumed stream, after the
  replay; a `WATCH FROM 0` replays from the oldest buffered event
  and carries the JOINED too; and a bad `FROM` is refused up front
  (`WATCH FROM` and `WATCH FROM xyz` answer `ERR invalid watch
  filter ...`). The gap path — a `FROM` below the oldest buffered
  event, answered with a `GAP` line and a replay from the oldest —
  needs 128 events to evict the ring, so it is covered
  deterministically in the unit test instead.
- `tests/test_watch_heartbeat.sh`: integration test for the `WATCH`
  heartbeat (0.32.0) — two nodes converge from one shared peer
  file and a `WATCH` on node 1 sends a `HEARTBEAT` line with a
  unix timestamp close to now once 5 s pass with nothing sent, and
  a second one about 5 s later, with no `EVENT` line while the
  cluster is quiet; a third node joining through `--seed` then
  streams `JOINED` on the same connection, after the heartbeats,
  and the stream stays open for the whole test.
- `tests/test_watch_subscribers.sh`: integration test for the `WATCH`
  subscriber count (0.33.0) — two nodes converge from one shared
  peer file and with no watcher connected `METRICS` reports
  `watch_subscribers=0 watch_sessions=0` and `JSON` carries
  `"watch":{"subscribers":0,"sessions":0}`; one `WATCH` opening on
  node 1 takes both to 1, a second takes both to 2, the first
  watcher going away drops subscribers back to 1 while sessions
  stays at 2, and the second going away leaves subscribers at 0
  with sessions still at 2 — the session count only ever grows.
- `tests/test_watch_delivery.sh`: integration test for the `WATCH`
  drop and delivery counts (0.34.0) — two nodes converge from one
  shared peer file and with no watcher connected `METRICS` reports
  `watch_delivered=0 watch_filtered=0 watch_dropped=0` and `JSON`
  carries the same three zeros with an empty `streams` array; an
  unfiltered `WATCH` on node 1 streams the `DEAD` event when node 2
  is killed and `METRICS` reports at least 1 delivered while `JSON`
  shows session 1 with at least 1 delivered; a second `WATCH`
  filtered to `DEAD` only skips node 2's `REVIVED` on restart and
  `METRICS` reports at least 1 filtered while `JSON` shows session
  2 with 0 delivered and at least 1 filtered and no non-`DEAD`
  event reaches it; both watchers going away leaves subscribers at
  0 with sessions still at 2, the totals persisted, dropped still
  0, and `JSON` listing no live streams. The drop count itself — a
  gap in the event ring — is covered deterministically in the unit
  test (`tests/test_control.c`): it needs 128 events to evict the
  ring, so the integration test does not flood the cluster to
  force one.
- `tests/test_watch_backpressure.sh`: integration test for the
  `WATCH` backpressure / pause (0.37.0) — two nodes converge from
  one shared peer file with node 1 on `--event-log-cap 4` and with
  no watcher connected `METRICS` reports `watch_paused=0` and
  `JSON` carries `"paused":0` with an empty `streams` array; one
  `WATCH` opening on node 1 is session 1 and unpaused, `WATCH
  PAUSE 1` answers `OK paused 1` (twice: idempotent) and `METRICS`
  reports `watch_paused=1` with `JSON` showing session 1
  `"paused":true`, killing node 2 marks it `DEAD` on node 1 but no
  `DEAD` event reaches the paused stream, and `WATCH RESUME 1`
  answers `OK resumed 1` with the held `DEAD` event replayed from
  the ring; bad pause / resume commands are refused (unknown
  session, non-numeric session, missing session); and pausing
  again through five events (restart, kill, `LEAVE`, restart,
  kill) against a ring that holds four ends the stream on resume
  with `ERR gap: events lost, reconnect` and `METRICS` reporting
  at least 1 dropped event. The pause itself — no event delivered
  while paused, the held event replayed on resume, the idempotent
  replies and the refused commands — is covered deterministically
  in the unit test (`tests/test_control.c`).
- `tests/test_prometheus.sh`: integration test for the Prometheus
  exposition format (0.35.0) — two nodes converge from one shared
  peer file and `PROMETHEUS` answers in Prometheus text format
  (`# HELP` / `# TYPE` per family, then `name value` lines, then
  `END`) with the membership gauges (alive=2, total=2), zeroed
  counters, a `pulse_member_info` gauge per member labelled by
  member, state and source, and the peer-file gauges from the
  initial load (peers=2, added=2, bad=0); node 2 is killed with
  SIGKILL — an unclean death, so no graceful-leave announcement —
  and node 1's suspicion and death of it move the exposition
  (suspected and dead totals above 0, the dead member's info gauge
  at state="DEAD", the detection-latency histogram gaining one
  sample); and a node with no `--peer-file` exposes no
  `pulse_peer_file_*` metric at all. Two nodes in `--phi-accrual`
  mode expose the per-member phi gauge (0.42.0, agreeing with
  `JSON`), the threshold gauge (0.43.0), the phi history gauges
  (0.51.0) and the phi history histogram (0.52.0, cumulative buckets
  with the `+Inf` bucket equal to the samples gauge) — none of them
  for self, and none at all without `--phi-accrual` — and a killed
  peer keeps its last-known phi, its frozen history gauges and its
  frozen histogram until GC. The exact counter, gauge and
  histogram values are covered deterministically in the unit test
  (`tests/test_control.c`), which serves canned metrics instead of
  waiting on a real cluster.

## Roadmap

- ~~Garbage collection of old DEAD entries.~~ Done in 0.2.0.
- ~~Phi-accrual failure detection as an alternative to fixed timeouts.~~ Done in 0.3.0.
- ~~A TCP control plane: query membership, force-leave a member.~~ Done in 0.4.0.
- ~~Optional shared secret so only trusted devices can join.~~ Done in 0.5.0.
- ~~IPv6.~~ Done in 0.6.0.
- ~~Seed rejoin retry when a seed is down at startup.~~ Done in 0.7.0.
- ~~Cluster-size-aware gossip fanout: digest to 1 + floor(log2(peers)) random peers per round, capped at 4.~~ Done in 0.8.0.
- ~~Prioritized gossip digests: the most recently changed SUSPECT/DEAD entries lead each digest (up to 4, most recent first), so failure news is never stuck behind the random window when the table is larger than the 16-entry digest.~~ Done in 0.9.0.
- ~~Failure-detector metrics: detection latency (last heartbeat to SUSPECT) and suspect-to-dead duration, with transition counts, exposed as a `METRICS` command on the control plane.~~ Done in 0.10.0.
- ~~Machine-readable cluster view: a `JSON` control-plane command returning status, members, and metrics as one JSON object, so tooling can poll cluster state without scraping the text protocol.~~ Done in 0.11.0.
- ~~Membership persistence: save the known member list to disk periodically and on clean shutdown, and reload it on startup, so a restarted node re-discovers its old peers without a seed — re-probing every restored entry instead of trusting it.~~ Done in 0.12.0.
- ~~Anti-entropy full-table sync: periodically exchange the whole membership table with a random peer (SYNC, answered with the peer's own full table), so a node that missed many incremental digests converges in one exchange; tables too big for one datagram rotate across successive SYNCs.~~ Done in 0.13.0.
- ~~Graceful leave: a node that stops cleanly announces its departure to its peers (a GOSSIP digest with its own entry DEAD), so they mark it DEAD at once instead of waiting for the probe timeout; a crash sends nothing, so the probe path still covers unclean deaths, and a restart at a higher incarnation refutes the DEAD entry.~~ Done in 0.14.0.
- ~~JOIN answered with a full-table SYNC: a joiner gets the seed's whole membership table at once instead of the capped 16-entry digest, so it converges immediately instead of waiting for gossip rounds to rotate the missing members through; the joiner answers with its own full table, so one exchange converges both sides.~~ Done in 0.15.0.
- ~~SYNC replies only on new information: a SYNC is answered with the receiver's full table only when the exchange taught it something new (new/revived sender, refutation, or a merged entry that added or changed a member); a SYNC that changes nothing gets no reply, fixing the 0.13.0 reply ping-pong in which a converged pair burned a CPU core exchanging identical tables forever.~~ Done in 0.15.0.
- ~~Gossip delta encoding: instead of sending the full membership table in every gossip exchange, send only entries that changed since the peer's last known version (per-peer versions tracked from a table version bumped on every change, each entry stamped with the version it last changed at), falling back to a full SYNC when the peer has no recorded version or is more than a digest's worth of changes behind; a converged cluster sends no gossip bytes at all until something changes, and the periodic anti-entropy SYNC heals any delta lost to UDP.~~ Done in 0.16.0.
- ~~Detection-latency histogram: the locally measured detection latencies are bucketed (<100, 100–249, 250–499, 500–999, ≥1000 ms) alongside the existing mean/max, and exposed as `hist` in the `METRICS` text line and as `detect_latency_ms.hist` in `JSON`, so the mean cannot hide a slow tail — a detection that usually lands in ~200 ms but sometimes takes over a second is exactly what the ward-device use case needs to see.~~ Done in 0.17.0.
- ~~Adaptive probe interval: while any peer is SUSPECT the probe cycle shortens from the base interval to a fast interval (default half the base, `--probe-interval-fast-ms` to set it, floored at 25 ms, never slower than base), so direct probes, indirect PING_REQ, the suspect-to-dead sweep and gossip rounds all run faster exactly while the cluster is mid-incident, then return to base when the table is suspect-free; each switch is logged as an `ADAPTIVE` line and the fast cycles are counted as `adaptive_fast_cycles` in `METRICS` and `JSON`.~~ Done in 0.18.0.
- ~~Suspicion-refutation window: a SUSPECT that comes back ALIVE is counted as a refuted suspicion (`suspect_refuted` in `METRICS` and `JSON`), with the time it sat SUSPECT recorded as a refute latency (mean/max/samples) — locally revived peers add a measured sample, gossip-learned refutations count only, and a DEAD peer coming back is a restart, not a refutation — so an operator can see how long false suspicions stay visible to the cluster.~~ Done in 0.19.0.
- ~~Member metadata/annotations: each node carries up to 8 `key=value` annotations about itself (`role=worker`, `zone=us-east-1`, `version=1.2.3`), set with repeatable `--meta` flags or the control plane's `META` command, gossiped with its membership entry as an `;k=v,k=v` suffix (entries from older nodes parse unchanged), visible in `MEMBERS` and as a `meta` object per member in `JSON`, and persisted across restarts — the node's own set in `<data-dir>/meta` (flags win, otherwise the saved set is restored) and peers' sets as an optional fifth field in `<data-dir>/members`; an entry carrying no annotations never wipes stored ones.~~ Done in 0.20.0.
- ~~Probe RTT per peer: each node measures the round-trip time of its direct probes (PING send to ACK receipt) per peer and reports the last RTT and the mean over all answered probes per member in `JSON` as `rtt_ms` / `rtt_mean_ms` (`null` until a probe has been answered, always `null` for self), so an operator can see which peers are slow before they are ever suspected.~~ Done in 0.21.0.
- ~~Slow-peer warning: a peer whose mean probe RTT rises above a configurable threshold (`--slow-peer-rtt-ms`, 0 = disabled) is flagged `"slow":true` per member in `JSON` with a `slow_peers` count in `METRICS` and `JSON`, and the node logs a `SLOW` line once per slow episode (cleared on recovery, so a re-crossing warns again) — the mean, not the last RTT, so one slow probe is noise and only a consistently slow peer warns, while the peer is still ALIVE instead of only after it is suspected.~~ Done in 0.22.0.
- ~~Seedless bootstrap via a peer list file: a node reads its initial peers from a file (`--peer-file`, one `host:port` per line, comments and blank lines ignored), probes them on startup and joins the cluster without a `--seed` flag — the entries get exactly the `--seed` treatment (JOIN at startup, probing, ~5 s rejoin retry), bad lines are logged and skipped, and a missing file is a clean startup error instead of a silent solo start.~~ Done in 0.23.0.
- ~~Live peer-file reload on SIGHUP: while a `--peer-file` is in use, SIGHUP re-reads the file and merges it into the seed list without restarting — newly listed peers get the `--seed` treatment (seed list, table, JOIN at once), peers removed from the file are dropped from the table only once they stop answering (a still-ALIVE peer is kept and remembered, so a later reload drops it the moment it goes quiet), a peer named with `--seed` is never added or dropped by a reload, self is never dropped, and a reload that cannot read the file keeps the old list; every reload is logged (`PEERFILE RELOAD` summary plus `ADD`/`DROP`/`KEEP` lines), and SIGHUP on a node with no `--peer-file` is a no-op.~~ Done in 0.24.0.
- ~~RELOAD control-plane command over TCP: the same live peer-file reload SIGHUP triggers, driven from the control plane (`RELOAD`, answered `OK peers=N added=A dropped=D kept=K bad=B`) so an operator who cannot signal the process can still reload the roster; a node with no `--peer-file` answers `ERR no peer file`, an unreadable file answers `ERR cannot read peer file <path>: <reason>` and keeps the old list, and SIGHUP and RELOAD run the same code serialized on one lock so the two triggers can never interleave.~~ Done in 0.25.0.
- ~~Auto-reload on peer-file mtime change: while a `--peer-file` is in use, the node stats the file once a second and re-runs the same reload merge when its mtime moves — no SIGHUP, no control-plane call, no restart — with the reload logged (`PEERFILE <file> changed on disk, auto-reloading` plus the usual summary) and the watch baseline refreshed by every manual reload so it never re-runs right after one; an edit that preserves the mtime is invisible to the watch, a kept peer that dies after its edit is dropped at the next file change (or by normal GC), and a deleted file is skipped quietly until it reappears.~~ Done in 0.26.0.
- ~~PEERS control-plane command over TCP: one `PEER <id> <incarnation> <state> <last-seen-ms|-> <seed\|peer-file\|discovered\|self>` line per member, then `END`, so an operator can see at a glance how long since each peer was last heard from directly and where the peer came from — a `--seed` flag, the `--peer-file` roster (or a peer removed from it but kept because it was still ALIVE), or gossip/a JOIN from a peer the node was never told about; the flag outranks the file, self reports as `self` with a `-` last-seen, and a peer never heard from reports `-` too.~~ Done in 0.27.0.
- ~~METRICS/JSON exposure of peer-file state: `METRICS` reports the `--peer-file` path, the wall-clock time of the last successful load or reload, the file's mtime at that load, and the counts from that load (`pf_peers` / `pf_added` / `pf_dropped` / `pf_kept` / `pf_bad`), and `JSON` carries the same state as a top-level `peer_file` object (or `null` with no `--peer-file`) plus a `source` (`seed` / `peer-file` / `discovered` / `self`) on every member, so a peer-file peer is distinguishable from a discovered one in the machine-readable view too; the initial load at startup counts as the first reload, a failed reload leaves the last successful state untouched, and a node with no `--peer-file` reports `-` / `null`.~~ Done in 0.28.0.
- ~~Control-plane event stream (`WATCH`): a long-lived TCP connection that pushes membership events as they happen — `JOINED`, `SUSPECTED`, `DEAD`, `REVIVED`, `DROPPED`, `REFUTED`, each with a sequence number, the member id, its incarnation and the unix time — with the same id/incarnation/state shape as `PEERS` for the opening snapshot, so tooling can watch a cluster without polling `STATUS` / `MEMBERS` / `JSON` once a second; a slow consumer is dropped instead of buffering unboundedly, and the stream starts with a full snapshot so a late subscriber does not miss the current table.~~ Done in 0.29.0.
- ~~Per-peer event filtering on the `WATCH` stream: a watcher can subscribe to only certain event types, only certain peer ids, or both — `WATCH JOINED,DEAD`, `WATCH 127.0.0.1:17102`, or `WATCH JOINED,DEAD 127.0.0.1:17102`, in any order — with the filter applied server-side before sending, so a filtered-out event never crosses the wire; the snapshot is narrowed by the id list too, so a watcher subscribed to one peer sees only that peer, a filtered-out event still consumes its sequence number so a filtered stream's numbers jump by design, and a bad filter is refused up front with `ERR invalid watch filter <reason>`.~~ Done in 0.30.0.
- ~~Control-plane sends use `MSG_NOSIGNAL`: a client that goes away mid-send fails the send instead of killing the node with `SIGPIPE` — a `WATCH` client disconnecting just as an `EVENT` is being written would otherwise take the whole process down with it.~~ Done in 0.30.0.
- ~~WATCH replay / resume from a sequence number: a watcher can reconnect with `WATCH FROM <seq>` and the server replays the buffered events from that seq, in order, after the snapshot and `END` and before live events, so a client that dropped catches up instead of starting from now — `FROM 0` replays from the oldest buffered event, a `FROM` past the latest starts live, and a `FROM` below the oldest buffered event replays from the oldest instead, after a `GAP <requested> <oldest>` line that says how much was lost, with the stream staying open; `FROM` composes with the type/id filters in any order, and a bad `FROM` is refused up front with `ERR invalid watch filter <reason>`.~~ Done in 0.31.0.
- ~~WATCH heartbeat on idle streams: a stream that has sent nothing for 5 s sends `HEARTBEAT <unix-seconds>`, so a client on a quiet cluster can tell "nothing happened" from "the connection died silently" — a half-open TCP connection otherwise looks exactly like a quiet cluster. The heartbeat carries no sequence number and is sent regardless of the type/id filter, and any line sent (EVENT, GAP, or HEARTBEAT) resets the 5 s clock, so a busy cluster never sees one.~~ Done in 0.32.0.
- ~~WATCH subscriber count in METRICS and JSON: `METRICS` reports `watch_subscribers` (how many `WATCH` streams are connected right now) and `watch_sessions` (how many sessions the node has served since startup), and `JSON` carries the same counts as a top-level `watch` object, so an operator polling cluster state can see how many consumers are attached to the event stream — a client that went away drops out of the current count within about a second, while the session count only ever grows, so 0 subscribers with sessions above 0 means watchers that all went away and 0 / 0 means none ever connected.~~ Done in 0.33.0.
- ~~WATCH drop and delivery counts in METRICS and JSON: each stream counts the `EVENT` lines it sent (`delivered`), the events it consumed but skipped because its type/id filter excluded them (`filtered`), and the events it lost to a gap in the bounded event ring (`dropped` — a replay `GAP`, or a mid-stream gap that ends the stream with `ERR gap`), and `METRICS` reports the three as lifetime totals while `JSON` carries the same three totals plus a `streams` array with the same three counts per currently-connected stream, keyed by its session number, so an operator can see whether consumers are keeping up with the cluster instead of only how many are connected.~~ Done in 0.34.0.
- ~~Prometheus exposition format: a `PROMETHEUS` control-plane command answering the same values `METRICS` and `JSON` report as Prometheus text format — membership counts, slow peers, uptime and current watch subscribers as gauges, the lifetime transition totals as counters, the three latency groups as mean/max/samples gauges (0 with no samples, since Prometheus has no null), the detection latency as a real histogram (the same five buckets `METRICS` reports as `hist`, cumulative, with `_sum` and `_count`), one `pulse_member_info` gauge per member labelled by member, state and source, and the peer-file gauges only when the node runs with `--peer-file` — so a monitoring stack can scrape the cluster without parsing the text protocol; the reply still ends with the control plane's `END` trailer, since this is the exposition format, not an HTTP endpoint.~~ Done in 0.35.0.
- ~~Configurable WATCH event-log ring size: a node started with `--event-log-cap N` (1 to 100000, default 128) holds the last N membership events for replay instead of a fixed 128 — the ring is heap-allocated at startup, so a busy cluster can hold more history for reconnecting watchers and a small node can hold less — and the cap is reported on the control plane so an operator can see how far back a replay can reach: `watch_event_log_cap=N` in `METRICS`, `"event_log_cap":N` in the `watch` object in `JSON`, and `pulse_watch_event_log_cap` as a gauge in `PROMETHEUS`; the gap rules are unchanged, a slow consumer still gets `ERR gap` past the ring, and a bad cap is a clean startup error.~~ Done in 0.36.0.
- ~~WATCH backpressure / pause: a slow consumer can stop the flow without closing the connection — `WATCH PAUSE <session>` on a separate connection pauses the stream with that session number (the number `JSON` reports per stream), and `WATCH RESUME <session>` resumes it, replaying what the bounded ring still holds from the last consumed sequence, or ending with `ERR gap` when the ring has scrolled past the resume point; the paused state is visible as `watch_paused=N` in `METRICS`, `"paused":N` in the `watch` object and `"paused":true/false` per stream in `JSON`, and `pulse_watch_paused` as a gauge in `PROMETHEUS`; both commands are idempotent, and a paused stream still sends its heartbeat and ends when its client goes away, so pausing never stalls the node and never grows memory.~~ Done in 0.37.0.
- ~~WATCH event-log persistence across restarts: the node appends each membership event to `<data-dir>/events` as it records it and loads the file back into the ring on startup, so sequence numbers continue across a restart instead of resetting to 1 and a `WATCH FROM <seq>` issued after a restart can still replay the events recorded before it; the file is compacted to the last `--event-log-cap` events on startup and on clean shutdown, a missing file loads zero events, malformed lines are skipped, and a load or append failure never stops the node — it just starts at sequence 1.~~ Done in 0.38.0.
- ~~WATCH state filter: a watcher can subscribe to events by the member state they land on — `WATCH STATE SUSPECT,DEAD` — instead of only by event type or peer id: JOINED, REVIVED and REFUTED land on ALIVE, SUSPECTED on SUSPECT, DEAD on DEAD, and DROPPED lands on no state at all so it never matches a state filter; the snapshot is narrowed by the member's current state too, `STATE` composes with the type/id filters and `FROM` in any order, and a bad state is refused up front with `ERR invalid watch filter unknown member state <name>`.~~ Done in 0.39.0.
- ~~Per-member probe RTT in PROMETHEUS: the last and mean direct-probe round-trip time `JSON` reports as `rtt_ms` / `rtt_mean_ms` ride as `pulse_member_rtt_ms` and `pulse_member_rtt_mean_ms` gauges, one line per member that has answered a probe — self never probes itself and a peer never heard from has no line, a value of 0 is a real sub-millisecond probe rather than a missing one, and a dead member keeps its last RTT — so a scraper can alert on a slow peer without polling `JSON`.~~ Done in 0.40.0.
- ~~Configurable gossip digest size: a node started with `--gossip-digest-size N` (1 to 64, default 16) covers N members per gossip round's digest instead of a fixed 16 — a large cluster trades convergence speed for bandwidth by shrinking it, a small one can grow it to the whole table, the urgent SUSPECT/DEAD leading slots are clamped to the digest (a digest of 1 still leads with the single most urgent entry), the delta-to-SYNC threshold follows the size, and the size is reported on the control plane so an operator can see it without reading the command line: `gossip_digest_size=N` in `METRICS`, `"gossip_digest_size":N` top-level in `JSON`, and `pulse_gossip_digest_size` as a gauge in `PROMETHEUS`; a bad size is a clean startup error.~~ Done in 0.41.0.
- ~~Phi per member in PROMETHEUS: the phi failure-detector value `JSON` reports as `phi` per member rides as a `pulse_member_phi` gauge, one line per member that has a phi — phi mode on, not self, heartbeat history with at least two intervals — two decimals like `JSON`, so a scraper can alert on a rising phi before the threshold suspicion fires; a node without `--phi-accrual` emits no such line, a peer never heard from has none, and a dead member keeps its last-known phi until GC.~~ Done in 0.42.0.
- ~~Phi suspicion threshold in PROMETHEUS: the phi value at which a peer is suspected rides as a `pulse_phi_threshold` gauge — the `--phi-threshold` the node started with, 8.0 by default, two decimals like the phi gauges — so a scraper can alert on a rising phi against the threshold that actually fires the suspicion instead of hard-coding 8.0; it is emitted always, like the other config gauges, whether or not `--phi-accrual` is on.~~ Done in 0.43.0.
- ~~Per-member phi in METRICS: the phi failure-detector value `JSON` reports as `phi` per member and `PROMETHEUS` rides as a `pulse_member_phi` gauge now also rides in the text protocol — one `PHI <id> <value>` line after the `METRICS` line for each member that has a phi, under the same rule as the other two (phi mode on, not self, heartbeat history with at least two intervals), the live phi at reply time, two decimals like `JSON` — so an operator polling the text protocol can watch a peer's phi rise without parsing `JSON` or scraping Prometheus; a node without `--phi-accrual` emits no `PHI` line at all, a peer never heard from has none, and a dead member keeps its last-known phi until GC.~~ Done in 0.44.0.
- ~~Phi suspicion threshold in METRICS: the phi value at which a peer is suspected rides as `phi_threshold=<N.NN>` at the end of the `METRICS` line — the `--phi-threshold` the node started with, 8.0 by default, two decimals like the `pulse_phi_threshold` gauge in `PROMETHEUS` — emitted always, like the other config gauges, whether or not `--phi-accrual` is on, so a text-protocol consumer can judge a rising `PHI` line against the threshold that actually fires the suspicion instead of hard-coding 8.0.~~ Done in 0.45.0.
- ~~Phi suspicion threshold in JSON: the phi value at which a peer is suspected now also rides in `JSON` as a top-level `"phi_threshold":N.NN` number, after `"gossip_digest_size"` — the `--phi-threshold` the node started with, 8.0 by default, two decimals like `METRICS` and the `pulse_phi_threshold` gauge in `PROMETHEUS` — emitted always, like the other config fields, whether or not `--phi-accrual` is on, so a JSON consumer can judge a rising per-member `"phi"` against the threshold that actually fires the suspicion instead of hard-coding 8.0.~~ Done in 0.46.0.
- ~~Phi history in JSON: each member in `JSON` carries a `"phi_history"` — the last 16 phi values sampled for that peer, as `[[age_ms, phi], ...]` pairs oldest first, `age_ms` the ms since the node started — sampled once per probe cycle for ALIVE and SUSPECT peers and frozen when the peer goes DEAD, so an operator can see how a peer's suspicion evolved (near 0 while it answers, climbing past the threshold as it goes quiet) instead of only its live `"phi"`; `null` under the same rule as `"phi"` itself (phi-accrual off, self, or no heartbeat history), `[]` before the first sample, and polling `JSON` never moves the history it reads.~~ Done in 0.47.0.
- ~~Probe RTT histogram: every answered direct probe adds one sample to a global RTT histogram — mean/max/samples plus five buckets (<1, 1–4, 5–9, 10–49, ≥50 ms), a sample exactly at a bound landing in the higher bucket like the detection-latency histogram — exposed as `probe_rtt_ms` at the end of the `METRICS` line, as a `probe_rtt_ms` object in the `JSON` metrics object, and as a real `pulse_probe_rtt_ms` histogram (cumulative buckets, `_sum`, `_count`) with mean/max/samples gauges in `PROMETHEUS`, so the mean RTT cannot hide a slow tail; a probe never answered adds no sample, and the histogram is local measurement, never gossiped or persisted.~~ Done in 0.48.0.
- ~~Probe RTT in phi-accrual mode: the phi-mode probe creates a pending entry marked `rtt_only` — it exists only to match the ACK and measure the round-trip time, never to escalate to an indirect probe or suspect on age — so the per-member `rtt_ms` / `rtt_mean_ms` and the probe-RTT histogram are populated in `--phi-accrual` mode exactly as in the fixed-timeout mode, feeding the same `METRICS` / `JSON` / `PROMETHEUS` sample path; unanswered phi-mode probes leave stale entries that the probe cycle sweeps after twice the ack timeout, so the 32 pending slots never fill, and suspicion still comes from phi alone.~~ Done in 0.49.0.
- ~~Probe-interval jitter: each probe cycle sleeps the adaptive interval times (1 + u) with u uniform in [-j, +j] (`--probe-jitter-frac`, default 0.1, clamped to [0, 0.9], 0 disables it) — an explicit xorshift32 state seeded once per node from time/pid/port, so nodes that start together drift out of lockstep within a few cycles while each node's mean interval stays the base; the jitter applies to the sleep only, so the `ADAPTIVE` log lines and `adaptive_fast_cycles` still see the unjittered interval, the jittered sleep never drops below the 25 ms floor, and failure detection semantics are untouched (suspicion still comes from the fixed ack timeout or from phi alone).~~ Done in 0.50.0.
Done in 0.50.0.
- ~~Phi history in PROMETHEUS: the last 16 sampled phi values `JSON` reports as `"phi_history"` ride as `pulse_member_phi_history_latest` / `pulse_member_phi_history_max` / `pulse_member_phi_history_samples` gauges, one line per member that has a phi — the latest is the most recent probe-cycle sample, the max is over the whole window, and the samples gauge says how many of the 16 slots are filled — under the same rule as the live `pulse_member_phi` gauge (phi mode on, not self, heartbeat history with at least two intervals, and at least one sample taken), so a scraper can alert on a phi that climbed and fell back without polling `JSON`; a dead member keeps its frozen history gauges until GC.~~ Done in 0.51.0.
- ~~Phi history histogram in PROMETHEUS: the same 16 sampled phi values the history gauges summarise ride as a real histogram per member, `pulse_member_phi_history` — cumulative buckets at `le="0.5"`, `"1"`, `"2"`, `"4"`, `"8"` and `"+Inf"`, plus `_sum` and `_count` — under the same rule as the gauges (phi mode on, not self, heartbeat history with at least two intervals, and at least one sample taken), so a scraper can compute quantiles of a peer's phi history instead of trusting the latest and max alone: a peer that sat near 0 with one spike to 7.9 reads differently from one that climbed steadily; the `+Inf` bucket always equals the samples gauge for the same member, a sample exactly at a bound lands in that bucket, and a dead member keeps its frozen histogram until GC.~~ Done in 0.52.0.
- ~~Seed-rejoin count: every re-JOIN the ~5 s seed-rejoin retry sends is counted — one count per re-JOIN, whether the seed was SUSPECT/DEAD in the table or already garbage-collected — as `seed_rejoins` in `METRICS` and in the `JSON` metrics object (right after `joins`) and as `pulse_seed_rejoin_total` in `PROMETHEUS`, so an operator can see the retry firing, and how often, without reading the log; a node whose seeds were up at startup, or with no seeds, reports 0, and the count is local lifetime measurement for this process, never gossiped and reset on restart, like the other transition totals.~~ Done in 0.53.0.
- ~~Per-seed seed-rejoin breakdown: the `seed_rejoins` total is broken down per seed — one entry per seed that has been re-joined, in first-rejoin order, the counts always summing to the total — as `seed_rejoins_by_seed=<id>=N,<id>=N` in `METRICS` (right after `seed_rejoins`, `-` when no seed has been re-joined yet), as `"seed_rejoins_by_seed":{"<id>":N}` in the `JSON` metrics object (`{}` when empty), and as one labelled sample per re-joined seed in `PROMETHEUS` — `pulse_seed_rejoin_total{seed="<id>"} N`, a seed never re-joined has no line and a node that re-joined nothing emits the HELP/TYPE pair with no samples — so an operator can see which seed a node keeps rejoining instead of only how often the retry fires; a seed that was never re-joined has no entry anywhere, and the breakdown is local lifetime measurement for this process, never gossiped and reset on restart, like the total.~~ Done in 0.54.0.
- ~~Per-seed last rejoin timestamp: each seed that has been re-joined also carries the wall-clock time of its most recent re-JOIN, as unix milliseconds — `seed_last_rejoin_ms_by_seed=<id>=N,<id>=N` in `METRICS` (right after the breakdown, `-` when no seed has been re-joined yet), `"seed_last_rejoin_ms":{"<id>":N}` in the `JSON` metrics object (`{}` when empty), and one labelled gauge sample per re-joined seed in `PROMETHEUS` — `pulse_seed_last_rejoin_ms{seed="<id>"} N`, a seed never re-joined has no line and a node that re-joined nothing emits the HELP/TYPE pair with no samples — so an operator can tell when each seed was last retried, and whether a climbing count is still firing or stopped when the cluster merged; a seed that was never re-joined has no entry anywhere, never a fabricated zero, entries appear in first-rejoin order like the counts, and a later re-JOIN to the same seed moves only its timestamp; the timestamps are local lifetime measurement for this process, never gossiped and reset on restart, like the counts.~~ Done in 0.55.0.
- ~~Seed-rejoin backoff: a seed that keeps not answering is re-JOINed on a doubling schedule instead of every ~5 s forever — the first retry still fires on the ~5 s tick, then each consecutive unanswered attempt doubles the wait (10 s, 20 s, capped at 30 s, jittered with the probe jitter), so a permanently dead seed is probed less and less often while a seed that comes back is still re-joined within 30 s at worst, and the backoff resets the moment the seed is ALIVE again, so the next outage starts again at the base interval; the current wait rides per seed as `seed_rejoin_backoff_ms_by_seed=<id>=N,<id>=N` in `METRICS` (right after the last-rejoin timestamps, `-` when no seed has been re-joined yet), as `"seed_rejoin_backoff_ms":{"<id>":N}` in the `JSON` metrics object (`{}` when empty), and as one labelled gauge sample per re-joined seed in `PROMETHEUS` — `pulse_seed_rejoin_backoff_ms{seed="<id>"} N`, a seed never re-joined has no line and a node that re-joined nothing emits the HELP/TYPE pair with no samples — 0 while the seed is ALIVE (no backoff pending), the scheduled delay while it is down; a seed that was never re-joined has no entry anywhere, and the backoff state is local lifetime measurement for this process, never gossiped and reset on restart, like the counts.~~ Done in 0.56.0.
- ~~One-retry JOIN send on every one-shot `JOIN`: the 0.56.0 immediate retry on a failed re-JOIN send — the send socket is `connect()`ed to the first seed, so a stale ICMP port-unreachable from a dead seed can fail a send that would have gone out fine a moment later, and the error is consumed by the failed call so the retry succeeds — now covers the startup `JOIN` to each seed and a peer-file reload's `JOIN` to each newly listed peer as well, as one shared `gossip_send_join_retry` instead of three copies of the pattern, so a one-shot merge message is never lost to that stale error (the reload case matters most: with a dead first seed a stale error is pending about half the time, and the rejoin retry that would cover a lost reload `JOIN` can be backed off up to 30 s); a `JOIN` still goes out exactly once on the happy path, and the mechanism is covered by a unit test that reproduces the stale error on a connected socket and checks the plain send fails while the retry delivers exactly one datagram.~~ Done in 0.57.0.
- ~~Suspicion age per member: each member carries how long it has been SUSPECT, in ms — `suspect_ms` in `JSON` (`null` unless the member is SUSPECT), one `SUSPECT <id> <ms>` line per SUSPECT member after the `PHI` lines in `METRICS`, and one `pulse_member_suspect_ms{member="<id>"}` gauge sample per SUSPECT member in `PROMETHEUS` — measured from when the member's current state started on this node (for a suspicion learned from gossip, the time since this node learned of it), so an operator can judge how close a suspicion is to the suspect timeout instead of only learning about it when the member goes DEAD; the age grows while the member sits SUSPECT and disappears the moment it revives or goes DEAD, and ALIVE and DEAD members report `null` / have no line or sample; covered by unit tests for the age helper and an integration test that crashes a peer, checks all three surfaces while it is SUSPECT, and checks they are all clear again once it goes DEAD.~~ Done in 0.58.0.
- ~~Time since last contact per member: each member carries how long since this node last heard from it, in ms — `last_seen_ms` in `JSON` (`null` for self and for a peer never heard from), one `LASTSEEN <id> <ms>` line per member heard from after the `SUSPECT` lines in `METRICS`, and one `pulse_member_last_seen_ms{member="<id>"}` gauge sample per member heard from in `PROMETHEUS` — measured from the member's last direct contact on this node (a probe ACK or a gossip message carrying the member), so an operator can see a peer going quiet before the suspicion even fires, and how long a DEAD member has been silent; a live peer's age stays under a few probe intervals, a dying peer's climbs toward the suspect timeout, and a DEAD member keeps its line and gauge sample until GC, the age growing; covered by unit tests for the age helper and an integration test that checks all three surfaces for a live peer, self, and a never-started peer, and that the age grows while a crashed peer sits SUSPECT.~~ Done in 0.59.0.
- ~~Phi threshold crossing in the event log: a SUSPECTED event this node raises itself in phi-accrual mode carries the phi that crossed the threshold — a trailing ` phi=<value>` on the `WATCH` `EVENT` line and in the persisted `<data-dir>/events` file, so the record of a suspicion says why it fired instead of only that it did — two decimals, the same value the `SUSPECT ... phi=` log line printed; only a real crossing carries a phi, a peer suspected by the no-history guard or the fixed timeout and a suspicion learned from gossip carry none, and every other event writes the plain five fields so existing consumers see byte-identical lines; covered by unit tests for the phi round-trip through save/load and for old-format lines loading with no phi, and an integration test that kills a peer in a phi-mode cluster and checks the `WATCH` SUSPECTED event carries a phi at least the 8.0 threshold, the DEAD event carries none, and the persisted events file carries the same phi.~~ Done in 0.60.0.
- ~~Last-contact age in the event log: every `WATCH` `EVENT` line for a member this node heard from carries a trailing ` last_seen_ms=<ms>` — how long since this node last heard from the member when the event was recorded, the same value `JSON` reports as `last_seen_ms`, frozen at the event instead of recomputed at replay time, so a replayed SUSPECTED event still shows how quiet the peer was when it was suspected and a DEAD event shows a larger age than the SUSPECTED event before it — after the phi field when both are present; self, a member never heard from, and a member already gone from the table carry no field, never a fabricated zero, and the persisted `<data-dir>/events` file carries the same trailing field so the age survives a restart and a replay; covered by unit tests for the age round-trip through save/load (phi + age together, age alone, and old-format lines loading with neither) and an integration test that checks the live `WATCH` SUSPECTED age is at least one ack timeout, the DEAD age is larger, the persisted file carries the same age, and a replay shows the never-started peer's SUSPECTED event with no field.~~ Done in 0.61.0.
- ~~Quiet-peer early warning: an ALIVE peer whose last-contact age rises above a configurable threshold (`--quiet-peer-ms`, 0 = disabled) is flagged `"quiet":true` per member in `JSON` with a `quiet_peers` count in `METRICS` and `JSON` and a `pulse_quiet_peers` gauge in `PROMETHEUS`, and the node logs a `QUIET` line once per quiet episode (re-armed on the next direct contact, so a re-crossing warns again) and records a `QUIET` event on the `WATCH` stream — a new event type, landing on ALIVE for the state filter and subscribable by name, carrying the age as a trailing ` last_seen_ms=<ms>` and persisted in `<data-dir>/events` like every other event — so a peer going quiet is visible before the suspicion even fires, instead of only when it is suspected; the flag clears the moment the peer is suspected, a peer never heard from has no age and is never quiet, and self is never counted (its age would be uptime); covered by unit tests for the quiet predicate, the count with self excluded, the re-arm on contact, and the QUIET round-trip through save/load, and an integration test that crashes a peer with a long ack timeout, checks the `QUIET` log line and event fire exactly once with an age at least the threshold and under the suspicion delay, the `JSON` flag / `METRICS` count / `PROMETHEUS` gauge all read quiet while the peer is still ALIVE and all clear once it is SUSPECT, and the persisted file and a replay carry the same event.~~ Done in 0.62.0.
- ~~Per-member probe RTT in METRICS: the last and mean direct-probe round-trip time `JSON` reports as `rtt_ms` / `rtt_mean_ms` and `PROMETHEUS` as `pulse_member_rtt_ms` / `pulse_member_rtt_mean_ms` now also ride in the text protocol — one `RTT <id> <last_ms> <mean_ms>` line per member that has answered a probe, after the `LASTSEEN` lines in `METRICS` — so an operator polling the text protocol can watch a peer's RTT climb without parsing `JSON` or scraping Prometheus; self and a peer never heard from have no line (rtt_ms is -1 until a probe is answered), a 0 is a real sub-millisecond probe rather than a missing one, and a DEAD member keeps its last RTT until GC, so the line is the last measured RTT, not a live one, for a dead member; covered by unit tests for the exact canned lines (last 3, mean 2; last 0, mean 0) with self and the never-heard-from peer absent, and an integration test that checks the live line's values are non-negative and its mean agrees with `JSON`, that self and a never-started peer have no line, that the line rides after the `LASTSEEN` lines, and that it stays present with the same frozen values while a crashed peer sits SUSPECT.~~ Done in 0.63.0.
- ~~Worst probe RTT per member: the largest single direct-probe round-trip time over all answered probes per peer rides as `rtt_max_ms` in `JSON` (`null` until a probe has been answered, always `null` for self) and as a `pulse_member_rtt_max_ms` gauge in `PROMETHEUS`, one line per member that has answered a probe — self and a peer never heard from have no line, a value of 0 is a real worst case of sub-millisecond probes rather than a missing one, and a DEAD member keeps its worst RTT until GC — so the mean cannot hide one 60 ms probe among sub-millisecond ones; covered by unit tests for the max tracking (only ever grows, never falls on a faster probe, -1 with no samples) and the canned JSON / PROMETHEUS lines (max 7 for the peer with samples 3, 7, 0, 0; max 0 for the sub-millisecond peer; absent for self and the never-heard-from peer), and an integration test that checks the live gauge exists for a probed peer, is absent for self, and stays present while a crashed peer sits DEAD.~~ Done in 0.64.0.
- ~~Worst probe RTT per member in METRICS: the largest single direct-probe round-trip time over all answered probes per peer — the value `JSON` already reports as `rtt_max_ms` and `PROMETHEUS` as `pulse_member_rtt_max_ms` — now also rides in the text protocol, as the fourth field of the `RTT` line (`RTT <id> <last_ms> <mean_ms> <max_ms>`), after the `LASTSEEN` lines in `METRICS`, under the same rule as the last and mean (self and a peer never heard from have no line, a value of 0 is a real worst case of sub-millisecond probes rather than a missing one, and a DEAD member keeps its worst RTT until GC) — so a text-protocol consumer can see the slow tail per peer instead of only the mean; the field is appended after the last and mean, so a consumer reading the first three fields by position still gets the same last and mean, but a strict parser expecting exactly three numbers sees a format change; covered by unit tests for the exact canned lines (last 3, mean 2, max 7; last 0, mean 0, max 0) with self and the never-heard-from peer absent, and an integration test that checks the live line's max is non-negative, at least the last and the mean and at least the `JSON` max read a moment earlier, that it agrees exactly with `JSON` while a crashed peer sits SUSPECT, and that the whole line stays frozen a second apart.~~ Done in 0.65.0.
- ~~Per-member probe counts: each member counts its direct probes — how many this node sent and how many were answered, the probe success rate — as `probes_sent` / `probes_answered` in `JSON` (always numbers, never `null`: 0 sent for self and for a peer never probed, sent climbing with answered 0 for a peer probed but never answered), one `PROBES <id> <sent> <answered>` line per probed member after the `RTT` lines in `METRICS`, and `pulse_member_probes_sent_total` / `pulse_member_probes_answered_total` counters in `PROMETHEUS` (one labelled sample per probed member; self and a peer never probed have none) — so a peer that is probed but never answers is distinguishable from a peer never probed at all, and a peer that answers one probe in ten shows it in the rate even when its mean RTT is fine; sent counts only direct PINGs from the probe cycle (an indirect PING_REQ probe is not counted — its ACK goes to the requester and adds no RTT sample here either), answered increments with the RTT sample so it always equals the sample count, and a DEAD member keeps its counts until GC; covered by unit tests for the counting (sent moves on probe, answered moves with the RTT sample, unknown ids ignored, probed-but-never-answered stays 0 answered) and the exact canned lines (5 sent / 4 answered, 2 / 1, 0 / 0 for self and the never-heard-from peer) on all three surfaces, and an integration test that checks the live line's sent is at least answered and at least 1, that it agrees with `JSON` and the `PROMETHEUS` counters, that self has no line and the never-started peer has sent at least 1 with answered 0, that the lines ride after the `RTT` lines, and that answered stays frozen while a crashed peer sits SUSPECT.~~ Done in 0.66.0.
- ~~Probe counts in the event log: every `WATCH` `EVENT` line for a member this node has probed carries trailing ` probes_sent=<n> probes_answered=<n>` — how many direct probes this node had sent the member and how many were answered when the event was recorded, the same values `JSON` reports as `probes_sent` / `probes_answered` and `METRICS` as a `PROBES` line, frozen at the event instead of recomputed at replay time — after the ` last_seen_ms=<ms>` field when it is present; a SUSPECTED carries sent at least answered with answered at least 1 for a peer that used to answer, a DEAD carries a sent count at least as large with answered frozen, and a SUSPECTED for a peer that never answered carries sent at least 1 with answered 0, while self, a member never probed, and a member already gone from the table carry no fields, never a fabricated zero, and the persisted `<data-dir>/events` file carries the same trailing fields so the counts survive a restart and a replay; covered by unit tests for the counts round-trip through save/load (47 sent / 45 answered on a phi-mode SUSPECTED, no counts on an event recorded without them) and an integration test that checks the live `WATCH` SUSPECTED counts are sane, the DEAD sent grew while answered stayed frozen, the persisted file carries the same counts, a JOINED for a just-added member carries no probe fields, and a replay shows the never-started peer's SUSPECTED with sent at least 1, answered 0, and no last_seen_ms.~~ Done in 0.67.0.
- ~~Low-probe-rate alert: when an ALIVE member's recent probe success rate — answered / sent over its last 20 direct probes — falls below a configurable fraction (`--low-probe-rate-frac`, 0 disables it), the node logs one `LOWRATE <id> rate=0.40 threshold=0.50` line per low-rate episode, records one `LOWRATE` event on the `WATCH` stream (carrying the rate as a trailing ` probe_rate=<value>`, frozen at the crossing, persisted in `<data-dir>/events` and replayable), and reports the peer as `"low_probe_rate":true` with its `"probe_rate"` in `JSON`, a `low_probe_rate_peers` count plus a `LOWRATE <id> <rate>` line in `METRICS`, and a `pulse_low_probe_rate_peers` gauge plus a `pulse_member_probe_rate` gauge in `PROMETHEUS` — so a peer answering one probe in ten is visible while it is still ALIVE, before the suspicion fires; the rate is judged only once at least 5 probes are in the window, the threshold is a strict less-than, only direct probes count, self and a peer never probed are never low, SUSPECT/DEAD peers are not low but keep their last window rate, and the warning re-arms only when the rate recovers to the threshold or above; covered by unit tests for the window (fill, eviction at 20, stale ACKs, threshold edge, SUSPECT/DEAD not low) and the exact canned lines on all three surfaces plus the event round-trip, and an integration test that kills a peer and checks the single log line and event, the flag and rate while ALIVE, the counts and gauges, the clearing on SUSPECT, and the persisted file and replay.~~ Done in 0.68.0.
- Low-probe-rate recovery event: when a flagged member's windowed rate recovers to the threshold or above, record a `RATEOK` event on the `WATCH` stream (and log a `RATEOK` line) so a consumer watching the alert lifecycle sees the recovery too, instead of the flag silently clearing — the mirror of `LOWRATE`, completing the alert lifecycle.
