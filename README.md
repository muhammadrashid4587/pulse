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
  `JOIN` to the seed every ~5 s (logged as `REJOIN`) until the seed
  answers, so a late-starting seed still merges the cluster instead
  of leaving the node isolated forever. See "Seed rejoin" below.
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
- Slow-peer warning: when a peer's mean probe RTT rises above a
  configurable threshold (`--slow-peer-rtt-ms`), the node logs a
  `SLOW` line once per episode and reports the peer as `"slow":true`
  in `JSON` (with a `slow_peers` count in `METRICS` and `JSON`), so
  a congested link or a struggling device is visible while the peer
  is still ALIVE, instead of only after it is suspected. See
  "Slow-peer warning" below.
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
  member, state and source, the last and mean probe RTT per member
  as gauges, and the peer-file gauges when the node runs with
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
| `--ack-timeout-ms` | 100 | How long to wait for an ACK before an indirect probe |
| `--slow-peer-rtt-ms` | 0 (disabled) | Warn when a peer's mean probe RTT is above this many ms (see "Slow-peer warning") |
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
"Slow-peer warning"), `SYNC <id>` (a periodic full-table exchange was sent to that
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
# METRICS suspected=1 dead=1 refutes=0 joins=2 adaptive_fast_cycles=3 suspect_refuted=0 detect_latency_ms mean=203 max=203 samples=1 hist=[0,0,1,0,0] suspect_to_dead_ms mean=301 max=301 samples=1 refute_latency_ms mean=- max=- samples=0 slow_peers=0 peer_file=peers last_reload=1795450000 last_mtime=1795449900 pf_peers=3 pf_added=1 pf_dropped=0 pf_kept=0 pf_bad=0 watch_subscribers=0 watch_sessions=0 watch_delivered=0 watch_filtered=0 watch_dropped=0 watch_paused=0
# END

./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101","uptime":42,"alive":2,"suspect":0,"dead":0,"total":2,"members":[{"id":"127.0.0.1:17101","incarnation":1,"state":"ALIVE","phi":null,"meta":{"role":"worker","zone":"us-east-1"},"rtt_ms":null,"rtt_mean_ms":null,"slow":false,"source":"self"},{"id":"127.0.0.1:17102","incarnation":1,"state":"ALIVE","phi":0.42,"meta":{},"rtt_ms":1,"rtt_mean_ms":0,"slow":false,"source":"peer-file"}],"metrics":{"suspected":1,"dead":1,"refutes":0,"joins":2,"adaptive_fast_cycles":3,"suspect_refuted":0,"slow_peers":0,"detect_latency_ms":{"mean":203,"max":203,"samples":1,"hist":[0,0,1,0,0]},"suspect_to_dead_ms":{"mean":301,"max":301,"samples":1},"refute_latency_ms":{"mean":null,"max":0,"samples":0}},"peer_file":{"path":"peers","last_reload":1795450000,"last_mtime":1795449900,"peers":3,"added":1,"dropped":0,"kept":0,"bad":0},"watch":{"subscribers":0,"sessions":0,"delivered":0,"filtered":0,"dropped":0,"paused":0,"streams":[]}}
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
| `METRICS` | One `METRICS suspected=N dead=N refutes=N joins=N adaptive_fast_cycles=N suspect_refuted=N detect_latency_ms mean=X max=Y samples=N hist=[a,b,c,d,e] suspect_to_dead_ms mean=X max=Y samples=N refute_latency_ms mean=X max=Y samples=N slow_peers=N peer_file=<path\|-> last_reload=<unix\|-> last_mtime=<unix\|-> pf_peers=<N\|-> pf_added=<N\|-> pf_dropped=<N\|-> pf_kept=<N\|-> pf_bad=<N\|-> watch_subscribers=<N> watch_sessions=<N> watch_delivered=<N> watch_filtered=<N> watch_dropped=<N> watch_event_log_cap=<N> gossip_digest_size=<N>` line, then `END`. `detect_latency_ms` is last-direct-contact to SUSPECT for suspicions this node detected itself; `suspect_to_dead_ms` is SUSPECT to DEAD. Mean/max are `-` when there are no samples. `hist` is the detection-latency histogram: counts of locally measured detection latencies in the buckets <100 ms, 100–249, 250–499, 500–999, ≥1000 ms, so the mean cannot hide a slow tail. Transitions learned from gossip bump the counts but add no latency sample, so samples can be smaller than the counts. `suspect_refuted` is SUSPECT to ALIVE transitions — suspicions that turned out wrong — and `refute_latency_ms` is how long those peers sat SUSPECT before coming back (mean/max/samples, `-` with no samples); a refutation learned from gossip bumps the count without a sample, and a DEAD peer coming back is a restart, not a refutation. `slow_peers` is how many members currently count as slow — mean probe RTT above `--slow-peer-rtt-ms` (see "Slow-peer warning"); 0 when the warning is disabled. The peer-file fields (0.28.0) are the `--peer-file` state — the path, the wall-clock time of the last successful load or reload, the file's mtime at that load, and the counts from that load (the same numbers as the `PEERFILE RELOAD` log line) — and are all `-` when the node runs without `--peer-file`; see "Peer-file state in METRICS and JSON". `watch_subscribers` / `watch_sessions` (0.33.0) are how many `WATCH` streams are connected right now and how many sessions the node has served since startup; see "WATCH subscriber count in METRICS and JSON". `watch_delivered` / `watch_filtered` / `watch_dropped` (0.34.0) are the lifetime totals of EVENT lines streams sent, events streams consumed but skipped because their filter excluded them, and events streams lost to a gap in the bounded event ring; see "WATCH drop and delivery counts in METRICS and JSON". `watch_paused` (0.37.0) is how many `WATCH` streams are paused right now — a paused stream is still connected and still counts as a subscriber, it just is not consuming events; see "WATCH backpressure: pausing a stream". `watch_event_log_cap` (0.36.0) is how many events the `WATCH` event ring holds — the `--event-log-cap` the node started with, 128 by default — so an operator can see how far back a replay can reach. `gossip_digest_size` (0.41.0) is how many members one gossip round's digest covers — the `--gossip-digest-size` the node started with, 16 by default — so an operator can see whether the node is trading convergence speed for bandwidth. |
| `JSON` | `STATUS`, `MEMBERS`, and `METRICS` combined into one machine-readable JSON object on a single line, then `END`. Member phi is a number when phi-accrual is on and the peer has heartbeat history, `null` otherwise (including self). Latency means are `null` when there are no samples; max is `0` then. The detection-latency histogram is `detect_latency_ms.hist`, an array of the same five bucket counts. Each member also carries a `meta` object with its annotations (`{}` when it has none), and `rtt_ms` / `rtt_mean_ms` with its probe round-trip times (see "Probe RTT" below; both `null` until a probe has been answered, and always `null` for self), plus `slow` — `true` when the peer's mean RTT is above `--slow-peer-rtt-ms`, `false` otherwise (see "Slow-peer warning") — and `source`, where the peer came from (`seed`, `peer-file`, `discovered`, or `self`, the same provenance `PEERS` reports). The metrics object also carries `slow_peers`, the count of currently slow members. A top-level `peer_file` object carries the `--peer-file` state (path, `last_reload` / `last_mtime` as unix seconds, and the counts from the last load), or `null` when the node runs without `--peer-file` (0.28.0). A top-level `watch` object carries `subscribers` (how many `WATCH` streams are connected right now) and `sessions` (how many `WATCH` sessions the node has served since startup) (0.33.0), plus the delivery / drop counts (0.34.0): `delivered` (EVENT lines sent in total), `filtered` (events skipped by a stream's filter), `dropped` (events lost to a gap in the event ring), `paused` (0.37.0, how many `WATCH` streams are paused right now), `event_log_cap` (0.36.0, how many events the `WATCH` ring holds — the `--event-log-cap` the node started with, 128 by default), and a `streams` array with the same three counts per currently-connected stream, keyed by its `session` number, plus `paused` per stream. A top-level `gossip_digest_size` (0.41.0) carries how many members one gossip round's digest covers — the `--gossip-digest-size` the node started with, 16 by default; see "WATCH subscriber count in METRICS and JSON", "WATCH drop and delivery counts in METRICS and JSON", and "WATCH backpressure: pausing a stream". Built for tooling that polls cluster state without scraping the text protocol. |
| `PROMETHEUS` | The same values `METRICS` and `JSON` report, as Prometheus text exposition format, then `END`. Each metric family carries a `# HELP` and a `# TYPE` line: membership counts (`pulse_members_alive`, `pulse_members_suspect`, `pulse_members_dead`, `pulse_members_total`), `pulse_slow_peers`, `pulse_uptime_seconds` and `pulse_watch_subscribers` as gauges; the lifetime transition totals (`pulse_suspected_total`, `pulse_dead_total`, `pulse_refutes_total`, `pulse_joins_total`, `pulse_adaptive_fast_cycles_total`, `pulse_suspect_refuted_total`, `pulse_watch_sessions_total`, `pulse_watch_delivered_total`, `pulse_watch_filtered_total`, `pulse_watch_dropped_total`) as counters; the event ring capacity (`pulse_watch_event_log_cap`, 0.36.0) and the paused-stream count (`pulse_watch_paused`, 0.37.0) as gauges; the gossip digest size (`pulse_gossip_digest_size`, 0.41.0) as a gauge; the three latency groups as `mean` / `max` / `samples` gauges (`pulse_detect_latency_ms_mean`, `pulse_suspect_to_dead_ms_mean`, `pulse_refute_latency_ms_mean`, and the matching `max` and `samples` — mean and max are 0 with no samples, since Prometheus has no null, and the `samples` gauge says whether the mean is real); the detection latency also as a real histogram (`pulse_detect_latency_ms_bucket{le="100"|"250"|"500"|"1000"|"+Inf"}`, plus `_sum` and `_count`, the same five buckets `METRICS` reports as `hist`). Each member rides as a `pulse_member_info{member="<id>",state="<ALIVE\|SUSPECT\|DEAD>",source="<seed\|peer-file\|discovered\|self>"} 1` gauge, so a scraper can alert on a member's state without polling `JSON`. The last and mean direct-probe round-trip time per member ride as `pulse_member_rtt_ms{member="<id>"}` and `pulse_member_rtt_mean_ms{member="<id>"}` gauges (0.40.0), one line per member that has answered a probe — self and a never-heard-from peer have no line, and a dead member keeps its last RTT. The peer-file gauges (`pulse_peer_file_peers`, `pulse_peer_file_added`, `pulse_peer_file_dropped`, `pulse_peer_file_kept`, `pulse_peer_file_bad`, `pulse_peer_file_last_reload_seconds`, `pulse_peer_file_last_mtime_seconds`) are only emitted when the node runs with `--peer-file`: an absent metric is how Prometheus says "not applicable". This is the exposition format, not an HTTP endpoint — scrape it with `pulse-ctl`, or point a scraper at a tiny exporter that forwards the command. See "Prometheus exposition format (PROMETHEUS)" below. |
| `WATCH [types] [ids] [STATE <list>] [FROM <seq>]` | A long-lived stream: `OK watching`, then one `SNAPSHOT <id> <incarnation> <ALIVE\|SUSPECT\|DEAD> <last-seen-ms\|-> <seed\|peer-file\|discovered\|self>` line per member (the `PEERS` shape), then `END`, then one `EVENT <seq> <JOINED\|SUSPECTED\|DEAD\|REVIVED\|DROPPED\|REFUTED> <id> <incarnation> <unix-seconds>` line per membership event as it happens, until the client closes the connection. Sequence numbers are 1-based and monotonic for the node's life; a slow consumer that falls more than the event log holds gets `ERR gap: events lost, reconnect` and must reconnect and re-read the snapshot. Runs on its own thread, so other commands keep answering while a client watches. An optional filter (0.30.0) narrows the stream server-side: a comma-separated type list (`WATCH JOINED,DEAD`), a comma-separated peer-id list (`WATCH 127.0.0.1:17102`), or both, in any order — the snapshot is narrowed by the id list too, a filtered-out event still consumes its sequence number so a filtered stream's numbers jump by design, and a bad filter is refused up front with `ERR invalid watch filter <reason>`. A state filter (0.39.0) narrows the stream by the member state an event leaves its member in (`WATCH STATE SUSPECT,DEAD`): JOINED, REVIVED and REFUTED land on ALIVE, SUSPECTED on SUSPECT, DEAD on DEAD, and DROPPED lands on no state at all so it never matches a state filter — the snapshot is narrowed by the member's current state too, and `STATE` composes with the type/id filters and `FROM`, in any order; a bad state is refused up front with `ERR invalid watch filter unknown member state <name>`. Replay / resume (0.31.0): `WATCH FROM <seq>` replays the buffered events from that seq, in order, after the snapshot and `END` and before live events, so a reconnecting client catches up instead of starting from now — `FROM 0` replays from the oldest buffered event, a `FROM` past the latest starts live, and a `FROM` below the oldest buffered event replays from the oldest instead, after a `GAP <requested> <oldest>` line; `FROM` composes with the type/id filters, in any order, and a bad `FROM` is refused up front. Heartbeat (0.32.0): a stream that has sent nothing for 5 s sends `HEARTBEAT <unix-seconds>`, so a client on a quiet cluster can tell "nothing happened" from "the connection died silently" — it carries no sequence number and is sent regardless of the filter, and any line sent resets the 5 s clock. Backpressure / pause (0.37.0): a slow consumer can stop the flow without closing the connection — `WATCH PAUSE <session>` on a separate connection pauses the stream with that session number (the number `JSON` reports per stream), and `WATCH RESUME <session>` resumes it; a paused stream stops consuming events, and on resume it replays what the ring still holds, or ends with `ERR gap` when the ring has scrolled past the resume point. Both are idempotent; a session that is not a live stream answers `ERR unknown watch session <n>`. Event-log persistence (0.38.0): the event log is saved to `<data-dir>/events` as events are recorded and loaded back on startup, so sequence numbers continue across a restart instead of resetting to 1 and a `WATCH FROM <seq>` after a restart can still replay pre-restart events. See "WATCH backpressure: pausing a stream" and "Event-log persistence across restarts" below. |
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

So every ~5 s the node re-sends `JOIN` to each seed that is not
currently ALIVE in its membership table (logged as `REJOIN <id>`
when the seed had already been garbage-collected). When the seed
comes up it answers the `JOIN` with a full-table SYNC, both sides
learn about each other, and the cluster merges. The seed is
deliberately not re-added to the table by the retry itself: it comes
back through the normal path (direct contact re-adds it), so a seed
that never answers — wrong secret, wrong family, genuinely dead —
stays garbage-collected instead of cycling back into the table.
A seed that never comes up costs one UDP datagram per retry interval
and nothing else; a seed of the other address family is skipped, and
a seed that is already ALIVE is never re-joined.

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
a `JOIN` at startup, probes it, and re-sends `JOIN` every ~5 s
while it is not ALIVE, so a peer that starts late still merges.
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
it joins the seed list (so the ~5 s rejoin retry covers it while it
is down), is added to the membership table, and gets a `JOIN` at
once, so it merges as soon as it starts answering.

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
the sequence numbers knows it missed events.

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

The cycle-based cadences inside the probe cycle — the ~5 s seed
rejoin, the anti-entropy SYNC, the gossip round every other cycle —
are counted in cycles, so they speed up in wall time along with the
loop while the cluster is unstable. That is the point: a node that
missed many digests heals faster, and a suspect is swept to DEAD
sooner, exactly when the cluster needs it. The timeouts themselves
do not change — `--ack-timeout-ms` and `--suspect-timeout-ms` are
still wall-clock durations — so the fast interval only makes the
checks that enforce them run more often, it never redefines them.

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
That measurement is now kept per peer — the last RTT in ms and the
mean over every probe that peer has answered — and reported per
member in `JSON` as `rtt_ms` / `rtt_mean_ms`:

```sh
./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101",...,"members":[{"id":"127.0.0.1:17101",...,"rtt_ms":null,"rtt_mean_ms":null},{"id":"127.0.0.1:17102",...,"rtt_ms":1,"rtt_mean_ms":0}],...}
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
  out, and is fed by `member_mark_alive`.
- `tests/test_phi.sh`: integration test for phi-accrual — a 3-node
  cluster in `--phi-accrual` mode converges, a killed node is SUSPECTED
  with a phi value and then marked DEAD by both survivors, and a
  restarted node comes back ALIVE.
- `tests/test_metrics.c`: unit tests for the metrics module — a fresh
  struct formats with zero counts and `-` latencies and an all-zero
  histogram, locally detected suspicions feed mean/max detection latency
  and bucket into the histogram (<100, 100–249, 250–499, 500–999,
  ≥1000 ms, with boundary samples landing in the higher bucket),
  gossip-learned transitions bump the counts without adding a latency
  sample or a histogram count, and refutes/joins are plain counters.
- `tests/test_adaptive.c`: unit tests for the adaptive probe interval
  — the default fast interval is half the base floored at 25 ms, no
  suspects means the base interval whatever the fast setting, suspects
  mean the configured fast interval (or the default when it is 0),
  the fast interval is clamped to the 25 ms floor and never slower
  than base, a base below the floor has nothing to speed up, and a
  nonsense base falls back to the floor instead of spinning.
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
  nodes converge on alive=2.
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
  thread, and the gap when the ring overflows past 128 events), and
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
  totals depend on thread-exit timing.
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
  `pulse_peer_file_*` metric at all. The exact counter, gauge and
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
