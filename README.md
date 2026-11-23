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
  picture as one machine-readable JSON object (`JSON`), setting an
  annotation on the node (`META <key=value>`), triggering a live
  peer-file reload (`RELOAD`), or
  force-removing a member (`LEAVE <id>`). See
  "Control plane" below. Query it with the bundled `pulse-ctl` client
  or plain netcat.
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
  whole 16-entry window every round. A converged cluster sends no
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
  same reload over TCP. See "Live peer-file reload" below.

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
| `--control-port` | — (disabled) | TCP port for the control plane (`MEMBERS`, `STATUS`, `LEAVE`, `RELOAD`) |
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
file — see "Live peer-file reload"), `PEERFILE ADD <id>` /
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
./pulse-node --port 17101 --control-port 18401 --data-dir /tmp/p1

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

./pulse-ctl 127.0.0.1:18401 METRICS
# METRICS suspected=1 dead=1 refutes=0 joins=2 adaptive_fast_cycles=3 suspect_refuted=0 detect_latency_ms mean=203 max=203 samples=1 hist=[0,0,1,0,0] suspect_to_dead_ms mean=301 max=301 samples=1 refute_latency_ms mean=- max=- samples=0 slow_peers=0
# END

./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101","uptime":42,"alive":2,"suspect":0,"dead":0,"total":2,"members":[{"id":"127.0.0.1:17101","incarnation":1,"state":"ALIVE","phi":null,"meta":{"role":"worker","zone":"us-east-1"},"rtt_ms":null,"rtt_mean_ms":null,"slow":false},{"id":"127.0.0.1:17102","incarnation":1,"state":"ALIVE","phi":0.42,"meta":{},"rtt_ms":1,"rtt_mean_ms":0,"slow":false}],"metrics":{"suspected":1,"dead":1,"refutes":0,"joins":2,"adaptive_fast_cycles":3,"suspect_refuted":0,"slow_peers":0,"detect_latency_ms":{"mean":203,"max":203,"samples":1,"hist":[0,0,1,0,0]},"suspect_to_dead_ms":{"mean":301,"max":301,"samples":1},"refute_latency_ms":{"mean":null,"max":0,"samples":0}}}
# END
```

Or with netcat, if it is installed:

```sh
printf 'MEMBERS\n' | nc 127.0.0.1 18401
```

Commands:

| Command | Reply |
| ------- | ----- |
| `MEMBERS` | One `MEMBER <id> <incarnation> <ALIVE\|SUSPECT\|DEAD> <phi\|-> <annotations\|->` line per member, then `END`. Phi is the peer's current phi (two decimals) when phi-accrual is on and the peer has heartbeat history; `-` otherwise, including self. Annotations are the member's comma-separated `key=value` list, `-` when it carries none. |
| `STATUS` | One `STATUS id=<self> uptime=<seconds> alive=N suspect=N dead=N total=N` line, then `END`. |
| `META <key=value>` | Set one annotation on this node itself: `OK <key=value>`. The change is gossiped with the next digest and persisted to the data dir, so it survives a restart. Bad syntax: `ERR invalid meta <arg>`; a ninth distinct key: `ERR meta full (max 8)`; missing argument: `ERR usage: META <key=value>`. |
| `LEAVE <member-id>` | `OK <id>`: the member is dropped from the table and a tombstone is recorded, which is gossiped as DEAD so peers mark it DEAD and GC it on their own retention. A live member comes back — direct contact is proof of life, and gossip refutation bumps its incarnation — so `LEAVE` cannot declare a healthy node dead. Unknown member: `ERR unknown member <id>`; self: `ERR cannot leave self`; missing id: `ERR usage: LEAVE <member-id>`. |
| `RELOAD` | Re-read `--peer-file` and merge it into the seed list — the same reload SIGHUP triggers, for an operator who cannot send the node a signal: `OK peers=N added=A dropped=D kept=K bad=B`. `peers` is how many peers the file lists; `added`/`dropped`/`kept` are the merge outcome (a still-ALIVE peer removed from the file is kept, never dropped on a file edit alone); `bad` is how many lines were skipped as malformed. No `--peer-file`: `ERR no peer file`. Unreadable file: `ERR cannot read peer file <path>: <reason>`, and the old list is kept. |
| `METRICS` | One `METRICS suspected=N dead=N refutes=N joins=N adaptive_fast_cycles=N suspect_refuted=N detect_latency_ms mean=X max=Y samples=N hist=[a,b,c,d,e] suspect_to_dead_ms mean=X max=Y samples=N refute_latency_ms mean=X max=Y samples=N slow_peers=N` line, then `END`. `detect_latency_ms` is last-direct-contact to SUSPECT for suspicions this node detected itself; `suspect_to_dead_ms` is SUSPECT to DEAD. Mean/max are `-` when there are no samples. `hist` is the detection-latency histogram: counts of locally measured detection latencies in the buckets <100 ms, 100–249, 250–499, 500–999, ≥1000 ms, so the mean cannot hide a slow tail. Transitions learned from gossip bump the counts but add no latency sample, so samples can be smaller than the counts. `suspect_refuted` is SUSPECT to ALIVE transitions — suspicions that turned out wrong — and `refute_latency_ms` is how long those peers sat SUSPECT before coming back (mean/max/samples, `-` with no samples); a refutation learned from gossip bumps the count without a sample, and a DEAD peer coming back is a restart, not a refutation. `slow_peers` is how many members currently count as slow — mean probe RTT above `--slow-peer-rtt-ms` (see "Slow-peer warning"); 0 when the warning is disabled. |
| `JSON` | `STATUS`, `MEMBERS`, and `METRICS` combined into one machine-readable JSON object on a single line, then `END`. Member phi is a number when phi-accrual is on and the peer has heartbeat history, `null` otherwise (including self). Latency means are `null` when there are no samples; max is `0` then. The detection-latency histogram is `detect_latency_ms.hist`, an array of the same five bucket counts. Each member also carries a `meta` object with its annotations (`{}` when it has none), and `rtt_ms` / `rtt_mean_ms` with its probe round-trip times (see "Probe RTT" below; both `null` until a probe has been answered, and always `null` for self), plus `slow` — `true` when the peer's mean RTT is above `--slow-peer-rtt-ms`, `false` otherwise (see "Slow-peer warning"). The metrics object also carries `slow_peers`, the count of currently slow members. Built for tooling that polls cluster state without scraping the text protocol. |
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

A digest carries at most 16 entries, but the table holds up to 64
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
  histogram) on a fresh node, and after a kill it lists
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
- `tests/test_control.c`: unit tests for the control-plane dispatch
  (0.25.0) — fake hooks serve canned state and the test drives the
  real `control_listen` / `control_serve` over loopback: `RELOAD`
  answers `OK peers=3 added=1 dropped=0 kept=0 bad=0` on success,
  `ERR no peer file` with no peer file, and `ERR cannot read peer
  file ...` for an unreadable file; a node with no reload hook
  answers `ERR reload not supported` instead of crashing on a NULL
  hook; and `MEMBERS` on an empty table is just `END`, with an
  unknown command still a clean `ERR unknown command`.
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
