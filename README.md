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
  picture as one machine-readable JSON object (`JSON`), or
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
| `--data-dir` | `.` | Directory holding the `incarnation` and `members` state files |
| `--probe-interval-ms` | 100 | How often to probe a random peer |
| `--ack-timeout-ms` | 100 | How long to wait for an ACK before an indirect probe |
| `--suspect-timeout-ms` | 300 | How long a SUSPECT lasts before becoming DEAD |
| `--dead-retention` | 60 | Seconds a DEAD entry stays in the table before GC |
| `--phi-accrual` | off | Suspect on phi instead of the fixed ack timeout |
| `--phi-threshold` | 8.0 | Phi value at which a peer is suspected (phi mode only) |
| `--sync-interval-ms` | 10000 | How often to exchange full tables (anti-entropy SYNC) with a random peer |
| `--control-port` | — (disabled) | TCP port for the control plane (`MEMBERS`, `STATUS`, `LEAVE`) |
| `--secret` / `--shared-secret` | — (none) | Shared secret for joins: only peers presenting the same secret are accepted |
| `--ipv6` | off | Run over IPv6: bind `::`, ids are `[::1]:port`, control plane on `::1` |
| `--bind HOST` | — (wildcard) | Bind address override, a literal IP: decides the family and the default id host (`--bind ::1` is IPv6, `--bind 127.0.0.1` is IPv4) |

Each node logs timestamped events to stdout: `JOIN`, `SUSPECT`, `DEAD`,
`GC` (a DEAD entry removed after its retention expired), `ALIVE`,
`REFUTE`, `LEAVE` (this node announced its clean shutdown to its
peers), `SYNC <id>` (a periodic full-table exchange was sent to that
peer), and a `STATUS alive=N suspect=N dead=N total=N` line
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
# MEMBER 127.0.0.1:17101 1 ALIVE -
# MEMBER 127.0.0.1:17102 1 ALIVE 0.42
# END

./pulse-ctl 127.0.0.1:18401 STATUS
# STATUS id=127.0.0.1:17101 uptime=42 alive=2 suspect=0 dead=0 total=2
# END

./pulse-ctl 127.0.0.1:18401 LEAVE 127.0.0.1:17102
# OK 127.0.0.1:17102

./pulse-ctl 127.0.0.1:18401 METRICS
# METRICS suspected=1 dead=1 refutes=0 joins=2 detect_latency_ms mean=203 max=203 samples=1 suspect_to_dead_ms mean=301 max=301 samples=1
# END

./pulse-ctl 127.0.0.1:18401 JSON
# {"id":"127.0.0.1:17101","uptime":42,"alive":2,"suspect":0,"dead":0,"total":2,"members":[{"id":"127.0.0.1:17101","incarnation":1,"state":"ALIVE","phi":null},{"id":"127.0.0.1:17102","incarnation":1,"state":"ALIVE","phi":0.42}],"metrics":{"suspected":1,"dead":1,"refutes":0,"joins":2,"detect_latency_ms":{"mean":203,"max":203,"samples":1},"suspect_to_dead_ms":{"mean":301,"max":301,"samples":1}}}
# END
```

Or with netcat, if it is installed:

```sh
printf 'MEMBERS\n' | nc 127.0.0.1 18401
```

Commands:

| Command | Reply |
| ------- | ----- |
| `MEMBERS` | One `MEMBER <id> <incarnation> <ALIVE\|SUSPECT\|DEAD> <phi\|->` line per member, then `END`. Phi is the peer's current phi (two decimals) when phi-accrual is on and the peer has heartbeat history; `-` otherwise, including self. |
| `STATUS` | One `STATUS id=<self> uptime=<seconds> alive=N suspect=N dead=N total=N` line, then `END`. |
| `LEAVE <member-id>` | `OK <id>`: the member is dropped from the table and a tombstone is recorded, which is gossiped as DEAD so peers mark it DEAD and GC it on their own retention. A live member comes back — direct contact is proof of life, and gossip refutation bumps its incarnation — so `LEAVE` cannot declare a healthy node dead. Unknown member: `ERR unknown member <id>`; self: `ERR cannot leave self`; missing id: `ERR usage: LEAVE <member-id>`. |
| `METRICS` | One `METRICS suspected=N dead=N refutes=N joins=N detect_latency_ms mean=X max=Y samples=N suspect_to_dead_ms mean=X max=Y samples=N` line, then `END`. `detect_latency_ms` is last-direct-contact to SUSPECT for suspicions this node detected itself; `suspect_to_dead_ms` is SUSPECT to DEAD. Mean/max are `-` when there are no samples. Transitions learned from gossip bump the counts but add no latency sample, so samples can be smaller than the counts. |
| `JSON` | `STATUS`, `MEMBERS`, and `METRICS` combined into one machine-readable JSON object on a single line, then `END`. Member phi is a number when phi-accrual is on and the peer has heartbeat history, `null` otherwise (including self). Latency means are `null` when there are no samples; max is `0` then. Built for tooling that polls cluster state without scraping the text protocol. |
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

## Tests

```sh
make test
```

- `tests/test_member.c`: unit tests for the membership table — merge
  precedence, incarnation rules, refutation, state transitions, peer
  selection.
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
  struct formats with zero counts and `-` latencies, locally detected
  suspicions feed mean/max detection latency, gossip-learned
  transitions bump the counts without adding a latency sample, and
  refutes/joins are plain counters.
- `tests/test_control.sh`: integration test for the TCP control plane —
  `MEMBERS` lists the node itself, `STATUS` reports id and totals,
  `METRICS` reports zero counts and no samples on a fresh node and
  the detected kill with measured latencies after a peer dies, `JSON`
  returns one line of valid JSON with the node itself as the only
  member and zeroed metrics on a fresh node, and after a kill it lists
  the dead member with state `DEAD` and numeric latency means,
  unknown commands and bad `LEAVE` usage get clean `ERR` lines, a
  second node appears in `MEMBERS` after joining, `LEAVE` removes it
  from the listing immediately, the live node comes back via direct
  contact, and `LEAVE` of a dead node removes it for good.
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
  empty file, and ages clamp at one day.
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
