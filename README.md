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
  table with a random peer, so news of a dead node spreads in O(log n)
  rounds.
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
  (`STATUS`), or force-removing a member (`LEAVE <id>`). See
  "Control plane" below. Query it with the bundled `pulse-ctl` client
  or plain netcat.
- Optional shared secret (`--secret`): nodes started with the same
  secret only accept traffic from peers presenting it, so two clusters
  on the same network cannot accidentally merge. See "Shared secret"
  below. Nodes without a secret behave exactly as before.

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
```

State codes in gossip entries: `A` alive, `S` suspect, `D` dead.
Member ids are `host:port`, so a gossip entry is self-contained.

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
again — that is the refutation.

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
| `--data-dir` | `.` | Directory holding the `incarnation` state file |
| `--probe-interval-ms` | 100 | How often to probe a random peer |
| `--ack-timeout-ms` | 100 | How long to wait for an ACK before an indirect probe |
| `--suspect-timeout-ms` | 300 | How long a SUSPECT lasts before becoming DEAD |
| `--dead-retention` | 60 | Seconds a DEAD entry stays in the table before GC |
| `--phi-accrual` | off | Suspect on phi instead of the fixed ack timeout |
| `--phi-threshold` | 8.0 | Phi value at which a peer is suspected (phi mode only) |
| `--control-port` | — (disabled) | TCP port for the control plane (`MEMBERS`, `STATUS`, `LEAVE`) |
| `--secret` / `--shared-secret` | — (none) | Shared secret for joins: only peers presenting the same secret are accepted |

Each node logs timestamped events to stdout: `JOIN`, `SUSPECT`, `DEAD`,
`GC` (a DEAD entry removed after its retention expired), `ALIVE`,
`REFUTE`, and a `STATUS alive=N suspect=N dead=N total=N` line
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
- `tests/test_control.sh`: integration test for the TCP control plane —
  `MEMBERS` lists the node itself, `STATUS` reports id and totals,
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

## Roadmap

- ~~Garbage collection of old DEAD entries.~~ Done in 0.2.0.
- ~~Phi-accrual failure detection as an alternative to fixed timeouts.~~ Done in 0.3.0.
- ~~A TCP control plane: query membership, force-leave a member.~~ Done in 0.4.0.
- ~~Optional shared secret so only trusted devices can join.~~ Done in 0.5.0.
- IPv6.
