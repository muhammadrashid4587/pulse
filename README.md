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

## What it is not

- Not a service mesh, not a consensus protocol, not encrypted, not
  authenticated. Any process on the network can join and any member can
  claim another is dead. It detects crashes on a trusted LAN; it does not
  defend against a malicious network.
- Not a replacement for Raft or Paxos. It answers one question — "is that
  node still there?" — and leaves agreement to something else.
- Dead members are garbage-collected after a retention period (default
  60 s, `--dead-retention`): the entry is dropped, a tombstone stops
  gossip from resurrecting it, and a rejoining node comes back at a
  higher incarnation. Removals are not sent on the wire — each node
  GCs on its own retention timer, and tombstones (kept twice as long)
  stop a peer that still holds the DEAD entry from re-adding it here.

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

## Roadmap

- ~~Garbage collection of old DEAD entries.~~ Done in 0.2.0.
- Phi-accrual failure detection as an alternative to fixed timeouts.
- A TCP control plane: query membership, force-leave a member.
- Optional shared secret so only trusted devices can join.
- IPv6.
