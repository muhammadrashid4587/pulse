#ifndef PULSE_CONTROL_H
#define PULSE_CONTROL_H

#include <stdint.h>

#include "member.h"

/* TCP control plane for pulse-node: a tiny line-based protocol on a
 * separate TCP port, one command per connection, plain text in and out
 * (netcat-friendly). Commands:
 *
 *   MEMBERS
 *       One line per member, then END:
 *       MEMBER <id> <incarnation> <ALIVE|SUSPECT|DEAD> <phi|'-'>
 *       phi is the peer's current phi value (two decimals) when
 *       phi-accrual is on and the peer has heartbeat history, '-'
 *       otherwise (including self, which has no phi history).
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
 *   anything else
 *       ERR unknown command
 *
 * The server is deliberately dumb: it knows nothing about the node's
 * threads or locks. All state access goes through the hooks, which the
 * node implements (snapshot copies the table under the node's lock;
 * force_leave removes under the lock). One connection is served at a
 * time; the server is a control interface, not a data path.
 */

typedef struct {
    void *ctx;
    /* Copy the membership table into out (hook locks as needed). */
    void (*snapshot)(void *ctx, member_table_t *out);
    /* Force-remove a member. Returns 1 if removed, 0 if the member is
     * unknown, -1 if the id is this node itself. */
    int (*force_leave)(void *ctx, const char *id);
    /* 1 when phi-accrual suspicion is on (phi values are meaningful). */
    int (*phi_enabled)(void *ctx);
    const char *self_id;
    int64_t start_ms; /* monotonic ms when the node started */
} control_hooks_t;

/* Create, bind and listen on a TCP socket for the control plane.
 * Returns the listen fd, or -1 on error (with errno set). */
int control_listen(int port);

/* Serve connections on fd until accept() fails (e.g. the fd is shut
 * down from another thread). Returns when the server stops. */
void control_serve(int fd, const control_hooks_t *hooks);

#endif
