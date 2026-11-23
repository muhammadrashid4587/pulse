#ifndef PULSE_PEERFILE_H
#define PULSE_PEERFILE_H

#include "member.h"

/* Seedless bootstrap: a node reads its initial peers from a file
 * instead of --seed flags.
 *
 * The file holds one peer per line, a `host:port` id in the same
 * form as --seed (IPv4 `127.0.0.1:17101`, or bracketed IPv6
 * `[::1]:17101`). Blank lines and lines starting with `#` (after
 * leading whitespace) are ignored, so the file can carry comments
 * and spacing. Each entry is validated with member_parse_addr and
 * stored in canonical form (a bare IPv6 id formats back bracketed),
 * so a peer-file entry behaves exactly like a --seed flag: the node
 * sends it a JOIN at startup, probes it, and re-sends JOIN every
 * ~5 s while it is not ALIVE (seed rejoin).
 *
 * A peer file is meant to be shared: every node in a cluster can
 * start from the same file listing all members (including itself —
 * joining yourself is harmless), and no node needs to know which
 * peer is "the seed" or to be started in any particular order.
 */

/* Load the peer ids from path into ids (at most max_ids entries).
 * On success returns 0 and sets *count_out to the number loaded
 * (duplicates dropped silently) and *bad_out to the number of lines
 * skipped as malformed — each bad line is reported on stderr with
 * its line number. Returns -1 (with errno set) when the file cannot
 * be opened at all: a missing peer file is a startup error, not an
 * empty peer list. Lines past max_ids are counted in *bad_out. */
int pulse_peerfile_load(const char *path, char ids[][PULSE_ID_LEN],
                        int max_ids, int *count_out, int *bad_out);

#endif
