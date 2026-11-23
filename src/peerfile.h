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

/* Live peer-file reload (0.24.0): set difference between the peer list
 * a node already has (old_ids, old_n) and the list just read from the
 * file (new_ids, new_n). added[] gets the ids in new but not old,
 * removed[] the ids in old but not new; *n_added_out / *n_removed_out
 * are their counts. Both output arrays must hold at least max(old_n,
 * new_n) entries. Duplicates inside one list collapse to a single
 * entry. Pure set arithmetic: it says nothing about whether a removed
 * peer may actually be dropped — the caller decides that from the
 * membership table (a peer that is still ALIVE is kept, never dropped
 * on the strength of a file edit alone). */
void pulse_peerfile_diff(char old_ids[][PULSE_ID_LEN], int old_n,
                         char new_ids[][PULSE_ID_LEN], int new_n,
                         char added[][PULSE_ID_LEN], int *n_added_out,
                         char removed[][PULSE_ID_LEN], int *n_removed_out);

#endif
