#ifndef PULSE_PERSIST_H
#define PULSE_PERSIST_H

#include <stdint.h>

#include "member.h"

/* Membership persistence: a node remembers its peers across restart.
 *
 * The data dir holds a `members` file, one line per known member:
 *
 *     <id> <incarnation> <state-code> <age-seconds>
 *
 * e.g. `127.0.0.1:17102 3 A 12` — member 127.0.0.1:17102 at incarnation
 * 3, last seen ALIVE 12 seconds ago. State codes are the gossip codes:
 * A alive, S suspect, D dead. The file is written atomically (tmp file
 * + rename) so a crash mid-write never leaves a half-written file.
 *
 * Nothing in the file is trusted. Every restored entry is re-probed
 * through the normal failure-detection path, and a restored entry that
 * stays silent goes SUSPECT then DEAD again like any other peer:
 *
 *   - ALIVE entries come back ALIVE, with last_seen aged by the
 *     recorded age, so the prober picks them up immediately.
 *   - SUSPECT and DEAD entries come back SUSPECT with a fresh clock,
 *     so they get a full suspect window of probing before the sweep
 *     can declare them DEAD — a peer that was DEAD when we shut down
 *     may have restarted since, and a peer that was SUSPECT may be
 *     fine now. Neither verdict survives the restart.
 *   - Entries the node never actually heard from (last_seen == 0,
 *     e.g. a seed that never answered) are not saved at all: there is
 *     nothing to remember about them, and the seed-rejoin retry
 *     already covers seeds.
 *   - Self is never saved: self is re-added at startup with a fresh
 *     (higher) incarnation.
 *
 * Malformed lines are skipped, not fatal: a hand-edited or truncated
 * file must never stop a node from starting. A missing file loads
 * zero members, which is the first-start case.
 */

/* Save the table to <data-dir>/members. Returns the number of members
 * written, or -1 if the file could not be written. */
int pulse_members_save(const char *data_dir, const member_table_t *t,
                       const char *self_id, int64_t now_ms);
/* Load <data-dir>/members into the table (fresh at startup). Returns
 * the number of members restored, 0 when there is no file, or -1 if
 * the file exists but could not be read at all. */
int pulse_members_load(const char *data_dir, member_table_t *t,
                       const char *self_id, int64_t now_ms);

#endif
