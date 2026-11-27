/* Unit tests for the membership table (no networking). */
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "member.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

static struct sockaddr_storage addr(const char *id) {
    struct sockaddr_storage a;
    if (member_parse_addr(id, &a) != 0) {
        memset(&a, 0, sizeof a);
    }
    return a;
}

int main(void) {
    member_table_t t;
    member_table_init(&t);
    int64_t now = 1000;

    /* add / find / address round-trip */
    struct sockaddr_storage a1 = addr("127.0.0.1:17101");
    member_t *m = member_add(&t, "127.0.0.1:17101", &a1, 1);
    CHECK(m != NULL);
    CHECK(t.count == 1);
    CHECK(member_find(&t, "127.0.0.1:17101") == m);
    CHECK(member_find(&t, "127.0.0.1:17102") == NULL);
    char idbuf[PULSE_ID_LEN];
    member_format_id(&a1, idbuf, sizeof idbuf);
    CHECK(strcmp(idbuf, "127.0.0.1:17101") == 0);
    CHECK(member_parse_addr("not-an-address", &a1) != 0);
    CHECK(member_parse_addr("127.0.0.1:99999", &a1) != 0);
    /* adding the same id twice returns the existing entry */
    CHECK(member_add(&t, "127.0.0.1:17101", &a1, 5) == m);
    CHECK(t.count == 1);
    CHECK(m->incarnation == 1); /* re-add does not bump incarnation */

    /* merge: a new member via gossip */
    struct sockaddr_storage a2 = addr("127.0.0.1:17102");
    int r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17102", &a2, 1,
                         MEMBER_ALIVE, now);
    CHECK(r & MERGE_NEW);
    CHECK(t.count == 2);

    /* merge: higher incarnation wins, even to a worse state */
    r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17102", &a2, 2,
                     MEMBER_SUSPECT, now);
    CHECK(r & MERGE_CHANGED);
    CHECK(member_find(&t, "127.0.0.1:17102")->state == MEMBER_SUSPECT);
    CHECK(member_find(&t, "127.0.0.1:17102")->incarnation == 2);

    /* merge: lower incarnation is ignored */
    r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17102", &a2, 1,
                     MEMBER_ALIVE, now);
    CHECK(r == MERGE_NONE);
    CHECK(member_find(&t, "127.0.0.1:17102")->state == MEMBER_SUSPECT);

    /* merge: same incarnation, worse state wins (SUSPECT -> DEAD) */
    r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17102", &a2, 2,
                     MEMBER_DEAD, now);
    CHECK(r & MERGE_CHANGED);
    CHECK(member_find(&t, "127.0.0.1:17102")->state == MEMBER_DEAD);

    /* merge: same incarnation, better state does NOT win (DEAD stays) */
    r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17102", &a2, 2,
                     MEMBER_ALIVE, now);
    CHECK(r == MERGE_NONE);
    CHECK(member_find(&t, "127.0.0.1:17102")->state == MEMBER_DEAD);

    /* gossip about self: SUSPECT at our incarnation must be refuted */
    r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17101", &a1, 1,
                     MEMBER_SUSPECT, now);
    CHECK(r & MERGE_SELF_SUSPECTED);
    CHECK(member_find(&t, "127.0.0.1:17101")->state == MEMBER_ALIVE);
    /* gossip about self at a lower incarnation is stale, ignore it */
    r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17101", &a1, 1,
                     MEMBER_DEAD, now);
    CHECK(r & MERGE_SELF_SUSPECTED); /* inc 1 >= our inc 1, still refutable */
    member_t *self = member_find(&t, "127.0.0.1:17101");
    self->incarnation = 5;
    r = member_merge(&t, "127.0.0.1:17101", "127.0.0.1:17101", &a1, 1,
                     MEMBER_DEAD, now);
    CHECK(r == MERGE_NONE); /* stale: inc 1 < our inc 5 */

    /* mark_suspect / mark_dead / mark_alive transitions */
    struct sockaddr_storage a3 = addr("127.0.0.1:17103");
    member_add(&t, "127.0.0.1:17103", &a3, 1);
    CHECK(member_mark_suspect(&t, "127.0.0.1:17103", now) == 1);
    CHECK(member_mark_suspect(&t, "127.0.0.1:17103", now) == 0); /* already */
    CHECK(member_mark_dead(&t, "127.0.0.1:17103", now) == 1);
    CHECK(member_mark_dead(&t, "127.0.0.1:17103", now) == 0); /* already */
    CHECK(member_mark_suspect(&t, "127.0.0.1:17103", now) == 0); /* dead stays */
    CHECK(member_mark_alive(&t, "127.0.0.1:17103", &a3, 1, now) == 1); /* revive */
    CHECK(member_find(&t, "127.0.0.1:17103")->state == MEMBER_ALIVE);
    CHECK(member_mark_alive(&t, "127.0.0.1:17103", &a3, 1, now) == 0);
    CHECK(member_mark_suspect(&t, "127.0.0.1:17999", now) == 0); /* unknown */
    CHECK(member_mark_dead(&t, "127.0.0.1:17999", now) == 0);

    /* counts and random peer selection */
    CHECK(member_count_state(&t, MEMBER_ALIVE) == 2); /* .101, .103 */
    CHECK(member_count_state(&t, MEMBER_DEAD) == 1);  /* .102 */
    CHECK(member_count_state(&t, MEMBER_SUSPECT) == 0);
    for (int i = 0; i < 20; i++) {
        member_t *p = member_random_peer(&t, "127.0.0.1:17101");
        CHECK(p != NULL);
        CHECK(strcmp(p->id, "127.0.0.1:17101") != 0); /* never self */
        CHECK(p->state != MEMBER_DEAD);               /* never dead */
    }
    /* a table with only self + dead has no peer */
    member_table_t t2;
    member_table_init(&t2);
    member_add(&t2, "127.0.0.1:17101", &a1, 1);
    member_add(&t2, "127.0.0.1:17102", &a2, 1);
    member_mark_dead(&t2, "127.0.0.1:17102", now);
    CHECK(member_random_peer(&t2, "127.0.0.1:17101") == NULL);

    /* garbage collection of DEAD entries (retention 60 s, tombstones 120 s) */
    member_table_t t3;
    member_table_init(&t3);
    struct sockaddr_storage a4 = addr("127.0.0.1:17104");
    struct sockaddr_storage a5 = addr("127.0.0.1:17105");
    struct sockaddr_storage a6 = addr("127.0.0.1:17106");
    member_add(&t3, "127.0.0.1:17104", &a4, 1);
    member_add(&t3, "127.0.0.1:17105", &a5, 1);
    member_add(&t3, "127.0.0.1:17106", &a6, 1);
    member_mark_dead(&t3, "127.0.0.1:17104", now); /* dead since 1000 */
    member_mark_suspect(&t3, "127.0.0.1:17105", now);
    char removed[8][PULSE_ID_LEN];
    /* before the retention expires, nothing is collected */
    CHECK(member_gc(&t3, now + 59999, 60000, 120000, removed, 8) == 0);
    CHECK(member_find(&t3, "127.0.0.1:17104") != NULL);
    /* at the retention boundary the DEAD entry goes, ALIVE/SUSPECT stay */
    CHECK(member_gc(&t3, now + 60000, 60000, 120000, removed, 8) == 1);
    CHECK(strcmp(removed[0], "127.0.0.1:17104") == 0);
    CHECK(member_find(&t3, "127.0.0.1:17104") == NULL);
    CHECK(t3.count == 2);
    CHECK(member_find(&t3, "127.0.0.1:17105") != NULL); /* SUSPECT kept */
    CHECK(member_find(&t3, "127.0.0.1:17106") != NULL); /* ALIVE kept */
    /* the tombstone blocks gossip from resurrecting it at the same inc */
    r = member_merge(&t3, "127.0.0.1:17106", "127.0.0.1:17104", &a4, 1,
                     MEMBER_DEAD, now + 60000);
    CHECK(r == MERGE_NONE);
    CHECK(member_find(&t3, "127.0.0.1:17104") == NULL);
    /* ... but a rejoin at a higher incarnation clears the tombstone */
    r = member_merge(&t3, "127.0.0.1:17106", "127.0.0.1:17104", &a4, 2,
                     MEMBER_ALIVE, now + 60000);
    CHECK(r & MERGE_NEW);
    CHECK(member_find(&t3, "127.0.0.1:17104")->state == MEMBER_ALIVE);
    /* direct contact revives a GC'd member even at the same incarnation */
    member_mark_dead(&t3, "127.0.0.1:17104", now + 60000);
    CHECK(member_gc(&t3, now + 120000, 60000, 120000, removed, 8) == 1);
    CHECK(member_find(&t3, "127.0.0.1:17104") == NULL);
    CHECK(member_mark_alive(&t3, "127.0.0.1:17104", &a4, 2, now + 120000) == 1);
    CHECK(member_find(&t3, "127.0.0.1:17104")->state == MEMBER_ALIVE);
    /* the tombstone itself expires: after 2x retention, stale gossip
     * can re-add the id again (nobody holds the DEAD entry by then) */
    member_mark_dead(&t3, "127.0.0.1:17104", now + 120000);
    CHECK(member_gc(&t3, now + 180000, 60000, 120000, removed, 8) == 1);
    CHECK(member_find(&t3, "127.0.0.1:17104") == NULL);
    /* tombstone recorded at 180000, expires at 180000 + 120000 */
    r = member_merge(&t3, "127.0.0.1:17106", "127.0.0.1:17104", &a4, 2,
                     MEMBER_DEAD, now + 180000);
    CHECK(r == MERGE_NONE); /* tombstone still live */
    CHECK(member_gc(&t3, now + 300000, 60000, 120000, removed, 8) == 0);
    r = member_merge(&t3, "127.0.0.1:17106", "127.0.0.1:17104", &a4, 2,
                     MEMBER_DEAD, now + 300000);
    CHECK(r & MERGE_NEW); /* tombstone expired, entry accepted again */

    /* probe RTT (0.21.0): -1 until a probe is answered, then last and
     * mean; unknown ids and negative samples are ignored, and
     * recording an RTT is not a gossip-visible change. Worst RTT
     * (0.64.0): the largest single sample so far — it only ever
     * grows, never falls when a later probe is faster. */
    member_t *m2 = member_find(&t, "127.0.0.1:17102");
    CHECK(m2->rtt_ms == -1);
    CHECK(m2->rtt_samples == 0);
    CHECK(m2->rtt_max_ms == -1);
    CHECK(member_rtt_max_ms(m2) == -1);
    uint64_t v_before = t.version;
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 3, 0) == 1);
    CHECK(m2->rtt_ms == 3);
    CHECK(m2->rtt_samples == 1);
    CHECK(m2->rtt_sum_ms == 3);
    CHECK(m2->rtt_max_ms == 3);
    CHECK(member_rtt_max_ms(m2) == 3);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 7, 0) == 1);
    CHECK(m2->rtt_ms == 7);
    CHECK(m2->rtt_samples == 2);
    CHECK(m2->rtt_sum_ms == 10);
    CHECK(m2->rtt_max_ms == 7);
    CHECK(t.version == v_before); /* RTT is local, not gossip content */
    CHECK(member_note_rtt(&t, "127.0.0.1:17999", 5, 0) == 0);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", -1, 0) == 0);
    CHECK(m2->rtt_ms == 7);

    /* slow-peer warning (0.22.0): a peer is slow when its mean RTT is
     * above the threshold; the threshold is a strict greater-than
     * (mean == threshold is not slow), 0 disables the warning, and a
     * peer with no samples is never slow. member_note_rtt returns 2
     * on the first crossing above the threshold (the caller warns
     * once per episode), 1 while already warned, and 3 on the sample
     * that clears a warned episode (0.73.0, so the caller can log
     * the recovery once too) — a second crossing then returns 2
     * again. */
    CHECK(member_rtt_mean_ms(m2) == 5);
    CHECK(member_is_slow(m2, 0) == 0);  /* disabled */
    CHECK(member_is_slow(m2, 5) == 0);  /* mean == threshold: not slow */
    CHECK(member_is_slow(m2, 4) == 1);
    CHECK(member_count_slow(&t, 4) == 1);
    CHECK(member_count_slow(&t, 5) == 0);
    CHECK(member_count_slow(&t, 0) == 0);
    /* m2's mean is 5 (samples 3, 7). A 10 ms sample pushes the mean
     * to 6, above a threshold of 5: first crossing returns 2. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 10, 5) == 2);
    CHECK(member_rtt_mean_ms(m2) == 6);
    CHECK(member_rtt_max_ms(m2) == 10);
    CHECK(member_is_slow(m2, 5) == 1);
    /* Still slow, already warned: no second warning. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 10, 5) == 1);
    CHECK(member_rtt_mean_ms(m2) == 7);
    CHECK(member_rtt_max_ms(m2) == 10);
    /* Four 0 ms samples drag the mean to (30+0)/8 = 3, back below
     * the threshold: the warning clears. The max stays 10: a fast
     * probe never erases the worst one. The first 0 leaves the
     * mean at 6, still slow; the second brings it to exactly 5 —
     * not slow, so that sample returns 3, the recovery, and the
     * two after it are ordinary samples again. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 1);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 3);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 1);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 1);
    CHECK(member_rtt_mean_ms(m2) == 3);
    CHECK(member_rtt_max_ms(m2) == 10);
    CHECK(member_is_slow(m2, 5) == 0);
    CHECK(member_count_slow(&t, 5) == 0);
    /* A fresh crossing after recovery warns again. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 100, 5) == 2);
    CHECK(member_rtt_max_ms(m2) == 100);
    CHECK(member_is_slow(m2, 5) == 1);
    /* A peer with no samples is never slow, whatever the threshold. */
    member_t *m3 = member_find(&t, "127.0.0.1:17103");
    CHECK(m3->rtt_samples == 0);
    CHECK(member_rtt_mean_ms(m3) == -1);
    CHECK(member_rtt_max_ms(m3) == -1);
    CHECK(member_is_slow(m3, 1) == 0);

    /* Per-member probe counts (0.66.0): sent and answered start at
     * 0; sent moves on member_note_probe_sent (the probe cycle's
     * PING), answered moves with each RTT sample in
     * member_note_rtt, so answered always equals rtt_samples —
     * a probe never answered adds to sent and to nothing else.
     * An unknown id is ignored, and counting is not a
     * gossip-visible change. m2 already has 9 answered samples
     * from the RTT and slow-peer blocks above (3, 7, 10, 10, 0,
     * 0, 0, 0, 100), so its answered count is 9 before this
     * block sends any. The sequence numbers are the probe
     * cycle's: each sent probe records its seq in the
     * recent-probe window (0.68.0), so the ACK can flip exactly
     * that probe's outcome — the window side effect is checked
     * in the low-probe-rate block below, on a fresh table. */
    CHECK(m2->probes_sent == 0);
    CHECK(m2->probes_answered == 9);
    CHECK(m2->probes_answered == m2->rtt_samples);
    member_note_probe_sent(&t, "127.0.0.1:17102", 101);
    member_note_probe_sent(&t, "127.0.0.1:17102", 102);
    member_note_probe_sent(&t, "127.0.0.1:17102", 103);
    CHECK(m2->probes_sent == 3);
    CHECK(m2->probes_answered == 9);
    CHECK(t.version == v_before); /* counts are local, not gossip */
    member_note_probe_sent(&t, "127.0.0.1:17999", 104); /* unknown: ignored */
    CHECK(m2->probes_sent == 3);
    /* One more answered probe moves answered (and rtt_samples)
     * together; sent is untouched by the answer. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 4, 0) == 1);
    CHECK(m2->probes_sent == 3);
    CHECK(m2->probes_answered == 10);
    CHECK(m2->rtt_samples == 10);
    /* A peer probed but never answered: sent climbs, answered
     * stays 0 — the "probed, never answered" case, not "never
     * probed". */
    CHECK(m3->probes_sent == 0);
    CHECK(m3->probes_answered == 0);
    member_note_probe_sent(&t, "127.0.0.1:17103", 201);
    member_note_probe_sent(&t, "127.0.0.1:17103", 202);
    CHECK(m3->probes_sent == 2);
    CHECK(m3->probes_answered == 0);
    CHECK(m3->rtt_samples == 0);
    CHECK(member_rtt_mean_ms(m3) == -1);

    /* suspicion age (0.58.0): -1 unless the member is SUSPECT, then
     * now minus when it went SUSPECT; the age resets when the member
     * revives, and a DEAD member reports -1 — its suspicion is over. */
    CHECK(member_suspect_age_ms(m3, now) == -1); /* ALIVE */
    CHECK(member_mark_suspect(&t, "127.0.0.1:17103", now + 500) == 1);
    CHECK(member_suspect_age_ms(m3, now + 500) == 0);
    CHECK(member_suspect_age_ms(m3, now + 800) == 300);
    CHECK(member_mark_alive(&t, "127.0.0.1:17103", &a3, 1, now + 800) == 1);
    CHECK(member_suspect_age_ms(m3, now + 800) == -1); /* revived */
    CHECK(member_suspect_age_ms(m2, now) == -1); /* DEAD */
    CHECK(member_suspect_age_ms(NULL, now) == -1);

    /* time since last contact (0.59.0): -1 for a peer never heard
     * from, then now minus last_seen once contact is recorded;
     * the age keeps growing after the peer goes DEAD — how long
     * it has been silent — and NULL is -1. */
    member_table_t t4;
    member_table_init(&t4);
    member_t *m4 = member_add(&t4, "127.0.0.1:17104", &a4, 1);
    CHECK(member_last_seen_age_ms(m4, now) == -1); /* never heard from */
    CHECK(member_mark_suspect(&t4, "127.0.0.1:17104", now + 100) == 1);
    CHECK(member_mark_alive(&t4, "127.0.0.1:17104", &a4, 1, now + 200) == 1);
    CHECK(member_last_seen_age_ms(m4, now + 200) == 0);
    CHECK(member_last_seen_age_ms(m4, now + 700) == 500);
    CHECK(member_mark_dead(&t4, "127.0.0.1:17104", now + 700) == 1);
    CHECK(member_last_seen_age_ms(m4, now + 1700) == 1500); /* DEAD, still aging */
    CHECK(member_last_seen_age_ms(NULL, now) == -1);

    /* quiet-peer warning (0.62.0): a peer is quiet when it is ALIVE
     * and its last-contact age is above the threshold; the
     * threshold is a strict greater-than (age == threshold is not
     * quiet), 0 disables the warning, a peer never heard from has
     * no age and is never quiet, and SUSPECT/DEAD peers are not
     * quiet — the suspicion already says they are silent, so the
     * warning is only for a peer still counted ALIVE. */
    member_table_t t5;
    member_table_init(&t5);
    member_t *m5 = member_add(&t5, "127.0.0.1:17105", &a4, 1);
    CHECK(member_is_quiet(m5, 300, now) == 0); /* never heard from */
    CHECK(member_count_quiet(&t5, "127.0.0.1:17101", 300, now) == 0);
    CHECK(member_mark_alive(&t5, "127.0.0.1:17105", &a4, 1, now) == 0);
    CHECK(member_is_quiet(m5, 300, now) == 0); /* just heard from */
    CHECK(member_is_quiet(m5, 0, now + 1000) == 0); /* disabled */
    CHECK(member_is_quiet(m5, 500, now + 500) == 0); /* age == threshold */
    CHECK(member_is_quiet(m5, 499, now + 500) == 1); /* age above it */
    CHECK(member_count_quiet(&t5, "127.0.0.1:17101", 499, now + 500) == 1);
    CHECK(member_count_quiet(&t5, "127.0.0.1:17101", 500, now + 500) == 0);
    /* Self is never counted as quiet (0.62.0): self's last_seen is
     * startup time, so its age is uptime, not a contact age — a
     * count that included self would report a quiet peer on every
     * node up longer than the threshold. */
    CHECK(member_count_quiet(&t5, "127.0.0.1:17105", 499, now + 500) == 0);
    /* SUSPECT and DEAD peers are not quiet, whatever their age. */
    CHECK(member_mark_suspect(&t5, "127.0.0.1:17105", now + 500) == 1);
    CHECK(member_is_quiet(m5, 499, now + 1500) == 0);
    CHECK(member_mark_dead(&t5, "127.0.0.1:17105", now + 1500) == 1);
    CHECK(member_is_quiet(m5, 499, now + 2500) == 0);
    CHECK(member_is_quiet(NULL, 499, now) == 0);
    /* Re-arm (0.72.0): direct contact does NOT clear quiet_warned
     * any more — the probe cycle owns the flag, so a peer that was
     * quiet and answers again keeps it set until the cycle sees
     * its age back below the threshold and clears it with a
     * QUIETOK line and event. Clearing it here would lose the
     * recovery: the cycle would never see a flagged peer that
     * answered again. The age still drops to 0 on contact, so the
     * peer reads not-quiet the moment it answers. */
    m5->quiet_warned = 1;
    CHECK(member_mark_alive(&t5, "127.0.0.1:17105", &a4, 1, now + 2600) == 1);
    CHECK(m5->quiet_warned == 1);
    CHECK(member_is_quiet(m5, 499, now + 2600) == 0); /* just heard from */

    /* Low-probe-rate warning (0.68.0): a peer is low when it is
     * ALIVE, at least PULSE_PROBE_WINDOW_MIN direct probes are in
     * its window, and its recent success rate — answered / sent
     * over the last PULSE_PROBE_WINDOW probes — is strictly below
     * the threshold (a rate exactly at the threshold is not low),
     * 0 disables the warning, and a peer never probed has no rate
     * and is never low. The window is matched by sequence number:
     * an ACK flips exactly its own probe's slot, a stale ACK for
     * a slot that already slid out flips nothing, and the oldest
     * outcome leaves the window once it holds 20. */
    member_table_t t6;
    member_table_init(&t6);
    member_t *m6 = member_add(&t6, "127.0.0.1:17106", &a4, 1);
    CHECK(member_probe_rate(m6) == -1); /* never probed: no rate */
    CHECK(member_is_low_probe_rate(m6, 0.5) == 0);
    CHECK(member_count_low_probe_rate(&t6, "127.0.0.1:17101", 0.5) == 0);
    /* Four probes, all answered: a rate of 1, but only four in
     * the window — below the minimum, so not judged yet. */
    for (uint32_t s = 1; s <= 4; s++) {
        member_note_probe_sent(&t6, "127.0.0.1:17106", s);
        member_note_probe_answered(&t6, "127.0.0.1:17106", s);
    }
    CHECK(m6->probe_window_count == 4);
    CHECK(member_probe_rate(m6) == 1.0);
    CHECK(member_is_low_probe_rate(m6, 0.5) == 0); /* too few to judge */
    /* A fifth probe, unanswered: 4/5 = 0.8 — judged now, and not
     * low at a 0.5 threshold. */
    member_note_probe_sent(&t6, "127.0.0.1:17106", 5);
    CHECK(member_probe_rate(m6) == 0.8);
    CHECK(member_is_low_probe_rate(m6, 0.5) == 0);
    CHECK(member_is_low_probe_rate(m6, 0.8) == 0); /* rate == threshold */
    CHECK(member_is_low_probe_rate(m6, 0.81) == 1);
    CHECK(member_is_low_probe_rate(m6, 0) == 0); /* disabled */
    /* Five more probes, none answered: 4/10 = 0.4 — low at 0.5. */
    for (uint32_t s = 6; s <= 10; s++)
        member_note_probe_sent(&t6, "127.0.0.1:17106", s);
    CHECK(member_probe_rate(m6) == 0.4);
    CHECK(member_is_low_probe_rate(m6, 0.5) == 1);
    CHECK(member_count_low_probe_rate(&t6, "127.0.0.1:17101", 0.5) == 1);
    /* Self is never counted as low (0.68.0): self never probes
     * itself, so it has no window to judge — a count that
     * included self would need a window self never fills. */
    CHECK(member_count_low_probe_rate(&t6, "127.0.0.1:17106", 0.5) == 0);
    /* An ACK for one of the unanswered probes flips exactly that
     * slot: 5/10 = 0.5 — back at the threshold, so not low. */
    member_note_probe_answered(&t6, "127.0.0.1:17106", 8);
    CHECK(member_probe_rate(m6) == 0.5);
    CHECK(member_is_low_probe_rate(m6, 0.5) == 0);
    /* Ten more unanswered probes fill the window to 20: the five
     * answered probes are still in it, so the rate is 5/20. Four
     * more unanswered probes slide the oldest outcomes out — the
     * four answered probes (seqs 1-4) leave one at a time, so the
     * rate falls to 1/20 (only seq 8 still answered). */
    for (uint32_t s = 11; s <= 20; s++)
        member_note_probe_sent(&t6, "127.0.0.1:17106", s);
    CHECK(m6->probe_window_count == 20);
    CHECK(member_probe_rate(m6) == 0.25);
    CHECK(member_is_low_probe_rate(m6, 0.5) == 1);
    for (uint32_t s = 21; s <= 24; s++)
        member_note_probe_sent(&t6, "127.0.0.1:17106", s);
    CHECK(m6->probe_window_count == 20);
    CHECK(member_probe_rate(m6) == 0.05);
    /* A stale ACK for a probe whose slot already slid out (seq 1)
     * flips nothing: the rate stays 0.05, and the answer is not
     * counted twice. */
    member_note_probe_answered(&t6, "127.0.0.1:17106", 1);
    CHECK(member_probe_rate(m6) == 0.05);
    /* An answered probe for an unknown id, and a duplicate answer
     * for a slot already flipped, are both ignored. */
    member_note_probe_answered(&t6, "127.0.0.1:17999", 8);
    CHECK(member_probe_rate(m6) == 0.05);
    member_note_probe_answered(&t6, "127.0.0.1:17106", 8);
    CHECK(member_probe_rate(m6) == 0.05);
    /* SUSPECT and DEAD peers are not low, whatever their rate:
     * the suspicion already says they are silent. */
    CHECK(member_mark_suspect(&t6, "127.0.0.1:17106", now) == 1);
    CHECK(member_is_low_probe_rate(m6, 0.5) == 0);
    CHECK(member_count_low_probe_rate(&t6, "127.0.0.1:17101", 0.5) == 0);
    /* ...but the rate itself is still measurable: a DEAD member
     * keeps its last window rate until GC, like its RTT. */
    CHECK(member_mark_dead(&t6, "127.0.0.1:17106", now) == 1);
    CHECK(member_probe_rate(m6) == 0.05);
    CHECK(member_probe_rate(NULL) == -1);
    CHECK(member_is_low_probe_rate(NULL, 0.5) == 0);

    /* Probe-rate history (0.71.0): the last 16 sampled windowed
     * rates as [[age_ms, rate], ...] pairs, oldest first — the
     * same shape as phi_history. A peer never probed has no rate
     * to sample, so sampling records nothing; a sample in the
     * same millisecond as the last one is skipped, so the ring
     * holds distinct times; and once 16 samples are in, the
     * oldest slides out, so the ring always holds the most
     * recent windowed rates. */
    member_table_t t7;
    member_table_init(&t7);
    member_t *m7 = member_add(&t7, "127.0.0.1:17107", &a4, 1);
    char rhbuf[512];
    CHECK(member_probe_rate_history_json(m7, now, rhbuf, sizeof rhbuf) == 2);
    CHECK(strcmp(rhbuf, "[]") == 0);
    member_sample_probe_rate(m7, now + 100);
    CHECK(member_probe_rate_history_json(m7, now, rhbuf, sizeof rhbuf) == 2);
    CHECK(strcmp(rhbuf, "[]") == 0); /* never probed: no rate to sample */
    /* Four probes, all answered: rate 1.00, sampled at +100. */
    for (uint32_t s = 1; s <= 4; s++) {
        member_note_probe_sent(&t7, "127.0.0.1:17107", s);
        member_note_probe_answered(&t7, "127.0.0.1:17107", s);
    }
    member_sample_probe_rate(m7, now + 100);
    CHECK(m7->probe_rate_hist_count == 1);
    CHECK(member_probe_rate_history_json(m7, now, rhbuf, sizeof rhbuf) > 2);
    CHECK(strcmp(rhbuf, "[[100,1.00]]") == 0);
    /* Same-millisecond sample: skipped, not duplicated. */
    member_sample_probe_rate(m7, now + 100);
    CHECK(m7->probe_rate_hist_count == 1);
    /* A fifth probe, unanswered: 4/5 = 0.80, sampled at +200. */
    member_note_probe_sent(&t7, "127.0.0.1:17107", 5);
    member_sample_probe_rate(m7, now + 200);
    CHECK(m7->probe_rate_hist_count == 2);
    CHECK(strcmp(rhbuf, "[[100,1.00]]") == 0); /* rhbuf not re-written */
    CHECK(member_probe_rate_history_json(m7, now, rhbuf, sizeof rhbuf) > 2);
    CHECK(strcmp(rhbuf, "[[100,1.00],[200,0.80]]") == 0);
    /* Twenty more samples, one per 100 ms, each at the current
     * rate (still 0.80 — no new probes): 22 samples in all, so
     * the ring holds the last 16 — the six earliest (100..600)
     * slide out and the oldest kept is the one at +700. */
    for (int k = 0; k < 20; k++)
        member_sample_probe_rate(m7, now + 300 + k * 100);
    CHECK(m7->probe_rate_hist_count == 16);
    CHECK(member_probe_rate_history_json(m7, now, rhbuf, sizeof rhbuf) > 2);
    CHECK(strstr(rhbuf, "[700,0.80]") == rhbuf + 1); /* oldest first */
    CHECK(strstr(rhbuf, "[100,1.00]") == NULL);      /* slid out */
    CHECK(strstr(rhbuf, "[200,0.80]") == NULL);      /* slid out */
    CHECK(strstr(rhbuf, "[600,0.80]") == NULL);      /* slid out */
    CHECK(strstr(rhbuf, "[2200,0.80]") != NULL);     /* newest kept */
    /* A buffer too small for even one pair, and a NULL member,
     * both fail instead of writing a truncated array. */
    CHECK(member_probe_rate_history_json(m7, now, rhbuf, 4) == -1);
    CHECK(member_probe_rate_history_json(NULL, now, rhbuf, sizeof rhbuf) == -1);
    member_sample_probe_rate(NULL, now + 3000); /* no crash */

    /* Probe-rate history stats and histogram (0.74.0): the
     * latest and minimum of the sampled rates, and how many
     * samples the ring holds — the minimum, not the maximum,
     * because the rate fails downward — plus cumulative bucket
     * counts over the same samples. A fresh member has no
     * samples: latest/min 0.0, count 0, an all-zero histogram
     * with sum 0.0; a NULL member reports the same instead of
     * crashing. */
    member_table_t t8;
    member_table_init(&t8);
    member_t *m8 = member_add(&t8, "127.0.0.1:17108", &a4, 1);
    double latest, mn, hsum;
    int n, hcounts[PULSE_PROBE_RATE_HIST_NBOUNDS + 1], hcount;
    member_probe_rate_history_stats(m8, &latest, &mn, &n);
    CHECK(latest == 0.0 && mn == 0.0 && n == 0);
    member_probe_rate_history_hist(m8, (const double[]){0.5, 0.75, 0.9,
                                                         0.95, 1.0},
                                   PULSE_PROBE_RATE_HIST_NBOUNDS, hcounts,
                                   &hsum, &hcount);
    CHECK(hcount == 0 && hsum == 0.0 && hcounts[0] == 0 &&
          hcounts[PULSE_PROBE_RATE_HIST_NBOUNDS] == 0);
    member_probe_rate_history_stats(NULL, &latest, &mn, &n);
    CHECK(latest == 0.0 && mn == 0.0 && n == 0);
    member_probe_rate_history_hist(NULL, (const double[]){0.5, 0.75, 0.9,
                                                           0.95, 1.0},
                                   PULSE_PROBE_RATE_HIST_NBOUNDS, hcounts,
                                   &hsum, &hcount);
    CHECK(hcount == 0 && hsum == 0.0 &&
          hcounts[PULSE_PROBE_RATE_HIST_NBOUNDS] == 0);
    /* Five probes: answer three (0.60), sample; answer a fourth
     * (0.80), sample; answer the fifth (1.00), sample — so the
     * latest is the recovery to 1.00 while the minimum is the
     * 0.60 dip, and the histogram holds one sample in each of
     * the 0.75, 0.9 and 1.0 buckets. */
    for (uint32_t s = 1; s <= 5; s++)
        member_note_probe_sent(&t8, "127.0.0.1:17108", s);
    member_note_probe_answered(&t8, "127.0.0.1:17108", 1);
    member_note_probe_answered(&t8, "127.0.0.1:17108", 2);
    member_note_probe_answered(&t8, "127.0.0.1:17108", 3);
    member_sample_probe_rate(m8, now + 100);
    member_note_probe_answered(&t8, "127.0.0.1:17108", 4);
    member_sample_probe_rate(m8, now + 200);
    member_note_probe_answered(&t8, "127.0.0.1:17108", 5);
    member_sample_probe_rate(m8, now + 300);
    member_probe_rate_history_stats(m8, &latest, &mn, &n);
    CHECK(n == 3);
    CHECK(fabs(latest - 1.00) < 1e-9);
    CHECK(fabs(mn - 0.60) < 1e-9);
    static const double rbounds[] = PULSE_PROBE_RATE_HIST_BOUNDS;
    member_probe_rate_history_hist(m8, rbounds,
                                   PULSE_PROBE_RATE_HIST_NBOUNDS, hcounts,
                                   &hsum, &hcount);
    CHECK(hcount == 3);
    CHECK(fabs(hsum - 2.40) < 1e-9);
    CHECK(hcounts[0] == 0); /* le 0.5: none */
    CHECK(hcounts[1] == 1); /* le 0.75: the 0.60 sample */
    CHECK(hcounts[2] == 2); /* le 0.9: 0.60 and 0.80 */
    CHECK(hcounts[3] == 2); /* le 0.95: same two */
    CHECK(hcounts[4] == 3); /* le 1.0: all three */
    CHECK(hcounts[5] == 3); /* +Inf: every sample */
    /* A sample exactly at a bound lands in that bucket: 3 of 4
     * answered is exactly 0.75, so it counts under le 0.75 and
     * not under le 0.5. */
    member_table_t t9;
    member_table_init(&t9);
    member_t *m9 = member_add(&t9, "127.0.0.1:17109", &a4, 1);
    for (uint32_t s = 1; s <= 4; s++)
        member_note_probe_sent(&t9, "127.0.0.1:17109", s);
    member_note_probe_answered(&t9, "127.0.0.1:17109", 1);
    member_note_probe_answered(&t9, "127.0.0.1:17109", 2);
    member_note_probe_answered(&t9, "127.0.0.1:17109", 3);
    member_sample_probe_rate(m9, now + 100);
    member_probe_rate_history_stats(m9, &latest, &mn, &n);
    CHECK(n == 1);
    CHECK(fabs(latest - 0.75) < 1e-9);
    CHECK(fabs(mn - 0.75) < 1e-9);
    member_probe_rate_history_hist(m9, rbounds,
                                   PULSE_PROBE_RATE_HIST_NBOUNDS, hcounts,
                                   &hsum, &hcount);
    CHECK(hcount == 1);
    CHECK(hcounts[0] == 0);
    CHECK(hcounts[1] == 1);
    CHECK(hcounts[5] == 1);
    /* The wrapped ring (0.71.0's m7, 16 samples all 0.80): the
     * stats read the last 16, not the 22 taken, and the
     * histogram buckets all 16 under le 0.9. */
    member_probe_rate_history_stats(m7, &latest, &mn, &n);
    CHECK(n == 16);
    CHECK(fabs(latest - 0.80) < 1e-9);
    CHECK(fabs(mn - 0.80) < 1e-9);
    member_probe_rate_history_hist(m7, rbounds,
                                   PULSE_PROBE_RATE_HIST_NBOUNDS, hcounts,
                                   &hsum, &hcount);
    CHECK(hcount == 16);
    CHECK(fabs(hsum - 12.80) < 1e-9);
    CHECK(hcounts[0] == 0);
    CHECK(hcounts[1] == 0);
    CHECK(hcounts[2] == 16);
    CHECK(hcounts[5] == 16);

    /* Flap counter (0.75.0): one completed ALIVE -> SUSPECT ->
     * ALIVE cycle (a refuted suspicion) or ALIVE -> SUSPECT ->
     * DEAD (the peer died after being suspected) is one flap,
     * and the recent count is how many completed inside the
     * last 10 minutes. A suspicion alone is not a flap yet —
     * the cycle only completes when the member leaves SUSPECT —
     * and a direct ALIVE -> DEAD with no suspicion in between
     * is not one either. */
    member_table_t tf;
    member_table_init(&tf);
    member_t *mf = member_add(&tf, "127.0.0.1:17110", &a4, 1);
    CHECK(mf->flap_count == 0);
    CHECK(member_flaps_recent(mf, now) == 0);
    CHECK(member_flaps_recent(NULL, now) == 0);
    CHECK(member_mark_suspect(&tf, "127.0.0.1:17110", now + 100) == 1);
    CHECK(mf->flap_count == 0); /* suspected, not yet flapped */
    CHECK(member_flaps_recent(mf, now + 100) == 0);
    CHECK(member_mark_alive(&tf, "127.0.0.1:17110", &a4, 1, now + 200) == 1);
    CHECK(mf->flap_count == 1);
    CHECK(member_flaps_recent(mf, now + 200) == 1);
    /* A second suspicion that ends in death is a second flap. */
    CHECK(member_mark_suspect(&tf, "127.0.0.1:17110", now + 300) == 1);
    CHECK(member_mark_dead(&tf, "127.0.0.1:17110", now + 400) == 1);
    CHECK(mf->flap_count == 2);
    CHECK(member_flaps_recent(mf, now + 400) == 2);
    /* A restart is not a flap: the DEAD -> ALIVE leg is a new
     * life, and the SUSPECT -> DEAD leg already counted the
     * cycle when the peer died — and a direct ALIVE -> DEAD
     * after the restart counts nothing either. */
    CHECK(member_mark_alive(&tf, "127.0.0.1:17110", &a4, 2, now + 500) == 1);
    CHECK(mf->flap_count == 2);
    CHECK(member_mark_dead(&tf, "127.0.0.1:17110", now + 600) == 1);
    CHECK(mf->flap_count == 2);
    /* A third cycle, back through SUSPECT and ALIVE. */
    CHECK(member_mark_alive(&tf, "127.0.0.1:17110", &a4, 3, now + 700) == 1);
    CHECK(member_mark_suspect(&tf, "127.0.0.1:17110", now + 800) == 1);
    CHECK(member_mark_alive(&tf, "127.0.0.1:17110", &a4, 3, now + 900) == 1);
    CHECK(mf->flap_count == 3);
    /* The 10-minute window: the three flaps completed at +200,
     * +400 and +900, so at exactly one window after the first
     * all three still count, one millisecond later the first
     * has aged out, and a window after the last none do. */
    CHECK(member_flaps_recent(mf, now + 200 + PULSE_FLAP_WINDOW_MS) == 3);
    CHECK(member_flaps_recent(mf, now + 201 + PULSE_FLAP_WINDOW_MS) == 2);
    CHECK(member_flaps_recent(mf, now + 901 + PULSE_FLAP_WINDOW_MS) == 0);
    /* The ring holds the last 16 flap times: 20 more cycles in
     * quick succession push the lifetime count to 23, but the
     * recent count saturates at 16 — the honest limit of the
     * bounded ring, documented on the surfaces. */
    for (int k = 0; k < 20; k++) {
        member_mark_suspect(&tf, "127.0.0.1:17110", now + 1000 + k * 10);
        member_mark_alive(&tf, "127.0.0.1:17110", &a4, 3,
                          now + 1005 + k * 10);
    }
    CHECK(mf->flap_count == 23);
    CHECK(mf->flap_hist_count == 16);
    CHECK(member_flaps_recent(mf, now + 1200) == 16);
    /* Gossip-learned transitions count the same way (0.75.0):
     * a member that arrives SUSPECT from gossip — never ALIVE
     * on this node — and is then reported ALIVE is not a flap,
     * since the cycle never started here; but once it is ALIVE
     * here, a gossip-learned SUSPECT -> ALIVE and a
     * gossip-learned SUSPECT -> DEAD each count one. */
    member_table_t tg;
    member_table_init(&tg);
    CHECK(member_merge(&tg, "127.0.0.1:17101", "127.0.0.1:17111", &a4,
                       1, MEMBER_SUSPECT, now) == MERGE_NEW);
    member_t *mg = member_find(&tg, "127.0.0.1:17111");
    CHECK(mg != NULL);
    CHECK(mg->flap_count == 0);
    CHECK(member_merge(&tg, "127.0.0.1:17101", "127.0.0.1:17111", &a4,
                       2, MEMBER_ALIVE, now + 100) == MERGE_CHANGED);
    CHECK(mg->flap_count == 0);
    CHECK(member_merge(&tg, "127.0.0.1:17101", "127.0.0.1:17111", &a4,
                       2, MEMBER_SUSPECT, now + 200) == MERGE_CHANGED);
    CHECK(member_merge(&tg, "127.0.0.1:17101", "127.0.0.1:17111", &a4,
                       3, MEMBER_ALIVE, now + 300) == MERGE_CHANGED);
    CHECK(mg->flap_count == 1);
    CHECK(member_flaps_recent(mg, now + 300) == 1);
    CHECK(member_merge(&tg, "127.0.0.1:17101", "127.0.0.1:17111", &a4,
                       3, MEMBER_SUSPECT, now + 400) == MERGE_CHANGED);
    CHECK(member_merge(&tg, "127.0.0.1:17101", "127.0.0.1:17111", &a4,
                       3, MEMBER_DEAD, now + 500) == MERGE_CHANGED);
    CHECK(mg->flap_count == 2);
    CHECK(member_flaps_recent(mg, now + 500) == 2);

    /* Flap-rate warning (0.76.0): a member counts as flap-rate
     * when it is ALIVE and at least threshold of its completed
     * flap cycles fall inside the last 10 minutes — the count
     * is the same member_flaps_recent the surfaces report, so
     * the predicate and the surfaces agree. The comparison is
     * at or above: a threshold of 3 fires on the third flap,
     * not the fourth. A disabled threshold (0 or negative)
     * never counts, a peer that never flapped never counts, and
     * a SUSPECT or DEAD peer does not count either — the
     * warning is for a peer still counted ALIVE. The count only
     * falls as flaps age out of the window, so a peer at the
     * threshold stops counting a window after its earliest flap
     * — that is the recovery the probe cycle turns into a
     * FLAPOK. The table count excludes self: self never
     * suspects itself, so it has no flaps to judge. */
    member_table_t tr;
    member_table_init(&tr);
    member_t *mr = member_add(&tr, "127.0.0.1:17112", &a4, 1);
    CHECK(member_is_flap_rate(NULL, 1, now) == 0);
    CHECK(member_is_flap_rate(mr, 0, now + 100) == 0);
    CHECK(member_is_flap_rate(mr, -1, now + 100) == 0);
    CHECK(member_is_flap_rate(mr, 1, now + 100) == 0);
    /* Three cycles: SUSPECT -> ALIVE, SUSPECT -> ALIVE,
     * SUSPECT -> ALIVE, completing at +200, +400 and +600. */
    CHECK(member_mark_suspect(&tr, "127.0.0.1:17112", now + 100) == 1);
    CHECK(member_mark_alive(&tr, "127.0.0.1:17112", &a4, 1,
                            now + 200) == 1);
    CHECK(member_mark_suspect(&tr, "127.0.0.1:17112", now + 300) == 1);
    CHECK(member_mark_alive(&tr, "127.0.0.1:17112", &a4, 1,
                            now + 400) == 1);
    CHECK(member_mark_suspect(&tr, "127.0.0.1:17112", now + 500) == 1);
    CHECK(member_mark_alive(&tr, "127.0.0.1:17112", &a4, 1,
                            now + 600) == 1);
    CHECK(mr->flap_count == 3);
    CHECK(member_is_flap_rate(mr, 1, now + 700) == 1);
    CHECK(member_is_flap_rate(mr, 3, now + 700) == 1);
    CHECK(member_is_flap_rate(mr, 4, now + 700) == 0);
    /* A SUSPECT peer does not count, even at the threshold. */
    CHECK(member_mark_suspect(&tr, "127.0.0.1:17112", now + 800) == 1);
    CHECK(member_is_flap_rate(mr, 1, now + 800) == 0);
    /* A DEAD peer does not count either — and the suspicion
     * that just ended in death is a fourth flap. */
    CHECK(member_mark_dead(&tr, "127.0.0.1:17112", now + 900) == 1);
    CHECK(mr->flap_count == 4);
    CHECK(member_is_flap_rate(mr, 1, now + 900) == 0);
    /* The recovery: revive the peer first — a DEAD peer never
     * counts, so the ageing has to be judged on an ALIVE one.
     * A window after the earliest flap it ages out, so 3 of
     * the 4 still count — at the threshold, but below 4 — and
     * a window after the last, none do. */
    CHECK(member_mark_alive(&tr, "127.0.0.1:17112", &a4, 2,
                            now + 1000) == 1);
    CHECK(member_is_flap_rate(mr, 4,
                              now + 201 + PULSE_FLAP_WINDOW_MS) == 0);
    CHECK(member_is_flap_rate(mr, 3,
                              now + 201 + PULSE_FLAP_WINDOW_MS) == 1);
    CHECK(member_is_flap_rate(mr, 1,
                              now + 901 + PULSE_FLAP_WINDOW_MS) == 0);
    /* The table count: mr is ALIVE again with all four flaps
     * recent, so it counts at a threshold of 4 and not at 5 —
     * and the count is 0 with the warning disabled, and 0 when
     * the member itself is passed as self. */
    CHECK(member_count_flap_rate(&tr, "127.0.0.1:17101", 4,
                                 now + 1000) == 1);
    CHECK(member_count_flap_rate(&tr, "127.0.0.1:17101", 5,
                                 now + 1000) == 0);
    CHECK(member_count_flap_rate(&tr, "127.0.0.1:17101", 0,
                                 now + 1000) == 0);
    CHECK(member_count_flap_rate(&tr, "127.0.0.1:17112", 4,
                                 now + 1000) == 0);

    /* gossip fanout: 1 + floor(log2(peers)), capped at 4 */
    CHECK(member_gossip_fanout(0) == 1);
    CHECK(member_gossip_fanout(1) == 1);
    CHECK(member_gossip_fanout(2) == 2);
    CHECK(member_gossip_fanout(3) == 2);
    CHECK(member_gossip_fanout(4) == 3);
    CHECK(member_gossip_fanout(7) == 3);
    CHECK(member_gossip_fanout(8) == 4);
    CHECK(member_gossip_fanout(16) == 4);
    CHECK(member_gossip_fanout(63) == 4);

    printf("test_member: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
