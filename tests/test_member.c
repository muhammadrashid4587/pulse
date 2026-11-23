/* Unit tests for the membership table (no networking). */
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
     * recording an RTT is not a gossip-visible change */
    member_t *m2 = member_find(&t, "127.0.0.1:17102");
    CHECK(m2->rtt_ms == -1);
    CHECK(m2->rtt_samples == 0);
    uint64_t v_before = t.version;
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 3, 0) == 1);
    CHECK(m2->rtt_ms == 3);
    CHECK(m2->rtt_samples == 1);
    CHECK(m2->rtt_sum_ms == 3);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 7, 0) == 1);
    CHECK(m2->rtt_ms == 7);
    CHECK(m2->rtt_samples == 2);
    CHECK(m2->rtt_sum_ms == 10);
    CHECK(t.version == v_before); /* RTT is local, not gossip content */
    CHECK(member_note_rtt(&t, "127.0.0.1:17999", 5, 0) == 0);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", -1, 0) == 0);
    CHECK(m2->rtt_ms == 7);

    /* slow-peer warning (0.22.0): a peer is slow when its mean RTT is
     * above the threshold; the threshold is a strict greater-than
     * (mean == threshold is not slow), 0 disables the warning, and a
     * peer with no samples is never slow. member_note_rtt returns 2
     * on the first crossing above the threshold (the caller warns
     * once per episode), 1 while already warned, and 1 again once
     * the mean recovers — a second crossing then returns 2 again. */
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
    CHECK(member_is_slow(m2, 5) == 1);
    /* Still slow, already warned: no second warning. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 10, 5) == 1);
    CHECK(member_rtt_mean_ms(m2) == 7);
    /* Four 0 ms samples drag the mean to (30+0)/8 = 3, back below
     * the threshold: the warning clears. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 1);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 1);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 1);
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 0, 5) == 1);
    CHECK(member_rtt_mean_ms(m2) == 3);
    CHECK(member_is_slow(m2, 5) == 0);
    CHECK(member_count_slow(&t, 5) == 0);
    /* A fresh crossing after recovery warns again. */
    CHECK(member_note_rtt(&t, "127.0.0.1:17102", 100, 5) == 2);
    CHECK(member_is_slow(m2, 5) == 1);
    /* A peer with no samples is never slow, whatever the threshold. */
    member_t *m3 = member_find(&t, "127.0.0.1:17103");
    CHECK(m3->rtt_samples == 0);
    CHECK(member_rtt_mean_ms(m3) == -1);
    CHECK(member_is_slow(m3, 1) == 0);

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
