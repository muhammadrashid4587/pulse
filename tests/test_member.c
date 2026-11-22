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

static struct sockaddr_in addr(const char *id) {
    struct sockaddr_in a;
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
    struct sockaddr_in a1 = addr("127.0.0.1:17101");
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
    struct sockaddr_in a2 = addr("127.0.0.1:17102");
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
    struct sockaddr_in a3 = addr("127.0.0.1:17103");
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

    printf("test_member: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
