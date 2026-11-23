/* Unit tests for membership persistence (save/load of the members file). */
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "member.h"
#include "persist.h"

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

static void add_member(member_table_t *t, const char *id, uint32_t inc,
                       member_state_t state, int64_t last_seen,
                       int64_t state_since) {
    struct sockaddr_storage a = addr(id);
    member_t *m = member_add(t, id, &a, inc);
    m->state = state;
    m->last_seen_ms = last_seen;
    m->state_since_ms = state_since;
}

int main(void) {
    const char *dir = "/tmp/pulse-persist-test";
    mkdir(dir, 0777);
    int64_t now = 100000;

    /* Missing file loads zero members: the first-start case. */
    member_table_t t;
    member_table_init(&t);
    CHECK(pulse_members_load("/tmp/pulse-persist-test-nonexistent", &t,
                             "127.0.0.1:17101", now) == 0);
    CHECK(t.count == 0);

    /* Save a table with one ALIVE, one SUSPECT, one DEAD member, one
     * never-contacted seed (last_seen 0), and self. */
    member_table_init(&t);
    add_member(&t, "127.0.0.1:17101", 2, MEMBER_ALIVE, now, now); /* self */
    add_member(&t, "127.0.0.1:17102", 3, MEMBER_ALIVE, now - 12000, now - 12000);
    add_member(&t, "127.0.0.1:17103", 1, MEMBER_SUSPECT, now - 5000, now - 500);
    add_member(&t, "[::1]:17104", 5, MEMBER_DEAD, now - 30000, now - 30000);
    add_member(&t, "127.0.0.1:17105", 1, MEMBER_ALIVE, 0, 0); /* never seen */
    CHECK(pulse_members_save(dir, &t, "127.0.0.1:17101", now) == 3);
    /* Self and the never-seen seed are not saved; ALIVE, SUSPECT and
     * DEAD are. */
    FILE *f = fopen("/tmp/pulse-persist-test/members", "r");
    CHECK(f != NULL);
    char contents[1024];
    size_t nread = fread(contents, 1, sizeof contents - 1, f);
    contents[nread] = '\0';
    fclose(f);
    CHECK(strstr(contents, "127.0.0.1:17101") == NULL); /* self skipped */
    CHECK(strstr(contents, "127.0.0.1:17105") == NULL); /* never seen skipped */
    CHECK(strstr(contents, "127.0.0.1:17102 3 A 12") != NULL);
    CHECK(strstr(contents, "127.0.0.1:17103 1 S 5") != NULL);
    CHECK(strstr(contents, "[::1]:17104 5 D 30") != NULL);

    /* Load into a fresh table: everything comes back, nothing is
     * trusted blindly. */
    member_table_t t2;
    member_table_init(&t2);
    CHECK(pulse_members_load(dir, &t2, "127.0.0.1:17101", now + 60000) == 3);
    CHECK(t2.count == 3);
    member_t *m = member_find(&t2, "127.0.0.1:17102");
    CHECK(m != NULL);
    CHECK(m->incarnation == 3);
    CHECK(m->state == MEMBER_ALIVE);
    /* last_seen aged by the recorded 12 s, relative to the load time */
    CHECK(m->last_seen_ms == now + 60000 - 12000);
    /* fresh state clock: not suspected on arrival for being old */
    CHECK(m->state_since_ms == now + 60000);
    m = member_find(&t2, "127.0.0.1:17103");
    CHECK(m != NULL);
    CHECK(m->state == MEMBER_SUSPECT);
    CHECK(m->state_since_ms == now + 60000); /* fresh suspect window */
    /* A DEAD entry is NOT trusted as dead: it comes back SUSPECT, to
     * be re-probed — it may have restarted since. */
    m = member_find(&t2, "[::1]:17104");
    CHECK(m != NULL);
    CHECK(m->incarnation == 5);
    CHECK(m->state == MEMBER_SUSPECT);
    CHECK(m->state_since_ms == now + 60000);
    /* IPv6 address survived the round trip. */
    char idbuf[PULSE_ID_LEN];
    member_format_id(&m->addr, idbuf, sizeof idbuf);
    CHECK(strcmp(idbuf, "[::1]:17104") == 0);
    /* Self in the file is skipped even if present. */
    CHECK(member_find(&t2, "127.0.0.1:17101") == NULL);

    /* Malformed lines are skipped, good lines still load. */
    f = fopen("/tmp/pulse-persist-test/members", "w");
    fprintf(f, "this is not a member line\n");
    fprintf(f, "127.0.0.1:17102 3 A 7\n");
    fprintf(f, "not-an-address 1 A 0\n");
    fprintf(f, "127.0.0.1:17103 2 Q 4\n"); /* unknown code -> ALIVE */
    fprintf(f, "127.0.0.1:17106 1 A not-a-number\n");
    fprintf(f, "\n");
    fclose(f);
    member_table_t t3;
    member_table_init(&t3);
    CHECK(pulse_members_load(dir, &t3, "127.0.0.1:17101", now) == 2);
    CHECK(member_find(&t3, "127.0.0.1:17102") != NULL);
    CHECK(member_find(&t3, "127.0.0.1:17103") != NULL);
    CHECK(member_find(&t3, "127.0.0.1:17103")->state == MEMBER_ALIVE);
    CHECK(member_find(&t3, "127.0.0.1:17106") == NULL);

    /* A save with no peers writes an empty file, so a stale file can
     * never resurrect a shrunken cluster. */
    member_table_t t4;
    member_table_init(&t4);
    add_member(&t4, "127.0.0.1:17101", 2, MEMBER_ALIVE, now, now);
    CHECK(pulse_members_save(dir, &t4, "127.0.0.1:17101", now) == 0);
    member_table_t t5;
    member_table_init(&t5);
    CHECK(pulse_members_load(dir, &t5, "127.0.0.1:17101", now) == 0);
    CHECK(t5.count == 0);

    /* Ages clamp at one day: a node down for a week does not come back
     * with a last_seen in the far past. */
    member_table_t t6;
    member_table_init(&t6);
    add_member(&t6, "127.0.0.1:17102", 1, MEMBER_ALIVE, now - 999999999, now);
    CHECK(pulse_members_save(dir, &t6, "127.0.0.1:17101", now) == 1);
    member_table_t t7;
    member_table_init(&t7);
    CHECK(pulse_members_load(dir, &t7, "127.0.0.1:17101", now) == 1);
    CHECK(member_find(&t7, "127.0.0.1:17102")->last_seen_ms ==
          now - 86400 * 1000);

    /* Annotations ride along in the members file as a fifth field and
     * survive the round trip; a member with none saves "-". */
    member_table_t t8;
    member_table_init(&t8);
    add_member(&t8, "127.0.0.1:17102", 1, MEMBER_ALIVE, now - 3000, now);
    member_t *m8 = member_find(&t8, "127.0.0.1:17102");
    CHECK(member_meta_set(m8, "role", "worker") == 0);
    CHECK(member_meta_set(m8, "zone", "us-east-1") == 0);
    add_member(&t8, "127.0.0.1:17103", 1, MEMBER_ALIVE, now - 1000, now);
    CHECK(pulse_members_save(dir, &t8, "127.0.0.1:17101", now) == 2);
    f = fopen("/tmp/pulse-persist-test/members", "r");
    CHECK(f != NULL);
    nread = fread(contents, 1, sizeof contents - 1, f);
    contents[nread] = '\0';
    fclose(f);
    CHECK(strstr(contents,
                 "127.0.0.1:17102 1 A 3 role=worker,zone=us-east-1") != NULL);
    CHECK(strstr(contents, "127.0.0.1:17103 1 A 1 -") != NULL);
    member_table_t t9;
    member_table_init(&t9);
    CHECK(pulse_members_load(dir, &t9, "127.0.0.1:17101", now) == 2);
    m = member_find(&t9, "127.0.0.1:17102");
    CHECK(m != NULL);
    CHECK(m->meta_count == 2);
    CHECK(strcmp(m->meta[0].key, "role") == 0);
    CHECK(strcmp(m->meta[1].value, "us-east-1") == 0);
    m = member_find(&t9, "127.0.0.1:17103");
    CHECK(m != NULL);
    CHECK(m->meta_count == 0);
    /* A malformed annotations field is dropped; the entry still loads. */
    f = fopen("/tmp/pulse-persist-test/members", "w");
    fprintf(f, "127.0.0.1:17102 1 A 3 role=worker,bad\n");
    fclose(f);
    member_table_t t10;
    member_table_init(&t10);
    CHECK(pulse_members_load(dir, &t10, "127.0.0.1:17101", now) == 1);
    m = member_find(&t10, "127.0.0.1:17102");
    CHECK(m != NULL);
    CHECK(m->meta_count == 0);

    /* The node's own annotations: the meta file round-trips, a
     * missing file loads zero, and malformed lines are skipped. */
    meta_kv_t saved_meta[PULSE_MAX_META];
    int n_saved = 0;
    CHECK(pulse_meta_load("/tmp/pulse-persist-test-nonexistent",
                          saved_meta, PULSE_MAX_META, &n_saved) == 0);
    CHECK(n_saved == 0);
    meta_kv_t mine[2];
    snprintf(mine[0].key, sizeof mine[0].key, "role");
    snprintf(mine[0].value, sizeof mine[0].value, "worker");
    snprintf(mine[1].key, sizeof mine[1].key, "version");
    snprintf(mine[1].value, sizeof mine[1].value, "1.2.3");
    CHECK(pulse_meta_save(dir, mine, 2) == 2);
    CHECK(pulse_meta_load(dir, saved_meta, PULSE_MAX_META, &n_saved) == 2);
    CHECK(n_saved == 2);
    CHECK(strcmp(saved_meta[0].key, "role") == 0);
    CHECK(strcmp(saved_meta[1].value, "1.2.3") == 0);
    f = fopen("/tmp/pulse-persist-test/meta", "w");
    fprintf(f, "role=worker\n");
    fprintf(f, "not a pair\n");
    fprintf(f, "zone=us-east-1\n");
    fprintf(f, "role=gateway\n"); /* duplicate key: first value wins */
    fclose(f);
    CHECK(pulse_meta_load(dir, saved_meta, PULSE_MAX_META, &n_saved) == 2);
    CHECK(n_saved == 2);
    CHECK(strcmp(saved_meta[0].value, "worker") == 0);
    CHECK(strcmp(saved_meta[1].key, "zone") == 0);

    unlink("/tmp/pulse-persist-test/members");
    unlink("/tmp/pulse-persist-test/meta");
    rmdir(dir);

    printf("test_persist: %d checks, %d failures\n", checks, failures);
    return failures != 0;
}
