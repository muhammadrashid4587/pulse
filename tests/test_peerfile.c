/* Unit tests for seedless bootstrap: loading the peer list file. */
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "member.h"
#include "peerfile.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f) { printf("FAIL: cannot write %s\n", path); failures++; return; }
    fputs(content, f);
    fclose(f);
}

int main(void) {
    char ids[16][PULSE_ID_LEN];
    int count = -1, bad = -1;
    const char *path = "/tmp/pulse-peerfile-test.txt";

    /* Missing file is an error, not an empty list. */
    unlink("/tmp/pulse-peerfile-test-missing.txt");
    CHECK(pulse_peerfile_load("/tmp/pulse-peerfile-test-missing.txt",
                              ids, 16, &count, &bad) == -1);

    /* Comments, blank lines, valid IPv4 and bracketed IPv6, bad lines
     * skipped, duplicates dropped, a bare IPv6 id canonicalized. */
    write_file(path,
               "# ward cluster peers\n"
               "\n"
               "127.0.0.1:17601\n"
               "   \n"
               "127.0.0.1:17602  \n"
               "not-a-peer\n"
               "[::1]:17603\n"
               "127.0.0.1:17601\n"   /* duplicate: dropped, not bad */
               "127.0.0.1:notaport\n"
               "127.0.0.1\n"
               "::1:17604\n"         /* bare IPv6: loads bracketed */
               "  # indented comment\n"
               "256.0.0.1:17605\n");
    CHECK(pulse_peerfile_load(path, ids, 16, &count, &bad) == 0);
    CHECK(count == 4);
    CHECK(bad == 4);
    CHECK(strcmp(ids[0], "127.0.0.1:17601") == 0);
    CHECK(strcmp(ids[1], "127.0.0.1:17602") == 0);
    CHECK(strcmp(ids[2], "[::1]:17603") == 0);
    CHECK(strcmp(ids[3], "[::1]:17604") == 0);

    /* An empty file loads zero peers and is not an error: a node
     * with an empty peer file starts alone, like one with no seeds. */
    write_file(path, "# nothing here yet\n\n");
    CHECK(pulse_peerfile_load(path, ids, 16, &count, &bad) == 0);
    CHECK(count == 0);
    CHECK(bad == 0);

    /* CRLF line endings work. */
    write_file(path, "127.0.0.1:17601\r\n127.0.0.1:17602\r\n");
    CHECK(pulse_peerfile_load(path, ids, 16, &count, &bad) == 0);
    CHECK(count == 2);
    CHECK(bad == 0);

    /* The cap counts overflow as bad, not as loaded. */
    write_file(path, "127.0.0.1:17601\n127.0.0.1:17602\n127.0.0.1:17603\n");
    CHECK(pulse_peerfile_load(path, ids, 2, &count, &bad) == 0);
    CHECK(count == 2);
    CHECK(bad == 1);

    /* Live reload diff (0.24.0): added is in new but not old,
     * removed is in old but not new, unchanged peers are in
     * neither. */
    char old_ids[16][PULSE_ID_LEN], new_ids[16][PULSE_ID_LEN];
    char added[16][PULSE_ID_LEN], removed[16][PULSE_ID_LEN];
    int n_added = -1, n_removed = -1;
    snprintf(old_ids[0], PULSE_ID_LEN, "127.0.0.1:17601");
    snprintf(old_ids[1], PULSE_ID_LEN, "127.0.0.1:17602");
    snprintf(old_ids[2], PULSE_ID_LEN, "127.0.0.1:17603");
    snprintf(new_ids[0], PULSE_ID_LEN, "127.0.0.1:17602");
    snprintf(new_ids[1], PULSE_ID_LEN, "127.0.0.1:17603");
    snprintf(new_ids[2], PULSE_ID_LEN, "127.0.0.1:17604");
    pulse_peerfile_diff(old_ids, 3, new_ids, 3, added, &n_added,
                        removed, &n_removed);
    CHECK(n_added == 1);
    CHECK(n_removed == 1);
    CHECK(strcmp(added[0], "127.0.0.1:17604") == 0);
    CHECK(strcmp(removed[0], "127.0.0.1:17601") == 0);
    /* Identical lists: nothing added, nothing removed. */
    pulse_peerfile_diff(old_ids, 3, old_ids, 3, added, &n_added,
                        removed, &n_removed);
    CHECK(n_added == 0);
    CHECK(n_removed == 0);
    /* Empty old list: everything is added. Empty new list:
     * everything is removed. */
    pulse_peerfile_diff(old_ids, 0, new_ids, 3, added, &n_added,
                        removed, &n_removed);
    CHECK(n_added == 3);
    CHECK(n_removed == 0);
    pulse_peerfile_diff(old_ids, 3, new_ids, 0, added, &n_added,
                        removed, &n_removed);
    CHECK(n_added == 0);
    CHECK(n_removed == 3);
    /* Duplicates inside one list collapse to a single entry. */
    snprintf(new_ids[3], PULSE_ID_LEN, "127.0.0.1:17604");
    pulse_peerfile_diff(old_ids, 3, new_ids, 4, added, &n_added,
                        removed, &n_removed);
    CHECK(n_added == 1);
    CHECK(strcmp(added[0], "127.0.0.1:17604") == 0);

    unlink(path);
    printf("test_peerfile: %d checks, %d failures\n", checks, failures);
    return failures != 0;
}
