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

    unlink(path);
    printf("test_peerfile: %d checks, %d failures\n", checks, failures);
    return failures != 0;
}
