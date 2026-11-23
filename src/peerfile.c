#define _POSIX_C_SOURCE 200809L
#include "peerfile.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

/* Trim leading and trailing whitespace (including CR, so CRLF files
 * work) in place; returns a pointer to the first non-space char. */
static char *trim(char *s) {
    while (*s && isspace((unsigned char)*s)) s++;
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - 1])) s[--n] = '\0';
    return s;
}

int pulse_peerfile_load(const char *path, char ids[][PULSE_ID_LEN],
                        int max_ids, int *count_out, int *bad_out) {
    FILE *f = fopen(path, "r");
    if (!f) return -1; /* errno set by fopen: missing file is fatal */
    int count = 0, bad = 0, lineno = 0;
    char line[1024];
    while (fgets(line, sizeof line, f)) {
        lineno++;
        char *s = trim(line);
        if (s[0] == '\0') continue; /* blank line */
        if (s[0] == '#') continue;  /* comment line */
        struct sockaddr_storage addr;
        /* The whole trimmed line must be one id: a line longer than
         * an id, or one with trailing junk, must not parse by
         * accident (member_parse_addr rejects the trailing junk
         * itself, since the port field would not be a bare number). */
        if (strlen(s) >= PULSE_ID_LEN ||
            member_parse_addr(s, &addr) != 0) {
            fprintf(stderr, "peer file %s line %d: skipping bad peer '%s'\n",
                    path, lineno, s);
            bad++;
            continue;
        }
        /* Store the canonical form, so a bare IPv6 id and its
         * bracketed spelling are the same peer. */
        char id[PULSE_ID_LEN];
        member_format_id(&addr, id, sizeof id);
        int dup = 0;
        for (int i = 0; i < count; i++) {
            if (strcmp(ids[i], id) == 0) { dup = 1; break; }
        }
        if (dup) continue;
        if (count >= max_ids) {
            fprintf(stderr,
                    "peer file %s line %d: skipping peer '%s' (too many peers, max %d)\n",
                    path, lineno, s, max_ids);
            bad++;
            continue;
        }
        snprintf(ids[count++], PULSE_ID_LEN, "%s", id);
    }
    if (ferror(f)) {
        int e = errno;
        fclose(f);
        errno = e;
        return -1;
    }
    fclose(f);
    *count_out = count;
    *bad_out = bad;
    return 0;
}

static int id_in_list(char ids[][PULSE_ID_LEN], int n, const char *id) {
    for (int i = 0; i < n; i++) {
        if (strcmp(ids[i], id) == 0) return 1;
    }
    return 0;
}

void pulse_peerfile_diff(char old_ids[][PULSE_ID_LEN], int old_n,
                         char new_ids[][PULSE_ID_LEN], int new_n,
                         char added[][PULSE_ID_LEN], int *n_added_out,
                         char removed[][PULSE_ID_LEN], int *n_removed_out) {
    int n_added = 0, n_removed = 0;
    for (int i = 0; i < new_n; i++) {
        if (!id_in_list(old_ids, old_n, new_ids[i]) &&
            !id_in_list(added, n_added, new_ids[i]))
            snprintf(added[n_added++], PULSE_ID_LEN, "%s", new_ids[i]);
    }
    for (int i = 0; i < old_n; i++) {
        if (!id_in_list(new_ids, new_n, old_ids[i]) &&
            !id_in_list(removed, n_removed, old_ids[i]))
            snprintf(removed[n_removed++], PULSE_ID_LEN, "%s", old_ids[i]);
    }
    *n_added_out = n_added;
    *n_removed_out = n_removed;
}
