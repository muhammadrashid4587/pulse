#define _POSIX_C_SOURCE 200809L
#include "persist.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PULSE_MAX_AGE_S 86400 /* ages clamp at one day */

int pulse_members_save(const char *data_dir, const member_table_t *t,
                       const char *self_id, int64_t now_ms) {
    char path[600], tmp[608];
    snprintf(path, sizeof path, "%s/members", data_dir);
    snprintf(tmp, sizeof tmp, "%s/members.tmp", data_dir);
    FILE *f = fopen(tmp, "w");
    if (!f) return -1;
    int n = 0;
    for (int i = 0; i < t->count; i++) {
        const member_t *m = &t->members[i];
        if (strcmp(m->id, self_id) == 0) continue;
        if (m->last_seen_ms == 0) continue; /* never heard from */
        int64_t age = (now_ms - m->last_seen_ms) / 1000;
        if (age < 0) age = 0;
        if (age > PULSE_MAX_AGE_S) age = PULSE_MAX_AGE_S;
        fprintf(f, "%s %u %c %lld\n", m->id, m->incarnation,
                member_state_code(m->state), (long long)age);
        n++;
    }
    if (fclose(f) != 0) return -1;
    if (rename(tmp, path) != 0) return -1;
    return n;
}

int pulse_members_load(const char *data_dir, member_table_t *t,
                       const char *self_id, int64_t now_ms) {
    char path[600];
    snprintf(path, sizeof path, "%s/members", data_dir);
    FILE *f = fopen(path, "r");
    if (!f) return 0; /* no file: first start */
    char line[256];
    int n = 0;
    while (fgets(line, sizeof line, f)) {
        char id[PULSE_ID_LEN];
        unsigned inc;
        char code;
        long long age;
        /* Malformed lines are skipped, not fatal: a truncated or
         * hand-edited file must never stop a node from starting. */
        if (sscanf(line, "%63s %u %c %lld", id, &inc, &code, &age) != 4)
            continue;
        if (strcmp(id, self_id) == 0) continue;
        if (age < 0) age = 0;
        if (age > PULSE_MAX_AGE_S) age = PULSE_MAX_AGE_S;
        member_state_t saved = member_state_from_code(code);
        struct sockaddr_storage addr;
        if (member_parse_addr(id, &addr) != 0) continue;
        member_t *m = member_add(t, id, &addr, inc);
        if (!m) continue;
        /* Re-probe, don't trust: every restored entry gets a fresh
         * state clock, so a SUSPECT/DEAD verdict from before the
         * restart cannot carry a peer straight to DEAD without a
         * probe, and an ALIVE entry keeps its aged last_seen so the
         * prober picks it up at once. */
        m->incarnation = inc;
        m->state = (saved == MEMBER_ALIVE) ? MEMBER_ALIVE : MEMBER_SUSPECT;
        m->last_seen_ms = now_ms - age * 1000;
        m->state_since_ms = now_ms;
        n++;
    }
    int err = ferror(f);
    fclose(f);
    if (err) return -1;
    return n;
}
