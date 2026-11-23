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
        /* Annotations ride along as an optional fifth field
         * ("k=v,k=v", or "-" when the member carries none), so a
         * restarted node restores what it knew about its peers until
         * gossip refreshes it. Lines without the field (older files,
         * hand-written ones) still load. */
        char meta[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
        if (member_meta_format(m, meta, sizeof meta) < 0 || meta[0] == '\0')
            snprintf(meta, sizeof meta, "-");
        fprintf(f, "%s %u %c %lld %s\n", m->id, m->incarnation,
                member_state_code(m->state), (long long)age, meta);
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
    char line[1024];
    int n = 0;
    while (fgets(line, sizeof line, f)) {
        char id[PULSE_ID_LEN];
        unsigned inc;
        char code;
        long long age;
        /* The annotations field is optional: a line may have four
         * fields (older files) or five. A malformed annotations
         * field is dropped, not fatal — the entry still loads. */
        char meta[PULSE_MAX_META * (PULSE_META_MAX_LEN + 1)];
        meta[0] = '\0';
        int fields = sscanf(line, "%63s %u %c %lld %519s", id, &inc, &code,
                            &age, meta);
        /* Malformed lines are skipped, not fatal: a truncated or
         * hand-edited file must never stop a node from starting. */
        if (fields < 4)
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
        if (fields == 5 && meta[0] != '\0' && strcmp(meta, "-") != 0) {
            int nmeta = 0;
            if (member_meta_parse_list(meta, m->meta, PULSE_MAX_META,
                                       &nmeta) == 0)
                m->meta_count = nmeta;
        }
        n++;
    }
    int err = ferror(f);
    fclose(f);
    if (err) return -1;
    return n;
}

/* The node's own annotations live in <data-dir>/meta, one "key=value"
 * per line, written atomically like the members file. A node started
 * with --meta flags saves those (replacing any saved file); a node
 * started without flags loads the saved ones, so its annotations
 * survive a restart even when the operator forgets the flags. */
int pulse_meta_save(const char *data_dir, const meta_kv_t *meta,
                    int meta_count) {
    char path[600], tmp[608];
    snprintf(path, sizeof path, "%s/meta", data_dir);
    snprintf(tmp, sizeof tmp, "%s/meta.tmp", data_dir);
    FILE *f = fopen(tmp, "w");
    if (!f) return -1;
    for (int i = 0; i < meta_count; i++)
        fprintf(f, "%s=%s\n", meta[i].key, meta[i].value);
    if (fclose(f) != 0) return -1;
    if (rename(tmp, path) != 0) return -1;
    return meta_count;
}

int pulse_meta_load(const char *data_dir, meta_kv_t *meta, int max,
                    int *count_out) {
    *count_out = 0;
    char path[600];
    snprintf(path, sizeof path, "%s/meta", data_dir);
    FILE *f = fopen(path, "r");
    if (!f) return 0; /* no file: first start, or no annotations yet */
    char line[128];
    while (fgets(line, sizeof line, f)) {
        /* Strip the trailing newline; a malformed line is skipped,
         * not fatal. */
        size_t n = strlen(line);
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r'))
            line[--n] = '\0';
        if (*count_out >= max) break;
        char key[PULSE_META_KEY_LEN + 1];
        char value[PULSE_META_VAL_LEN + 1];
        if (member_meta_parse(line, key, sizeof key, value,
                              sizeof value) != 0)
            continue;
        /* A duplicate key in the file keeps the first value. */
        int dup = 0;
        for (int i = 0; i < *count_out; i++) {
            if (strcmp(meta[i].key, key) == 0) {
                dup = 1;
                break;
            }
        }
        if (dup) continue;
        snprintf(meta[*count_out].key, sizeof meta[*count_out].key, "%s",
                 key);
        snprintf(meta[*count_out].value, sizeof meta[*count_out].value,
                 "%s", value);
        (*count_out)++;
    }
    int err = ferror(f);
    fclose(f);
    if (err) return -1;
    return *count_out;
}
