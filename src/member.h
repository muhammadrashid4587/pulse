#ifndef PULSE_MEMBER_H
#define PULSE_MEMBER_H

#include <netinet/in.h>
#include <stdint.h>

#define PULSE_MAX_MEMBERS 64
#define PULSE_ID_LEN 64

typedef enum {
    MEMBER_ALIVE = 0,
    MEMBER_SUSPECT = 1,
    MEMBER_DEAD = 2
} member_state_t;

typedef struct {
    char id[PULSE_ID_LEN]; /* "host:port" */
    struct sockaddr_in addr;
    uint32_t incarnation;
    member_state_t state;
    int64_t last_seen_ms;   /* monotonic ms of last direct contact */
    int64_t state_since_ms; /* when the current state started */
} member_t;

typedef struct {
    member_t members[PULSE_MAX_MEMBERS];
    int count;
} member_table_t;

int64_t pulse_now_ms(void);
void member_table_init(member_table_t *t);
member_t *member_find(member_table_t *t, const char *id);
member_t *member_add(member_table_t *t, const char *id,
                     const struct sockaddr_in *addr, uint32_t incarnation);

/* Parse a "host:port" id into a sockaddr_in. Returns 0 on success. */
int member_parse_addr(const char *id, struct sockaddr_in *addr);
void member_format_id(const struct sockaddr_in *addr, char *id, size_t len);

/* Merge results (bit flags). */
#define MERGE_NONE 0
#define MERGE_NEW 1
#define MERGE_CHANGED 2
#define MERGE_SELF_SUSPECTED 4

/* Merge one remote membership entry into the table, SWIM-style:
 * higher incarnation wins; at equal incarnation DEAD > SUSPECT > ALIVE.
 * Entries about self are never applied; if one claims self is SUSPECT or
 * DEAD at an incarnation >= our own, MERGE_SELF_SUSPECTED is returned so
 * the caller can refute by bumping its incarnation. */
int member_merge(member_table_t *t, const char *self_id, const char *id,
                 const struct sockaddr_in *addr, uint32_t incarnation,
                 member_state_t state, int64_t now);

int member_mark_suspect(member_table_t *t, const char *id, int64_t now);
int member_mark_dead(member_table_t *t, const char *id, int64_t now);
/* Direct contact: the member is alive. Returns 1 if the member was new or
 * its state changed, 0 if it was already ALIVE. */
int member_mark_alive(member_table_t *t, const char *id,
                      const struct sockaddr_in *addr, uint32_t incarnation,
                      int64_t now);

int member_count_state(const member_table_t *t, member_state_t state);
/* Random ALIVE or SUSPECT peer other than self, or NULL if there is none.
 * The returned pointer is only valid while the caller holds the table lock. */
member_t *member_random_peer(member_table_t *t, const char *self_id);

const char *member_state_name(member_state_t s);
char member_state_code(member_state_t s);
member_state_t member_state_from_code(char c);

#endif
