#ifndef PULSE_SECRET_H
#define PULSE_SECRET_H

#include <stddef.h>

/* Optional shared secret for joins: a toy-grade join gate, not real
 * authentication. A node started with a secret appends an HMAC-SHA256
 * tag (first 8 bytes, 16 lowercase hex chars) to every UDP message it
 * sends, and drops any incoming datagram whose tag is missing or
 * wrong. A node without a secret sends untagged messages and accepts
 * anything, which is the current default behaviour.
 *
 * The tag is computed over the message body exactly as sent. The
 * secret itself never goes on the wire. The tag is truncated to 8
 * bytes because this is a gate against accidental cross-talk between
 * clusters (two wards, two test rigs on the same LAN), not a defence
 * against a determined attacker: the secret is passed on the command
 * line (visible in ps), messages are not encrypted, and there is no
 * replay protection beyond the tag covering the message content.
 *
 * SHA-256 and HMAC are implemented in-tree (secret.c) so pulse keeps
 * zero dependencies beyond libc and pthreads.
 */

#define PULSE_SECRET_MAX 128
#define PULSE_TAG_HEX_LEN 16 /* 8 bytes of HMAC, hex-encoded */

/* Compute the tag for a message. out_hex gets 16 hex chars + NUL. */
void pulse_secret_tag(const char *secret, const unsigned char *msg,
                      size_t msg_len, char out_hex[PULSE_TAG_HEX_LEN + 1]);

/* Verify a tag against a message. Returns 1 on match, 0 otherwise.
 * The comparison is constant-time over the 16 hex chars. */
int pulse_secret_verify(const char *secret, const unsigned char *msg,
                        size_t msg_len, const char *tag_hex);

/* Gate for a received datagram. If secret is NULL or empty, every
 * datagram is accepted (returns 1). Otherwise the datagram must end
 * with " <16 hex tag>" whose HMAC covers everything before the space;
 * returns 1 if it does, 0 if the tag is missing, malformed, or wrong. */
int pulse_secret_check(const char *secret, const unsigned char *buf,
                       size_t len);

/* SHA-256, for the unit tests (and for anything else that needs a hash). */
typedef struct {
    unsigned int state[8];
    unsigned long long bitlen;
    unsigned char data[64];
    unsigned int datalen;
} sha256_ctx_t;

void sha256_init(sha256_ctx_t *ctx);
void sha256_update(sha256_ctx_t *ctx, const unsigned char *data, size_t len);
void sha256_final(sha256_ctx_t *ctx, unsigned char hash[32]);

#endif
