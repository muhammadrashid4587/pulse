/* SHA-256 (FIPS 180-4) and HMAC-SHA256, implemented in-tree so pulse
 * keeps zero dependencies beyond libc and pthreads. The shared-secret
 * join gate uses HMAC-SHA256(secret, message), truncated to the first
 * 8 bytes and hex-encoded (16 chars) as a trailing token on every UDP
 * message. */
#define _POSIX_C_SOURCE 200809L
#include "secret.h"

#include <string.h>

#define ROTR(x, n) (((x) >> (n)) | ((x) << (32 - (n))))
#define CH(x, y, z) (((x) & (y)) ^ (~(x) & (z)))
#define MAJ(x, y, z) (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))
#define EP0(x) (ROTR(x, 2) ^ ROTR(x, 13) ^ ROTR(x, 22))
#define EP1(x) (ROTR(x, 6) ^ ROTR(x, 11) ^ ROTR(x, 25))
#define SIG0(x) (ROTR(x, 7) ^ ROTR(x, 18) ^ ((x) >> 3))
#define SIG1(x) (ROTR(x, 17) ^ ROTR(x, 19) ^ ((x) >> 10))

static const unsigned int K[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu,
    0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u, 0xd807aa98u, 0x12835b01u,
    0x243185beu, 0x550c7dc3u, 0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u,
    0xc19bf174u, 0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
    0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau, 0x983e5152u,
    0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u,
    0x06ca6351u, 0x14292967u, 0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu,
    0x53380d13u, 0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u, 0xd192e819u,
    0xd6990624u, 0xf40e3585u, 0x106aa070u, 0x19a4c116u, 0x1e376c08u,
    0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu,
    0x682e6ff3u, 0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u
};

void sha256_init(sha256_ctx_t *ctx) {
    ctx->datalen = 0;
    ctx->bitlen = 0;
    ctx->state[0] = 0x6a09e667u;
    ctx->state[1] = 0xbb67ae85u;
    ctx->state[2] = 0x3c6ef372u;
    ctx->state[3] = 0xa54ff53au;
    ctx->state[4] = 0x510e527fu;
    ctx->state[5] = 0x9b05688cu;
    ctx->state[6] = 0x1f83d9abu;
    ctx->state[7] = 0x5be0cd19u;
}

static void sha256_transform(sha256_ctx_t *ctx, const unsigned char *data) {
    unsigned int a, b, c, d, e, f, g, h, t1, t2, m[64];
    for (int i = 0, j = 0; i < 16; i++, j += 4)
        m[i] = ((unsigned int)data[j] << 24) |
               ((unsigned int)data[j + 1] << 16) |
               ((unsigned int)data[j + 2] << 8) |
               ((unsigned int)data[j + 3]);
    for (int i = 16; i < 64; i++)
        m[i] = SIG1(m[i - 2]) + m[i - 7] + SIG0(m[i - 15]) + m[i - 16];

    a = ctx->state[0]; b = ctx->state[1]; c = ctx->state[2];
    d = ctx->state[3]; e = ctx->state[4]; f = ctx->state[5];
    g = ctx->state[6]; h = ctx->state[7];

    for (int i = 0; i < 64; i++) {
        t1 = h + EP1(e) + CH(e, f, g) + K[i] + m[i];
        t2 = EP0(a) + MAJ(a, b, c);
        h = g; g = f; f = e; e = d + t1;
        d = c; c = b; b = a; a = t1 + t2;
    }

    ctx->state[0] += a; ctx->state[1] += b; ctx->state[2] += c;
    ctx->state[3] += d; ctx->state[4] += e; ctx->state[5] += f;
    ctx->state[6] += g; ctx->state[7] += h;
}

void sha256_update(sha256_ctx_t *ctx, const unsigned char *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        ctx->data[ctx->datalen] = data[i];
        ctx->datalen++;
        if (ctx->datalen == 64) {
            sha256_transform(ctx, ctx->data);
            ctx->bitlen += 512;
            ctx->datalen = 0;
        }
    }
}

void sha256_final(sha256_ctx_t *ctx, unsigned char hash[32]) {
    unsigned int i = ctx->datalen;

    if (ctx->datalen < 56) {
        ctx->data[i++] = 0x80;
        while (i < 56) ctx->data[i++] = 0x00;
    } else {
        ctx->data[i++] = 0x80;
        while (i < 64) ctx->data[i++] = 0x00;
        sha256_transform(ctx, ctx->data);
        memset(ctx->data, 0, 56);
    }

    ctx->bitlen += ctx->datalen * 8;
    ctx->data[63] = (unsigned char)(ctx->bitlen);
    ctx->data[62] = (unsigned char)(ctx->bitlen >> 8);
    ctx->data[61] = (unsigned char)(ctx->bitlen >> 16);
    ctx->data[60] = (unsigned char)(ctx->bitlen >> 24);
    ctx->data[59] = (unsigned char)(ctx->bitlen >> 32);
    ctx->data[58] = (unsigned char)(ctx->bitlen >> 40);
    ctx->data[57] = (unsigned char)(ctx->bitlen >> 48);
    ctx->data[56] = (unsigned char)(ctx->bitlen >> 56);
    sha256_transform(ctx, ctx->data);

    for (i = 0; i < 4; i++) {
        for (int j = 0; j < 8; j++) {
            hash[i + (j * 4)] = (unsigned char)((ctx->state[j] >> (24 - i * 8)) & 0xff);
        }
    }
}

static void hmac_sha256(const unsigned char *key, size_t key_len,
                        const unsigned char *msg, size_t msg_len,
                        unsigned char out[32]) {
    unsigned char key_block[64];
    unsigned char hashed_key[32];
    const unsigned char *k = key;
    size_t klen = key_len;

    if (klen > 64) {
        sha256_ctx_t ctx;
        sha256_init(&ctx);
        sha256_update(&ctx, key, key_len);
        sha256_final(&ctx, hashed_key);
        k = hashed_key;
        klen = 32;
    }
    memset(key_block, 0, sizeof key_block);
    memcpy(key_block, k, klen);

    unsigned char ipad[64], opad[64];
    for (int i = 0; i < 64; i++) {
        ipad[i] = key_block[i] ^ 0x36;
        opad[i] = key_block[i] ^ 0x5c;
    }

    unsigned char inner[32];
    sha256_ctx_t ctx;
    sha256_init(&ctx);
    sha256_update(&ctx, ipad, 64);
    sha256_update(&ctx, msg, msg_len);
    sha256_final(&ctx, inner);

    sha256_init(&ctx);
    sha256_update(&ctx, opad, 64);
    sha256_update(&ctx, inner, 32);
    sha256_final(&ctx, out);
}

static const char HEX[] = "0123456789abcdef";

void pulse_secret_tag(const char *secret, const unsigned char *msg,
                      size_t msg_len, char out_hex[PULSE_TAG_HEX_LEN + 1]) {
    unsigned char mac[32];
    hmac_sha256((const unsigned char *)secret, strlen(secret), msg, msg_len,
                mac);
    for (int i = 0; i < 8; i++) {
        out_hex[i * 2] = HEX[(mac[i] >> 4) & 0xf];
        out_hex[i * 2 + 1] = HEX[mac[i] & 0xf];
    }
    out_hex[PULSE_TAG_HEX_LEN] = '\0';
}

int pulse_secret_verify(const char *secret, const unsigned char *msg,
                        size_t msg_len, const char *tag_hex) {
    char expect[PULSE_TAG_HEX_LEN + 1];
    pulse_secret_tag(secret, msg, msg_len, expect);
    /* Constant-time over the 16 chars: no early exit on mismatch. */
    unsigned int diff = 0;
    for (int i = 0; i < PULSE_TAG_HEX_LEN; i++)
        diff |= (unsigned int)(expect[i] ^ tag_hex[i]);
    return diff == 0;
}

static int is_hex16(const unsigned char *s, size_t n) {
    if (n != PULSE_TAG_HEX_LEN) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = s[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return 0;
    }
    return 1;
}

int pulse_secret_check(const char *secret, const unsigned char *buf,
                       size_t len) {
    if (!secret || secret[0] == '\0') return 1; /* no secret: accept all */
    if (len == 0) return 0;
    /* The tag is the last whitespace-delimited token. */
    size_t end = len;
    while (end > 0 && (buf[end - 1] == ' ' || buf[end - 1] == '\t' ||
                       buf[end - 1] == '\r' || buf[end - 1] == '\n'))
        end--;
    if (end == 0) return 0;
    size_t start = end;
    while (start > 0 && buf[start - 1] != ' ' && buf[start - 1] != '\t' &&
           buf[start - 1] != '\r' && buf[start - 1] != '\n')
        start--;
    if (start == 0) return 0; /* no body before the token */
    if (!is_hex16(buf + start, end - start)) return 0;
    char tag[PULSE_TAG_HEX_LEN + 1];
    memcpy(tag, buf + start, PULSE_TAG_HEX_LEN);
    tag[PULSE_TAG_HEX_LEN] = '\0';
    /* The HMAC covers everything before the space that precedes the tag. */
    size_t body_len = start - 1;
    return pulse_secret_verify(secret, buf, body_len, tag);
}
