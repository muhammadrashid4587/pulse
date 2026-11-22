/* Unit tests for the shared-secret join gate (no networking):
 * SHA-256 and HMAC-SHA256 against published test vectors, the tag
 * round trip, and pulse_secret_check's accept/drop decisions. */
#include <stdio.h>
#include <string.h>

#include "gossip.h"
#include "secret.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

static void hex_of(const unsigned char *in, size_t n, char *out) {
    static const char h[] = "0123456789abcdef";
    for (size_t i = 0; i < n; i++) {
        out[i * 2] = h[(in[i] >> 4) & 0xf];
        out[i * 2 + 1] = h[in[i] & 0xf];
    }
    out[n * 2] = '\0';
}

static int sha256_hex(const char *msg, char *out) {
    sha256_ctx_t ctx;
    unsigned char hash[32];
    sha256_init(&ctx);
    sha256_update(&ctx, (const unsigned char *)msg, strlen(msg));
    sha256_final(&ctx, hash);
    hex_of(hash, 32, out);
    return 0;
}

int main(void) {
    char hex[65];

    /* SHA-256, FIPS 180-4 vectors. */
    sha256_hex("", hex);
    CHECK(strcmp(hex, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") == 0);
    sha256_hex("abc", hex);
    CHECK(strcmp(hex, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad") == 0);
    sha256_hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", hex);
    CHECK(strcmp(hex, "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1") == 0);
    /* One million 'a's: exercises multi-block update. */
    {
        sha256_ctx_t ctx;
        unsigned char hash[32];
        char block[1000];
        memset(block, 'a', sizeof block);
        sha256_init(&ctx);
        for (int i = 0; i < 1000; i++)
            sha256_update(&ctx, (const unsigned char *)block, sizeof block);
        sha256_final(&ctx, hash);
        hex_of(hash, 32, hex);
        CHECK(strcmp(hex, "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0") == 0);
    }

    /* HMAC-SHA256, RFC 4231 test case 2 (key "Jefe", data "what do ya
     * want for nothing?"): checked through the tag, which is the first
     * 8 bytes of the HMAC, hex-encoded. Case 1's key is 0x0b * 20,
     * which is not a C string, so it is not exercised here; the tag
     * tests below cover wrong-secret and tampered-message behaviour. */
    {
        char tag[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag("Jefe", (const unsigned char *)"what do ya want for nothing?",
                         strlen("what do ya want for nothing?"), tag);
        CHECK(strcmp(tag, "5bdcc146bf60754e") == 0);
    }

    /* Tag shape: 16 lowercase hex chars, deterministic. */
    {
        char tag1[PULSE_TAG_HEX_LEN + 1], tag2[PULSE_TAG_HEX_LEN + 1];
        const char *body = "PING 7 127.0.0.1:17102 3";
        pulse_secret_tag("ward-alpha", (const unsigned char *)body, strlen(body), tag1);
        pulse_secret_tag("ward-alpha", (const unsigned char *)body, strlen(body), tag2);
        CHECK(strcmp(tag1, tag2) == 0);
        CHECK(strlen(tag1) == PULSE_TAG_HEX_LEN);
        int all_hex = 1;
        for (int i = 0; i < PULSE_TAG_HEX_LEN; i++)
            if (!((tag1[i] >= '0' && tag1[i] <= '9') ||
                  (tag1[i] >= 'a' && tag1[i] <= 'f')))
                all_hex = 0;
        CHECK(all_hex);
        /* A different secret, or a different body, gives a different tag. */
        char tag3[PULSE_TAG_HEX_LEN + 1], tag4[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag("ward-beta", (const unsigned char *)body, strlen(body), tag3);
        pulse_secret_tag("ward-alpha", (const unsigned char *)"PING 8 127.0.0.1:17102 3",
                         strlen("PING 8 127.0.0.1:17102 3"), tag4);
        CHECK(strcmp(tag1, tag3) != 0);
        CHECK(strcmp(tag1, tag4) != 0);
    }

    /* Verify: round trip, wrong secret, tampered message, tampered tag. */
    {
        const char *body = "GOSSIP 127.0.0.1:17101 1 2 127.0.0.1:17101:1:A 127.0.0.1:17102:1:A";
        char tag[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag("ward-alpha", (const unsigned char *)body, strlen(body), tag);
        CHECK(pulse_secret_verify("ward-alpha", (const unsigned char *)body, strlen(body), tag));
        CHECK(!pulse_secret_verify("ward-beta", (const unsigned char *)body, strlen(body), tag));
        CHECK(!pulse_secret_verify("ward-alpha", (const unsigned char *)"GOSSIP 127.0.0.1:17101 1 2 127.0.0.1:17101:1:A 127.0.0.1:17102:1:S",
                                   strlen("GOSSIP 127.0.0.1:17101 1 2 127.0.0.1:17101:1:A 127.0.0.1:17102:1:S"), tag));
        char bad[PULSE_TAG_HEX_LEN + 1];
        strcpy(bad, tag);
        bad[0] = (bad[0] == '0') ? '1' : '0';
        CHECK(!pulse_secret_verify("ward-alpha", (const unsigned char *)body, strlen(body), bad));
    }

    /* pulse_secret_check: no secret accepts everything, including
     * garbage and untagged messages. */
    CHECK(pulse_secret_check(NULL, (const unsigned char *)"PING 1 127.0.0.1:17102 1", 25) == 1);
    CHECK(pulse_secret_check("", (const unsigned char *)"garbage", 7) == 1);
    CHECK(pulse_secret_check(NULL, (const unsigned char *)"", 0) == 1);

    /* With a secret: correctly tagged passes, everything else drops. */
    {
        const char *body = "PING 7 127.0.0.1:17102 3";
        char tag[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag("ward-alpha", (const unsigned char *)body, strlen(body), tag);
        char tagged[256];
        snprintf(tagged, sizeof tagged, "%s %s", body, tag);
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)tagged, strlen(tagged)) == 1);
        /* Untagged. */
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)body, strlen(body)) == 0);
        /* Tagged with the wrong secret. */
        char wrong[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag("ward-beta", (const unsigned char *)body, strlen(body), wrong);
        char wrong_tagged[256];
        snprintf(wrong_tagged, sizeof wrong_tagged, "%s %s", body, wrong);
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)wrong_tagged, strlen(wrong_tagged)) == 0);
        /* Last token is 16 hex chars but not the right tag. */
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)"PING 7 127.0.0.1:17102 3 0123456789abcdef",
                                 strlen("PING 7 127.0.0.1:17102 3 0123456789abcdef")) == 0);
        /* Last token is not hex at all. */
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)"PING 7 127.0.0.1:17102 3 not-a-tag-here!!",
                                 strlen("PING 7 127.0.0.1:17102 3 not-a-tag-here!!")) == 0);
        /* Empty datagram and a bare tag with no body. */
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)"", 0) == 0);
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)"0123456789abcdef", 16) == 0);
        /* Trailing whitespace after the tag is tolerated. */
        char trailing[256];
        snprintf(trailing, sizeof trailing, "%s  ", tagged);
        CHECK(pulse_secret_check("ward-alpha", (const unsigned char *)trailing, strlen(trailing)) == 1);
    }

    /* A tagged message still parses: the trailing token is ignored by
     * gossip_parse, so the tag needs no protocol change. */
    {
        const char *body = "PING 7 127.0.0.1:17102 3";
        char tag[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag("ward-alpha", (const unsigned char *)body, strlen(body), tag);
        char tagged[256];
        snprintf(tagged, sizeof tagged, "%s %s", body, tag);
        gmsg_t msg;
        CHECK(gossip_parse(tagged, strlen(tagged), &msg) == 0);
        CHECK(msg.type == GMSG_PING);
        CHECK(msg.seq == 7);
        CHECK(strcmp(msg.from_id, "127.0.0.1:17102") == 0);
        CHECK(msg.from_inc == 3);
    }
    {
        const char *body = "GOSSIP 127.0.0.1:17101 1 1 127.0.0.1:17102:1:A";
        char tag[PULSE_TAG_HEX_LEN + 1];
        pulse_secret_tag("ward-alpha", (const unsigned char *)body, strlen(body), tag);
        char tagged[256];
        snprintf(tagged, sizeof tagged, "%s %s", body, tag);
        gmsg_t msg;
        CHECK(gossip_parse(tagged, strlen(tagged), &msg) == 0);
        CHECK(msg.type == GMSG_GOSSIP);
        CHECK(msg.entry_count == 1);
        CHECK(strcmp(msg.entries[0].id, "127.0.0.1:17102") == 0);
        CHECK(msg.entries[0].state == MEMBER_ALIVE);
    }

    printf("test_secret: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
