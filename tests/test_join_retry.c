/* Unit tests for the one-retry JOIN send (0.57.0): a JOIN is the one
 * message whose delivery matters for merging, so gossip_send_join_retry
 * sends it again at once when the first attempt fails. The failure it
 * guards against is real and reproducible on loopback: the send socket
 * is connect()ed to the first seed, so when that seed is down the
 * kernel reports the ICMP port-unreachable as ECONNREFUSED on the next
 * sendto — even to a different, live peer — and the call fails even
 * though the datagram would have gone out fine a moment later. The
 * error is consumed by the failed call, so the immediate retry
 * succeeds and the datagram goes out exactly once.
 *
 * The test drives a real connected UDP socket: it primes a stale ICMP
 * error by sending to a port nothing listens on, waits for the ICMP to
 * arrive, and then checks that a plain JOIN fails with ECONNREFUSED
 * while the retry helper succeeds and the peer receives exactly one
 * JOIN. The happy path (no pending error) is checked too: the helper
 * sends exactly once, never a duplicate. */
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "gossip.h"
#include "member.h"

static int checks = 0, failures = 0;

#define CHECK(cond) do { \
    checks++; \
    if (!(cond)) { \
        failures++; \
        printf("FAIL line %d: %s\n", __LINE__, #cond); \
    } \
} while (0)

static void sleep_ms(long ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

static void make_addr(struct sockaddr_storage *a, const char *ip,
                      uint16_t port) {
    memset(a, 0, sizeof *a);
    struct sockaddr_in *in = (struct sockaddr_in *)a;
    in->sin_family = AF_INET;
    in->sin_port = htons(port);
    inet_pton(AF_INET, ip, &in->sin_addr);
}

/* Prime a stale ICMP error on the connected socket: one datagram to
 * the dead port goes out, and the port-unreachable comes back and
 * sits pending on the socket until the next send consumes it. */
static void prime_stale_error(int send_sock,
                              const struct sockaddr_storage *dead) {
    ssize_t n = sendto(send_sock, "X", 1, 0,
                       (const struct sockaddr *)dead,
                       pulse_addr_len(dead));
    CHECK(n == 1);
    /* The ICMP is generated asynchronously; on loopback it is back
     * well within 100 ms, and the wait is what makes the failure
     * below deterministic instead of a race. */
    sleep_ms(100);
}

int main(void) {
    struct sockaddr_storage dead, live;
    make_addr(&dead, "127.0.0.1", 17991); /* nothing listens here */
    make_addr(&live, "127.0.0.1", 17992); /* the receiver below */

    int recv_sock = socket(AF_INET, SOCK_DGRAM, 0);
    CHECK(recv_sock >= 0);
    struct sockaddr_in rb;
    memset(&rb, 0, sizeof rb);
    rb.sin_family = AF_INET;
    rb.sin_port = htons(17992);
    inet_pton(AF_INET, "127.0.0.1", &rb.sin_addr);
    CHECK(bind(recv_sock, (struct sockaddr *)&rb, sizeof rb) == 0);
    struct timeval tv = { 0, 200000 }; /* 200 ms: loopback is instant */
    CHECK(setsockopt(recv_sock, SOL_SOCKET, SO_RCVTIMEO, &tv,
                     sizeof tv) == 0);

    int send_sock = socket(AF_INET, SOCK_DGRAM, 0);
    CHECK(send_sock >= 0);
    /* The node connects its send socket to the first seed; here the
     * only seed is the dead port, exactly the failing case. */
    CHECK(connect(send_sock, (const struct sockaddr *)&dead,
                  pulse_addr_len(&dead)) == 0);

    char buf[128];

    /* 1. Happy path: no pending error, the helper sends once. */
    CHECK(gossip_send_join_retry(send_sock, &live, "127.0.0.1:17993",
                                 1) == 0);
    ssize_t n = recv(recv_sock, buf, sizeof buf - 1, 0);
    CHECK(n > 0);
    if (n > 0) {
        buf[n] = '\0';
        CHECK(strcmp(buf, "JOIN 127.0.0.1:17993 1") == 0);
    }
    /* Exactly one datagram: the retry only runs on failure, so a
     * successful first send is never followed by a second. */
    n = recv(recv_sock, buf, sizeof buf - 1, 0);
    CHECK(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    /* 2. Control: with a stale ICMP error pending, a plain JOIN
     * fails with ECONNREFUSED and nothing arrives — this is the
     * hazard the retry exists for, reproduced, not assumed. */
    prime_stale_error(send_sock, &dead);
    errno = 0;
    CHECK(gossip_send_join(send_sock, &live, "127.0.0.1:17993", 1) != 0);
    CHECK(errno == ECONNREFUSED);
    n = recv(recv_sock, buf, sizeof buf - 1, 0);
    CHECK(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    /* 3. The retry: a fresh stale error pending, the helper's first
     * attempt fails and consumes it, the immediate retry succeeds,
     * and the peer receives exactly one JOIN. */
    prime_stale_error(send_sock, &dead);
    CHECK(gossip_send_join_retry(send_sock, &live, "127.0.0.1:17993",
                                 1) == 0);
    n = recv(recv_sock, buf, sizeof buf - 1, 0);
    CHECK(n > 0);
    if (n > 0) {
        buf[n] = '\0';
        CHECK(strcmp(buf, "JOIN 127.0.0.1:17993 1") == 0);
    }
    n = recv(recv_sock, buf, sizeof buf - 1, 0);
    CHECK(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    close(send_sock);
    close(recv_sock);

    printf("test_join_retry: %d checks, %d failures\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
