/* pulse-ctl: a tiny client for the pulse-node TCP control plane.
 *
 * Usage: pulse-ctl <host:port> <COMMAND> [args...]
 *
 * Connects to the node's control port, sends the command line, prints
 * the reply to stdout. Exit 0 on success, 1 on a connection or
 * send/recv failure. The node answers ERR lines itself for bad
 * commands; those are printed, not treated as client errors.
 *
 * Example:
 *   pulse-ctl 127.0.0.1:18401 MEMBERS
 *   pulse-ctl 127.0.0.1:18401 LEAVE 127.0.0.1:17402
 *
 * The same thing works with netcat, if it is installed:
 *   printf 'MEMBERS\n' | nc 127.0.0.1 18401
 */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <host:port> <COMMAND> [args...]\n",
                argv[0]);
        return 2;
    }
    const char *addr = argv[1];
    const char *colon = strrchr(addr, ':');
    if (!colon || colon == addr) {
        fprintf(stderr, "bad address (want host:port): %s\n", addr);
        return 2;
    }
    char host[128];
    size_t hlen = (size_t)(colon - addr);
    if (hlen >= sizeof host) {
        fprintf(stderr, "host too long: %s\n", addr);
        return 2;
    }
    memcpy(host, addr, hlen);
    host[hlen] = '\0';
    int port = atoi(colon + 1);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "bad port: %s\n", addr);
        return 2;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }
    struct sockaddr_in dst;
    memset(&dst, 0, sizeof dst);
    dst.sin_family = AF_INET;
    dst.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &dst.sin_addr) != 1) {
        fprintf(stderr, "bad host (IPv4 only): %s\n", host);
        close(fd);
        return 2;
    }
    if (connect(fd, (struct sockaddr *)&dst, sizeof dst) < 0) {
        fprintf(stderr, "connect %s: %s\n", addr, strerror(errno));
        close(fd);
        return 1;
    }

    /* Send the command as one line: argv[2..] joined by spaces. */
    for (int i = 2; i < argc; i++) {
        if (i > 2 && send(fd, " ", 1, 0) != 1) goto send_fail;
        size_t len = strlen(argv[i]);
        if (send(fd, argv[i], len, 0) != (ssize_t)len) goto send_fail;
    }
    if (send(fd, "\n", 1, 0) != 1) goto send_fail;

    /* Print the reply until the node closes the connection. */
    char buf[1024];
    for (;;) {
        ssize_t n = recv(fd, buf, sizeof buf, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv");
            close(fd);
            return 1;
        }
        if (n == 0) break;
        fwrite(buf, 1, (size_t)n, stdout);
    }
    close(fd);
    return 0;

send_fail:
    fprintf(stderr, "send to %s: %s\n", addr, strerror(errno));
    close(fd);
    return 1;
}
