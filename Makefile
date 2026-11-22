CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -g -Isrc
LDFLAGS ?= -pthread -lm

NODE_SRC = src/node.c src/member.c src/gossip.c src/phi.c src/secret.c src/control.c
HEADERS = src/member.h src/gossip.h src/phi.h src/secret.h src/control.h

.PHONY: all test clean

all: pulse-node pulse-ctl test_member test_phi test_secret

pulse-node: $(NODE_SRC) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ $(NODE_SRC) $(LDFLAGS)

pulse-ctl: src/pulse_ctl.c
	$(CC) $(CFLAGS) -o $@ src/pulse_ctl.c

test_member: tests/test_member.c src/member.c src/phi.c src/member.h src/phi.h
	$(CC) $(CFLAGS) -o $@ tests/test_member.c src/member.c src/phi.c $(LDFLAGS)

test_phi: tests/test_phi.c src/member.c src/phi.c src/member.h src/phi.h
	$(CC) $(CFLAGS) -o $@ tests/test_phi.c src/member.c src/phi.c $(LDFLAGS)

test_secret: tests/test_secret.c src/member.c src/phi.c src/gossip.c src/secret.c src/member.h src/phi.h src/gossip.h src/secret.h
	$(CC) $(CFLAGS) -o $@ tests/test_secret.c src/member.c src/phi.c src/gossip.c src/secret.c $(LDFLAGS)

test: pulse-node pulse-ctl test_member test_phi test_secret
	./test_member
	./test_phi
	./test_secret
	bash tests/test_cluster.sh
	bash tests/test_gc.sh
	bash tests/test_phi.sh
	bash tests/test_control.sh
	bash tests/test_secret.sh

clean:
	rm -f pulse-node pulse-ctl test_member test_phi test_secret
