CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -g -Isrc
LDFLAGS ?= -pthread

NODE_SRC = src/node.c src/member.c src/gossip.c
HEADERS = src/member.h src/gossip.h

.PHONY: all test clean

all: pulse-node test_member

pulse-node: $(NODE_SRC) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ $(NODE_SRC) $(LDFLAGS)

test_member: tests/test_member.c src/member.c src/member.h
	$(CC) $(CFLAGS) -o $@ tests/test_member.c src/member.c

test: pulse-node test_member
	./test_member
	bash tests/test_cluster.sh
	bash tests/test_gc.sh

clean:
	rm -f pulse-node test_member
