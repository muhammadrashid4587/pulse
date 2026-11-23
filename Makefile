CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -g -Isrc
LDFLAGS ?= -pthread -lm

NODE_SRC = src/node.c src/member.c src/gossip.c src/phi.c src/secret.c src/control.c src/metrics.c src/persist.c src/adaptive.c
HEADERS = src/member.h src/gossip.h src/phi.h src/secret.h src/control.h src/metrics.h src/persist.h src/adaptive.h

.PHONY: all test clean

all: pulse-node pulse-ctl test_member test_phi test_secret test_ipv6 test_gossip test_metrics test_persist test_adaptive test_meta

pulse-node: $(NODE_SRC) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ $(NODE_SRC) $(LDFLAGS)

pulse-ctl: src/pulse_ctl.c src/member.c src/phi.c src/member.h src/phi.h
	$(CC) $(CFLAGS) -o $@ src/pulse_ctl.c src/member.c src/phi.c $(LDFLAGS)

test_member: tests/test_member.c src/member.c src/phi.c src/member.h src/phi.h
	$(CC) $(CFLAGS) -o $@ tests/test_member.c src/member.c src/phi.c $(LDFLAGS)

test_phi: tests/test_phi.c src/member.c src/phi.c src/member.h src/phi.h
	$(CC) $(CFLAGS) -o $@ tests/test_phi.c src/member.c src/phi.c $(LDFLAGS)

test_secret: tests/test_secret.c src/member.c src/phi.c src/gossip.c src/secret.c src/member.h src/phi.h src/gossip.h src/secret.h
	$(CC) $(CFLAGS) -o $@ tests/test_secret.c src/member.c src/phi.c src/gossip.c src/secret.c $(LDFLAGS)

test_ipv6: tests/test_ipv6.c src/member.c src/phi.c src/gossip.c src/secret.c src/member.h src/phi.h src/gossip.h src/secret.h
	$(CC) $(CFLAGS) -o $@ tests/test_ipv6.c src/member.c src/phi.c src/gossip.c src/secret.c $(LDFLAGS)

test_gossip: tests/test_gossip.c src/member.c src/phi.c src/gossip.c src/secret.c src/member.h src/phi.h src/gossip.h src/secret.h
	$(CC) $(CFLAGS) -o $@ tests/test_gossip.c src/member.c src/phi.c src/gossip.c src/secret.c $(LDFLAGS)

test_metrics: tests/test_metrics.c src/metrics.c src/metrics.h
	$(CC) $(CFLAGS) -o $@ tests/test_metrics.c src/metrics.c $(LDFLAGS)

test_persist: tests/test_persist.c src/member.c src/phi.c src/persist.c src/member.h src/phi.h src/persist.h
	$(CC) $(CFLAGS) -o $@ tests/test_persist.c src/member.c src/phi.c src/persist.c $(LDFLAGS)

test_adaptive: tests/test_adaptive.c src/adaptive.c src/adaptive.h
	$(CC) $(CFLAGS) -o $@ tests/test_adaptive.c src/adaptive.c $(LDFLAGS)

test_meta: tests/test_meta.c src/member.c src/phi.c src/gossip.c src/secret.c src/member.h src/phi.h src/gossip.h src/secret.h
	$(CC) $(CFLAGS) -o $@ tests/test_meta.c src/member.c src/phi.c src/gossip.c src/secret.c $(LDFLAGS)

test: pulse-node pulse-ctl test_member test_phi test_secret test_ipv6 test_gossip test_metrics test_persist test_adaptive test_meta
	./test_member
	./test_phi
	./test_secret
	./test_ipv6
	./test_gossip
	./test_metrics
	./test_persist
	./test_adaptive
	./test_meta
	bash tests/test_cluster.sh
	bash tests/test_gc.sh
	bash tests/test_rejoin.sh
	bash tests/test_phi.sh
	bash tests/test_control.sh
	bash tests/test_secret.sh
	bash tests/test_ipv6.sh
	bash tests/test_persist.sh
	bash tests/test_sync.sh
	bash tests/test_leave.sh
	bash tests/test_join_sync.sh
	bash tests/test_adaptive.sh
	bash tests/test_refute.sh
	bash tests/test_meta.sh

clean:
	rm -f pulse-node pulse-ctl test_member test_phi test_secret test_ipv6 test_gossip test_metrics test_persist test_adaptive test_meta
