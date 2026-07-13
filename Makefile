STD=-std=c99
OPTIMIZATION?=-O2

ifdef SANITIZER
ifeq ($(SANITIZER),address)
   CFLAGS+=-fsanitize=address -fno-sanitize-recover=all -fno-omit-frame-pointer
else
ifeq ($(SANITIZER),undefined)
   CFLAGS+=-fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer
else
    $(error "unknown sanitizer=${SANITIZER}")
endif
endif
endif

FINAL_CFLAGS=$(STD) $(OPTIMIZATION) $(CFLAGS) -I./include -I./

CTRIP_CC=$(CC) $(FINAL_CFLAGS)
GTID_LIB=lib/libgtid.a
GTID_OBJ=gtid.o gtid_util.o
XREDIS_COMMANDS=./xredis/xredis_commands.def
AR=ar
ARFLAGS=rcu
DEBUG=-g -ggdb


PREFIX?=.
INSTALL_DIR?=$(PREFIX)/bin
INSTALL=cp -rf

# Parent Redis tree (ror_swap root when built as deps/xredis-gtid).
REDIS_ROOT ?= ../..
TEST_MODULES_DIR = $(REDIS_ROOT)/tests/modules

%.o: %.c
	echo $(CTRIP_CC)
	$(CTRIP_CC) $(DEBUG) -MMD -o $@ -c $<

# Keep 'all' as the first target so bare `make` (deps build) still builds libgtid.
all: $(GTID_LIB)

$(XREDIS_COMMANDS):
	$(PYTHON) ./utils/generate_cmdparse_commands.py


$(GTID_LIB): $(GTID_OBJ) $(XREDIS_COMMANDS)
	@mkdir -p lib
	$(AR) $(ARFLAGS) $(GTID_LIB) $(GTID_OBJ)

bench:  $(GTID_LIB) ./gtid_bench.o
	$(CTRIP_CC)  -g -ggdb  -o  gtid_bench  gtid_bench.o ./lib/libgtid.a -lm -ldl 

noopt:
	$(MAKE) OPTIMIZATION="-O0"

clean:
	rm -rf $(GTID_LIB) $(GTID_OBJ) gtid_test.o gtid_bench.o *.d
	rm -rf gtid_test debug gtid_bench
	rm -rf xredis/xredis_commands.def

test:  $(GTID_LIB) ./gtid_test.o
	$(CTRIP_CC)  -g -ggdb  -o  gtid_test  gtid_test.o ./lib/libgtid.a -lm -ldl
	./gtid_test

# Only invoked from Redis src/Makefile test / test-asan (not part of all).
.PHONY: build-test-modules
build-test-modules:
	@$(MAKE) -C $(TEST_MODULES_DIR) propagate.so

install: all
	@mkdir -p $(INSTALL_DIR)
	$(INSTALL) include $(INSTALL_DIR)
	$(INSTALL) lib $(INSTALL_DIR)


