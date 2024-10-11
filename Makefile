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
AR=ar
ARFLAGS=rcu
DEBUG=-g -ggdb


PREFIX?=.
INSTALL_DIR?=$(PREFIX)/bin
INSTALL=cp -rf

%.o: %.c
	echo $(CTRIP_CC)
	$(CTRIP_CC) $(DEBUG) -MMD -o $@ -c $<


$(GTID_LIB): $(GTID_OBJ)
	@mkdir -p lib
	$(AR) $(ARFLAGS) $(GTID_LIB) $(GTID_OBJ)

bench:  $(GTID_LIB) ./gtid_bench.o
	$(CTRIP_CC)  -g -ggdb  -o  gtid_bench  gtid_bench.o ./lib/libgtid.a -lm -ldl

all: $(GTID_LIB)

noopt:
	$(MAKE) OPTIMIZATION="-O0"

clean:
	rm -rf $(GTID_LIB) $(GTID_OBJ) gtid_test.o gtid_bench.o *.d
	rm -rf gtid_test debug gtid_bench

test:  $(GTID_LIB) ./gtid_test.o
	$(CTRIP_CC)  -g -ggdb  -o  gtid_test  gtid_test.o ./lib/libgtid.a -lm -ldl
	./gtid_test


install: all
	@mkdir -p $(INSTALL_DIR)
	$(INSTALL) include $(INSTALL_DIR)
	$(INSTALL) lib $(INSTALL_DIR)


