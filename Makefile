STD=-std=c99
OPT=-O2

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

FINAL_CFLAGS=$(STD) $(OPT) $(CFLAGS) -I./include -I./

CTRIP_CC=$(CC) $(FINAL_CFLAGS)
XREDIS_GTID_LIB=lib/libgtid.a
XREDIS_GTID_OBJ=gtid.o util.o
AR=ar
ARFLAGS=rcu
DEBUG=-g -ggdb


PREFIX?=.
INSTALL_DIR?=$(PREFIX)/bin
INSTALL=cp -rf

%.o: %.c
	echo $(CTRIP_CC)
	$(CTRIP_CC) $(DEBUG) -MMD -o $@ -c $<


$(XREDIS_GTID_LIB): $(XREDIS_GTID_OBJ)
	@mkdir -p lib
	$(AR) $(ARFLAGS) $(XREDIS_GTID_LIB) $(XREDIS_GTID_OBJ)

bench:  $(XREDIS_GTID_LIB) ./gtid_bench.o
	$(CTRIP_CC)  -g -ggdb  -o  gtid_bench  gtid_bench.o ./lib/libgtid.a -lm -ldl

all: $(XREDIS_GTID_LIB)

clean:
	rm -rf $(XREDIS_GTID_LIB) $(XREDIS_GTID_OBJ) ./gtid_test.o ./gtid_bench.o
	rm -rf gtid_test debug gtid_bench

test:  $(XREDIS_GTID_LIB) ./gtid_test.o
	$(CTRIP_CC)  -g -ggdb  -o  gtid_test  gtid_test.o ./lib/libgtid.a -lm -ldl
	./gtid_test


install: all
	@mkdir -p $(INSTALL_DIR)
	$(INSTALL) include $(INSTALL_DIR)
	$(INSTALL) lib $(INSTALL_DIR)


