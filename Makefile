
STD=-std=c99
FINAL_CFLAGS=$(STD) $(CFLAGS) -I./include -I./


CTRIP_CC=$(CC) $(FINAL_CFLAGS)
XREDIS_GTID_LIB=lib/libgtid.a
XREDIS_GTID_OBJ=gtid.o zmalloc.o util.o
AR=ar
ARFLAGS=rcu
DEBUG=-g -ggdb


PREFIX?=.
INSTALL_DIR?=$(PREFIX)/bin
INSTALL=cp -rf


%.o: %.c 
	echo $(CTRIP_CC)
	$(CTRIP_CC) $(DEBUG) -MMD -o $@ -c $< -DXREDIS_GTID_TEST


$(XREDIS_GTID_LIB): $(XREDIS_GTID_OBJ)
	@mkdir -p lib
	$(AR) $(ARFLAGS) $(XREDIS_GTID_LIB) $(XREDIS_GTID_OBJ)


all: $(XREDIS_GTID_LIB)

clean:
	rm -rf $(XREDIS_GTID_LIB) $(XREDIS_GTID_OBJ) ./gtid_test.o
	rm -rf gtid_test debug

test:  $(XREDIS_GTID_LIB) ./gtid_test.o	
	$(CTRIP_CC)  -g -ggdb  -o  gtid_test  gtid_test.o ./lib/libgtid.a -lm -ldl -DXREDIS_GTID_TEST
	./gtid_test

install: all
	@mkdir -p $(INSTALL_DIR)
	$(INSTALL) include $(INSTALL_DIR)
	$(INSTALL) lib $(INSTALL_DIR)


