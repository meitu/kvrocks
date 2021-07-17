# kvrocks Makefile

# NO_BUILD_DEPENDENCIES was used to build the homebrew binary
# with brew installed dependencies, so we should use the clang
# as well to compati with those dependencies.
ifndef NO_BUILD_DEPENDENCIES
CXX=g++
endif
CXXFLAGS= -O2 -std=c++11 -Wall -Wpedantic -Wsign-compare -Wreturn-type -fno-omit-frame-pointer -DNDEBUG=1 -Wno-unused-result
LDFLAGS= -pthread -ldl
FINAL_CXXFLAGS+= $(CXXFLAGS) $(DEBUG)
DEBUG= -g -ggdb

INSTALL_DIR= /usr/local
BIN_DIR= $(INSTALL_DIR)/bin
INSTALL= /usr/bin/install

release_hdr := $(shell sh -c './mkreleasehdr.sh')
detect_platform := $(shell sh -c '../build_detect_platform make_config.mk')
include make_config.mk
LDFLAGS+= $(PLATFORM_LDFLAGS)

ifdef NO_BUILD_DEPENDENCIES
FINAL_CXXFLAGS+= -I. -DDISABLE_USE_PTHREADS
FINAL_LIBS+= -lsnappy -ljemalloc -lglog -levent -lrocksdb
else
EXTERNAL_LIBRARY_PATH= $(realpath ../external)
GLOG_PATH= $(EXTERNAL_LIBRARY_PATH)/glog
GLOG= $(GLOG_PATH)/.libs/libglog.a
LIBEVENT_PATH= $(EXTERNAL_LIBRARY_PATH)/libevent
LIBEVENT= $(LIBEVENT_PATH)/.libs/libevent.a
LIBEVENT_PTHREADS= $(LIBEVENT_PATH)/.libs/libevent_pthreads.a
JEMALLOC_PATH= $(EXTERNAL_LIBRARY_PATH)/jemalloc
JEMALLOC= $(JEMALLOC_PATH)/lib/libjemalloc.a
ROCKSDB_PATH= $(EXTERNAL_LIBRARY_PATH)/rocksdb
ROCKSDB= $(ROCKSDB_PATH)/librocksdb.a
# Include paths to dependencies
FINAL_CXXFLAGS+= -I$(JEMALLOC_PATH)/include \
				  -I$(ROCKSDB_PATH)/include \
				  -I$(LIBEVENT_PATH)/include \
				  -I$(GLOG_PATH)/src \
				  -I.
FINAL_LIBS+= $(GLOG) $(LIBEVENT) $(LIBEVENT_PTHREADS) $(JEMALLOC) $(ROCKSDB)
endif

SHARED_OBJS= cluster.o compact_filter.o config.o cron.o encoding.o event_listener.o lock_manager.o \
			   log_collector.o redis_bitmap.o redis_bitmap_string.o redis_cmd.o redis_connection.o redis_db.o \
			   redis_hash.o redis_list.o redis_metadata.o redis_slot.o redis_pubsub.o redis_reply.o \
			   redis_request.o redis_set.o redis_string.o redis_zset.o redis_geo.o replication.o \
			   server.o stats.o storage.o task_runner.o util.o geohash.o worker.o redis_sortedint.o \
			   compaction_checker.o table_properties_collector.o
KVROCKS_OBJS= $(SHARED_OBJS) main.o

UNITTEST_OBJS= $(SHARED_OBJS) ../tests/main.o ../tests/t_metadata_test.o ../tests/compact_test.o \
			   ../tests/config_test.o ../tests/cron_test.o ../tests/log_collector_test.o ../tests/cluster_test.o \
			   ../tests/rwlock_test.o ../tests/string_reply_test.o ../tests/string_util_test.o ../tests/t_bitmap_test.o \
			   ../tests/t_encoding_test.o ../tests/t_hash_test.o ../tests/t_list_test.o ../tests/t_set_test.o \
			   ../tests/task_runner_test.o  ../tests/t_string_test.o ../tests/t_zset_test.o ../tests/t_geo_test.o \
			    ../tests/t_sortedint_test.o

K2RDIR= ../tools/kvrocks2redis
KVROCKS2REDIS_OBJS= $(SHARED_OBJS) $(K2RDIR)/main.o $(K2RDIR)/config.o $(K2RDIR)/parser.o \
					$(K2RDIR)/redis_writer.o $(K2RDIR)/sync.o $(K2RDIR)/util.o $(K2RDIR)/writer.o


KVROCKS_CXX=$(QUIET_CXX)$(CXX) $(FINAL_CXXFLAGS)
KVROCKS_LD=$(QUIET_LINK)$(CXX) $(FINAL_CXXFLAGS)
KVROCKS_INSTALL=$(QUIET_INSTALL)$(INSTALL)

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CXX = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_INSTALL = @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
endif

PROG=kvrocks

all: $(PROG) kvrocks2redis
	@echo ""
	@printf $(MAKECOLOR)"Hint: It's a good idea to run 'make test' ;)"$(ENDCOLOR)
	@echo ""

Makefile.dep: $(GLOG) $(LIBEVENT)
	- $(KVROCKS_CXX) -MM *.cc > Makefile.dep 2> /dev/null || true

ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
-include Makefile.dep
endif

.PHONY: all

lint:
	cd ..; ./cpplint.sh
	cd ..; ./cppcheck.sh

ifdef NO_BUILD_DEPENDENCIES
$(PROG): $(KVROCKS_OBJS)
	$(KVROCKS_LD) -o $(PROG) $(KVROCKS_OBJS) $(FINAL_LIBS) $(LDFLAGS)
else
$(PROG): $(GLOG) $(LIBEVENT) $(ROCKSDB) $(KVROCKS_OBJS)
	$(KVROCKS_LD) -o $(PROG) $(KVROCKS_OBJS) $(FINAL_LIBS) $(LDFLAGS)
	
$(GLOG):
	cd $(GLOG_PATH);./autogen.sh; ./configure --disable-shared; \
		$(MAKE) CXXFLAGS='-fPIC' -C $(GLOG_PATH)/

$(LIBEVENT):
	cd $(LIBEVENT_PATH); ./autogen.sh; \
		./configure --enable-shared=no --disable-openssl; \
		$(MAKE) -C $(LIBEVENT_PATH)/

$(ROCKSDB): $(JEMALLOC)
	export ROCKSDB_DISABLE_BZIP=1 ROCKSDB_DISABLE_ZLIB=1 ROCKSDB_DISABLE_LZ4=1; \
	export JEMALLOC=1 JEMALLOC_INCLUDE=-I$(JEMALLOC_PATH)/include JEMALLOC_LIB=$(JEMALLOC); \
	$(MAKE) USE_RTTI=1 PORTABLE=1 -C $(ROCKSDB_PATH)/ static_lib

$(JEMALLOC):
	cd $(JEMALLOC_PATH); ./autogen.sh; \
	   	./configure --enable-autogen --disable-libdl --enable-static --with-jemalloc-prefix=""; \
		$(MAKE) -C $(JEMALLOC_PATH)/
endif

kvrocks2redis: $(PROG) $(KVROCKS2REDIS_OBJS)
	$(KVROCKS_LD) -o kvrocks2redis $(KVROCKS2REDIS_OBJS) $(FINAL_LIBS) $(LDFLAGS)

unittest: $(UNITTEST_OBJS)
	$(KVROCKS_LD) -o unittest $(UNITTEST_OBJS) $(FINAL_LIBS) $(LDFLAGS) -lgtest

test: unittest
	./unittest

%.o: %.cc
	$(KVROCKS_CXX) -c $< -o $@

clean:
	- rm -rf *.o $(PROG) Makefile.dep make_config.mk
	- rm -rf ../tests/*.o unittest
	- rm -rf $(K2RDIR)/*.o kvrocks2redis

distclean: clean
ifndef NO_BUILD_DEPENDENCIES
	- make -C $(ROCKSDB_PATH)/ clean
	- make -C $(LIBEVENT_PATH)/ clean
	- make -C $(JEMALLOC_PATH)/ distclean
	- make -C $(GLOG_PATH)/ distclean
endif

install: all
	@mkdir -p $(INSTALL_DIR)
	$(KVROCKS_INSTALL) $(PROG) $(BIN_DIR)

uninstall:
	rm -rf $(INSTALL_DIR)/$(PROG)
