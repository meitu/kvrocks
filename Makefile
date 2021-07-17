# Top level makefile, the real shit is at src/Makefile

default: all

ifndef NO_BUILD_DEPENDENCIES
clone_dependencies := $(shell sh -c 'git submodule init && git submodule update')
endif

.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

.PHONY: install
