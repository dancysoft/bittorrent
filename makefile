
ARCH ?= $(shell uname)

ifeq ($(ARCH),Darwin)
LISP = /Applications/AllegroCL64.app/Contents/Resources/mlisp
else
LISP ?= mlisp
endif

runlisp = $(LISP) -q -batch -L build.tmp -kill

compile: clean
	rm -f build.tmp
	echo '(setq excl::*break-on-warnings* t)' >> build.tmp
	echo '(load "load.cl")' >> build.tmp
	echo '(exit 0)' >> build.tmp
	$(runlisp)

clean: FORCE
	rm -fr *.fasl *.tmp

FORCE:
