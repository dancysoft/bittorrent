
acldir = /Applications/AllegroCL64.app/Contents/Resources
mlisp = $(acldir)/mlisp

compile: clean
	rm -fr build.tmp
	echo '(setq excl::*break-on-warnings* t)' >> build.tmp
	echo '(load "load.cl")' >> build.tmp
	echo '(exit 0)' >> build.tmp
	$(mlisp) -batch -L build.tmp -kill

clean: FORCE
	rm -fr *.fasl *.tmp

FORCE:
