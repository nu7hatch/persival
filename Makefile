include $(GOROOT)/src/Make.inc

TARG=github.com/nu7hatch/persival
GOFILES=\
	bucket.go \
	log.go

include $(GOROOT)/src/Make.pkg
