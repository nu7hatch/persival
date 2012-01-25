include $(GOROOT)/src/Make.inc

TARG=github.com/nu7hatch/persival
GOFILES=\
	persival.go \
	log.go

include $(GOROOT)/src/Make.pkg
