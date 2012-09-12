TARGET=s3rocket

CC=gcc
CFLAGS=-g -O2 $(shell curl-config --cflags) -Wall
LDFLAGS=-ls3 -lpthread -lm $(shell curl-config --libs)
LD=gcc
UNAME=$(shell uname)

#
# GnuTLS needs to have it's special init function called in order
# to work in multi-threaded processes.
#
# There's no easy way to detect if this is necessary, since GnuTLS
# is an indirect dependency via libcurl, and libcurl can be built
# with or without it.
#
# Up-to-date debian/ubuntu installs seem to use gnutls for curl by default, 
# so on linux, we do it always. If this causes problems, try disabling it
#
ifeq ($(UNAME), Linux)
    CFLAGS += -DHAVE_GNUTLS
endif

SOURCES =	\
    main.c	\

ifndef DESTDIR
    DESTDIR := /usr
endif

OBJECTS=$(SOURCES:%.c=build/%.o)

default : $(TARGET)

build/%.o : %.c build
	$(CC) $(CFLAGS) -c -o $@ $<
	$(CC) -MM $(CLFAGS) $< > build/$*.d

$(TARGET): $(OBJECTS)
	$(LD) -o $(TARGET) $(OBJECTS) $(LDFLAGS)

.PHONY : clean
clean: 
	rm -Rf build/*
	rm -f $(TARGET)

build:
	@mkdir -p build

install: $(TARGET)
	install -m 0755 $(TARGET) $(DESTDIR)/bin/$(TARGET)

