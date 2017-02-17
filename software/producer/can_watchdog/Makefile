CFLAGS := -L/usr/local/lib -Wall
LDLIBS := -lavro -lrdkafka -lpthread -lrt

all:	can_watchdog

can_watchdog: can_watchdog.c

clean:
	-$(RM) can_watchdog
