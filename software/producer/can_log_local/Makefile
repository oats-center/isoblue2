CFLAGS := -L/usr/local/lib -Wall
LDLIBS := -lavro -lrdkafka -lpthread -lrt

all:	can_log_local

can_log_local: can_log_local.c

clean:
	-$(RM) can_log_local
