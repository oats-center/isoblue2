CFLAGS := -L/usr/local/lib -Wall -DDEBUG
LDLIBS := -lavro -lrdkafka -lpthread -lrt

all:	can_filtered_log

can_filtered_log: can_filtered_log.c

clean:
	-$(RM) can_filtered_log
