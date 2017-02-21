CFLAGS := -L/usr/local/lib -Wall
LDLIBS := -lavro -lrdkafka -lpthread -lrt

all:	kafka_can_log

kafka_can_log: kafka_can_log.c

clean:
	-$(RM) kafka_can_log
