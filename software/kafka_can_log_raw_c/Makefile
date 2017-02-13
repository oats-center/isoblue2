CFLAGS := -L/usr/local/lib -Wall
LDLIBS := -lavro -lrdkafka -lpthread -lrt

all:	can_log_raw_avro

can_log_raw_avro: can_log_raw_avro.c

clean:
	-$(RM) can_log_raw_avro
