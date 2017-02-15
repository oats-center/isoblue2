CFLAGS := -L/usr/local/lib -Wall
LDLIBS := -lavro -lrdkafka -lpthread -lrt

all:	can_msg_rate

can_msg_rate: can_msg_rate.c

clean:
	-$(RM) can_msg_rate
