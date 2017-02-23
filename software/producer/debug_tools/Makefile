CFLAGS := -L/usr/local/lib -Wall
LDLIBS := -lavro -lrdkafka -lpthread -lrt

all:	can_msg_rate heartbeat net_strength

can_msg_rate: can_msg_rate.c

heartbeat: heartbeat.c

net_strength: net_strength.c

clean:
	-$(RM) can_msg_rate
	-$(RM) heartbeat 
	-$(RM) net_strength 
