#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <errno.h>

#include <net/if.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/time.h>

#include <linux/can.h>
#include <linux/can/raw.h>

#include <avro.h>
#include <librdkafka/rdkafka.h>

static int frame_cnt = 0;
static char *key = 0;
static char *brokers = "localhost:9092";

/* Timer handler */
void timer_handler(int signum) {
	static rd_kafka_t *rk = NULL;
	static rd_kafka_topic_t *rkt = NULL;
	static rd_kafka_conf_t *conf = NULL;
	static rd_kafka_topic_conf_t *topic_conf = NULL;
	static char errstr[512];

	if (conf == NULL) {
		conf = rd_kafka_conf_new();
		/* Kafka conf */
		rd_kafka_conf_set(conf, "batch.num.messages", "20000", errstr,
				sizeof(errstr));
		rd_kafka_conf_set(conf, "queue.buffering.max.messages", "1000000", errstr,
				sizeof(errstr));
		rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1", errstr,
				sizeof(errstr));
		rd_kafka_conf_set(conf, "log.connection.close", "false", errstr,
				sizeof(errstr));
	}

	if (topic_conf == NULL) {
		topic_conf = rd_kafka_topic_conf_new();
		/* Kafka topic conf */
		rd_kafka_topic_conf_set(topic_conf, "request.required.acks", "0", errstr,
				sizeof(errstr));
	}

	if (rk == NULL) {
		/* Create Kafka producer */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
			fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
		}
	}
	
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		fprintf(stderr, "%% No valid brokers specified\n");
		exit(EXIT_FAILURE);
	}

	if (rkt == NULL) {
		/* Create new Kafka topic */
		rkt = rd_kafka_topic_new(rk, "dmsgrate", topic_conf);
	}

	if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
			&frame_cnt, sizeof(frame_cnt), key, strlen(key) - 1, NULL) == -1) {
		fprintf(stderr, "%% Failed to produce to topic %s "
						"partition %i: %s\n",
						rd_kafka_topic_name(rkt), RD_KAFKA_PARTITION_UA,
						rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_poll(rk, 0);
	}

	rd_kafka_poll(rk, 0);

	frame_cnt = 0;
}

int main(int argc, char *argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Invalid arguments\n");
		return EXIT_FAILURE;
	}
	/* fp for opening uuid file */
	FILE *fp;
	char *uuid = 0;
	long length;

	/* Timer stuff variables */
	struct sigaction sa;
	struct itimerval timer;

	/* CAN stuff variables */
	int s;
	struct sockaddr_can addr;
	struct ifreq ifr;
	can_err_mask_t err_mask;

	/* Get the id */
	fp = fopen(argv[1], "r");
	if (fp) {
		fseek(fp, 0, SEEK_END);
		length = ftell(fp);
		fseek(fp, 0, SEEK_SET);
		uuid = malloc(length - 1);
		if (uuid) {
			fread(uuid, 1, length - 1, fp);
		}
		fclose(fp);
	} else {
		perror("ID file");
		return EXIT_FAILURE;
	}

	/* Install timer_handler as the signal handler for SIGVTALRM. */
	memset (&sa, 0, sizeof(sa));
	sa.sa_handler = &timer_handler;
	sigaction(SIGALRM, &sa, NULL);

	/* Configure the timer to expire after 1 sec... */
	timer.it_value.tv_sec = 1;
	timer.it_value.tv_usec = 0;
	/* ... and every 1 sec after that. */
	timer.it_interval.tv_sec = 1;
	timer.it_interval.tv_usec = 0;

	/* Start a real timer. It counts down whenever this process is
	 * executing. */
	setitimer(ITIMER_REAL, &timer, NULL);

	/* Create CAN socket */
	if ((s = socket(PF_CAN, SOCK_RAW, CAN_RAW)) < 0) {
		return EXIT_FAILURE;
	}

	/* Get the interface ID from arguments */
	strcpy(ifr.ifr_name, argv[2]);
	ioctl(s, SIOCGIFINDEX, &ifr);

	/* Create the key */
	char *tmp = (char *) malloc(1 + strlen("-") + strlen(ifr.ifr_name));
	strcpy(tmp, ifr.ifr_name);
	strcat(tmp, "-");
	key = (char *) malloc(1 + strlen(tmp) + strlen(uuid)); 
	strcpy(key, tmp);
	strcat(key, uuid);

	free(tmp);

	/* Listen on specified CAN interfaces */
	addr.can_family  = AF_CAN;
	addr.can_ifindex = ifr.ifr_ifindex;

	if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		return EXIT_FAILURE;
	}

	/* Receive all error frames */
	err_mask = CAN_ERR_MASK;
	setsockopt(s, SOL_CAN_RAW, CAN_RAW_ERR_FILTER, &err_mask, sizeof(err_mask));

	/* Timestamp frames */
	const int val = 1;
	setsockopt(s, SOL_SOCKET, SO_TIMESTAMP, &val, sizeof(val));

	/* Buffer received CAN frames */
	struct can_frame cf;
	struct msghdr msg = { 0 };
	struct iovec iov = { 0 };
	char ctrlmsg[CMSG_SPACE(sizeof(struct timeval))];

	/* Construct msghdr to use to recevie messages from socket */
	msg.msg_name = &addr;
	msg.msg_namelen = sizeof(addr);
	msg.msg_control = ctrlmsg;
	msg.msg_controllen = sizeof(ctrlmsg);
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	iov.iov_base = &cf;
	iov.iov_len = sizeof(cf);

	while (1) {
		/*
		double timestamp;
		long sec;
		long usec;
		*/

		/* Recieve CAN frames */
		if (recvmsg(s, &msg, 0) <= 0) {
			if (errno != EINTR) {
				perror("recvmsg");
			}
			continue;
		}

		frame_cnt++;
	}

	return EXIT_SUCCESS;
}
