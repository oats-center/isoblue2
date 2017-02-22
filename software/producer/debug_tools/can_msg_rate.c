/*
 * CAN Frame Message Rate Kafka Producer 
 *
 * Author: Yang Wang <wang701@purdue.edu>
 *
 * Copyright (C) 2017 Purdue University
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

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
static char key[100];
static char *brokers = "localhost:9092";

const char D_MSG_RATE_SCHEMA[] =
"{\"type\":\"record\",\
  \"name\":\"dmsgrate\",\
  \"fields\":[\
	{\"name\": \"timestamp\", \"type\": \"double\"},\
	{\"name\": \"msgrate\", \"type\": \"int\"}]}";

/* Timer handler */
void timer_handler(int signum) {
	/* Kafka static variables */
	static rd_kafka_t *rk = NULL;
	static rd_kafka_topic_t *rkt = NULL;
	static rd_kafka_conf_t *conf = NULL;
	static rd_kafka_topic_conf_t *topic_conf = NULL;
	static char errstr[512];

	/* Avro static variables */
	static avro_writer_t writer = NULL;
	static avro_schema_t d_msg_rate_schema = NULL;
	static avro_datum_t d_msg_rate = NULL;
	static char buf[20];

	/* timeval struct */
	struct timeval tp;
	double timestamp;

	/* Broker conf */
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

	/* Kafka topic conf */
	if (topic_conf == NULL) {
		topic_conf = rd_kafka_topic_conf_new();
		rd_kafka_topic_conf_set(topic_conf, "request.required.acks", "0", errstr,
				sizeof(errstr));
	}

	/* Create Kafka producer */
	if (rk == NULL) {
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

	/* Initialize the schema structure from JSON */
	if (d_msg_rate_schema == NULL) {
		if (avro_schema_from_json_literal(D_MSG_RATE_SCHEMA, &d_msg_rate_schema)) {
			fprintf(stderr, "Unable to parse d_msg_rate schema\n");
			exit(EXIT_FAILURE);
		}
	}

	/* Create avro writer */
	if (writer == NULL) {
		writer = avro_writer_memory(buf, sizeof(buf));
	}

	/* Create avro record based on the schema */
	if (d_msg_rate == NULL) { 
		d_msg_rate = avro_record(d_msg_rate_schema);
	}

	gettimeofday(&tp, NULL);
	timestamp = tp.tv_sec + tp.tv_usec / 1000000.0;

	avro_datum_t ts_datum = avro_double(timestamp);
	avro_datum_t mr_datum = avro_int32(frame_cnt);

	if (avro_record_set(d_msg_rate, "timestamp", ts_datum)
		|| avro_record_set(d_msg_rate, "msgrate", mr_datum)) {
		fprintf(stderr, "Unable to set record to d_msg_rate\n");
		exit(EXIT_FAILURE);
	}

	if (avro_write_data(writer, d_msg_rate_schema, d_msg_rate)) {
		fprintf(stderr, "unable to write d_msg_rate datum to memory\n");
		exit(EXIT_FAILURE);
	}

	if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
			buf, avro_writer_tell(writer), key, strlen(key) - 1, NULL) == -1) {
		fprintf(stderr, "%% Failed to produce to topic %s "
						"partition %i: %s\n",
						rd_kafka_topic_name(rkt), RD_KAFKA_PARTITION_UA,
						rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_poll(rk, 0);
	}

	rd_kafka_poll(rk, 0);

	/* Clear the count */
	frame_cnt = 0;
	
	/* Decrement all our references to prevent memory from leaking */
	avro_datum_decref(ts_datum);
	avro_datum_decref(mr_datum);

	/* Reset the writer */
	avro_writer_reset(writer);
}

int main(int argc, char *argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Invalid arguments\n");
		return EXIT_FAILURE;
	}
	/* fp for opening uuid file */
	FILE *fp;
	char *id = 0;
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
		id = malloc(length);
		if (id) {
			fread(id, 1, length, fp);
			if (id[length - 1] == '\n') {
				id[--length] = '\0';
			}
		}
		fclose(fp);
	} else {
		perror("ISOBlue ID file");
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
		perror("socket");
		return EXIT_FAILURE;
	}

	/* Get the interface ID from arguments */
	strcpy(ifr.ifr_name, argv[2]);
	ioctl(s, SIOCGIFINDEX, &ifr);

	/* Create the key */
	strcpy(key, ifr.ifr_name);
	strcat(key, ":");
	strcat(key, id);

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
