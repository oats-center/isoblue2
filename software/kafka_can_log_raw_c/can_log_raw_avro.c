/*
 * Raw CAN frame Kafka producer
 *
 * Author: Yang Wang <wang701@purdue.edu>
 * 				 Alex Layton <awlayton@purdue.edu>
 *
 * Copyright (C) 2016 Purdue University
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

#include <net/if.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/time.h>

#include <linux/can.h>
#include <linux/can/raw.h>

#include <avro.h>

#ifdef DEFLATE_CODEC
#define QUICKSTOP_CODEC  "deflate"
#else
#define QUICKSTOP_CODEC  "null"
#endif


char buf[22];
const char RAW_CAN_SCHEMA[] =
"{\"type\":\"record\",\
  \"name\":\"rawcan\",\
  \"fields\":[\
	{\"name\": \"timestamp\", \"type\": \"double\"},\
	{\"name\": \"is_error_frame\", \"type\": \"boolean\"},\
	{\"name\": \"extended_id\", \"type\": \"boolean\"},\
	{\"name\": \"arbitration_id\", \"type\": \"int\"},\
	{\"name\": \"dlc\", \"type\": \"int\"},\
	{\"name\": \"payload\", \"type\": \"bytes\"},\
	{\"name\": \"is_remote_frame\", \"type\": \"boolean\"}]}";

avro_schema_t raw_can_schema;
avro_writer_t writer;

void print_frame(FILE *fd, char interface[], long sec, long usec,
		struct can_frame *cf) {
	int i;

	/* Output CAN frame */
	fprintf(fd, "<0x%08x> [%u]", cf->can_id, cf->can_dlc);
	for(i = 0; i < cf->can_dlc; i++) {
		fprintf(fd, " %02x", cf->data[i]);
	}
	/* Output timestamp and CAN interface */
	fprintf(fd, "\t%ld.%06ld\t%s\r\n", sec, usec, interface);

	/* Flush output after each frame */
	fflush(fd);
}

/* Parse schema into a schema data structure */
void init_schema(void)
{
	//TODO: load from file
	if (avro_schema_from_json_literal(RAW_CAN_SCHEMA, &raw_can_schema)) {
					fprintf(stderr, "Unable to parse raw can schema\n");
					exit(EXIT_FAILURE);
	}
}

/* Create a datum to match the person schema and save it */
void add_raw_can_frame(FILE *fd, avro_writer_t writer, double timestamp, struct can_frame *cf)
{
	uint8_t is_extended_frame = cf->can_id & CAN_EFF_FLAG;

	avro_datum_t raw_can = avro_record(raw_can_schema);
	avro_datum_t ts_datum = avro_double(timestamp);
	avro_datum_t rf_datum = avro_boolean(cf->can_id & CAN_RTR_FLAG > 0);
	avro_datum_t ie_datum = avro_boolean(cf->can_id & CAN_ERR_FLAG > 0);
	avro_datum_t ei_datum = avro_boolean(is_extended_frame);

	avro_datum_t aid_datum;

	if (is_extended_frame) {
		aid_datum = avro_int32(cf->can_id & CAN_EFF_MASK);
	} else {
		aid_datum = avro_int32(cf->can_id & CAN_SFF_MASK);
	}

  avro_datum_t dlc_datum = avro_int32(cf->can_dlc);
	avro_datum_t pl_datum = avro_bytes(cf->data, 8);

	if (avro_record_set(raw_can, "timestamp", ts_datum)
			|| avro_record_set(raw_can, "is_remote_frame", rf_datum)
			|| avro_record_set(raw_can, "is_error_frame", ie_datum)
			|| avro_record_set(raw_can, "extended_id", ei_datum)
			|| avro_record_set(raw_can, "dlc", dlc_datum)
			|| avro_record_set(raw_can, "payload", pl_datum)
			|| avro_record_set(raw_can, "arbitration_id", aid_datum)) {
		fprintf(stderr, "Unable to set record\n");
		exit(EXIT_FAILURE);
	}

	if (avro_write_data(writer, raw_can_schema, raw_can)) {
					fprintf(stderr, "Unable to write raw_can datum to memory\n");
					exit(EXIT_FAILURE);
	}
/*
        if (avro_file_writer_append(db, raw_can)) {
                fprintf(stderr,
                        "Unable to write raw_can datum to memory buffer\nMessage: %s\n", avro_strerror());
                exit(EXIT_FAILURE);
        }
*/

	int64_t msg_size = avro_size_data(writer, raw_can_schema, raw_can);

	fprintf(stdout, "msg size is %lld\nwriter_tell is %lld\n", msg_size, avro_writer_tell(writer));

	fprintf(stdout, "sizeof(buf[0])=%d\n", sizeof(buf[0]));

	fwrite(buf, sizeof(buf[0]), msg_size, fd);

	avro_writer_reset(writer);

	fflush(fd);

	/* Decrement all our references to prevent memory from leaking */
	avro_datum_decref(ts_datum);
	avro_datum_decref(rf_datum);
	avro_datum_decref(ie_datum);
	avro_datum_decref(ei_datum);
	avro_datum_decref(aid_datum);
	avro_datum_decref(dlc_datum);
	avro_datum_decref(pl_datum);
	avro_datum_decref(raw_can);

	//fprintf(stdout, "Successfully added %s, %s id=%"PRId64"\n", last, first, id);
}

int main(int argc, char *argv[]) {
	int s;
	int rval;

	struct sockaddr_can addr;
	can_err_mask_t err_mask;
	struct ifreq ifr;
	FILE *fo, *fe, *fd;

	const char *dbname = "raw_can.db";

	/* Initialize the schema structure from JSON */
	init_schema();

/*
	remove(dbname);
	rval = avro_file_writer_create(dbname, raw_can_schema, &db);
	if (rval) {
					fprintf(stderr, "There was an error creating %s\n", dbname);
					fprintf(stderr, " error message: %s\n", avro_strerror());
					exit(EXIT_FAILURE);
	}
*/

	writer = avro_writer_memory(buf, sizeof(buf));
	fd = fopen(argv[3], "w");

	if((s = socket(PF_CAN, SOCK_RAW, CAN_RAW)) < 0) {
		return EXIT_FAILURE;
	}

	strcpy(ifr.ifr_name, argv[2]);
	ioctl(s, SIOCGIFINDEX, &ifr);

	/* Listen on all CAN interfaces */
	addr.can_family  = AF_CAN;
	addr.can_ifindex = ifr.ifr_ifindex;

	if(bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		return EXIT_FAILURE;
	}

	/* Receive all error frames */
	err_mask = CAN_ERR_MASK;
	setsockopt(s, SOL_CAN_RAW, CAN_RAW_ERR_FILTER,
			&err_mask, sizeof(err_mask));

	/* Timestamp frames */
	const int val = 1;
	setsockopt(s, SOL_SOCKET, SO_TIMESTAMP, &val, sizeof(val));

	/* Log to first argument as a file */
	if(argc > 1) {
		fo = fe = fopen(argv[1], "w");
	} else {
		fo = stdout;
		fe = stderr;
	}

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
	while(1) {
		// ADDED:
		double timestamp;

		// REMOVE WHEN YOU REMOVE print_frame
		long sec;
		long usec;

		/* Print received CAN frames */
		if(recvmsg(s, &msg, 0) <= 0) {
			perror("recvmsg");
			continue;
		}

		/* Find approximate receive time */
		struct cmsghdr *cmsg;
		for(cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL;
				cmsg = CMSG_NXTHDR(&msg, cmsg)) {
			if(cmsg->cmsg_level == SOL_SOCKET &&
					cmsg->cmsg_type == SO_TIMESTAMP) {
				// ADDED: Moved the time calc here to avoid the memcpy
				struct timeval *tv = (struct timeval *) CMSG_DATA(cmsg);
				timestamp = tv->tv_sec + tv->tv_usec / 1000000.0;
				
				// REMOVE WHEN YOU REMOVE print_frame
				sec = tv->tv_sec;
				usec = tv->tv_usec;
				break;
			}
		}

		/* Find name of receive interface */
		ifr.ifr_ifindex = addr.can_ifindex;
		ioctl(s, SIOCGIFNAME, &ifr);

		/* Print fames to STDOUT, errors to STDERR */
		// REMOVE AT SOME POINT
		if(cf.can_id & CAN_ERR_FLAG) {
			print_frame(fe, ifr.ifr_name, sec, usec, &cf);
		} else {
			print_frame(fo, ifr.ifr_name, sec, usec, &cf);
		}

		// NOTE: Error flag detection is in avro encoding
		add_raw_can_frame(fd, writer, timestamp, &cf);
	}

  avro_schema_decref(raw_can_schema);
	avro_writer_free(writer);

	return EXIT_SUCCESS;
}

