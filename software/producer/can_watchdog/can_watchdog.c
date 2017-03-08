/*
 * CAN Activity Watchdog 
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

static int frame_cnt = 0;

/* Timer handler */
void timer_handler(int signum) {
	if (frame_cnt == 0) {
		// HACK: to handle wakeup after suspend
		// sleep 1 second to have frame_cnt incremented.
		sleep(1);
#if DEBUG
		printf("No CAN activity in 1 min. Shutting down the ISOBlue...\n");
#endif
		system("echo mem > /sys/power/state");
	} else {
		/* Clear the count */
		frame_cnt = 0;
	}
}

int main(int argc, char *argv[]) {
	/* Timer stuff variables */
	struct sigaction sa;
	struct itimerval timer;

	/* CAN stuff variables */
	int s;
	struct sockaddr_can addr;
	can_err_mask_t err_mask;

	/* Install timer_handler as the signal handler for SIGVTALRM. */
	memset (&sa, 0, sizeof(sa));
	sa.sa_handler = &timer_handler;
	sigaction(SIGALRM, &sa, NULL);

	/* Configure the timer to expire after 1 min... */
	timer.it_value.tv_sec = 60;
	timer.it_value.tv_usec = 0;
	/* ... and every 1 min after that. */
	timer.it_interval.tv_sec = 60;
	timer.it_interval.tv_usec = 0;

	/* Start a real timer. It counts down whenever this process is
	 * executing. */
	setitimer(ITIMER_REAL, &timer, NULL);

	/* Create CAN socket */
	if ((s = socket(PF_CAN, SOCK_RAW, CAN_RAW)) < 0) {
		return EXIT_FAILURE;
	}

	/* Listen on specified CAN interfaces */
	addr.can_family  = AF_CAN;
	addr.can_ifindex = 0;

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
		/* Recieve CAN frames */
		if (recvmsg(s, &msg, 0) <= 0) {
			if (errno != EINTR) {
				perror("recvmsg");
			}
			continue;
		}
	
		/* Increase the count if there is CAN activity */
		frame_cnt++;
	}

	return EXIT_SUCCESS;
}
