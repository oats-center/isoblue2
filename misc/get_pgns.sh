#!/bin/sh

curl -o /opt/tmp_pgns http://cloudradio39.ecn.purdue.edu/pgns -m 30 > /dev/null 2>&1

if [ $? != 0 ]; then
	echo 'curl failed, check network status'
  udevadm trigger && echo 'try retriggering udev to setup Internet ...'
	exit 1
fi

diff /opt/pgns /opt/tmp_pgns > /dev/null 2>&1

if [ $? -eq 0 ]; then
	echo 'no changes in PGN list'
	rm -f /opt/tmp_pgns
else
	mv /opt/tmp_pgns /opt/pgns
	echo 'new changes in PGN list'
	for pid in `ps aux | grep --regexp="[k]afka_can_log.*\-f" | awk '{print $2}'`; do
		kill -s USR1 $pid
	done
fi
