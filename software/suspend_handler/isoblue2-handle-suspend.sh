#!/bin/sh

# /usr/lib/systemd/system-sleep

case $1/$2 in
	pre/*)
		echo "Going to $2..."
		systemctl stop kafka-mirror
		systemctl stop kafka-server
		systemctl stop zookeeper
		systemctl stop kafka-can-log-raw
		systemctl stop kafka-gps-log
		systemctl stop can-watchdog
		;;
	post/*)
		echo "Waking up from $2..."
		systemctl restart zookeeper
		systemctl restart kafka-server
		systemctl restart kafka-mirror
		systemctl restart kafka-can-log-raw
		systemctl restart kafka-gps-log
		systemctl restart can-watchdog
		;;
esac
