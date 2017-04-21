#!/bin/sh

SIZE1="$(du -a -c /media/sda1/kafka-logs/gps-0 | grep 'total' | cut -d$'\t' -f1)"
sleep 5
SIZE2="$(du -a -c /media/sda1/kafka-logs/gps-0 | grep 'total' | cut -d$'\t' -f1)"

if [ "$SIZE1" -eq "$SIZE2" ]; then
       echo "gps log size didn't change. Restarting gpsd, gps-log services..."
       systemctl stop gpsd
       systemctl stop gps-log@remote
       systemctl stop gps-log@gps
       systemctl start gpsd
       systemctl start gps-log@remote
       systemctl start gps-log@gps 
else
       echo "gps log size increased. No action required."
fi       
