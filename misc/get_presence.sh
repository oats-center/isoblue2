#!/bin/sh

cnt1=`ps aux | grep --regexp="[k]afka_can.*" | wc -l`
cnt2=`ps aux | grep --regexp="[k]afka_gps.*" | wc -l`

if [ $cnt1 -eq 4 ] && [ $cnt2 -eq 2 ]; then
	echo 0 > /sys/class/leds/LED_5_RED/brightness
	echo 255 > /sys/class/leds/LED_5_GREEN/brightness
else
	echo 0 > /sys/class/leds/LED_5_GREEN/brightness
	echo 255 > /sys/class/leds/LED_5_RED/brightness
fi
