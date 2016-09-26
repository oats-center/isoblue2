#!/usr/bin/env python

import time
import signal
import sys
import os

import can

from CanListenerWatchdog import CanListenerWatchdog

if __name__ == "__main__":

    def suspendHandler():
        os.system("echo mem > /sys/power/state")

    def sigterm_handler(signal, frame):
        bus0.shutdown()
        notifier0.stop()
        bus1.shutdown()
        notifier1.stop()
        sys.exit(0)

    channel0 = 'ib_imp'
    channel1 = 'ib_eng'

    bus0 = can.interface.Bus(channel0, bustype='socketcan')
    bus1 = can.interface.Bus(channel1, bustype='socketcan')

    watchdog0 = CanListenerWatchdog(10, suspendHandler)
    watchdog1 = CanListenerWatchdog(10, suspendHandler)

    notifier0 = can.Notifier(bus0, [watchdog0], timeout=0.1)
    notifier1 = can.Notifier(bus1, [watchdog1], timeout=0.1)

    while True:
        time.sleep(1)
        signal.signal(signal.SIGTERM, sigterm_handler)
