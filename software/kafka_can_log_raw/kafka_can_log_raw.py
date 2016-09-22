#!/usr/bin/env python

import datetime
import time

import can

from KafkaWriter import KafkaWriter

if __name__ == "__main__":

    channel0 = 'ib_imp'
    channel1 = 'ib_eng'

    bus0 = can.interface.Bus(channel0, bustype='socketcan')
    bus1 = can.interface.Bus(channel1, bustype='socketcan')

    kafka_writer0 = KafkaWriter('can0', 'localhost')
    kafka_writer1 = KafkaWriter('can1', 'localhost')

    notifier0 = can.Notifier(bus0, [kafka_writer0], timeout=0.1)
    notifier1 = can.Notifier(bus1, [kafka_writer1], timeout=0.1)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        bus0.shutdown()
        notifier0.stop()
        bus1.shutdown()
        notifier1.stop()
