#!/usr/bin/env python

import io
import sys
import re
import struct
import pprint
import argparse

import avro.schema
import avro.io

from kafka import KafkaConsumer
from struct import *

# ISOBUS message masks
MASK_2_BIT = ((1 << 2) - 1)
MASK_3_BIT = ((1 << 3) - 1)
MASK_8_BIT = ((1 << 8) - 1)

# avro schema path
schema_path = "../schema/raw_can.avsc"
cnt = 0

if __name__ == "__main__":
    # setup argparse
    parser = argparse.ArgumentParser(description="Consume Kafka messages.")
    parser.add_argument("-t", action="store", dest="topic", \
            help="Kafka topic for consuming")                         
    parser.add_argument("-a", action="store", dest="auto_offset_reset", \
            default="earliest", help="Auto offset reset")

    args = parser.parse_args()

    if args.topic is None:
        parser.print_help()
        sys.exit("You must specify a topic")

    # initialize the consumer
    consumer = KafkaConsumer(args.topic, \
            auto_offset_reset=args.auto_offset_reset, group_id=None)

    # load avro schema
    schema = avro.schema.parse(open(schema_path).read())

    # isobus message parser
    def parse(hex_message, timestamp=0):
        # J1939 header info:
        # http://www.ni.com/example/31215/en/
        # http://tucrrc.utulsa.edu/J1939_files/HeaderStructure.jpg
        header_hex = hex_message[:8]
        header = int(header_hex, 16)

        src = header & MASK_8_BIT
        header >>= 8
        pdu_ps = header & MASK_8_BIT
        header >>= 8
        pdu_pf = header & MASK_8_BIT
        header >>= 8
        res_dp = header & MASK_2_BIT
        header >>= 2
        priority = header & MASK_3_BIT

        pgn = res_dp
        pgn <<= 8
        pgn |= pdu_pf
        pgn <<= 8
        if pdu_pf >= 240:
            # pdu format 2 - broadcast message. PDU PS is an extension of
            # the identifier
            pgn |= pdu_ps

        payload_bytes = re.findall('[0-9a-fA-F]{2}', hex_message[8:])
        payload_int = int(''.join(reversed(payload_bytes)), 16)

        return {'pgn': pgn,
                'source': src,
                'priority': priority,
                'payload_int': payload_int,
                'payload_bytes': payload_bytes,
                'header': header_hex,
                'message': hex_message,
                'timestamp': timestamp}

    # iterate through received messages
    for msg in consumer:
        cnt = cnt + 1
        sys.stdout.write("\r%d" % (cnt))
