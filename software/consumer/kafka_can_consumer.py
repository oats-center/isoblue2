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
schema_path = "../kafka_can_log_raw/raw_can.avsc"

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
        # setup decoder
        bytes_reader = io.BytesIO(msg.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        user1 = reader.read(decoder)
        
        # disregard any null data
        if user1["data"] is None:
            continue

        # disregard any rubbish message
        if len(user1["data"]) < 8:
            continue	

        # unpack the binary data and convert it to a list
        data = struct.unpack("BBBBBBBB", user1["data"])
        data_list = list(data)

        # convert arbitration_id to hex, pad 0 to make it length 8
        arbitration_id = (hex(user1["arbitration_id"])[2:]).rjust(8, "0")

        # iterate through data_list and pad 0 if the length is not 2
        for i in range(len(data_list)):
            # convert each number to hex string
            data_list[i] = hex(data_list[i])[2:]
            # pad zero if the hex number length is 1
            if len(data_list[i]) == 1:
               data_list[i] = data_list[i].rjust(2, "0")

        # join hex string into one, make the message hex string
        data_payload = ''.join(data_list)
        message = arbitration_id + data_payload
        parsed_message = parse(message, user1["timestamp"])
        print parsed_message
