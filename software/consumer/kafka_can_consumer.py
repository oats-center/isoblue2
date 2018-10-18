#!/usr/bin/env python

import io
import sys
import re
import struct
import pprint
import argparse
import time
import ast
import json

import avro.schema
import avro.io

from kafka import KafkaConsumer
from kafka import TopicPartition
from struct import *

# ISOBUS message masks
MASK_2_BIT = ((1 << 2) - 1)
MASK_3_BIT = ((1 << 3) - 1)
MASK_8_BIT = ((1 << 8) - 1)

# avro schema path
schema_path = "/home/yang/isoblue2/software/schema/raw_can.avsc"
cnt = 0

if __name__ == "__main__":
    # setup argparse
    parser = argparse.ArgumentParser(description="Consume Kafka messages.")
    parser.add_argument("-t", action="store", dest="topic", \
            help="Kafka topic for consuming")
    parser.add_argument("-a", action="store", dest="auto_offset_reset", \
            default="latest", help="Auto offset reset")
    parser.add_argument("-i", action="store", dest="ibid", \
            help="ISOBlue ID")

    args = parser.parse_args()

    if args.topic is None:
        parser.print_help()
        sys.exit("You must specify a topic")

    if args.ibid is None:
        parser.print_help()
        sys.exit("You must supply an ISOBlue ID")

    # initialize the consumer
    consumer = KafkaConsumer(args.topic, \
            auto_offset_reset=args.auto_offset_reset, group_id=None)

    #consumer = KafkaConsumer()
    #partition = TopicPartition('remote', 0)
    #consumer.assign([partition])
    #consumer.seek(partition, 136802396)

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
        # check keys
        # disregard any message that does not have imp/tra key
        key_splited = msg.key.split(':')

        if len(key_splited) == 3:
            isoblue_id = key_splited[2]
            if isoblue_id == args.ibid:
                if key_splited[0] == 'imp' or key_splited[0] == 'tra':
                    # setup decoder
                    bytes_reader = io.BytesIO(msg.value)
                    decoder = avro.io.BinaryDecoder(bytes_reader)
                    reader = avro.io.DatumReader(schema)
                    raw_can_datum = reader.read(decoder)

                    # unpack the binary data and convert it to a list
                    data = struct.unpack("BBBBBBBB", raw_can_datum["payload"])
                    data_list = list(data)

                    # convert arbitration_id to hex, pad 0 to make it length 8
                    arbitration_id = (hex(raw_can_datum["arbitration_id"])[2:]).rjust(8, "0")

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
                    parsed_message = parse(message, raw_can_datum["timestamp"])
                    print parsed_message['timestamp'], parsed_message['pgn'], ''.join(parsed_message['payload_bytes'])
