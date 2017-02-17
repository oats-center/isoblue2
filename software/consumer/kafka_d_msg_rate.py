#!/usr/bin/env python

import io
import sys
import re
import struct

import avro.schema
import avro.io

from struct import *
from kafka import KafkaConsumer

topic = 'dmsgrate'
msg_rate_list = []

if __name__ == "__main__":
    # avro schema path
    schema_path = '../schema/d_msg_rate.avsc'

    # load avro schema
    schema = avro.schema.parse(open(schema_path).read())

    consumer = KafkaConsumer(topic, group_id=None)

    for message in consumer:
        # setup avro decoder
        bytes_reader = io.BytesIO(message.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        msg_rate_datum = reader.read(decoder)

        key = message.key
        key_splited = key.split(':')
        bus = key_splited[0]
        isoblue_id = key_splited[1:]
        frame_rate = msg_rate_datum['msgrate']
        timestamp = msg_rate_datum['timestamp']

        if not msg_rate_list:
            msg_rate = {}
            msg_rate['id'] = isoblue_id
            msg_rate['timestamp'] = timestamp
            msg_rate['frame_rate'] = {bus:frame_rate}
            msg_rate_list.append(msg_rate)

        for i in range(len(msg_rate_list)):
            if msg_rate_list[i]['id'] == isoblue_id:
                msg_rate['timestamp'] = timestamp
                msg_rate_list[i]['frame_rate'][bus] = frame_rate

        if not any(d['id'] == isoblue_id for d in msg_rate_list):
            d = {}
            d['id'] = isoblue_id
            d['timestamp'] = timestamp
            d['frame_rate'] = {bus:frame_rate}
            msg_rate_list.append(d)

        last_x = ''
        for x in msg_rate_list:
            print '' * len(str(last_x)) + '\r',
            print '{}\r'.format(str(x))
            last_x = str(x)
