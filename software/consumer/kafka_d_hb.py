#!/usr/bin/env python

import io
import sys
import re
import struct

import avro.schema
import avro.io

from struct import *
from kafka import KafkaConsumer

topic = 'debug'

if __name__ == "__main__":
    # avro schema path
    schema_path = '../schema/d_hb.avsc'

    # load avro schema
    schema = avro.schema.parse(open(schema_path).read())

    consumer = KafkaConsumer(topic, group_id=None)

    for message in consumer: 
        # disregard any message that does not have heartbeat key
        key_splited = message.key.split(':')
        if key_splited[0] != 'hb':
            continue

        # setup avro decoder
        bytes_reader = io.BytesIO(message.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        hb_datum = reader.read(decoder)

        isoblue_id = key_splited[1:]
        hb = hb_datum['heartbeat']
        timestamp = hb_datum['timestamp']

        if hb is True:
            print str(isoblue_id), 'was alive at', str(timestamp)
        else:
            print str(isoblue_id), 'was dead at', str(timestamp)
            
