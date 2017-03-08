#!/usr/bin/env python

import io
import sys
import re
import struct
import argparse

import avro.schema
import avro.io

from struct import *
from kafka import KafkaConsumer

topic = 'remote'

if __name__ == "__main__":

    # avro schema path
    schema_path = '../schema/test.avsc'

    # load avro schema
    schema = avro.schema.parse(open(schema_path).read())

    consumer = KafkaConsumer(topic, group_id=None)

    for message in consumer: 
        key_splited = message.key.split(':')
        if key_splited[0] != 'gps':
            continue

        isoblue_id = key_splited[1]

        # setup avro decoder
        bytes_reader = io.BytesIO(message.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        gps_datum = reader.read(decoder)

        print gps_datum
        print ''
