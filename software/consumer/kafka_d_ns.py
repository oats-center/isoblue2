#!/usr/bin/env python

import io
import sys
import re
import struct
import json

import avro.schema
import avro.io

import paho.mqtt.client as mqtt

from struct import *
from kafka import KafkaConsumer

from time import sleep

topic = 'debug'

if __name__ == "__main__":
    # avro schema path
    schema_path = '../schema/d_ns.avsc'

    # load avro schema
    schema = avro.schema.parse(open(schema_path).read())

    consumer = KafkaConsumer(topic, group_id=None)

    client = mqtt.Client(transport='websockets')
    client.connect('localhost', 1883, 60)
    client.loop_start()

    for message in consumer: 
        # disregard any message that does not have heartbeat key
        key_splited = message.key.split(':')
        if key_splited[0] != 'ns':
            continue

        # setup avro decoder
        bytes_reader = io.BytesIO(message.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        ns_datum = reader.read(decoder)

        isoblue_id = key_splited[1:]
        ns = ns_datum['strength']
        timestamp = ns_datum['timestamp']

        if ns:
            client.publish('ib1/ns', payload=json.dumps(ns_datum))
            print str(isoblue_id), 'network strength is', ns, 'at', str(timestamp)
            sleep(0.1)
