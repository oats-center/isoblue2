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
    schema_path = '../schema/gps.avsc'

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

        timestamp = gps_datum['timestamp']
        lat = gps_datum['lat']
        lon = gps_datum['lon']
        alt = gps_datum['alt']
        epx = gps_datum['epx']
        epy = gps_datum['epy']
        epv = gps_datum['epv']
        track = gps_datum['track']
        speed = gps_datum['speed']
        climb = gps_datum['climb']
        epd = gps_datum['epd']
        eps = gps_datum['eps']
        epc = gps_datum['epc']
        satellites = gps_datum['satellites']

        #print isoblue_id, lat, lon, alt, epx, epy, epv, track, speed, climb, epd, eps, epc, satellites
        print timestamp, lat, lon, speed 
