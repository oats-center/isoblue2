#!/usr/bin/env python

import io
import sys
import re
import struct
import argparse
import json

import avro.schema
import avro.io

import paho.mqtt.client as mqtt

from struct import *
from kafka import KafkaConsumer

from time import sleep

topic = 'remote'

if __name__ == "__main__":

    # avro schema path
    schema_path = '../schema/gps.avsc'

    # load avro schema
    schema = avro.schema.parse(open(schema_path).read())

    consumer = KafkaConsumer(topic, group_id=None)

    client = mqtt.Client(transport='websockets')
    client.connect('localhost', 1883, 60)
    client.loop_start()

    lat = []
    lon = []

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

	if gps_datum['object_name'] == 'TPV':
		client.publish('ib1/gps/tpv', payload=json.dumps(gps_datum['object']))
		print gps_datum['object']
		sleep(0.1)
	
'''	
	if gps_datum['object_name'] == 'TPV':
		gmap = gmplot.GoogleMapPlotter(40.427411, -86.912627, 17.5)
		lat.append(gps_datum['object']['lat'])
		lon.append(gps_datum['object']['lon'])
		gmap.plot(lat, lon, 'cornflowerblue', edge_width=10)
		gmap.draw("/var/www/html/mymap.html")
		insertapikey("/var/www/html/mymap.html", 'AIzaSyB0Dh_TULLREO7cbTXweSffZVrXdY8E37I')
'''
