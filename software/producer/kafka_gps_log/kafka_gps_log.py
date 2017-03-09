#!/usr/bin/env python

import io
import sys
import json
import argparse

import avro
import avro.schema
import avro.io

from gps3 import gps3

from kafka import KafkaProducer

from time import sleep
from datetime import datetime

schema_path = '/opt/schema/gps.avsc'
isoblue_id_path = '/opt/id'
#schema_path = '/home/yang/source/isoblue2/software/schema/test.avsc'
#isoblue_id_path = '/home/yang/source/isoblue2/test/uuid1'

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Push GPS data to Kafka.")
    parser.add_argument("-t", action="store", dest="topic", \
            help="Kafka topic")                         
    args = parser.parse_args()

    if args.topic is None:
        parser.print_help()
        sys.exit("You must specify a topic")

    # create kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost')

    # load avro schema and setup encoder
    try:
        f_s = open(schema_path)
        schema = avro.schema.parse(f_s.read())
        f_s.close()
    except IOError:
        print('cannot open schema file')
        sys.exit()

    try:
        f_id = open(isoblue_id_path)
        isoblue_id = f_id.read().strip('\n')
        f_id.close()
    except IOError:
        print('cannot open isoblue_id file')
        sys.exit()

    s = gps3.GPSDSocket()
    s.connect(host='127.0.0.1', port=2947)
    s.watch()

    timestamp = None
    last_tpv_timestamp = None

    try:
        for data in s:
            if data:
                new_data = json.loads(data)
                object_name = new_data.pop('class', 'ERROR')

                # convert 'n/a' to None for proper
                for key, value in new_data.iteritems():
                    if value == 'n/a':
                        new_data[key] = None
            
                # the object should be TPV now
                if object_name == 'TPV':
                    if new_data['time']:
                        utc_dt = datetime.strptime(new_data['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        timestamp = int((utc_dt - datetime(1970, 1, 1)).total_seconds())
                        new_data['time'] = timestamp
                        
                    last_tpv_timestamp = timestamp
                # the object should be SKY
                elif object_name == 'SKY':
                    # do we need anything else?
                    pass
                # the object should be PPS 
                elif object_name == 'PPS':
                    # do we need anything else?
                    pass
                # ditch other samples
                else:
                    continue

                # create the datum
                datum = {}
                datum['object_name'] = object_name
                datum['object'] = new_data
                if object_name == 'SKY':
                    datum['object']['time'] = last_tpv_timestamp

                gps_datum = avro.io.DatumWriter(schema)
                bytes_writer = io.BytesIO()
                encoder = avro.io.BinaryEncoder(bytes_writer)

                # write to the datum
                gps_datum.write(datum, encoder)
                # produce the message to Kafka
                gps_buf = bytes_writer.getvalue()
                producer.send(args.topic, key='gps:' + isoblue_id, value=gps_buf)

                datum = {}
                gps_datum = None

                sleep(0.1)

    except KeyboardInterrupt:

        s.close()
