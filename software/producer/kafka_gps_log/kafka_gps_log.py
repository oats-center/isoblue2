#!/usr/bin/env python

import io
import sys
import copy

import avro
import avro.schema
import avro.io

from gps3 import gps3

from kafka import KafkaProducer

from time import sleep
from datetime import datetime

topic = 'remote'
schema_path = '/opt/schema/gps.avsc'
isoblue_id_path = '/opt/id'
#schema_path = '/home/yang/source/isoblue2/software/schema/gps.avsc'
#isoblue_id_path = '/home/yang/source/isoblue2/test/uuid1'
timestamp_last = 0

if __name__ == "__main__":

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
    data_stream = gps3.DataStream()

    try:
        for new_data in s:
            if new_data:
                # unpack the data stream
                data_stream.unpack(new_data)

                # set 'n/a' to None for the purpose of avro serialization
                for key, value in data_stream.TPV.iteritems():
                    if value == 'n/a':
                        data_stream.TPV[key] = None
                for key, value in data_stream.SKY.iteritems():
                    if value == 'n/a':
                        data_stream.SKY[key] = None

                # get the unix timestamp
                timestamp = None
                if data_stream.TPV['time'] is not None:
                    utc_dt = datetime.strptime(data_stream.TPV['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    timestamp = int((utc_dt - datetime(1970, 1, 1)).total_seconds())
                    # ditch duplicate samples
                    # TODO: is there a better way to receive message?
                    if timestamp_last is not None and (timestamp_last - timestamp) == 0:
                        continue

                sat_list = None
                # convert satelittes message to string
                if data_stream.SKY['satellites'] is not None:
                    sat_list = list(data_stream.SKY['satellites'])

                # create avro GPS datum
                gps_datum = avro.io.DatumWriter(schema)
                bytes_writer = io.BytesIO()
                encoder = avro.io.BinaryEncoder(bytes_writer)

                datum = {}
                datum['TPV'] = {
                        'timestamp': timestamp,
                        'lat': data_stream.TPV['lat'],
                        'lon': data_stream.TPV['lon'],
                        'alt': data_stream.TPV['alt'],
                        'epx': data_stream.TPV['epx'],
                        'epy': data_stream.TPV['epy'],
                        'epv': data_stream.TPV['epv'],
                        'track': data_stream.TPV['track'],
                        'speed': data_stream.TPV['speed'],
                        'climb': data_stream.TPV['climb'],
                        'epd': data_stream.TPV['epd'],
                        'eps': data_stream.TPV['eps'],
                        'epc': data_stream.TPV['epc']
                        }
                datum['SKY'] = {
                        "xdop": data_stream.SKY['xdop'],
                        "ydop": data_stream.SKY['ydop'],
                        "vdop": data_stream.SKY['vdop'],
                        "tdop": data_stream.SKY['tdop'],
                        "hdop": data_stream.SKY['hdop'],
                        "pdop": data_stream.SKY['pdop'],
                        "gdop": data_stream.SKY['gdop'],
                        "satellites": sat_list 
                        }

                # write to the datum
                gps_datum.write(datum, encoder)
                
                # produce the message to Kafka
                gps_buf = bytes_writer.getvalue()
                producer.send(topic, key='gps:' + isoblue_id, value=gps_buf)

                timestamp_last = timestamp

    except KeyboardInterrupt:
        s.close()
