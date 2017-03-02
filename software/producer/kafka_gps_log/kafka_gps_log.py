#!/usr/bin/env python

import io
import sys

import avro
import avro.schema
import avro.io

from gps3.agps3threaded import AGPS3mechanism

from kafka import KafkaProducer

from time import sleep
from datetime import datetime

topic = 'gps'

#schema_path = '/opt/schema/gpsd.avsc'
#isoblue_id_path = '/opt/id'
schema_path = '/home/yang/source/isoblue2/software/schema/gps.avsc'
isoblue_id_path = '/home/yang/source/isoblue2/test/uuid1'

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
        
    # set up gps thread
    agps_thread = AGPS3mechanism()
    agps_thread.stream_data()
    agps_thread.run_thread()

    while True:
        timestamp = None
        satellites = None
        # set 'n/a' to None for object attributes
        for attr, value in agps_thread.data_stream.__dict__.iteritems():
            #print attr, 'is', value
            if value == 'n/a':
                setattr(agps_thread.data_stream, attr, None)
        if agps_thread.data_stream.time is not None:
            utc_dt = datetime.strptime(agps_thread.data_stream.time, '%Y-%m-%dT%H:%M:%S.%fZ')
            timestamp = int((utc_dt - datetime(1970, 1, 1)).total_seconds())
        if agps_thread.data_stream.satellites is not None:
            satellites = str(agps_thread.data_stream.satellites)

        lat = agps_thread.data_stream.lat
        lon = agps_thread.data_stream.lon
        alt = agps_thread.data_stream.alt
        epx = agps_thread.data_stream.epx
        epy = agps_thread.data_stream.epy
        epv = agps_thread.data_stream.epv
        track = agps_thread.data_stream.track
        speed = agps_thread.data_stream.speed
        climb = agps_thread.data_stream.climb
        epd = agps_thread.data_stream.epd
        eps = agps_thread.data_stream.eps
        epc = agps_thread.data_stream.epc

        #print('---------------------')
        #print(                   agps_thread.data_stream.time)
        #print('UNIX ts:{}    '.format(timestamp))
        #print('Lat:{}   '.format(agps_thread.data_stream.lat))
        #print('Lon:{}   '.format(agps_thread.data_stream.lon))
        #print('Speed:{} '.format(agps_thread.data_stream.speed))
        #print('---------------------')

        gps_datum = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        gps_datum.write({
            "timestamp":timestamp,
            "lat":lat,
            "lon":lon,
            "alt":alt,
            "epx":epx,
            "epy":epy,
            "epv":epv,
            "track":track,
            "speed":speed,
            "climb":climb,
            "epd":epd,
            "eps":eps,
            "epc":epc,
            "satellites":satellites 
            },
            encoder)

        gps_buf = bytes_writer.getvalue()
        producer.send(topic, key=isoblue_id, value=gps_buf)

        #TODO: by removing the sleep(1), epd somehow won't set to None
        #      and error out avro serialization
        sleep(1)
