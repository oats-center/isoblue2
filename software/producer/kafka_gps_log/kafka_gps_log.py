#!/usr/bin/env python

import io
import sys
import copy

import avro
import avro.schema
import avro.io

from gps3.agps3threaded import AGPS3mechanism

from kafka import KafkaProducer

from time import sleep
from datetime import datetime

topic = 'remote'
schema_path = '/opt/schema/gps.avsc'
isoblue_id_path = '/opt/id'
#schema_path = '/home/yang/source/isoblue2/software/schema/gps.avsc'
#isoblue_id_path = '/home/yang/source/isoblue2/test/uuid1'
timestamp_last = None
duplicates = 0

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

        # copy the object
        data_stream_copy = copy.deepcopy(agps_thread.data_stream)

        # set 'n/a' to None for object attributes
        for attr, value in data_stream_copy.__dict__.iteritems():
            #print attr, 'is', value
            if value == 'n/a':
                #print '*****************'
                #print attr, 'is n/a'
                setattr(data_stream_copy, attr, None)
                #print 'now', attr, 'is', getattr(data_stream_copy, attr)
                #print '*****************'

        # convert time to unix timestamp
        if data_stream_copy.time is not None:
            utc_dt = datetime.strptime(data_stream_copy.time, '%Y-%m-%dT%H:%M:%S.%fZ')
            timestamp = int((utc_dt - datetime(1970, 1, 1)).total_seconds())
            # ditch duplicate timestamps
            if timestamp_last is not None and (timestamp_last - timestamp) == 0:
                #print '!!!!!!!!!!!!!!!!!'
                #print '\tduplicate'
                #print '!!!!!!!!!!!!!!!!!'
                duplicates = duplicates + 1
                if duplicates > 10:
                    print 'Too many duplicates, are we having good GPS fix?'
                sleep(1)
                continue

        # convert satelittes message to string
        if data_stream_copy.satellites is not None:
            satellites = str(data_stream_copy.satellites)

        #print('---------------------')
        #print(                   data_stream_copy.time)
        #print('UNIX ts:{}    '.format(timestamp))
        #print('UNIX ts_last:{}    '.format(timestamp_last))
        #print('Lat:{}   '.format(data_stream_copy.lat))
        #print('Lon:{}   '.format(data_stream_copy.lon))
        #print('Speed:{} '.format(data_stream_copy.speed))
        #print('---------------------')

        gps_datum = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        gps_datum.write({
            "timestamp":timestamp,
            "lat":data_stream_copy.lat,
            "lon":data_stream_copy.lon,
            "alt":data_stream_copy.alt,
            "epx":data_stream_copy.epx,
            "epy":data_stream_copy.epy,
            "epv":data_stream_copy.epv,
            "track":data_stream_copy.track,
            "speed":data_stream_copy.speed,
            "climb":data_stream_copy.climb,
            "epd":data_stream_copy.epd,
            "eps":data_stream_copy.eps,
            "epc":data_stream_copy.epc,
            "satellites":satellites 
            },
            encoder)

        gps_buf = bytes_writer.getvalue()
        producer.send(topic, key='gps:' + isoblue_id, value=gps_buf)

        timestamp_last = timestamp

        sleep(1)
