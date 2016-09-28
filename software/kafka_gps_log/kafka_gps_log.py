#!/usr/bin/env python

import io
import signal
import sys
import avro
import avro.schema
import avro.io
import calendar

from gps3 import gps3
from kafka import KafkaProducer
from datetime import datetime

TOPIC = "gps"
SERVER_ADDR = "vip4.ecn.purdue.edu"

if __name__ == "__main__":

    # create gpsd socket and listens for new data
    gps_socket = gps3.GPSDSocket()
    data_stream = gps3.DataStream()
    gps_socket.connect()
    gps_socket.watch()

    # create kafka producer
    producer = KafkaProducer(bootstrap_servers=SERVER_ADDR)

    # load avro schema and setup encoder
    fp = open("/opt/isoblue2/kafka_gps_log/gps.avsc").read()
    schema = avro.schema.parse(fp)

    def sigterm_handler(signal, frame):
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)

    for new_data in gps_socket:
        if new_data:
            epoch_time = None
            data_stream.unpack(new_data)
            tmp = data_stream.TPV.copy()

#TODO: find a proper way to parse the time

#            if tmp['time'] != "n/a":
                # convert ISO8601 date and time to unix epoch time
#                utc_dt = datetime.strptime(tmp['time'], \
#                        '%Y-%m-%dT%H:%M:%S.%fZ')
#                epoch_time = (utc_dt - datetime(1970, 1, 1)).total_seconds()

            for key in tmp:
                # if any key value is empty, set it to None
                # so that it fits into the avro schema
                if tmp[key] == "n/a":
                    tmp[key] = None

            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write({
                "time":tmp["time"],
                "lat":tmp["lat"],
                "lon":tmp["lon"],
                "alt":tmp["alt"],
                "epx":tmp["epx"],
                "epy":tmp["epy"],
                "epv":tmp["epv"],
                "track":tmp["track"],
                "speed":tmp["speed"],
                "climb":tmp["climb"],
                "epd":tmp["epd"],
                "eps":tmp["eps"],
                "epc":tmp["epc"]
                },
                encoder)

            bytes_msg = bytes_writer.getvalue()
            producer.send(TOPIC, bytes_msg)
    
