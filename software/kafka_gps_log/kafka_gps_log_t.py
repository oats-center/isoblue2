#!/usr/bin/env python

import io
import avro
import avro.schema
import avro.io

from gps3.agps3threaded import AGPS3mechanism
from kafka import KafkaProducer

TOPIC = "gps"
SERVER_ADDR = "localhost"

if __name__ == "__main__":

    # Instantiate AGPS3 Mechanisms
    agps_thread = AGPS3mechanism()
    agps_thread.stream_data()
    agps_thread.run_thread()

    # create kafka producer
    producer = KafkaProducer(bootstrap_servers=SERVER_ADDR)

    # load avro schema and setup encoder
    fp = open("/opt/isoblue2/kafka_gps_log/gps.avsc").read()
    schema = avro.schema.parse(fp)
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)

    while True:
        sleep(1)
