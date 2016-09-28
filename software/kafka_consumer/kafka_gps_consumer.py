#!/usr/bin/env python

import avro.schema
import avro.io
import io

from kafka import KafkaConsumer

consumer = KafkaConsumer("gps")
schema_path = "../kafka_gps_log/gps.avsc"
schema = avro.schema.parse(open(schema_path).read())

for msg in consumer:
    print "***************************"
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    user1 = reader.read(decoder)
    print user1
