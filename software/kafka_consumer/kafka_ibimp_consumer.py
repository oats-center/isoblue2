#!/usr/bin/env python

import avro.schema
import avro.io
import io

from kafka import KafkaConsumer

consumer = KafkaConsumer("ib_imp", auto_offset_reset="earliest")
schema_path = "../kafka_can_log_raw/raw_can.avsc"
schema = avro.schema.parse(open(schema_path).read())

for msg in consumer:
    print msg.value
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    user1 = reader.read(decoder)
    print user1
