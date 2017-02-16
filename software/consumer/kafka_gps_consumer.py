#!/usr/bin/env python

import avro.schema
import avro.io
import io

from kafka import KafkaConsumer
from datetime import datetime

consumer = KafkaConsumer("gps", auto_offset_reset="earliest", group_id=None)
#consumer = KafkaConsumer("gps")
schema_path = "../kafka_gps_log/gps.avsc"
schema = avro.schema.parse(open(schema_path).read())

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    user1 = reader.read(decoder)

    if user1["time"] is None or user1["lat"] is None or user1["lon"] is None:
	continue

    print user1
