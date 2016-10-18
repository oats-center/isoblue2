import can
import io
import avro
import avro.schema
import avro.io
import sys

from can import Listener
from kafka import KafkaProducer

class KafkaWriter(Listener):
   '''Sends received CAN data to a kafka topic.
   '''

   def __init__(self, topic, producer):
       self.topic = topic
       self.producer = producer


   def on_message_received(self, msg):

       self.fp = open("/opt/isoblue2/kafka_can_log_raw/raw_can.avsc").read()
       schema = avro.schema.parse(self.fp)
       writer = avro.io.DatumWriter(schema)

       bytes_writer = io.BytesIO()
       encoder = avro.io.BinaryEncoder(bytes_writer)
       writer.write({
           "timestamp":msg.timestamp,
           "is_remote_frame":msg.is_remote_frame,
           "extended_id":msg.is_extended_id,
           "is_error_frame":msg.is_error_frame,
           "arbitration_id":msg.arbitration_id,
           "dlc":msg.dlc,
           "data":"".join(map(chr, msg.data))}, encoder)

       bytes_msg = bytes_writer.getvalue()

       self.producer.send(self.topic, bytes_msg)

   def stop(self):
       if self.kafka_setup is True:
           self.producer.flush()
