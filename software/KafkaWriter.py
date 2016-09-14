import can
import io
import avro
import avro.schema
import avro.io

from can import Listener
from kafka import KafkaProducer

class KafkaWriter(Listener):
   '''Sends received CAN data to a kafka topic.
   '''

   def __init__(self, topic, server_addr):
       self.topic = topic
       self.server_addr = server_addr
       self.kafka_setup = False

   def _create_producer(self):
       self.producer = KafkaProducer(bootstrap_servers=self.server_addr)

       self.kafka_setup = True

   def on_message_received(self, msg):
       if not self.kafka_setup:
           self._create_producer()

       self.fp = open("j1939msg.avsc").read()
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
