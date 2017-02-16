#!/usr/bin/env python

import io
import sys
import re
import struct
import argparse
import ast

from sys import stdout
from pprint import pprint
from struct import *
from kafka import KafkaConsumer

topic = 'dmsgrate'
msg_rate_list = []

consumer = KafkaConsumer(topic, group_id=None)

for message in consumer:
    key = message.key
    key_splited = key.split('-')
    bus = key_splited[0]
    isoblue_id = '-'.join(key_splited[1:])
    frame_rate = str(struct.unpack("<L", message.value)[0])

    if not msg_rate_list:
        msg_rate = {}
        msg_rate['id'] = isoblue_id
        msg_rate['frame_rate'] = {bus:frame_rate}
        msg_rate_list.append(msg_rate)

    for i in range(len(msg_rate_list)):
        if msg_rate_list[i]['id'] == isoblue_id:
            msg_rate_list[i]['frame_rate'][bus] = frame_rate

    if not any(d['id'] == isoblue_id for d in msg_rate_list):
        d = {}
        d['id'] = isoblue_id
        d['frame_rate'] = {bus:frame_rate}
        msg_rate_list.append(d)

    last_x = ''
    for x in msg_rate_list:
        print '' * len(str(last_x)) + '\r',
        print '{}\r'.format(str(x))
        last_x = str(x)
