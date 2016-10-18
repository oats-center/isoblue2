#!/usr/bin/env python

from can import Listener
from threading import Timer

class CanListenerWatchdog(Listener):
   '''Monitor CAN interfaces traffic using a watchdog.
   '''
   def __init__(self, timeout, userHandler=None):  # timeout in seconds
       self.timeout = timeout
       self.handler = userHandler if userHandler is not None else self.defaultHandler
       self.timer = Timer(self.timeout, self.handler)
       self.timer.start()

   def on_message_received(self, msg):
       self.timer.cancel()
       self.timer = Timer(self.timeout, self.handler)
       self.timer.start()

   def stop(self):
       self.timer.cancel()

   def defaultHandler(self):
       raise self
