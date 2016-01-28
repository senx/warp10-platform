#!/usr/bin/env python

import time
import random

from ws4py.client.threadedclient import WebSocketClient

class PlasmaPushClient(WebSocketClient):

  def __init__(self, scheme, protocols, token):
    WebSocketClient.__init__(self, scheme, protocols)
    self._token = token

  def opened(self):
    self.send("TOKEN %s" % (self._token))

  def closed(self, code, reason=None):
    print "Closed down", code, reason

  def received_message(self, m):
    print m

  def sendValue(self, m):
    self.send(m)

if __name__ == '__main__':
  WRITE_TOKEN = 'XXXX'
  try:
    ws = PlasmaPushClient('ws://HOST/api/v0/streamupdate', protocols=['http-only', 'chat'], token=WRITE_TOKEN)
    ws.connect()
    while True:
      time.sleep(2)
      ws.sendValue('// color.rgb{} %d' % random.randint(0,0xffffff))
    ws.run_forever()
  except KeyboardInterrupt:
    ws.close()
