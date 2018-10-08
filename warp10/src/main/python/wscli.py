#!/usr/bin/env python -u

import time
import random
import readline
import sys
import os

from ws4py.client.threadedclient import WebSocketClient

global prompt
prompt = ' '
global waiting
waiting = True

histfile = os.path.join(os.path.expanduser("~"), ".wshist")
try:
  readline.read_history_file(histfile)
  # default history len is -1 (infinite), which may grow unruly
  readline.set_history_length(1000)
except IOError:
  pass

import atexit
atexit.register(readline.write_history_file, histfile)

class WSCLI(WebSocketClient):

  _open = False

  def __init__(self, scheme, protocols):
    WebSocketClient.__init__(self, scheme, protocols)

  def opened(self):
    self._open = True

  def closed(self, code, reason=None):
    print ''
    print 'Warp 10 has closed the connection.'
    exit(-1)

  def received_message(self, m):
    if m.data[0:2] == 'WS':
      global prompt
      prompt = m.data
      global waiting
      waiting = False
    else:
      sys.stdout.write(m.data)
      sys.stdout.flush()

  def sendValue(self, m):
    self.send(m)

  def isOpen(self):
    return self._open

if __name__ == '__main__':
  try:
    if len(sys.argv) > 1:
      ws = WSCLI(sys.argv[1], protocols=['http-only', 'chat'])
    else:
      ws = WSCLI('ws://127.0.0.1:8080/api/v0/interactive', protocols=['http-only', 'chat'])
    ws.connect()
    while not ws.isOpen():
      time.sleep(0.1)
      continue

    
    while not ws.terminated:
      while waiting:
        continue
      line = raw_input(prompt)
      ws.sendValue(line.strip())
      waiting = True
   
    ws.run_forever()
  except KeyboardInterrupt:
    ws.close()
