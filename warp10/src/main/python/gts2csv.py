#!/usr/bin/env python
#
# Converts the output of /fetch with format 'text' into a set of CSV files
#

import sys
import re

tostdout = len(sys.argv) > 0 and '-' == sys.argv[1]

TSLLE = re.compile(r'^([0-9]+)/(([^:]+):([^/]+))?/([-+0-9]+)?$')

clslbls = None
tslle = None
value = None
out = None

if tostdout:
  out = sys.stdout

lastgts = None

for line in sys.stdin:
  if '=' == line[0]:
    (tslle,value) = line.strip()[1:].split(' ')
  else:
    (tslle,clslbls,value) = line.strip().split(' ')
    if not tostdout and out and (lastgts != clslbls):
      out.close()

    if not tostdout:
      out = open(clslbls,'a')

  match = re.search(TSLLE, tslle)

  ts = match.group(1)
  lat = match.group(3)
  lon = match.group(4)
  elev = match.group(5)

  if tostdout:
    leftcb = clslbls.index('{')
    out.write(clslbls[0:leftcb])
    out.write('\t')
    out.write(clslbls[leftcb+1:-1])
    out.write('\t')

  out.write(ts)
  out.write('\t')
  if lat:
    out.write(lat)
  out.write('\t')
  if lon:
    out.write(lon)
  out.write('\t')
  if elev:
    out.write(elev)
  out.write('\t')
  out.write(value)
  out.write('\r\n')

if not tostdout and out:
  out.close()
