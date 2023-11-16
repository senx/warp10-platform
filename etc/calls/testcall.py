#!/usr/bin/env -S python3 -u 

import pickle
import sys
import urllib.parse
import base64
import json

#
# Output the maximum number of instances of this 'callable' to spawn
# The absolute maximum is set in the configuration file via 'warpscript.call.maxcapacity'
#

print(1)

#
# An example of Warpscript to test this script: 
#  [ NOW ISO8601 1 ] ->PICKLE ->B64 'testcall.py' CALL JSON->
#

#
# Loop, reading stdin, doing our stuff and outputing to stdout
#

while True:
  try:
    #
    # Read input. 'CALL' will transmit a single string argument from the stack, URL encoding it before transmission.
    # The 'callable' should describe how its input is to be formatted.
    # For python callable, we recommend base64 encoded pickle content (generated via ->PICKLE).
    #
    line = sys.stdin.readline()
    line = line.strip()
    # Remove URL encoding done by CALL itself
    line = urllib.parse.unquote(line)
    # Remove Base64 encoding done by ->B64 function
    pickledArguments = base64.b64decode(line)
    # Deserialize object hierarchy
    args = pickle.loads(pickledArguments)

    #
    # Do our stuff
    #
    out = {}
    out['arguments'] = args
    out['my answer'] = "hello world"

    #
    # Output result (CALL expects one line, URL encoded, UTF-8).
    # Here we choose json output, but pickle serialization can also be used, see PICKLE-> documentation
    #
    jsonOutput = json.dumps(out)
    print(urllib.parse.quote(jsonOutput.encode('utf-8')))

  except Exception as err:
    #
    # If returning a content starting with ' ' (not URL encoded), then
    # the rest of the line is interpreted as a URL encoded UTF-8 of an error message
    # and will propagate the error to the calling WarpScript
    #
    print(' ' + "Error within the python CALL:" + urllib.parse.quote(repr(err).encode('utf-8')))

