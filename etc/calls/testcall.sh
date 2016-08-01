#!/bin/sh

#
# Warpscript to trigger this shell: NOW ISO8601 'testcall.sh' CALL
#

#
# Max instances (in case of concurrent calls)
#
echo 5 

while true
do
  read dateIso
  echo "${dateIso} - `hostname`"
done
