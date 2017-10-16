#!/bin/sh

#
# Script to create a snapshot of the leveldb (standalone) version of Warp.
#

#JAVA_HOME=/opt/java8
WARP10_USER=warp10

#
# Make sure the caller is warp10
#

if [ "`whoami`" != "${WARP10_USER}" ]
then
  echo "You must be ${WARP10_USER} to run this script."
  exit 1
fi

if [ "$#" -eq 1 ]; then
  # Name of snapshot
  SNAPSHOT=$1
  # default
  WARP10_HOME=/opt/warp10-@VERSION@
  LEVELDB_HOME=${WARP10_HOME}/leveldb
  PID_FILE=${WARP10_HOME}/logs/warp10.pid
elif [ "$#" -eq 4 ]; then
  # Name of snapshot
  SNAPSHOT=$1
  # default
  WARP10_HOME=$2
  LEVELDB_HOME=$3
  PID_FILE=$4
else
  echo "Usage: $0 'snapshot-name' ['{WARP10_HOME}' '{LEVELDB_HOME}' '{PID_FILE}']"
  exit 1
fi

WARP10_CONFIG=${WARP10_HOME}/etc/conf-standalone.conf
# Snapshot directory, MUST be on the same device as LEVELDB_HOME so we can create hard links
SNAPSHOT_DIR=${LEVELDB_HOME}/snapshots

# Path to the 'trigger' file
TRIGGER_PATH=${LEVELDB_HOME}/snapshot.trigger
# Path to the 'signal' file
SIGNAL_PATH=${LEVELDB_HOME}/snapshot.signal

if [ "" = "${SNAPSHOT}" ]
then
  echo "Snapshot name is empty."
  exit 1
fi

if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME not set";
  exit 1
fi

#
# Check if Warp instance is currently running
#

if [ ! -e ${PID_FILE} ] || [ "`${JAVA_HOME}/bin/jps -lm|grep -wE $(cat ${PID_FILE})|cut -f 1 -d' '`" = "" ]
then
  echo "No Warp 10 instance is currently running !"
  exit 1
fi

#
# Check if snapshot already exists
#

if [ -e "${SNAPSHOT_DIR}/${SNAPSHOT}" ]
then
  echo "Snapshot '${SNAPSHOT_DIR}/${SNAPSHOT}' already exists"
  exit 1
fi

#
# Check snapshots and leveldb data dir are on the same mount point
#

if [ "`df -P ${LEVELDB_HOME}|sed '1d'|awk '{ print $1 }'`" != "`df -P ${SNAPSHOT_DIR}|sed '1d'|awk '{ print $1 }'`" ]
then
  echo "'${SNAPSHOT_DIR}' and '${LEVELDB_HOME}' must be mounted onto the same mount point."
  exit 1
fi

#
# Bail out if 'signal' path exists
#

if [ -e "${SIGNAL_PATH}" ]
then
  echo "Signal file '${SIGNAL_PATH}' already exists, aborting."
  exit 1
fi

#
# Check if 'trigger' path exists, create it if not
#

if [ -e "${TRIGGER_PATH}" ]
then
  echo "Trigger file '${TRIGGER_PATH}' already exists, aborting"
  exit 1
else
  touch  ${TRIGGER_PATH}
fi

#
# Wait for the 'signal' file to appear
#

while [ ! -e "${SIGNAL_PATH}" ]
do
  sleep 1
done

#
# Create snapshot directory
#

mkdir ${SNAPSHOT_DIR}/${SNAPSHOT}
cd ${SNAPSHOT_DIR}/${SNAPSHOT}

#
# Create hard links of '.sst' files
#

find -L ${LEVELDB_HOME} -maxdepth 1 -name '*sst'|xargs echo|while read FILES; do if [ -n "${FILES}" ]; then ln ${FILES} ${SNAPSHOT_DIR}/${SNAPSHOT}; fi; done

if [ $? != 0 ]
then
  echo "Hard link creation failed - Cancel Snapshot !"
  rm -rf ${SNAPSHOT_DIR}/${SNAPSHOT}
  exit 1
fi

#
# Copy CURRENT and MANIFEST
#

cp ${LEVELDB_HOME}/CURRENT ${SNAPSHOT_DIR}/${SNAPSHOT}
cp ${LEVELDB_HOME}/MANIFEST-* ${SNAPSHOT_DIR}/${SNAPSHOT}
cp ${LEVELDB_HOME}/LOG* ${SNAPSHOT_DIR}/${SNAPSHOT}
cp ${LEVELDB_HOME}/*.log ${SNAPSHOT_DIR}/${SNAPSHOT}

#
# Remove 'trigger' file
#

rm -f ${TRIGGER_PATH}

#
# Snapshot configuration (contains hash/aes keys)
#

mkdir ${SNAPSHOT_DIR}/${SNAPSHOT}/warp10-config
# only warp10 user can have access to this config
chmod 700 ${SNAPSHOT_DIR}/${SNAPSHOT}/warp10-config
cp  ${WARP10_CONFIG} ${SNAPSHOT_DIR}/${SNAPSHOT}/warp10-config/
