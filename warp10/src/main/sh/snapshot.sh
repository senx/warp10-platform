#!/bin/sh
#
# Script to create a snapshot of the leveldb (standalone) version of Warp.
#

LEVELDB_HOME=/opt/warp10-@VERSION@/data

# Snapshot directory, MUST be on the same device as LEVELDB_HOME so we can create hard links
SNAPSHOT_DIR=${LEVELDB_HOME}/snapshots

# Path to the 'trigger' file
TRIGGER_PATH=${LEVELDB_HOME}/snapshot.trigger
# Path to the 'signal' file
SIGNAL_PATH=${LEVELDB_HOME}/snapshot.signal

# Name of snapshot
SNAPSHOT=$1

#JAVA_HOME=/opt/jdk1.8.0_31
WARP10_USER=warp10
WARP10_CLASS=io.warp10.standalone.Warp

if [ "" = "${SNAPSHOT}" ]
then
  echo "Snapshot name is empty."
  exit 1
fi

#
# Check if Warp instance is currently running
#

if [ "`su ${WARP10_USER} -c "${JAVA_HOME}/bin/jps -lm|grep ${WARP10_CLASS}|cut -f 1 -d' '"`" = "" ]
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
  su ${WARP10_USER} -c "touch  ${TRIGGER_PATH}"
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

su ${WARP10_USER} -c "mkdir ${SNAPSHOT_DIR}/${SNAPSHOT}"
cd ${SNAPSHOT_DIR}/${SNAPSHOT}

#
# Create hard links of '.sst' files
#

su ${WARP10_USER} -c "find -L ${LEVELDB_HOME} -maxdepth 1 -name '*sst'|xargs echo|while read FILES; do if [ \"${FILES}\" != \"\" ]; then ln ${FILES} ${SNAPSHOT_DIR}/${SNAPSHOT}; fi; done"

if [ $? != 0 ]
then
  echo "Hard link creation failed - Cancel Snapshot !"
  su ${WARP10_USER} -c "rm -rf ${SNAPSHOT_DIR}/${SNAPSHOT}"
  exit 1
fi

#
# Copy CURRENT and MANIFEST
#

su ${WARP10_USER} -c "cp ${LEVELDB_HOME}/CURRENT ${SNAPSHOT_DIR}/${SNAPSHOT}"
su ${WARP10_USER} -c "cp ${LEVELDB_HOME}/MANIFEST-* ${SNAPSHOT_DIR}/${SNAPSHOT}"

#
# Remove 'trigger' file
#

su ${WARP10_USER} -c "rm -f ${TRIGGER_PATH}"
