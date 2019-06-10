#!/bin/bash
#
#   Copyright 2018  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

### BEGIN INIT INFO
# Provides:          warp10
# Required-Start:
# Required-Stop:
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Warp data platform
# Description:       Warp stores sensor data
### END INIT INFO

# Source function library.
if [ -e /lib/lsb/init-functions ]; then
  . /lib/lsb/init-functions
fi

#JAVA_HOME=/opt/java8
#WARP10_HOME=/opt/warp10-@VERSION@
JMX_PORT=1098

# Strongly inspired by gradlew
# Determine the Java command to use to start the JVM.
if [ -n "$JAVA_HOME" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVACMD="$JAVA_HOME/jre/sh/java"
    elif [ -x "$JAVA_HOME/bin/java" ] ; then
        JAVACMD="$JAVA_HOME/bin/java"
    else
        JAVACMD="$JAVA_HOME/jre/bin/java"
    fi
    if [ ! -x "$JAVACMD" ] ; then
        echo "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME
Please set the JAVA_HOME variable in your environment or in $0 to match the location of your Java installation."
        exit 1
    fi
else
    JAVACMD="java"
    which java >/dev/null 2>&1 || (echo "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
Please set the JAVA_HOME variable in your environment or in $0 to match the location of your Java installation."; exit 1)
fi

#
# Check java version
#
JAVA_VERSION="`${JAVACMD} -version 2>&1 | head -n 1`"
CHECK_JAVA="`echo ${JAVA_VERSION} | egrep '.*\"1\.(7|8).*'`"
if [ "$CHECK_JAVA" == "" ]; then
  echo "You are using a non compatible java version: ${JAVA_VERSION}"
  echo "We recommend the latest update of OpenJDK 1.8"
  exit 1
fi

# If WARP10_HOME is not defined, set it to the parent directory
if [ -z "${WARP10_HOME}" ]; then
  WARP10_HOME=`cd $(dirname $0); cd $(pwd -P)/..; pwd -P`
fi

export WARP10_HOME

#
# Data directory that contains logs, leveldb, config defined ?
#
#WARP10_DATA_DIR=/data

if [ -z "${WARP10_DATA_DIR}" ]; then
  WARP10_DATA_DIR=${WARP10_HOME}
fi

#
# PID File
#
PID_FILE=${WARP10_HOME}/logs/warp10.pid

#
# File to indicate this is the first init of Warp 10 (bootstrap)
#
FIRSTINIT_FILE=${WARP10_HOME}/logs/.firstinit

#
# Quantum plugin - Plugin embeds Quantum
#
# To inhibit/activate Quantum use 'warp10.plugins' attribute in the Warp 10 config
QUANTUM_REVISION=@QUANTUM_VERSION@
QUANTUM_PLUGIN_JAR=${WARP10_HOME}/bin/warp10-quantum-plugin-${QUANTUM_REVISION}.jar
QUANTUM_PLUGIN_NAME=io.warp10.plugins.quantum.QuantumPlugin
# Is Quantum has been started ?
# Note: do not use this parameter to inhibit/activate Quantum (use Warp 10 config)
IS_QUANTUM_STARTED=true

IS_JAVA7=false

#
# Classpath
#
WARP10_REVISION=@VERSION@
export WARP10_USER=${WARP10_USER:=warp10}
WARP10_GROUP=${WARP10_GROUP:=warp10}
WARP10_CONFIG=${WARP10_HOME}/etc/conf-standalone.conf
WARP10_JAR=${WARP10_HOME}/bin/warp10-${WARP10_REVISION}.jar
WARP10_CLASS=io.warp10.standalone.Warp
WARP10_INIT=io.warp10.standalone.WarpInit
#
# The lib directory is dedicated to user libraries except of UDF(jars directory): extensions;..
#
WARP10_CP=${WARP10_HOME}/etc:${WARP10_JAR}:${WARP10_HOME}/lib/*
WARP10_HEAP=1g
WARP10_HEAP_MAX=1g
INITCONFIG=false

LEVELDB_HOME=${WARP10_DATA_DIR}/leveldb

SENSISION_EVENTS_DIR=/var/run/sensision/metrics

#
# Set SENSISIONID as environment variable to identify this instance by Sensision
#
export SENSISIONID=warp10

LOG4J_CONF=${WARP10_HOME}/etc/log4j.properties
JAVA_HEAP_DUMP=${WARP10_HOME}/logs/java.heapdump
# you can specialize your metrics for this instance of Warp10
#SENSISION_DEFAULT_LABELS=-Dsensision.default.labels=instance=warp10-test,env=dev
JAVA_OPTS="-Djava.awt.headless=true -Dlog4j.configuration=file:${LOG4J_CONF} -Dsensision.server.port=0 ${SENSISION_DEFAULT_LABELS} -Dsensision.events.dir=${SENSISION_EVENTS_DIR} -Xms${WARP10_HEAP} -Xmx${WARP10_HEAP_MAX} -XX:+UseG1GC"
export MALLOC_ARENA_MAX=1


moveDir() {
  dir=$1
  if [ -e ${WARP10_DATA_DIR}/${dir} ]; then
      echo "Error: ${WARP10_DATA_DIR}/${dir} already exists"
      exit 1
  fi
  su ${WARP10_USER} -c "mv ${WARP10_HOME}/${dir} ${WARP10_DATA_DIR}/ 2>&1"
  if [ $? != 0 ]; then
    echo "ERROR: move ${WARP10_HOME}/${dir} to ${WARP10_DATA_DIR}"
    exit 1
  fi
  ln -s ${WARP10_DATA_DIR}/${dir} ${WARP10_HOME}/${dir}
  chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/${dir}
  chmod 755 ${WARP10_DATA_DIR}/${dir}
}

#
# Exit this script if user doesn't match
#
isUser() {
  if [ "`whoami`" != "${1}" ]; then
    echo "You must be '${1}' to run this script."
    exit 1
  fi
}

#
# Return 0 if a Warp 10 instance is started
#
isStarted() {
  # Don't use 'ps -p' for docker compatibility
  if [ -e ${PID_FILE} ] && ps -Ao pid | grep "^\s*$(cat ${PID_FILE})$" > /dev/null; then
    return 0
  fi
  return 1
}

bootstrap() {
  echo "Bootstrap.."

  #
  # Make sure the caller is root
  #
  isUser root

  # warp10 user ?
  if ! id -u "${WARP10_USER}" >/dev/null 2>&1; then
    echo "User '${WARP10_USER}'' does not exist - Creating it.."
    # Create user warp10
    if [ "`which useradd`" = "" ]; then
      if [ "`which adduser`" != "" ]; then
        adduser -D -s -H -h ${WARP10_HOME} -s /bin/bash ${WARP10_USER}
      else
        echo "Cannot create the ${WARP10_USER} user with home directory ${WARP10_HOME}. Create it manually then run the script again."
        exit 1
      fi
    else
      useradd -d ${WARP10_HOME} -M -r ${WARP10_USER} -s /bin/bash
    fi
  fi

  #
  # If config file already exists then.. exit
  #
  if [ -e ${WARP10_CONFIG} ]; then
    echo "Config file already exists - Abort bootstrap..."
    exit 2
  fi

  # Fix ownership
  echo "Fix ownership.."
  echo "WARP10_HOME: ${WARP10_HOME}"

  chown -R ${WARP10_USER}:${WARP10_GROUP} ${WARP10_HOME}

  # Fix permissions
  echo "Fix permissions.."
  chmod 750 ${WARP10_HOME}
  chmod 755 ${WARP10_HOME}/bin
  chmod 755 ${WARP10_HOME}/etc
  chmod 755 ${WARP10_HOME}/logs
  chmod 755 ${WARP10_HOME}/macros
  chmod 755 ${WARP10_HOME}/jars
  chmod 755 ${WARP10_HOME}/lib
  chmod 755 ${WARP10_HOME}/templates
  chmod 755 ${WARP10_HOME}/warpscripts
  chmod 755 ${WARP10_HOME}/etc/throttle
  chmod 755 ${WARP10_HOME}/etc/trl
  chmod 755 ${WARP10_HOME}/etc/bootstrap
  chmod 644 ${WARP10_HOME}/etc/bootstrap/*.mc2
  chmod 755 ${WARP10_HOME}/bin/*.sh
  chmod 755 ${WARP10_HOME}/bin/*.init
  chmod 644 ${WARP10_HOME}/bin/*.service
  chmod 644 ${WARP10_HOME}/bin/warp10-@VERSION@.jar
  chmod -R 755 ${WARP10_HOME}/datalog
  chmod -R 755 ${WARP10_HOME}/datalog_done
  chmod -R 755 ${WARP10_HOME}/leveldb

  #
  # Test access to WARP10_HOME for WARP10_USER
  #
  su ${WARP10_USER} -c "ls ${WARP10_HOME} >/dev/null 2>&1"
  if [ $? != 0 ]; then
    echo "ERROR: ${WARP10_USER} user cannot access to ${WARP10_HOME}"
    exit 1
  fi

  #
  # ${WARP10_HOME} != ${WARP10_DATA_DIR}
  # A dedicated data directory has been provided
  # Move data to ${WARP10_DATA_DIR}/etc, ${WARP10_DATA_DIR}/logs, ${WARP10_DATA_DIR}/leveldb..
  #
  if [ "${WARP10_DATA_DIR}" != "${WARP10_HOME}" ]; then
    echo "WARP10_DATA_DIR: ${WARP10_DATA_DIR}"

    #
    # ${WARP10_DATA_DIR} exists ?
    #
    if [ ! -e ${WARP10_DATA_DIR} ]; then
      echo "${WARP10_DATA_DIR} does not exist - Creating it..."
      mkdir -p ${WARP10_DATA_DIR}
      if [ $? != 0 ]; then
        echo "${WARP10_DATA_DIR} creation failed"
        exit 1
      fi
    fi

    # force ownerships / permissions
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}
    chmod 750 ${WARP10_DATA_DIR}

    #
    # Test access to WARP10_DATA_DIR and its parent directories
    #
    su ${WARP10_USER} -c "ls ${WARP10_DATA_DIR} >/dev/null 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: Cannot access to ${WARP10_DATA_DIR}"
      exit 1
    fi

    # Move directories to ${WARP10_DATA_DIR}
    moveDir logs
    moveDir etc
    moveDir leveldb
    moveDir datalog
    moveDir datalog_done
    moveDir macros
    moveDir jars
    moveDir lib
    moveDir warpscripts

  fi

  WARP10_HOME_ESCAPED=$(echo ${WARP10_HOME} | sed 's/\\/\\\\/g' )           # Escape \
  WARP10_HOME_ESCAPED=$(echo ${WARP10_HOME_ESCAPED} | sed 's/\&/\\&/g' )    # Escape &
  WARP10_HOME_ESCAPED=$(echo ${WARP10_HOME_ESCAPED} | sed 's/|/\\|/g' )     # Escape | (separator for sed)

  LEVELDB_HOME_ESCAPED=$(echo ${LEVELDB_HOME} | sed 's/\\/\\\\/g' )           # Escape \
  LEVELDB_HOME_ESCAPED=$(echo ${LEVELDB_HOME_ESCAPED} | sed 's/\&/\\&/g' )    # Escape &
  LEVELDB_HOME_ESCAPED=$(echo ${LEVELDB_HOME_ESCAPED} | sed 's/|/\\|/g' )     # Escape | (separator for sed)

  sed -i -e 's|^standalone\.home.*|standalone.home = '${WARP10_HOME_ESCAPED}'|' ${WARP10_HOME}/templates/conf-standalone.template
  sed -i -e 's|^\(\s\{0,100\}\)WARP10_HOME=/opt/warp10-.*|\1WARP10_HOME='${WARP10_HOME_ESCAPED}'|' ${WARP10_HOME}/bin/snapshot.sh
  sed -i -e 's|^\(\s\{0,100\}\)LEVELDB_HOME=${WARP10_HOME}/leveldb|\1LEVELDB_HOME='${LEVELDB_HOME_ESCAPED}'|' ${WARP10_HOME}/bin/snapshot.sh

  sed -i -e 's|warpLog\.File=.*|warpLog.File='${WARP10_HOME_ESCAPED}'/logs/warp10.log|' ${WARP10_HOME}/etc/log4j.properties
  sed -i -e 's|warpscriptLog\.File=.*|warpscriptLog.File='${WARP10_HOME_ESCAPED}'/logs/warpscript.out|' ${WARP10_HOME}/etc/log4j.properties

  # Generate the configuration file with Worf
  # Generate read/write tokens valid for a period of 100 years. We use 'io.warp10.bootstrap' as application name.
  su ${WARP10_USER} -c "${JAVACMD} -cp ${WARP10_JAR} io.warp10.worf.Worf -q -a io.warp10.bootstrap -puidg -t -ttl 3153600000000 ${WARP10_HOME}/templates/conf-standalone.template -o ${WARP10_CONFIG}" >> ${WARP10_HOME}/etc/initial.tokens

  echo "Warp 10 config has been generated here: ${WARP10_CONFIG}"

  touch ${FIRSTINIT_FILE}

}

start() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  CHECK_JAVA7="`${JAVACMD} -version 2>&1 | head -n 1 | grep '.*\\"1.7.*'`"
  if [ "$CHECK_JAVA7" != "" ]; then
    IS_JAVA7=true
  fi

  if [ -f ${JAVA_HEAP_DUMP} ]; then
    mv ${JAVA_HEAP_DUMP} ${JAVA_HEAP_DUMP}-`date +%s`
  fi

  if isStarted; then
    echo "Start failed! - A Warp 10 instance is currently running"
    exit 1
  fi

  #
  # Config file exists ?
  #
  if [ ! -e ${WARP10_CONFIG} ]; then
    echo "Config file does not exist - Use 'bootstrap' command (it must be run as root)"
    exit 1
  fi

  LEVELDB_HOME="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'leveldb.home' | grep 'leveldb.home' | sed -e 's/^.*=//'`"

  #
  # Leveldb exists ?
  #
  if [ ! -e ${LEVELDB_HOME} ]; then
    echo "${LEVELDB_HOME} does not exist - Creating it..."
    mkdir -p ${LEVELDB_HOME} 2>&1
    if [ $? != 0 ]; then
      echo "${LEVELDB_HOME} creation failed"
      exit 1
    fi
  fi

  if [ "$(find -L ${LEVELDB_HOME} -maxdepth 1 -type f | wc -l)" -eq 0 ]; then
    echo "Init leveldb"
    # Create leveldb database
    echo \"Init leveldb database...\" >> ${WARP10_HOME}/logs/warp10.log
    ${JAVACMD} ${JAVA_OPTS} -cp ${WARP10_CP} ${WARP10_INIT} ${LEVELDB_HOME} >> ${WARP10_HOME}/logs/warp10.log 2>&1
  fi

  WARP10_LISTENSTO_HOST="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'standalone.host' | grep 'standalone.host' | sed -e 's/^.*=//'`"
  WARP10_LISTENSTO_PORT="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'standalone.port' | grep 'standalone.port' | sed -e 's/^.*=//'`"
  WARP10_LISTENSTO="${WARP10_LISTENSTO_HOST}:${WARP10_LISTENSTO_PORT}"

  #
  # Check if Warp10 Quantum plugin is defined
  #
  QUANTUM_PLUGIN="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'warp10.plugin.quantum' | grep ${QUANTUM_PLUGIN_NAME}`"

  if [ "$QUANTUM_PLUGIN" != "" ]; then
    if [ "$IS_JAVA7" = false ]; then
      IS_QUANTUM_STARTED=true
      # Add Quantum to WARP10_CP
      WARP10_CP=${QUANTUM_PLUGIN_JAR}:${WARP10_CP}
      QUANTUM_LISTENSTO_HOST="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'quantum.host' | grep 'quantum.host' | sed -e 's/^.*=//'`"
      QUANTUM_LISTENSTO_PORT="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'quantum.port' | grep 'quantum.port' | sed -e 's/^.*=//'`"
      QUANTUM_LISTENSTO="${QUANTUM_LISTENSTO_HOST}:${QUANTUM_LISTENSTO_PORT}"
    else
      echo "Start failed! - Quantum is only Java 1.8+ compliant - To start Warp 10 with Java7 comment out Quantum plugin in the Warp config file"
      exit 1
    fi
  else
    IS_QUANTUM_STARTED=false
    # Do not add Quantum to WARP10_CP
  fi

  #
  # Start Warp10 instance..
  #
  ${JAVACMD} ${JAVA_OPTS} -cp ${WARP10_CP} ${WARP10_CLASS} ${WARP10_CONFIG} >> ${WARP10_HOME}/logs/warp10.log 2>&1 &

  echo $! > ${PID_FILE}

  isStarted
  if [ $? -eq 1 ]; then
    echo "Start failed! - See warp10.log and warplog.log for more details"
    exit 1
  fi

  echo '  ___       __                           ____________   '
  echo '  __ |     / /_____ _______________      __<  /_  __ \  '
  echo '  __ | /| / /_  __ `/_  ___/__  __ \     __  /_  / / /  '
  echo '  __ |/ |/ / / /_/ /_  /   __  /_/ /     _  / / /_/ /   '
  echo '  ____/|__/  \__,_/ /_/    _  .___/      /_/  \____/    '
  echo '                           /_/                          '

  echo "##"
  echo "## Warp 10 listens on ${WARP10_LISTENSTO}"
  echo "##"
  if [ "$IS_QUANTUM_STARTED" = true ]; then
    echo "## Quantum listens on ${QUANTUM_LISTENSTO}"
    echo "##"
  fi

  if [ -e ${FIRSTINIT_FILE} ]; then

    #
    # Output the generated tokens
    #

    READ_TOKEN=`tail -n 1 ${WARP10_HOME}/etc/initial.tokens | sed -e 's/{"read":{"token":"//' -e 's/".*//'`
    WRITE_TOKEN=`tail -n 1 ${WARP10_HOME}/etc/initial.tokens | sed -e 's/.*,"write":{"token":"//' -e 's/".*//'`

    echo "##"
    echo "## An initial set of tokens was generated for you so you can immediately use Warp 10:"
    echo "##"
    echo "## Write Token: ${WRITE_TOKEN}"
    echo "## Read Token: ${READ_TOKEN}"
    echo "##"
    echo "## Push some test data using:"
    echo "##"
    echo "##   curl -H 'X-Warp10-Token: ${WRITE_TOKEN}' http://${WARP10_LISTENSTO}/api/v0/update --data-binary '// test{} 42'"
    echo "##"
    echo "## And read it back using:"
    echo "##"
    echo "##   curl 'http://${WARP10_LISTENSTO}/api/v0/fetch?token=${READ_TOKEN}&selector=~.*\{\}&now=now&timespan=-1'"
    echo "##"
    echo "## You can submit WarpScript for execution via:"
    echo "##"
    echo "##   curl http://${WARP10_LISTENSTO}/api/v0/exec --data-binary @path/to/WarpScriptFile"
    echo "##"
    if [ "$IS_QUANTUM_STARTED" = true ]; then
      echo "## The alternative to command-line interaction is Quantum, a web application to interact with the platform in an user-friendly way:"
      echo "##"
      echo "##   http://${QUANTUM_LISTENSTO}"
      echo "##"
    fi
    rm -f ${FIRSTINIT_FILE}

  fi

  # Check again 5s later (time for plugin load errors)
  sleep 5
  isStarted
  if [ $? -eq 1 ]; then
    echo "Start failed! - See warp10.log and warplog.log for more details"
    exit 1
  fi

}

stop() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if isStarted; then
    echo "Stop Warp 10..."
    kill $(cat ${PID_FILE})
    echo "Wait for Warp 10 to stop..."
    while $(kill -0 $(cat ${PID_FILE}) 2>/dev/null); do
      sleep 2
    done
    echo "Warp 10 stopped..."
    rm -f ${PID_FILE}
  else
    echo "No instance of Warp 10 is currently running"
  fi
}

status() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if isStarted; then
    ps -Ao pid,etime,args | grep "^\s*$(cat ${PID_FILE})\s"
  else
    echo "No instance of Warp 10 is currently running"
  fi
}

snapshot() {
  if [ $# -ne 2 -a $# -ne 3 ]; then
    echo $"Usage: $0 {snapshot 'snapshot_name' ['base_snapshot_name']}"
    exit 2
  fi
  # Name of snapshot
  SNAPSHOT=$2
  if [ $# -eq 2 ]; then
    ${WARP10_HOME}/bin/snapshot.sh ${SNAPSHOT} "${WARP10_HOME}" "${LEVELDB_HOME}" "${PID_FILE}"
  else
    BASE_SNAPSHOT=$3
    ${WARP10_HOME}/bin/snapshot.sh ${SNAPSHOT} ${BASE_SNAPSHOT} "${WARP10_HOME}" "${LEVELDB_HOME}" "${PID_FILE}"
  fi
}

worfcli() {
  echo ${JAVACMD} -cp ${WARP10_JAR} io.warp10.worf.Worf ${WARP10_CONFIG} -i
  ${JAVACMD} -cp ${WARP10_JAR} io.warp10.worf.Worf ${WARP10_CONFIG} -i
}

worf() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if [ "$#" -ne 3 ]; then
    echo "Usage: $0 $1 appName ttl(ms)"
    exit 1
  fi
  ${JAVACMD} -cp ${WARP10_JAR} io.warp10.worf.Worf ${WARP10_CONFIG} -puidg -t -a $2 -ttl $3
}

repair() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if isStarted; then
    echo "Repair has been cancelled! - Warp 10 instance must be stopped for repair"
    exit 1
  else
    echo "Repair Leveldb..."
    ${JAVACMD} -cp ${WARP10_JAR} io.warp10.standalone.WarpRepair ${LEVELDB_HOME}
  fi
}

# See how we were called.
case "$1" in
  bootstrap)
  bootstrap
  ;;
  start)
  start
  ;;
  jmxstart)
  JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=${JMX_PORT}"
  echo "## WARNING: JMX is enabled on port ${JMX_PORT}"
  start
  ;;
  stop)
  stop
  ;;
  status)
  status
  ;;
  restart)
  stop
  sleep 2
  start
  ;;
  jmxrestart)
  stop
  sleep 2
  JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=${JMX_PORT}"
  echo "## WARNING: JMX is enabled on port ${JMX_PORT}"
  start
  ;;
  worfcli)
  worfcli
  ;;
  worf)
  worf "$@"
  ;;
  snapshot)
  snapshot "$@"
  ;;
  repair)
  repair
  ;;
  *)
  echo $"Usage: $0 {bootstrap|start|jmxstart|stop|status|worfcli|worf appName ttl(ms)|snapshot 'snapshot_name'|repair|restart|jmxrestart}"
  exit 2
esac

exit $?
