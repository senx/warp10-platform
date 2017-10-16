#!/bin/sh

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

OS=$(uname -s)

#JAVA_HOME=/opt/java8
#WARP10_HOME=/opt/warp10-@VERSION@

if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME not set";
  exit 1
fi

# If WARP10_HOME is not defined, set it to the parent directory
if [ -z "${WARP10_HOME}" ]; then
  if [ "Darwin" = "${OS}" ]
  then
    pushd $(dirname $0)/.. > /dev/null 2>&1
    WARP10_HOME=`pwd`
    popd > /dev/null 2>&1
  else
    WARP10_HOME=$(dirname $(readlink -f $0))/..
  fi
fi

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
# Is Quantun has been started ?
# Note: do not use this parameter to inhibit/activate Quantum (use Warp 10 config)
IS_QUANTUM_STARTED=true

IS_JAVA7=false

#
# Classpath
#
WARP10_REVISION=@VERSION@
WARP10_USER=warp10
WARP10_GROUP=warp10
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

bootstrap() {
  echo "Bootstrap.."

  #
  # Make sure the caller is root
  #

  if [ "`whoami`" != "root" ]
  then
    echo "You must be root to run 'bootstrap' command."
    exit 1
  fi

  # warp10 user ?
  if ! id -u "${WARP10_USER}" >/dev/null 2>&1;
  then
    echo "User '${WARP10_USER}'' does not exist - Creating it.."
    # Create user warp10
    if [ "`which useradd`" = "" ]
    then
      if [ "`which adduser`" != "" ]
      then
        adduser -D -s -H -h ${WARP10_HOME} -s /bin/bash ${WARP10_USER}
      else
        echo "Hmmm that's embarassing but I do not know how to create the ${WARP10_USER} user with home directory ${WARP10_HOME}, could you do it for me and run the script again?"
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
    su ${WARP10_USER} -c "ls ${WARP10_DATA_DIR} 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: Cannot access to ${WARP10_DATA_DIR}"
      exit 1
    fi

    # Move logs dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/logs ]; then
      echo "Error: ${WARP10_DATA_DIR}/logs already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/logs ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/logs to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/logs ${WARP10_HOME}/logs
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/logs
    chmod 755 ${WARP10_DATA_DIR}/logs

    # Move etc dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/etc ]; then
      echo "Error: ${WARP10_DATA_DIR}/etc already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/etc ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/etc to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/etc ${WARP10_HOME}/etc
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/etc
    chmod 755 ${WARP10_DATA_DIR}/etc

    # Move leveldb dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/leveldb ]; then
      echo "Error: ${WARP10_DATA_DIR}/leveldb already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/leveldb ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/leveldb to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/leveldb ${WARP10_HOME}/leveldb
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/leveldb
    chmod 755 ${WARP10_DATA_DIR}/leveldb

    # Move datalog dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/datalog ]; then
      echo "Error: ${WARP10_DATA_DIR}/datalog already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/datalog ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/datalog to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/datalog ${WARP10_HOME}/datalog
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/datalog
    chmod 755 ${WARP10_DATA_DIR}/datalog

    # Move datalog_done dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/datalog_done ]; then
      echo "Error: ${WARP10_DATA_DIR}/datalog_done already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/datalog_done ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/datalog_done to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/datalog_done ${WARP10_HOME}/datalog_done
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/datalog_done
    chmod 755 ${WARP10_DATA_DIR}/datalog_done

    # Move macros dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/macros ]; then
      echo "Error: ${WARP10_DATA_DIR}/macros already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/macros ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/macros to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/macros ${WARP10_HOME}/macros
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/macros
    chmod 755 ${WARP10_DATA_DIR}/macros

    # Move jars dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/jars ]; then
      echo "Error: ${WARP10_DATA_DIR}/jars already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/jars ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/jars to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/jars ${WARP10_HOME}/jars
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/jars
    chmod 755 ${WARP10_DATA_DIR}/jars

    # Move lib to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/lib ]; then
      echo "Error: ${WARP10_DATA_DIR}/lib already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/lib ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/lib to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/lib ${WARP10_HOME}/lib
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/lib
    chmod 755 ${WARP10_DATA_DIR}/lib

    # Move warpscripts dir to ${WARP10_DATA_DIR}
    if [ -e ${WARP10_DATA_DIR}/warpscripts ]; then
      echo "Error: ${WARP10_DATA_DIR}/warpscripts already exists"
      exit 1
    fi
    su ${WARP10_USER} -c "mv ${WARP10_HOME}/warpscripts ${WARP10_DATA_DIR}/ 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: move ${WARP10_HOME}/warpscripts to ${WARP10_DATA_DIR}"
      exit 1
    fi
    ln -s ${WARP10_DATA_DIR}/warpscripts ${WARP10_HOME}/warpscripts
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/warpscripts
    chmod 755 ${WARP10_DATA_DIR}/warpscripts
  fi

  sed -i -e "s_^standalone\.home.*_standalone\.home = ${WARP10_HOME}_" ${WARP10_HOME}/templates/conf-standalone.template
  sed -i -e "s_^LEVELDB\_HOME=.*_LEVELDB\_HOME=${LEVELDB_HOME}_" ${WARP10_HOME}/bin/snapshot.sh

  sed -i -e "s_warpLog\.File=.*_warpLog\.File=${WARP10_HOME}/logs/warp10.log_" ${WARP10_HOME}/etc/log4j.properties
  sed -i -e "s_warpscriptLog\.File=.*_warpscriptLog\.File=${WARP10_HOME}/logs/warpscript.out_" ${WARP10_HOME}/etc/log4j.properties

  # Generate the configuration file with Worf
  # Generate read/write tokens valid for a period of 100 years. We use 'io.warp10.bootstrap' as application name.
  su ${WARP10_USER} -c "${JAVA_HOME}/bin/java -cp ${WARP10_JAR} io.warp10.worf.Worf -q -a io.warp10.bootstrap -puidg -t -ttl 3153600000000 ${WARP10_HOME}/templates/conf-standalone.template -o ${WARP10_CONFIG}" >> ${WARP10_HOME}/etc/initial.tokens

  echo "Warp 10 config has been generated here: ${WARP10_CONFIG}"

  touch ${FIRSTINIT_FILE}

}

start() {

  #
  # Make sure the caller is warp10
  #

  if [ "`whoami`" != "${WARP10_USER}" ]
  then
    echo "You must be ${WARP10_USER} to run this script."
    exit 1
  fi

  CHECK_JAVA7="`${JAVA_HOME}/bin/java -version 2>&1 | head -n 1 | grep '.*\\"1.7.*'`"
  if [ "$CHECK_JAVA7" != "" ]; then
    IS_JAVA7=true
  fi

  # warp10 user ?
  if ! id -u "${WARP10_USER}" >/dev/null 2>&1;
  then
    echo "User '${WARP10_USER}'' does not exist - Use 'bootstrap' command (it must be run as root)"
    exit 1
  fi

  if [ -f ${JAVA_HEAP_DUMP} ]; then
    mv ${JAVA_HEAP_DUMP} ${JAVA_HEAP_DUMP}-`date +%s`
  fi

  if [ -e ${PID_FILE} ] && [ "`${JAVA_HOME}/bin/jps -lm|grep -wE $(cat ${PID_FILE})|cut -f 1 -d' '`" != "" ]; then
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

  LEVELDB_HOME="`${JAVA_HOME}/bin/java -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'leveldb.home' | grep 'leveldb.home' | sed -e 's/^.*=//'`"

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
    ${JAVA_HOME}/bin/java ${JAVA_OPTS} -cp ${WARP10_CP} ${WARP10_INIT} ${LEVELDB_HOME} >> ${WARP10_HOME}/logs/warp10.log 2>&1
  fi

  WARP10_LISTENSTO_HOST="`${JAVA_HOME}/bin/java -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'standalone.host' | grep 'standalone.host' | sed -e 's/^.*=//'`"
  WARP10_LISTENSTO_PORT="`${JAVA_HOME}/bin/java -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'standalone.port' | grep 'standalone.port' | sed -e 's/^.*=//'`"
  WARP10_LISTENSTO="${WARP10_LISTENSTO_HOST}:${WARP10_LISTENSTO_PORT}"

  #
  # Check if Warp10 Quantum plugin is defined
  #
  QUANTUM_PLUGIN="`${JAVA_HOME}/bin/java -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'warp10.plugins' | grep ${QUANTUM_PLUGIN_NAME}`"

  if [ "$QUANTUM_PLUGIN" != "" ]; then
    if [ "$IS_JAVA7" = false ]; then
      IS_QUANTUM_STARTED=true
      # Add Quantum to WARP10_CP
      WARP10_CP=${QUANTUM_PLUGIN_JAR}:${WARP10_CP}
      QUANTUM_LISTENSTO_HOST="`${JAVA_HOME}/bin/java -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'quantum.host' | grep 'quantum.host' | sed -e 's/^.*=//'`"
      QUANTUM_LISTENSTO_PORT="`${JAVA_HOME}/bin/java -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${WARP10_CONFIG} 'quantum.port' | grep 'quantum.port' | sed -e 's/^.*=//'`"
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
  ${JAVA_HOME}/bin/java ${JAVA_OPTS} -cp ${WARP10_CP} ${WARP10_CLASS} ${WARP10_CONFIG} >> ${WARP10_HOME}/logs/warp10.log 2>&1 &

  echo $! > ${PID_FILE}

  if [ ! -e ${PID_FILE} ] || [ "`${JAVA_HOME}/bin/jps -lm|grep -wE $(cat ${PID_FILE})|cut -f 1 -d' '`" = "" ]; then
    echo "Start failed! - See warp10.log for more details"
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
}

stop() {

  #
  # Make sure the caller is warp10
  #

  if [ "`whoami`" != "${WARP10_USER}" ]
  then
    echo "You must be ${WARP10_USER} to run this script."
    exit 1
  fi

  echo "Stop Warp 10..."
  if [ -e ${PID_FILE} ] && [ "`${JAVA_HOME}/bin/jps -lm|grep -wE $(cat ${PID_FILE})|cut -f 1 -d' '`" != "" ]
  then
    kill `${JAVA_HOME}/bin/jps -lm|grep -wE $(cat ${PID_FILE})|cut -f 1 -d' '`
    rm -f ${PID_FILE}
  else
    echo "No instance of Warp 10 is currently running"
  fi
}

status() {
  
  #
  # Make sure the caller is warp10
  #

  if [ "`whoami`" != "${WARP10_USER}" ]
  then
    echo "You must be ${WARP10_USER} to run this script."
    exit 1
  fi
  if [ -e ${PID_FILE} ]
  then
    ${JAVA_HOME}/bin/jps -lm|grep -wE $(cat ${PID_FILE})
  fi
}

snapshot() {
  if [ $# -ne 2 ]; then
    echo $"Usage: $0 {snapshot 'snapshot_name'}"
    exit 2
  fi
  # Name of snapshot
  SNAPSHOT=$2
  ${WARP10_HOME}/bin/snapshot.sh ${SNAPSHOT} "${WARP10_HOME}" "${LEVELDB_HOME}" "${PID_FILE}"
}

worfcli() {
  ${JAVA_HOME}/bin/java -cp ${WARP10_JAR} io.warp10.worf.Worf ${WARP10_CONFIG} -i
}

worf() {
  
  #
  # Make sure the caller is warp10
  #

  if [ "`whoami`" != "${WARP10_USER}" ]
  then
    echo "You must be ${WARP10_USER} to run this script."
    exit 1
  fi

  if [ "$#" -ne 3 ]; then
    echo "Usage: $0 $1 appName ttl(ms)"
    exit 1
  fi
  ${JAVA_HOME}/bin/java -cp ${WARP10_JAR} io.warp10.worf.Worf ${WARP10_CONFIG} -puidg -t -a $2 -ttl $3
}

# See how we were called.
case "$1" in
  bootstrap)
  bootstrap
  ;;
  start)
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
  worfcli)
  worfcli
  ;;
  worf)
  worf "$@"
  ;;
  snapshot)
  snapshot "$@"
  ;;
  *)
  echo $"Usage: $0 {bootstrap|start|stop|status|worfcli|worf appName ttl(ms)|snapshot 'snapshot_name'}"
  exit 2
esac

exit $?
