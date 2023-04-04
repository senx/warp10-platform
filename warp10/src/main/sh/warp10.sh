#!/usr/bin/env bash
#
#   Copyright 2016-2023  SenX S.A.S.
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

#################################################################################
#                                                                               #
#  Tip: customize values in ${WARP10_HOME}/etc/warp10-env.sh, not in this file! #
#  Future updates will be simpler if you do not change this file.               #
#                                                                               #
#################################################################################

set -eu

##
## Source warp10-env.sh when the file exists to predefine user variables
##
BIN_DIR=$(dirname "$0")
if [ -f "${BIN_DIR}/../etc/warp10-env.sh" ]; then
  . "${BIN_DIR}/../etc/warp10-env.sh"
fi

warn() {
  echo "$*"
} >&2

die() {
  echo
  echo "$*"
  echo
  exit 1
} >&2

##
## Determine the Java command to use to start the JVM.
##
getJava() {
  if [ -n "${JAVA_HOME:-}" ]; then
    if [ -x "$JAVA_HOME/jre/sh/java" ]; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD=$JAVA_HOME/jre/sh/java
    else
      JAVACMD=$JAVA_HOME/bin/java
    fi
    if [ ! -x "$JAVACMD" ]; then
      die "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME

Please set the JAVA_HOME variable in ${WARP10_HOME}/etc/warp10-env.sh to match the
location of your Java installation."
    fi
  else
    JAVACMD=java
    which java >/dev/null 2>&1 || die "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.

Please set the JAVA_HOME variable in ${WARP10_HOME}/etc/warp10-env.sh to match the
location of your Java installation."
  fi
}

##
## Retrieve all the Warp 10 configuration files
##
getConfigFiles() {
  # Get configuration files from standard directory
  if [ -d "${WARP10_CONFIG_DIR}" ]; then
    CONFIG_FILES=$(find "${WARP10_CONFIG_DIR}" -type f -name \*.conf | sort | tr '\n' ' ' 2>/dev/null)
  fi

  # Get additional configuration files from extra directory
  if [ -d "${WARP10_EXT_CONFIG_DIR:-}" ]; then
    CONFIG_FILES="${CONFIG_FILES} $(find "${WARP10_EXT_CONFIG_DIR}" -type f -name \*.conf | sort | tr '\n' ' ' 2>/dev/null)"
  fi
}

##
## Retrieve Warp 10 Home.
## If WARP10_HOME is not defined, set it to the parent directory
##
getWarp10Home() {
  #WARP10_HOME=$(realpath -eL "${WARP10_HOME:-$(dirname "$0")/..}") # Does not work for Alpine}
  WARP10_HOME=${WARP10_HOME:-$(
    cd "$(dirname "$0")/.."
    pwd
  )}
  TMP_HOME=$(realpath "$(cd "${WARP10_HOME}" && pwd -P)")
  if [ "/" = "${TMP_HOME}" ]; then
    die "ERROR: Warp 10 should not be installed in the '/' directory."
  fi
}

checkRam() {
  if [ "1023m" = "${WARP10_HEAP}" ] || [ "1023m" = "${WARP10_HEAP_MAX}" ]; then
    warn "#### WARNING ####
## Warp 10 was launched with the default RAM setting (i.e. WARP10_HEAP=1023m and/or WARP10_HEAP_MAX=1023m).
## Please edit ${WARP10_HOME}/etc/warp10-env.sh to change the default values of WARP10_HEAD and WARP10_HEAP_MAX."
  fi
}

##
## Exit this script if user doesn't match
##
isWarp10User() {
  if [ -n "${WARP10_USER:+x}" ] && [ "$(id -u -n)" != "${WARP10_USER}" ]; then
    die "You must be '${WARP10_USER}' to run this script."
  fi
}

##
## Return 0 if a Warp 10 instance is started
##
isStarted() {
  # Don't use 'ps -p' or pgrep for docker alpine compatibility
  if [ -f "${PID_FILE}" ] && ps -Ao pid | grep "^\s*$(cat "${PID_FILE}")$" >/dev/null; then
    true
  else
    false
  fi
}

init() {
  echo
  echo "WARP10_HOME=${WARP10_HOME}"
  # If WARP10_USER is undefined set it to current user
  if [ -z "${WARP10_USER:+x}" ]; then
    WARP10_USER=$(id -u -n)
    echo
    warn "WARP10_USER is undefined, you may want to set it in ${WARP10_HOME}/etc/warp10-env.sh by adding WARP10_USER=${WARP10_USER}"
  else
    echo
    echo "WARP10_USER=${WARP10_USER}"
  fi

  ##
  ## Test if user exists
  ##
  if ! id -u "${WARP10_USER}" >/dev/null 2>&1; then
    die "ERROR: ${WARP10_USER} user does not exist, please create it before running this script."
  fi

  isWarp10User

  ##
  ##
  ## Exit if config files already exist
  ##
  getConfigFiles
  if [ -n "${CONFIG_FILES:-}" ]; then
    die "ERROR: Configuration files already exist - Abort initialization."
  fi

  echo "//
// This file contains configurations generated during initialization step.
//
" >"${WARP10_CONFIG_DIR}/99-init.conf"

  WARP10_HOME_ESCAPED=$(echo "${WARP10_HOME}" | sed 's/\\/\\\\/g')        # Escape '\'
  WARP10_HOME_ESCAPED=$(echo "${WARP10_HOME_ESCAPED}" | sed 's/\&/\\&/g') # Escape '&'
  WARP10_HOME_ESCAPED=$(echo "${WARP10_HOME_ESCAPED}" | sed 's/|/\\|/g')  # Escape '|' (separator for sed)

  ##
  ## Copy the template configuration file
  ##
  TEMPLATE=${TEMPLATE:=standalone}
  echo
  echo "Copying ${TEMPLATE} configuration files"
  for file in "${WARP10_HOME}/conf.templates/${TEMPLATE}"/*.template; do
    filename=$(basename "$file")
    cp "${file}" "${WARP10_CONFIG_DIR}/${filename%.template}"
  done

  sed -i -e "s|^User=warp10|User=${WARP10_USER}|" "${WARP10_HOME}/bin/warp10.service"
  sed -i -e 's|^ExecStart=.*|ExecStart='"${WARP10_HOME_ESCAPED}"'/bin/warp10.sh start|' "${WARP10_HOME}/bin/warp10.service"
  sed -i -e 's|^ExecStop=.*|ExecStop='"${WARP10_HOME_ESCAPED}"'/bin/warp10.sh stop|' "${WARP10_HOME}/bin/warp10.service"

  sed -i -e 's|warpLog\.File =.*|warpLog.File = '"${WARP10_HOME_ESCAPED}"'/logs/warp10.log|' "${WARP10_HOME}/etc/log4j.properties"
  sed -i -e 's|warpscriptLog\.File =.*|warpscriptLog.File = '"${WARP10_HOME_ESCAPED}"'/logs/warpscript.out|' "${WARP10_HOME}/etc/log4j.properties"
}

postInit() {
  ##
  ## Generate AES and hash keys
  ##
  echo
  echo "Generating AES and hash keys"
  res=$(${JAVACMD} -cp "${WARP10_JAR}" -Dfile.encoding=UTF-8 io.warp10.GenerateCryptoKeys)
  echo "
//
// AES and Hash definition
//
$(echo "$res" | grep -E 'class.hash.key|labels.hash.key|token.hash.key|app.hash.key|token.aes.key|scripts.aes.key|metasets.aes.key|logging.aes.key|fetch.hash.key')
" >>"${WARP10_CONFIG_DIR}/99-init.conf"

  echo
  echo "Warp 10 configuration has been generated in${WARP10_CONFIG_DIR}"
  echo
  echo "You can now configure the initial and maximum amount of RAM allocated to Warp 10."
  echo "Edit ${WARP10_HOME}/etc/warp10-env.sh and look for WARP10_HEAP and WARP10_HEAP_MAX variables."
  echo
}

distConf() {
  echo "Initializing Warp 10 distributed configuration"
  TEMPLATE="distributed"
  init
  postInit
}

leveldbConf() {
  echo "Initializing Warp 10 standalone configuration"
  init

  echo "
standalone.home = ${WARP10_HOME_ESCAPED}" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  echo "backend = leveldb" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  mv "${WARP10_CONFIG_DIR}/10-fdb.conf" "${WARP10_CONFIG_DIR}/10-fdb.conf.DISABLE"
  getConfigFiles


  ##
  ##  Init LevelDB
  ##
  LEVELDB_HOME="${WARP10_HOME_ESCAPED}/leveldb"
  echo "Initializing LevelDB"
  if ! mkdir -p "${LEVELDB_HOME}/snapshots"; then
    die "ERROR: ${LEVELDB_HOME} creation failed"
  fi
  chmod 700 "${LEVELDB_HOME}"

  ${JAVACMD} -cp "${WARP10_JAR}" io.warp10.standalone.WarpInit "${LEVELDB_HOME}" >>"${WARP10_HOME}/logs/warp10.log" 2>&1

  postInit
}

standalonePlusConf() {
  echo "Initializing Warp 10 standalone+ configuration"
  init

  echo "
backend = fdb
fdb.clusterfile=" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  mv "${WARP10_CONFIG_DIR}/10-leveldb.conf" "${WARP10_CONFIG_DIR}/10-leveldb.conf.DISABLE"
  getConfigFiles

  echo
  echo "Please define the your FoundationDB cluster with 'fdb.clusterfile'"
  echo "See ${WARP10_CONFIG_DIR}/10-fdb.conf for more settings."
  postInit
}

inmemoryConf() {
  echo "Initializing Warp 10 in-memory configuration"
  init
  echo "
backend = memory" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  mv "${WARP10_CONFIG_DIR}/10-fdb.conf" "${WARP10_CONFIG_DIR}/10-fdb.conf.DISABLE"
  mv "${WARP10_CONFIG_DIR}/10-leveldb.conf" "${WARP10_CONFIG_DIR}/10-leveldb.conf.DISABLE"
  getConfigFiles

  echo
  echo "in.memory.chunked = true" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  echo "in.memory.chunk.count = 2" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  echo "in.memory.chunk.length = 86400000000" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  echo "in.memory.load = ${WARP10_HOME}/memory.dump" >>"${WARP10_CONFIG_DIR}/99-init.conf"
  echo "in.memory.dump = ${WARP10_HOME}/memory.dump" >>"${WARP10_CONFIG_DIR}/99-init.conf"

  postInit
}

start() {
  getConfigFiles
  # Config file exists?
  if [ -z "${CONFIG_FILES}" ]; then
    die "ERROR: No configuration files - Use '${WARP10_HOME}/warp10.sh init' script before the very first launch
  WARNING: Since version 2.1.0, Warp 10 can use multiple configuration files. The files have to be present in ${WARP10_CONFIG_DIR}"
  fi

  isWarp10User

  if [ -f "${JAVA_HEAP_DUMP}" ]; then
    mv "${JAVA_HEAP_DUMP}" "${JAVA_HEAP_DUMP}-$(date +%s)"
  fi

  if isStarted; then
    die "ERROR: Start failed - A Warp 10 instance is currently running"
  fi

  #
  # Start Warp10 instance.
  # By default, standard and error output is redirected to warp10.log file, and error output is duplicated to standard output
  # As a consequence, if Warp 10 is launched by systemd, error messages will be in systemd journal too.
  #
  # shellcheck disable=SC2086
  ${JAVACMD} ${JAVA_OPTS} -cp ${WARP10_CP} ${WARP10_CLASS} ${CONFIG_FILES} > >(tee -a ${WARP10_HOME}/logs/warp10.log) 2>&1 &

  echo $! >"${PID_FILE}"

  if ! isStarted; then
    die "Start failed! - See ${WARP10_HOME}/logs/warp10.log for more details"
  fi

  # Check again 5s later (time for plugin load errors)
  sleep 5
  if ! isStarted; then
    die "Start failed! - See ${WARP10_HOME}/logs/warp10.log for more details"
  else
    # display a warning
    checkRam
  fi

}

stop() {
  isWarp10User
  if isStarted; then
    echo "Stopping Warp 10..."
    kill "$(cat "${PID_FILE}")"
    echo "Waiting for Warp 10 to stop..."
    while $(kill -0 $(cat "${PID_FILE}") 2>/dev/null); do
      sleep 2
    done
    echo "Warp 10 stopped..."
    rm -f "${PID_FILE}"
  else
    echo "No instance of Warp 10 is currently running"
  fi
}

status() {
  if isStarted; then
    ps -Ao pid,etime,args | grep "^\s*$(cat "${PID_FILE}")\s"
  else
    echo "No instance of Warp 10 is currently running"
  fi

  checkRam
}

tokengen() {
  if [ "$#" -ne 2 ]; then
    die "Usage: $0 tokengen envelope.mc2"
  fi
  ${JAVACMD} -cp "${WARP10_CP}" -Dlog4j.configuration=file:"${LOG4J_CONF}" -Dfile.encoding=UTF-8 io.warp10.TokenGen ${CONFIG_FILES} "$2" 2>/dev/null
}

run() {
  if [ "$#" -ne 2 ]; then
    die "Usage: $0 run script.mc2"
  fi
  ${JAVACMD} -cp "${WARP10_CP}" -Dlog4j.configuration=file:"${LOG4J_CONF}" -Dfile.encoding=UTF-8 -Dwarp10.config="${CONFIG_FILES}" io.warp10.WarpRun "$2"
}

repair() {
  isWarp10User

  if isStarted; then
    die "Operation has been cancelled! - Warp 10 instance must be stopped for repair"
  else
    echo "Repairing LevelDB..."
    ${JAVACMD} -cp "${WARP10_JAR}" -Dfile.encoding=UTF-8 io.warp10.standalone.WarpRepair "${LEVELDB_HOME}"
  fi
}

compact() {
  isWarp10User

  if isStarted; then
    die "Operation has been cancelled! - Warp 10 instance must be stopped for compaction"
  else
    echo "Compacting LevelDB..."
    ${JAVACMD} -cp "${WARP10_JAR}" io.warp10.leveldb.WarpCompact "${LEVELDB_HOME}" "$1" "$2"
  fi
}

##
## Initialize script
##
getWarp10Home
WARP10_CONFIG_DIR=${WARP10_HOME}/etc/conf.d
WARP10_REVISION=@VERSION@
WARP10_JAR=${WARP10_HOME}/bin/warp10-${WARP10_REVISION}.jar
WARP10_CLASS=io.warp10.Warp
PID_FILE=${WARP10_HOME}/logs/warp10.pid
getConfigFiles
getJava

JAVA_VERSION=$("${JAVACMD}" -XshowSettings:all -version 2>&1 | grep 'java.version = ' | sed -e 's/.* //' -e 's/^1\.//' -e 's/\..*//' -e 's/-.*//')
if [ "${JAVA_VERSION}" -gt 8 ]; then
  ## We need the following for PNGMetadata starting with JDK16
  JAVA_OPTS="--add-exports java.desktop/com.sun.imageio.plugins.png=ALL-UNNAMED ${JAVA_OPTS:-}"
  ## We need the following for FileUrlConnection starting with JDK9
  JAVA_OPTS="--add-exports java.base/sun.net.www.protocol.file=ALL-UNNAMED ${JAVA_OPTS:-}"
fi

##
## Enable JMX
##
if [ -n "${JMX_PORT:+x}" ]; then
  JAVA_OPTS="${JAVA_OPTS:-} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=${JMX_PORT}"
  warn "WARNING: JMX is enabled on port ${JMX_PORT}"
fi

##
## Classpath
## The lib directory is dedicated to user libraries (extensions, plugins...)
##
WARP10_CP=${WARP10_HOME}/etc:${WARP10_JAR}:${WARP10_HOME}/lib/*

SENSISION_EVENTS_DIR=/var/run/sensision/metrics

##
## Set SENSISIONID as environment variable to identify this instance by Sensision
##
export SENSISIONID=warp10

LOG4J_CONF=${WARP10_HOME}/etc/log4j.properties
JAVA_HEAP_DUMP=${WARP10_HOME}/logs/java.heapdump
# you can specialize your metrics for this instance of Warp 10
if [ -n "${WARP10_IDENT:+x}" ]; then
  SENSISION_DEFAULT_LABELS=-Dsensision.default.labels=instance=${WARP10_IDENT}
  JAVA_OPTS="${JAVA_OPTS} -Dwarp.ident=${WARP10_IDENT}"
fi
JAVA_OPTS="-Djava.awt.headless=true -Dlog4j.configuration=file:${LOG4J_CONF} -Dsensision.server.port=0 ${SENSISION_DEFAULT_LABELS:-} -Dsensision.events.dir=${SENSISION_EVENTS_DIR} -Dfile.encoding=UTF-8 -Xms${WARP10_HEAP} -Xmx${WARP10_HEAP_MAX} -XX:+UseG1GC ${JAVA_OPTS:-} ${JAVA_EXTRA_OPTS:-}"
export MALLOC_ARENA_MAX=1

# See how we were called.
case "${1:-}" in
init)
  case "${2:-}" in
  distributed)
    distConf
    ;;
  standalone)
    leveldbConf
    ;;
  standalone+)
    standalonePlusConf
    ;;
  in-memory)
    inmemoryConf
    ;;
  *)
    die "Usage: $0 init <distributed|standalone|standalone+|in-memory>"
    ;;
  esac
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
tokengen)
  tokengen "$@"
  ;;
run)
  run "$@"
  ;;
repair)
  repair
  ;;
compact)
  compact "$@"
  ;;
*)
  die "Usage: $0 {init|tokengen|start|stop|restart|status|repair|compact|run}"
  ;;
esac

exit $?
