#!/usr/bin/env sh
#
#   Copyright 2018-2023  SenX S.A.S.
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

set -eu

##
## Build the Warp 10 distribution archive.
## The archive generated is for installation and start up of a Warp 10 instance.
##
## This script includes the following steps:
##  - Create the directory hierarchy
##  - Copy startup scripts
##  - Copy log4j, README, config, runner, bootstrap
##  - Copy template configuration
##  - Copy Warp 10 jar
##  - Fix permissions
##

VERSION=$1
if [ -z "${VERSION:+x}" ]; then
  echo "Usage: $0 VERSION WARP_ROOT_PATH"
  exit 1
fi
WARP10_HOME=warp10-${VERSION}

# Warp 10 root project path (../warp10)
WARP_ROOT_PATH=$2
if [ -z "${WARP_ROOT_PATH:+x}" ]; then
  echo "Usage: $0 VERSION WARP_ROOT_PATH"
  exit 1
fi
if [ ! -d "${WARP_ROOT_PATH}" ]; then
  echo "${WARP_ROOT_PATH} folder does not exist."
  exit 1
fi

ARCHIVE=${WARP_ROOT_PATH}/archive

# Remove existing archive dir
rm -rf "${ARCHIVE}"

##
## Create the directory hierarchy
##
mkdir "${ARCHIVE}"
cd "${ARCHIVE}"
mkdir -p "${WARP10_HOME}/bin"
mkdir -p "${WARP10_HOME}/calls"
mkdir -p "${WARP10_HOME}/datalog"
mkdir -p "${WARP10_HOME}/etc/bootstrap"
mkdir -p "${WARP10_HOME}/etc/conf.d"
mkdir -p "${WARP10_HOME}/etc/throttle"
mkdir -p "${WARP10_HOME}/etc/trl"
mkdir -p "${WARP10_HOME}/jars"
mkdir -p "${WARP10_HOME}/lib"
mkdir -p "${WARP10_HOME}/logs"
mkdir -p "${WARP10_HOME}/macros"
mkdir -p "${WARP10_HOME}/tokens"
mkdir -p "${WARP10_HOME}/runners/test/60000"
mkdir -p "${WARP10_HOME}/runners/sensision/60000"

##
## Copy startup scripts
##
cp ../src/main/sh/warp10.service "${WARP10_HOME}/bin/warp10.service"
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/warp10.sh >"${WARP10_HOME}/bin/warp10.sh"

##
## Copy log4j, README, config, runner, bootstrap...
##
cp ../../etc/bootstrap/egress.mc2 "${WARP10_HOME}/etc/bootstrap/egress.mc2"
cp ../../etc/bootstrap/mobius.mc2 "${WARP10_HOME}/etc/bootstrap/mobius.mc2"
cp ../../etc/bootstrap/runner.mc2 "${WARP10_HOME}/etc/bootstrap/runner.mc2"
cp ../../etc/install/README.md "${WARP10_HOME}/README.md"
cp ../../CHANGELOG.md "${WARP10_HOME}/CHANGELOG.md"
cp ../../etc/runners/runner.mc2.DISABLE "${WARP10_HOME}/runners/test/60000/runner.mc2.DISABLE"
cp ../../etc/calls/testcall.sh "${WARP10_HOME}/calls/testcall.sh"
cp ../../etc/calls/testcall.py "${WARP10_HOME}/calls/testcall.py"
cp ../../etc/macros/README "${WARP10_HOME}/macros/README"
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/log4j.properties >"${WARP10_HOME}/etc/log4j.properties"
cp ../../etc/warp10-env.sh "${WARP10_HOME}/etc/warp10-env.sh"

##
## Copy template configuration
##
cp -r ../../etc/conf.templates "${WARP10_HOME}"
sed -i -e "s/@VERSION@/${VERSION}/g" "${WARP10_HOME}"/conf.templates/*/*
cp ../../etc/demo-tokengen.mc2 "${WARP10_HOME}/tokens/demo-tokengen.mc2"

# Copy jars
cp "../build/libs/warp10-${VERSION}.jar" "${WARP10_HOME}/bin/warp10-${VERSION}.jar"

##
## Fix permissions
##
find "${WARP10_HOME}" -type d -exec chmod 755 {} \;
find "${WARP10_HOME}" -type f -exec chmod 644 {} \;
find "${WARP10_HOME}" -type f \( -name \*.sh -o -name \*.py -o -name \*.init \) -exec chmod 755 {} \;
chmod 700 "${WARP10_HOME}/etc"

# Build tar
tar czpf "../build/libs/warp10-${VERSION}.tar.gz" "${WARP10_HOME}"

# Delete ARCHIVE
rm -rf "${ARCHIVE}"
