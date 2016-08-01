#!/bin/sh

#
# Build the distribution .tgz for Warp 10
#

VERSION=$1
# Warp 10 root project path (...../warp10)
WARP_ROOT_PATH=$2

WARP10_HOME=warp10-${VERSION}

ARCHIVE=${WARP_ROOT_PATH}/archive

# Remove existing archive dir
rm -rf ${ARCHIVE}

# Create the directory hierarchy
mkdir ${ARCHIVE}
cd ${ARCHIVE}
mkdir -p ${WARP10_HOME}/bin
mkdir -p ${WARP10_HOME}/templates
mkdir -p ${WARP10_HOME}/data/snapshots
mkdir -p ${WARP10_HOME}/etc/throttle
mkdir -p ${WARP10_HOME}/macros
mkdir -p ${WARP10_HOME}/jars
mkdir -p ${WARP10_HOME}/warpscripts/test/60000
mkdir -p ${WARP10_HOME}/calls
mkdir -p ${WARP10_HOME}/etc/bootstrap
mkdir -p ${WARP10_HOME}/etc/trl
mkdir -p ${WARP10_HOME}/logs

# Copy init and startup scripts
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/snapshot.sh >> ${WARP10_HOME}/bin/snapshot.sh
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/warp10-standalone.init >> ${WARP10_HOME}/bin/warp10-standalone.init
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/warp10-standalone.bootstrap >> ${WARP10_HOME}/bin/warp10-standalone.bootstrap
chmod 755 ${WARP10_HOME}/bin/warp10-standalone.bootstrap

# Copy log4j README, config, runner, bootstrap...
cp ../../etc/bootstrap/*.mc2 ${WARP10_HOME}/etc/bootstrap
cp ../../etc/install/README.md ${WARP10_HOME}
cp ${WARP_ROOT_PATH}/changelog.* ${WARP10_HOME}
cp ../../etc/warpscripts/*.mc2 ${WARP10_HOME}/warpscripts/test/60000
cp ../../etc/calls/*.sh ${WARP10_HOME}/warpscripts/calls
cp ../../etc/calls/*.py ${WARP10_HOME}/warpscripts/calls
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/log4j.properties >> ${WARP10_HOME}/etc/log4j.properties

# Copy template configuration
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/conf-standalone.template > ${WARP10_HOME}/templates/conf-standalone.template

# Copy jar
cp ../build/libs/warp10-${VERSION}.jar ${WARP10_HOME}/bin/warp10-${VERSION}.jar

chmod 755 ${WARP10_HOME}/bin
chmod 755 ${WARP10_HOME}/etc
chmod 755 ${WARP10_HOME}/templates
chmod 755 ${WARP10_HOME}/macros
chmod 755 ${WARP10_HOME}/jars
chmod -R 755 ${WARP10_HOME}/warpscripts
chmod 644 ${WARP10_HOME}/warpscripts/test/60000/*.mc2
chmod 755 ${WARP10_HOME}/calls
chmod 755 ${WARP10_HOME}/calls/*.sh
chmod 755 ${WARP10_HOME}/calls/*.py
chmod 755 ${WARP10_HOME}/etc/throttle
chmod 755 ${WARP10_HOME}/etc/trl
chmod 755 ${WARP10_HOME}/etc/bootstrap
chmod 644 ${WARP10_HOME}/etc/bootstrap/*.mc2
chmod 644 ${WARP10_HOME}/README.*
chmod -R 755 ${WARP10_HOME}/data
chmod 755 ${WARP10_HOME}/bin/*.sh
chmod 755 ${WARP10_HOME}/bin/*.init
chmod 644 ${WARP10_HOME}/bin/warp10-${VERSION}.jar

# Build tar
tar zcpf ../build/libs/warp10-${VERSION}.tar.gz ${WARP10_HOME}
