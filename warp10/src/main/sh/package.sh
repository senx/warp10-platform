#!/bin/sh

#
# Build the distribution .tgz for Warp 10
#

VERSION=$1
# Warp 10 root project path (../warp10)
WARP_ROOT_PATH=$2
QUANTUM_VERSION=$3

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
mkdir -p ${WARP10_HOME}/data/datalog
mkdir -p ${WARP10_HOME}/data/datalog_done
mkdir -p ${WARP10_HOME}/etc/throttle
mkdir -p ${WARP10_HOME}/macros
mkdir -p ${WARP10_HOME}/jars
mkdir -p ${WARP10_HOME}/warpscripts/test/60000
mkdir -p ${WARP10_HOME}/calls
mkdir -p ${WARP10_HOME}/etc/bootstrap
mkdir -p ${WARP10_HOME}/etc/trl
mkdir -p ${WARP10_HOME}/logs

# Get Quantum plugin
URL_QUANTUM_PLUGIN="https://dl.bintray.com/cityzendata/generic/io/warp10/warp10-quantum-plugin/${QUANTUM_VERSION}/warp10-quantum-plugin-${QUANTUM_VERSION}.jar"

cd ${WARP10_HOME}/bin
echo "curl -L ${URL_QUANTUM_PLUGIN} -o warp10-quantum-plugin-${QUANTUM_VERSION}.jar"
curl -L ${URL_QUANTUM_PLUGIN} -o warp10-quantum-plugin-${QUANTUM_VERSION}.jar

# test archive is ok
unzip -t warp10-quantum-plugin-${QUANTUM_VERSION}.jar > /dev/null
if [ $? -ne 0 ]; then
    echo "Error during Quantum download"
    exit 1
fi

cd ${ARCHIVE}
# Copy init and startup scripts
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/snapshot.sh >> ${WARP10_HOME}/bin/snapshot.sh
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/warp10-standalone.init >> ${WARP10_HOME}/bin/warp10-standalone.init
sed -i -e "s/@QUANTUM_VERSION@/${QUANTUM_VERSION}/g" ${WARP10_HOME}/bin/warp10-standalone.init

# Copy log4j README, config, runner, bootstrap...
cp ../../etc/bootstrap/*.mc2 ${WARP10_HOME}/etc/bootstrap
cp ../../etc/install/README.md ${WARP10_HOME}
cp ${WARP_ROOT_PATH}/changelog.* ${WARP10_HOME}
cp ../../etc/warpscripts/*.mc2* ${WARP10_HOME}/warpscripts/test/60000
cp ../../etc/calls/*.sh ${WARP10_HOME}/calls
cp ../../etc/calls/*.py ${WARP10_HOME}/calls
cp ../../etc/macros/* ${WARP10_HOME}/macros
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/log4j.properties >> ${WARP10_HOME}/etc/log4j.properties

# Copy template configuration
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/conf-standalone.template > ${WARP10_HOME}/templates/conf-standalone.template
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/conf-distributed.template > ${WARP10_HOME}/templates/conf-distributed.template

# Copy jars
cp ../build/libs/warp10-${VERSION}.jar ${WARP10_HOME}/bin/warp10-${VERSION}.jar

chmod 755 ${WARP10_HOME}/bin
chmod 755 ${WARP10_HOME}/etc
chmod 755 ${WARP10_HOME}/templates
chmod 755 ${WARP10_HOME}/macros
chmod 644 ${WARP10_HOME}/macros/README
chmod 755 ${WARP10_HOME}/jars
chmod -R 755 ${WARP10_HOME}/warpscripts
chmod 644 ${WARP10_HOME}/warpscripts/test/60000/*.mc2*
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
tar czpf ../build/libs/warp10-${VERSION}.tar.gz ${WARP10_HOME}
