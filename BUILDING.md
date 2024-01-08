# Warp 10 Setup & Build  

All command examples below are valid for the latest Ubuntu or other Debian-based release.

## Install Java 8 jdk
```bash
sudo apt install -y openjdk-8-jdk
```

## Install Thrift
You need Thrift in order to build the Warp 10 platform.
Currently, Warp 10 uses Thrift 0.17.0, to be compatible with other dependencies. 
All major distributions now ship Thrift 0.13+, so you will need to compile Thrift.

Download Thrift 0.17, compile and install it:
```bash
sudo apt install -y automake bison flex g++ git libboost-all-dev libevent-dev libssl-dev libtool make pkg-config wget
wget "http://archive.apache.org/dist/thrift/0.17.0/thrift-0.17.0.tar.gz"
echo "b272c1788bb165d99521a2599b31b97fa69e5931d099015d91ae107a0b0cc58f  thrift-0.17.0.tar.gz" | sha256sum -c -
tar xf thrift-0.17.0.tar.gz
cd thrift-0.17.0
sudo mkdir -p /opt/thrift-0.17.0
./bootstrap.sh
./configure \
    --prefix=/opt/thrift-0.17.0 \
    --without-cpp \
    --without-qt5 \
    --without-c_glib \
    --without-java \
    --without-kotlin \
    --without-erlang \
    --without-nodejs \
    --without-nodets \
    --without-lua \
    --without-python-sys-prefix \
    --without-python_prefix \
    --without-python_exec_prefix \
    --without-python \
    --without-py3 \
    --without-perl \
    --without-php \
    --without-php_extension \
    --without-dart \
    --without-ruby \
    --without-go \
    --without-swift \
    --without-rs \
    --without-haxe \
    --without-netstd \
    --without-d     
make -j
sudo make install
/opt/thrift-0.17.0/bin/thrift -version
```

`thrift -version` should return "Thrift version 0.17.0".

If you need several thrift versions on your system, you can use the `THRIFT_HOME` environment variable (example: `export THRIFT_HOME=/<path_to_thrift>/thrift-0.17.0`).

## Build and replace jar in an existing Warp 10 instance

Clone the repo master branch, compile, and copy the jar to your Warp 10 bin directory.
```bash
cd
sudo apt install -y git
git clone https://github.com/senx/warp10-platform.git
cd warp10-platform
export THRIFT_HOME=/opt/thrift-0.17.0
./gradlew pack
```
The jar file should be suffixed with the latest tag. 
Copy the jar from `warp10/build/libs/` to `/opt/warp10/bin`. 

If Warp 10 is running on the same machine that the one you are building on: 
```bash
cp warp10/build/libs/warp10-$(git describe).jar /opt/warp10/bin/
```

Then, update WARP10_REVISION in /opt/warp10/bin/warp10.sh with the version number you just compiled.

If Warp 10 is running on the same machine that the one you are building on:
```bash
sed -i "s/WARP10_REVISION=.*/WARP10_REVISION=$(git describe)/g" /opt/warp10/bin/warp10.sh
```

You can now restart Warp 10.


## IDE Setup

Import warp10-platform as a gradle project.

## RELEASE Procedure

The release & upload can only be performed on a clone with a git "porcelain" status (no new file or modifications)

### Configuration

Add your API key & credentials in the gradle.properties file.

### Git tag

Commit & push the change.

Add GIT tag `git tag -s 3.0.0 -m 'Warp 10 release 3.0.0'`
WARNING don't forget to push the TAG `git push origin 3.0.0`

### Building Warp 10

```
./gradlew build
```

If publishing, use
```
./gradlew publishMavenPublicationToNexusRepository -x test
```
