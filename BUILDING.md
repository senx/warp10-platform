# Warp 10 Setup & Build  

All commands examples below are valid for latest ubuntu or other debian based release.

## Install Java 8 jdk
```bash
sudo apt install openjdk-8-jdk
```

## Install Thrift
You need Thrift in order to build the Warp 10 platform.
Currently, Warp 10 uses Thrift 0.11.0, to be compatible with other dependencies. 
All major distributions now ships Thrift 0.13+, so you need to compile Thrift.

Download Thrift 0.11, compile and install it:
```bash
sudo apt install libtool g++ libboost-all-dev build-essential bison
wget "http://archive.apache.org/dist/thrift/0.11.0/thrift-0.11.0.tar.gz"
tar -xf thrift-0.11.0.tar.gz
cd thrift-0.11.0
sudo mkdir -p /opt/thrift-0.11.0
./bootstrap.sh
./configure --prefix=/opt/thrift-0.11.0 --with-qt4=no --with-qt5=no --with-c_glib=no --with-csharp=no --with-java=no --with-erlang=no --with-nodejs=no --with-lua=no --with-python=no --with-perl=no --with-php=no --with-php_extension=no --with-dart=no --with-ruby=no --with-haskell=no --with-go=no --with-rs=no --with-haxe=no --with-dotnetcore=no --with-d=no  --with-cpp=no
make -j$(nproc)
sudo make install
/opt/thrift-0.11.0/bin/thrift -version
```

`thrift -version` should return "Thrift version 0.11.0".

If you need several thrift versions on your system, you can use the `THRIFT_HOME` environment variable (example: `export THRIFT_HOME=/<path_to_thrift>/thrift-0.11.0`).

## Build and replace jar in an existing Warp 10 instance

Clone the repo master branch, compile, and copy the jar to your Warp 10 bin directory.
```bash
cd
sudo apt install git
git clone https://github.com/senx/warp10-platform.git
cd warp10-platform
export THRIFT_HOME=/opt/thrift-0.11.0
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

Add GIT tag `git tag -a 0.0.2-rc1 -m 'release candidate 0.0.2'`
WARNING don't forget to push the TAG `git push origin 0.0.2-rc1`

### Building Warp 10

```
./gradlew build
```

If publishing, use
```
./gradlew publishMavenPublicationToNexusRepository -x test
```
