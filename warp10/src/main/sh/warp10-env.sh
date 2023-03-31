#
#   Copyright 2023  SenX S.A.S.
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

############################################################################
#                                                                          #
#  This file defines environment variables used by warp10.sh               #
#                                                                          #
############################################################################

##
## Initial and maximum RAM
##
WARP10_HEAP=1023m
WARP10_HEAP_MAX=1023m

## JAVA_HOME is automatically detected by warp10.sh
## However, on a system with several version of java, you may want to specify the path explicitly.
## JAVA_HOME/bin/java or JAVA_HOME/jre/bin/java must exist.
#JAVA_HOME=/opt/java8

################################################## advanced options ###################################################

## If it exists on your system, JAVA_OPTS is automatically appended to java options by warp10.sh
## If you want to specify other options, you can define JAVA_EXTRA_OPTS here.
## JAVA_EXTRA_OPTS overrides JAVA_OPTS.
#JAVA_EXTRA_OPTS=""

##
## By default, if you start Warp 10 with JMX for debug, default port is 1098. You can override it here.
##
JMX_PORT=1098

############################################ power user deployment options ############################################

##
## By default, the initialization will create a user and a group for Warp 10. You may redefine it here:
##
#WARP10_USER=warp10
#WARP10_GROUP=warp10

##
## WARP10_HOME is automatically detected by warp10.sh as its parent directory. You may need to redefine it.
##
#WARP10_HOME=/opt/warp10

##
## By default, Warp 10 is deployed in $WARP10_HOME.
## An alternative deployment is to move the following folders to another volume:
##    calls, datalog-ng, etc, jars, <leveldb>, lib, logs, macros, runners.
## This can be done at bootstrap time or later, if you define WARP10_DATA_DIR.
#WARP10_DATA_DIR=/data

##
## All configurations are loaded from $WARP10_HOME/etc/conf.d.
## You can define an extra directory to store more personal configuration files.
## If you do so, warp10.sh will load *.conf files from this directory too.
#WARP10_EXT_CONFIG_DIR=/path/to/extra/personal/configs

