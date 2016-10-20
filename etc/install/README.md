# WARP 10 Install - Standalone version

## Download Standalone binary

Standalone binary is available via bintray: https://bintray.com/cityzendata/generic/warp10/

~~~
wget https://bintray.com/artifact/download/cityzendata/generic/warp10-1.0.1.tar.gz
tar xf warp10-1.0.1.tar.gz
cd warp10-1.0.1
~~~

## Start Warp 10 Standalone

Launch Warp 10 init script as `root`

Then the init script starts the Standalone mode with the right user `warp10`

~~~
./warp10-standalone.init start
~~~

Logs are available in the `logs` directory

Data are stored via leveldb in the `data` directory

## Data snapshot

Snapshot of leveldb data can be performed via the init script

~~~
./warp10-standalone.init snapshot 'snapshot_name'
~~~