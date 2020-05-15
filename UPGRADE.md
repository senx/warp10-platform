Upgrading from a previous Warp 10 instance to release `2.5.1` will require that you follow the steps below in order to ensure your setup works as expected.

# Init scripts

The `warp10-standalone.sh` script was modified in release `2.5.1` to use a more efficient configuration extraction implementation.

This results in this script to be incompatible with previous releases of Warp 10 and vice-versa, previous versions of this script are not compatible with release `2.5.1`.

In order to launch release `2.5.1` you need to update the `warp10-standalone.sh` script with the one from that specific release.

Modify the new `warp10-standalone.sh` script so it reflects some custom settings you might have configured (`JAVA_HOME`, `JAVA_OPTS`, ...).

Copy the existing `warp10-standalone.sh` for backup purposes.

Replace `warp10-standalone.sh` with your modified version from release `2.5.1`.

# Configuration

Some new features of Warp 10 `2.5.1` might need some additional configuration. We encourage you to look at the [blog](https://blog.senx.io/) post describing those features and to look at the configuration files included with `2.5.1` so you can decide what to add to your deployment configuration. 

# Data

The data format used by `2.5.1` has not changed with that of all previous releases, there is therefore nothing to do to make your data `2.5.1` ready.

# Restarting Warp 10

Restart your Warp 10 instance using `warp10-standalone.init`.


