# How to upgrade

Upgrading your Warp 10 instance from a previous release is usually straightforward and simply implies copying the `.jar` file (named `warp10-x.y.z.jar`) into the `bin` directory and modifying the `bin/warp10.sh` script so the `WARP10_REVISION` environment variable is set to the new revision (`x.y.z`).

As the data format is backward compatible, a simple restart of your Warp 10 instance will then be sufficient.

Upgrading to some releases might require some specific steps, they are detailed below.

# Upgrade to `3.0.0`

In this major release, the `warp10-standalone.sh` script was modified and renamed `warp10.sh`. `warp10.init` and `warp10.service` were updated accordingly. 

`3.0.0` comes with breaking changes: 
- the well known `AUTHENTICATE` functions was removed. It is replaced by a new security model based on fine-grained capabilities contained into tokens. 
- all the functions marked deprecated during the `2.x` lifetime were removed.

We encourage you to look at the [blog](https://blog.senx.io/) to read all the 3.x tagged articles to help you during the migration process.


# Upgrade to `2.6.0`

Upgrading from a previous Warp 10 instance to release `2.6.0` will require that you follow the steps below in order to ensure your setup works as expected.

## Init scripts

The `warp10-standalone.sh` script was modified in release `2.6.0` to use a more efficient configuration extraction implementation.

This results in this script to be incompatible with previous releases of Warp 10 and vice-versa, previous versions of this script are not compatible with release `2.6.0`.

In order to launch release `2.6.0` you need to update the `warp10-standalone.sh` script with the one from that specific release.

Modify the new `warp10-standalone.sh` script so it reflects some custom settings you might have configured (`JAVA_HOME`, `JAVA_OPTS`, ...).

Copy the existing `warp10-standalone.sh` for backup purposes.

Replace `warp10-standalone.sh` with your modified version from release `2.6.0`.

## Configuration

Some new features of Warp 10 `2.6.0` might need some additional configuration. We encourage you to look at the [blog](https://blog.senx.io/) post describing those features and to look at the configuration files included with `2.6.0` so you can decide what to add to your deployment configuration. 

## Data

The data format used by `2.6.0` has not changed with that of all previous releases, there is therefore nothing to do to make your data `2.6.0` ready.

## Restarting Warp 10

Restart your Warp 10 instance using `warp10-standalone.init` (as user `root`) or `warp10-standalone.sh` (as user `warp10`).