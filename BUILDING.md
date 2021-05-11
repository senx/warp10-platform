# Warp 10 Setup & Build  

## Install Thrift
You need Thrift in order to build the Warp 10 platform.
Currently, Warp 10 use Thrift 0.11.0

Install it and then add "/opt/thrift-0.11.0/bin" to your path and add `export THRIFT_HOME=/opt/thrift-0.11.0`

## IDE Setup

Import warp10-platform as a gradle project.

## RELEASE Procedure

The release & upload can only be performed on a clone with a git "porcelain" status (no new file or modifications)

### Configuration

Add your API key & credentials in the gradle.properties file.

### Git tag

Commit & push the change.

Adds GIT tag `git tag -a 0.0.2-rc1 -m 'release candidate 0.0.2'`
WARNING don't forget to push the TAG `git push origin 0.0.2-rc1`

### Building Warp 10

```
./gradlew build
```

If publishing, use
```
./gradlew publishMavenPublicationToNexusRepository -x test
```
