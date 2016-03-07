# WARP 10 IDE Setup & Build  

## Install Thrift on MAC OS
The current version of thrift used by Warp10 is thrift 0.9.1. Download the tar.gz on the official Apache Thrift page (http://archive.apache.org/dist/thrift/0.9.1/). Then execute the following commands: 

	./configure --without-python --prefix=/opt/thrift CPPFLAGS='-I/usr/local/opt/openssl/include'
	make

Make may raise Errors and warning, do not care about it and exec: 

	sudo make install
	
Then add "/opt/thrift/bin" to your path before follwing the next steps

## IDE Setup

### Eclipse Configuration
In order to avoid conflicts between gradle & Eclipse, please configure Eclipse default output path on another directory BEFORE importing the gradle project(bin for example in Preferences/Java/Build Path)

### Execute Gradle tasks
Generate Thrift source code with the tasks

    gradle warp10:generateThrift
    gradle token:generateThrift

### Import the project with Eclipse
Import the project in Eclipse as Gradle project.
If build/gen-java are not compiled on warp10 & token subproject, refresh it manually.

Warp10 should compile and run at this step.

### Import the project with IntelliJ
Open the project in IntelliJ with 'File/Open...'
The project should be imported as Gradle project by IntelliJ.

Warp10 should compile and run at this step.

### token or crypto not available on JCenter ???
It's possible on the head with DEV versions.
Activate MavenLocal() repository (uncomment it in the global repositories definition)

#### 1. install crypto on your local repo

    gradle crypto:install

#### 2. install token on  your local repo

    gradle token:install
    
#### 3. updates Eclipse External Dependencies
Warp10 Context Menu / Gradle / Refresh Gradle Project

## RELEASE Procedure

The release & upload can only be performed on a clone with a git "porcelain" status (no new file or modifications)

### Configuration

Add your API key & credentials in the gradle.properties file.

### GIT TAG

commit & push the change.

adds GIT tag `git tag -a 0.0.2-rc1 -m 'release candidate 0.0.2'`
WARNING don't forget to push the TAG `git push origin 0.0.2-rc1`

### Build Warp10
Build Warp10 in the order bellow

    gradle crypto:clean crypto:bintrayUpload   (if updated)
    gradle token:clean  token:bintrayUpload    (if updated)
    gradle warp10:clean warp10:bintrayUpload
    
