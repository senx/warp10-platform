# WARP 10 IDE Setup for debugging

## How to debug in an existing WARP 10 instance ? (IntelliJ)

You already have a standalone instance of WARP 10 on your computer, and you want to debug with your datas.

You already have a JDK.

You already have thrift in version 0.9.1

In the following howto, running instance is in `~/WARP10/warp10`. Local source directory is `~/warp10_src/warp10-platform`
Make sure your user has group permissions to access all files into your running instance.

+ Stop your running instance `./bin/warp10-standalone.sh stop`


```bash
% ls -1 ~/WARP10/warp10/
bin/
calls/
changelog.html
changelog.md
datalog/
datalog_done/
etc/
geodir/
jars/
leveldb/
lib/
logs/
macros/
README.md
templates/
warpscripts/

ls -1 ~/warp10_src/warp10-platform
build.gradle
BUILDING.md
changelog.js*
crypto/
etc/
gradle/
gradlew*
gradlew.bat
hbaseFilters/
Jenkinsfile
LICENSE
NOTICE
README.md
settings.gradle
token/
warp10/
warpscript/
```

+ Launch IntelliJ
+ file, new, project from existing sources. Select your source directory `~/warp10_src/warp10-platform`
+ Import project from external model, select gradle.
+ Tick 'use auto-import', 'using explicit module groups', 'use default gradle wrapper'.
+ Open in a new window
+ Wait for gradle to complete
+ In a separate terminal, prepare remaining gradle tasks, as described in BUILDING.md
~/warp10_src/warp10-platform/gradlew warp10:generateThrift
~/warp10_src/warp10-platform/gradlew token:generateThrift
~/warp10_src/warp10-platform/gradlew crypto:install
~/warp10_src/warp10-platform/gradlew token:install
+ Display gradle tool window : View, Tool windows, Gradle. Press refresh all gradle project button.
+ Create a test class called WarpTest in warp10-platform/warp10/src/test/java/io.warp10

```java

public class WarpTest {


  @Test
  public void testWarp() throws Exception {
    String warp10_home = "/home/xyz/WARP10/warp10"; //change with your absolute path.


    System.setProperty("log4j.configuration",
            new File(warp10_home + "/etc/log4j.properties").toURI().toString());

    System.setProperty("sensision.server.port", "0");


    Warp.main(new String[]{warp10_home + "/etc/conf-standalone.conf"});
  }
}

```

+ Up to your configuration, warp10 will try to load plugins. You must add them to your classpath in IntelliJ. 
Go to project settings, Libraries. Add A new one for each plugin jar you need. 
For example, add `~/WARP10/warp10/bin/warp10-quantum-plugin-3.0.3.jar`. When asked by IntelliJ, select all modules.

You can now run or debug the WarpTest class.

## How to use my own build ?

When you're done, you can build a fat jar and replace the warp10 jar in your `~/WARP10/warp10/bin/` directory.
In a separate terminal : 
```bash
~/warp10_src/warp10-platform/gradlew crypto:install
~/warp10_src/warp10-platform/gradlew token:install
~/warp10_src/warp10-platform/gradlew pack
```
Copy the jar from `~/warp10_src/warp10-platform/warp10/build/libs/` to your `~/WARP10/warp10/bin/`, keeping the jar file name.



