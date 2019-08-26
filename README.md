# PluginSDK
This contains the source code for the Plugin SDK of the TIBCO Team Studio analytics engine. The jars from this project are published to Maven Central Repository under the name-space com.alpinenow.

This project is written in scala, and built using sbt.

To build this project, it is preferable to use the sbt script provided (taken from https://github.com/paulp/sbt-extras with the license included in sbt-LICENSE.txt). 
This means that you do not need to have sbt already installed on your system.

Enter the sbt interactive console using 
```
./sbt
```

From there you can run tasks like
```
> compile
> test
```

Or you can enter sub projects and run tests from there:
```
> project alpine-model-pack
> test
```

Or run these commands in one line each:
```
./sbt compile
./sbt test
./sbt alpine-model-pack/test
```

To generate the Scaladoc, run 
```
./sbt unidoc
```
Then the documentation will appear in ``` target/scala-2.11/unidoc```
