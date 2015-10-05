//import com.typesafe.sbt.SbtGit.GitKeys

def publishParameters(module: String) = Seq(
  organization := "com.alpinenow",
  name := s"$module",
  version := "1.1",
  publishMavenStyle := true,
  pomExtra := (
    <scm>
    <url>git@github.com:AlpineNow/PluginSDK.git</url>
    <connection>scm:git@github.com:AlpineNow/PluginSDK.git</connection>
  </scm>
  <developers>
    <developer>
      <id>alpine</id>
      <name>alpine</name>
      <url>http://www.alpinenow.com</url>
      <email>support@alpinenow.com</email>
    </developer>
  </developers>
  ),
  homepage := Some(url("https://github.com/AlpineNow/PluginSDK")),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  scalacOptions in(Compile, doc) ++= Seq("-doc-footer", "Copyright (c) 2015 Alpine Data Labs."),
  crossPaths := false
)

// javax.servlet signing issues can be tricky, we can just exclude the dep
def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
  items.map(_.exclude(group, artifact))

def excludeJavaxServlet(items: Seq[ModuleID]) =
  excludeFromAll(items, "javax.servlet", "servlet-api")

lazy val sparkDependencies = excludeJavaxServlet(Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-catalyst_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-yarn_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-network-yarn_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-network-common_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-network-shuffle_2.10" % "1.3.1" % "provided",
  "com.databricks" % "spark-avro_2.10" % "1.0.0",
  "com.databricks" % "spark-csv_2.10" % "1.1.0"
))

val scalaTestDep = "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
val junitDependency = "junit" % "junit" % "4.11" % "test"
val gsonDependency = "com.google.code.gson" % "gson" % "2.3.1"
val jodaTimeDependency = "joda-time" % "joda-time" % "2.1"
val commonsIODependency = "commons-io" % "commons-io" % "2.4"
val apacheCommonsDependency = "org.apache.commons" % "commons-lang3" % "3.4"

lazy val miniClusterDependencies = excludeJavaxServlet(Seq(
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0" % "compile,test" classifier "" classifier "tests",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "compile,test" classifier "" classifier "tests" ,
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "compile,test" classifier "" classifier "tests" ,
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.6.0" % "compile,test" classifier "" classifier "tests",
  "org.apache.hadoop" % "hadoop-yarn-server-tests" % "2.6.0" % "compile,test" classifier "" classifier "tests",
  "org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % "2.6.0" % "compile,test" classifier "" classifier "tests",
  "org.apache.hadoop" % "hadoop-minicluster" % "2.6.0",
  // spark, not marked as provided
  "org.apache.spark" % "spark-core_2.10" % "1.3.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.1",
  "org.apache.spark" % "spark-catalyst_2.10" % "1.3.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.3.1",
  "org.apache.spark" % "spark-hive_2.10" % "1.3.1",
  "org.apache.spark" % "spark-yarn_2.10" % "1.3.1",
  "org.apache.spark" % "spark-network-yarn_2.10" % "1.3.1",
  "org.apache.spark" % "spark-network-common_2.10" % "1.3.1",
  "org.apache.spark" % "spark-network-shuffle_2.10" % "1.3.1",
  // test deps as compile deps so they are carried through
  "org.scalatest" % "scalatest_2.10" % "2.2.4",
  "junit" % "junit" % "4.11"
))

lazy val Common = Project(
  id = "common",
  base = file("common"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep,
      gsonDependency
    )
  ) ++ publishParameters("common")
)

lazy val PluginCore = Project(
  id = "plugin-core",
  base = file("plugin-core"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep
    )
  ) ++ publishParameters("plugin-core")
).dependsOn(Common)

lazy val PluginIOImpl = Project(
  id = "plugin-io-impl",
  base = file("plugin-io-impl"),
  settings = Seq(
    libraryDependencies ++= Seq(
      commonsIODependency,
      apacheCommonsDependency,
      scalaTestDep,
      gsonDependency
    )
  ) ++ publishParameters("plugin-io-impl")
).dependsOn(PluginCore, PluginModel)

lazy val PluginModel = Project(
  id = "plugin-model",
  base = file("plugin-model"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep
    )
  ) ++ publishParameters("plugin-model")
).dependsOn(PluginCore, ModelAPI)

lazy val PluginSpark = Project(
  id = "plugin-spark",
  base = file("plugin-spark"),
  settings = Seq(
    libraryDependencies ++= sparkDependencies ++ Seq(
      scalaTestDep
    )
  ) ++ publishParameters("plugin-spark")
).dependsOn(PluginCore, PluginIOImpl)


lazy val ModelAPI = Project(
  id = "alpine-model-api",
  base = file("alpine-model-api"),
  settings = Seq(
    libraryDependencies ++= Seq(
      gsonDependency,
      jodaTimeDependency,
      scalaTestDep
    )
  ) ++ publishParameters("alpine-model-api")
).dependsOn(Common)

lazy val ModelPack = Project(
  id = "alpine-model-pack",
  base = file("alpine-model-pack"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep
    )
  ) ++ publishParameters("alpine-model-pack")
).dependsOn(ModelAPI % "compile->compile;test->test")

lazy val PluginTest = Project(
  id = "plugin-test",
  base = file("plugin-test"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep,
      junitDependency
    ) ++ miniClusterDependencies
  ) ++ publishParameters("plugin-test")
).dependsOn(PluginCore, PluginSpark, PluginIOImpl)

lazy val root = (project in file("."))
  .settings(unidocSettings: _*)
  // Comment this out in adl, because it is not the official repo.
//  .settings(site.settings ++ ghpages.settings: _*)
//  .settings(
//    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
//    GitKeys.gitRemoteRepo := "git@github.com:AlpineNow/PluginSDK.git",
//  )
  // No need to publish the root
  .settings(publish := {}, publishLocal := {}, packagedArtifacts := Map.empty)
  // Junk publishTo (should not be used)
  .settings(publishTo :=
    Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))))
  .aggregate(
    Common,
    PluginCore,
    PluginSpark,
    PluginIOImpl,
    PluginModel,
    PluginSpark,
    ModelAPI,
    ModelPack,
    PluginTest
  )
