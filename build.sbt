import java.io.Serializable

import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.Keys.resolvers
//import com.typesafe.sbt.SbtGit.GitKeys

val sdkVersion = "1.11"
val javaSourceVersion = "1.7"
val javaTargetVersion = "1.7"
val scalaMajorVersion = "2.11"
val scalaFullVersion = "2.11.8"
val sparkVersion = "2.1.2"

scalaVersion := scalaFullVersion

def publishParameters(module: String) = Seq(
  organization := "com.alpinenow",
  name := module,
  version := sdkVersion,
  publishMavenStyle := true,
  pomExtra := <scm>
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
    </developers>,
  homepage := Some(url("https://github.com/AlpineNow/PluginSDK")),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  scalaVersion := scalaFullVersion,
  licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  scalacOptions in(Compile, doc) ++= Seq("-doc-footer", "Copyright (c) 2015 Alpine Data Labs."),
  javacOptions in compile ++= Seq("-source", javaSourceVersion, "-target", javaTargetVersion),
  crossPaths := false
)

val hadrianDependency = "com.opendatagroup" % "hadrian_SNAPSHOT_2.11" % "0.8.5"

// javax.servlet signing issues can be tricky, we can just exclude the dep
def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
  items.map(_.exclude(group, artifact))

def excludeJavaxServlet(items: Seq[ModuleID]) =
  excludeFromAll(items, "javax.servlet", "servlet-api")

useGpg := true
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

def excludeFromAll(items: Seq[ModuleID], group: String, artifacts: Seq[String]) =
  items.flatMap(x => artifacts.map(x.exclude(group, _)))

def excludeJPMML(items: Seq[ModuleID]): Seq[ModuleID] = {
  excludeFromAll(items, "org.jpmml", Seq("pmml-model", "pmml-evaluator"))
}

def sparkDependencies = excludeJPMML({
  Seq(
    "org.apache.spark" % s"spark-core_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-mllib_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-catalyst_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-sql_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-hive_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-yarn_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-unsafe_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-network-yarn_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-network-common_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-network-shuffle_$scalaMajorVersion" % sparkVersion,
    "com.databricks" % s"spark-avro_$scalaMajorVersion" % "3.2.0"
  )
})

val scalaTestDep = "org.scalatest" % s"scalatest_$scalaMajorVersion" % "2.2.4"
val gsonDependency = "com.google.code.gson" % "gson" % "2.3.1"
val jodaTimeDependency = "joda-time" % "joda-time" % "2.1"
val commonsIODependency = "commons-io" % "commons-io" % "2.4"
val commonsLang3Dependency = "org.apache.commons" % "commons-lang3" % "3.5"
val commonsDBUtilsDependency = "commons-dbutils" % "commons-dbutils" % "1.6"
//val prestoParserDependency = "com.facebook.presto" % "presto-parser" % "0.79" // Versions after this use Java 8.

lazy val Common = Project(
  id = "common",
  base = file("common"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep % "test",
      gsonDependency, jodaTimeDependency
    )
  ) ++ publishParameters("common")
)

lazy val PluginCore = Project(
  id = "plugin-core",
  base = file("plugin-core"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep,
      commonsDBUtilsDependency // Not directly used by this project, but included so that CO projects get it transitively.
    )
  ) ++ publishParameters("plugin-core")
).dependsOn(Common)

lazy val PluginIOImpl = Project(
  id = "plugin-io-impl",
  base = file("plugin-io-impl"),
  settings = Seq(
    libraryDependencies ++= Seq(
      commonsIODependency,
      commonsLang3Dependency,
      scalaTestDep % "test",
      gsonDependency
    )
  ) ++ publishParameters("plugin-io-impl")
).dependsOn(PluginCore, PluginModel)

lazy val PluginModel = Project(
  id = "plugin-model",
  base = file("plugin-model"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep % "test"
    )
  ) ++ publishParameters("plugin-model")
).dependsOn(PluginCore, ModelAPI)

lazy val PluginSpark = Project(
  id = "plugin-spark",
  base = file("plugin-spark"),
  settings = Seq(
    libraryDependencies ++= sparkDependencies ++ Seq(
      scalaTestDep % "test"
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
      scalaTestDep % "test",
      commonsLang3Dependency
    )
  ) ++ publishParameters("alpine-model-api")
).dependsOn(Common)

lazy val ModelPack = Project(
  id = "alpine-model-pack",
  base = file("alpine-model-pack"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep % "test",
      hadrianDependency % "test"
      //  prestoParserDependency
    )
  ) ++ publishParameters("alpine-model-pack") ++ Seq(
    // For Hadrian
    resolvers += "Artifactory" at "https://repo.alpinedata.tech/alpine-local-mvn"
  )
).dependsOn(ModelAPI % "compile->compile;test->test")

lazy val PluginTest = Project(
  id = "plugin-test",
  base = file("plugin-test"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep
    ) ++ sparkDependencies
  ) ++ publishParameters("plugin-test")
).dependsOn(PluginCore, PluginSpark, PluginIOImpl)

lazy val CompleteModule = Project(
  id = "alpine-complete-sdk",
  base = file("alpine-complete-sdk"), // Empty.
  settings = assemblySettings ++ Seq(
    scalaVersion := scalaFullVersion,
    test in assembly := {},
    jarName in assembly := s"alpine-sdk-assembly-$sdkVersion.jar",
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
      case PathList("org", "objenesis", xs@_*) => MergeStrategy.last
      case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
      case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
      case PathList("javax", "inject", xs@_*) => MergeStrategy.last
      case PathList("javax", "activation", xs@_*) => MergeStrategy.last
      case PathList("org", "apache", xs@_*) => MergeStrategy.last
      case PathList("com", "google", xs@_*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
      case PathList("com", "codahale", xs@_*) => MergeStrategy.last
      case PathList("com", "yammer", xs@_*) => MergeStrategy.last
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case "about.html" => MergeStrategy.rename
      case "overview.html" => MergeStrategy.rename
      case "plugin.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case "parquet.thrift" => MergeStrategy.last
      case "plugin.xml" => MergeStrategy.last
      case x => old(x)
    }
    })
).dependsOn(
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

lazy val root = (project in file("."))
  .settings(unidocSettings: _*)
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
