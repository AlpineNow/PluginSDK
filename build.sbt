//import com.typesafe.sbt.SbtGit.GitKeys

def publishParameters(module: String) = Seq(
  organization := "com.alpinenow",
  name := s"$module",
  version := "1.6",
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
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  scalacOptions in(Compile, doc) ++= Seq("-doc-footer", "Copyright (c) 2015 Alpine Data Labs."),
  javacOptions in compile ++= Seq("-source", javaSourceVersion, "-target", javaTargetVersion),
  crossPaths := false
)

// javax.servlet signing issues can be tricky, we can just exclude the dep
def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
  items.map(_.exclude(group, artifact))

def excludeJavaxServlet(items: Seq[ModuleID]) =
  excludeFromAll(items, "javax.servlet", "servlet-api")
lazy val javaSourceVersion = "1.7"
lazy val javaTargetVersion = "1.7"
lazy val scalaMajorVersion = "2.10"
lazy val sparkVersion = "1.5.1"

useGpg := true
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

def excludeFromAll(items: Seq[ModuleID], group: String, artifacts: Seq[String]) =
  items.flatMap(x => artifacts.map(x.exclude(group, _)))

def excludeJPMML(items: Seq[ModuleID]) : Seq[ModuleID] = {
  excludeFromAll(items, "org.jpmml", Seq("pmml-model", "pmml-evaluator"))
}

def sparkDependencies = excludeJPMML({
  Seq(
    "org.apache.spark" % s"spark-core_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-mllib_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-catalyst_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-sql_$scalaMajorVersion" % sparkVersion ,
    "org.apache.spark" % s"spark-hive_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-yarn_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-unsafe_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-network-yarn_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-network-common_$scalaMajorVersion" % sparkVersion,
    "org.apache.spark" % s"spark-network-shuffle_$scalaMajorVersion" % sparkVersion,
    "com.databricks" % "spark-avro_2.10" % "1.0.0",
    "com.databricks" % "spark-csv_2.10" % "1.3.0"
  )
})

val scalaTestDep = "org.scalatest" % "scalatest_2.10" % "2.2.4"
val gsonDependency = "com.google.code.gson" % "gson" % "2.3.1"
val jodaTimeDependency = "joda-time" % "joda-time" % "2.1"
val commonsIODependency = "commons-io" % "commons-io" % "2.4"
val apacheCommonsDependency = "org.apache.commons" % "commons-lang3" % "3.4"
//val prestoParserDependency = "com.facebook.presto" % "presto-parser" % "0.79" // Versions after this use Java 8.

lazy val Common = Project(
  id = "common",
  base = file("common"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep % "test",
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
      apacheCommonsDependency
    )
  ) ++ publishParameters("alpine-model-api")
).dependsOn(Common)

lazy val ModelPack = Project(
  id = "alpine-model-pack",
  base = file("alpine-model-pack"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep  % "test"
      //  prestoParserDependency
    )
  ) ++ publishParameters("alpine-model-pack")
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
