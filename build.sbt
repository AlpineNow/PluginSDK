import com.typesafe.sbt.SbtGit.GitKeys

def publishParameters(module: String) = Seq(
  name := s"$module",
  version := "0.9.9.9.1",
  publishTo := Some(Resolver.file(s"$module", new File("./alpine-maven-repo/releases/"))),
  publishMavenStyle := true,
  scalacOptions in(Compile, doc) ++= Seq("-doc-footer", "Copyright (c) 2015 Alpine Data Labs.")
)

lazy val sparkDependencies = Seq(
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
)

val scalaTestDep = "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
val gsonDependency = "com.google.code.gson" % "gson" % "2.3.1"
val jodaTimeDependency = "joda-time" % "joda-time" % "2.1"
val commonsIODependency = "commons-io" % "commons-io" % "2.4"
val apacheCommonsDependency = "org.apache.commons" % "commons-lang3" % "3.4"

lazy val PluginCore = Project(
  id = "plugin-core",
  base = file("plugin-core"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep
    )
  ) ++ publishParameters("plugin-core")
)

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
)

lazy val ModelPack = Project(
  id = "alpine-model-pack",
  base = file("alpine-model-pack"),
  settings = Seq(
    libraryDependencies ++= Seq(
      scalaTestDep
    )
  ) ++ publishParameters("alpine-model-pack")
).dependsOn(ModelAPI % "compile->compile;test->test")

lazy val root = (project in file("."))
  .settings(unidocSettings: _*)
  .settings(site.settings ++ ghpages.settings: _*)
  .settings(
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
    GitKeys.gitRemoteRepo := "git@github.com:AlpineNow/PluginSDK.git"
  )
  .aggregate(
    PluginCore,
    PluginSpark,
    PluginIOImpl,
    PluginModel,
    PluginSpark,
    ModelAPI,
    ModelPack
  )
