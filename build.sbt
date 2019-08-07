lazy val sparkVersion = "2.2.1"

lazy val root = (project in file(".")).
  settings(inThisBuild(List(
    organization := "com.slf",
    scalaVersion  := "2.11.12"
  ))).
  settings(
    name := "BulkCopy",
    version := "1.0",
    test in assembly :={},
    fork in Test := true,
    test in assembly :={},
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := true,
    resolvers ++=
      Seq(
        "repo" at "https://mvnrepository.com/artifact/",
        "repo1" at "http://central.maven.org/maven2/",
        "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
      ),

    libraryDependencies ++=
      Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.9.0" % "test",
        "org.scalatest" %%   "scalatest" % "2.2.4" % "test",
        "com.typesafe" % "config" % "1.3.4",
        "org.apache.spark" %% "spark-hive" % sparkVersion % "test",
        "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.1.jre8" % "provided",
        "org.postgresql" % "postgresql" % "9.3-1100-jdbc4" % "provided"
      )
  )

publishTo := Some("Internal Publish Repository" at "http://mvn-repo.dev.slfinternal.com/artifactory/local-release/")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

enablePlugins(UniversalPlugin)

  // removes all jar mappings in universal and appends the fat jar from assembly
mappings in Universal := {
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in Compile).value
  // filter out all jars
  val filtered = universalMappings filter {
    case (file, name) =>  ! name.endsWith(".jar")
  }
  // add the fat jar
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

// add config file, log4j, and submit script to mappings
mappings in Universal ++= {
  val log4j = (resourceDirectory in Compile).value / "log4j.properties"
  Seq(log4j -> "conf/log4j.properties")

}

