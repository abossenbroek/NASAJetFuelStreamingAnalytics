name := "NasaJetFuelStreamingAnalytics"

version := "0.1"

scalaVersion := "2.11.12"

logBuffered in Test := false

mainClass in (Compile, run) := Some(
  "org.abossenbroek.NASAJetFuelStreamingAnalytics.NasaStreaming")

libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-core" % "2.3.0" withSources () withJavadoc (),
    "org.apache.spark" %% "spark-sql" % "2.3.0" withSources () withJavadoc (),
    "org.apache.spark" %% "spark-streaming" % "2.3.0" withSources () withJavadoc (),
    "org.scalatest" %% "scalatest" % "3.0.5" % "test" withJavadoc (),
    "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0" withJavadoc(),
    "org.apache.logging.log4j" % "log4j-api" % "2.11.0"withJavadoc(),
    "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Runtime withJavadoc()
  )
