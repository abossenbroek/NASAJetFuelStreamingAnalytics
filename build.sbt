name := "NasaJetFuelStreamingAnalytics"

version := "0.1"

scalaVersion := "2.11.12"

mainClass in (Compile, run) := Some("com.bain.C9EA.streaming.NasaStreaming")

libraryDependencies ++=
  Seq("org.apache.spark" %% "spark-core" % "2.3.0" withSources() withJavadoc(),
      "org.apache.spark" %% "spark-sql" % "2.3.0" withSources() withJavadoc(),
        "org.apache.spark" %% "spark-streaming" % "2.3.0" withSources() withJavadoc(),
        "org.scalatest" %% "scalatest" % "3.0.5" % "test" withJavadoc())

