val scala3Version = "3.6.3"
val zioVersion = "2.1.16"
val circeVersion = "0.14.10"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Conductor Interview",
    organization := "com.rezo",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.3",
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-http" % "3.1.0",
      "dev.zio" %% "zio-nio" % "2.0.2",
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-kafka" % "2.11.0",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.apache.kafka" % "kafka-clients" % "3.9.0",
      "dev.zio" %% "zio-config-typesafe" % "4.0.3",
      "dev.zio" %% "zio-config-magnolia" % "4.0.3",
      "dev.zio" %% "zio-test" % zioVersion,
      "dev.zio" %% "zio-test-sbt" % zioVersion,
      "dev.zio" %% "zio-kafka-testkit" % "2.11.0"
    )
  )
