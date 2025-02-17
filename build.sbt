val scala3Version = "3.6.3"
val zioVersion = "2.1.15"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Conductor Interview",
    organization := "com.rezo",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
        "com.typesafe" % "config" % "1.4.3",
        "com.github.pureconfig" %% "pureconfig-core" % "0.17.8",
        
        "dev.zio" %% "zio" % zioVersion,
        "dev.zio" %% "zio-http" % "3.0.1",
    )
  )
