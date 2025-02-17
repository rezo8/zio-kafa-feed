
val scala3Version = "3.6.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Conductor Interview",
    organization := "com.rezo",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq()
  )
