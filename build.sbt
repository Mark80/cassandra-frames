
import scala.io.Source

val commonSettings: Seq[SettingsDefinition] = Seq(
  organization := "com.frames.cassandra",
  version := Source.fromFile("VERSION").getLines().take(1).next().trim + sys.env
    .getOrElse("VERSION_TAG", ""),
  parallelExecution in ThisBuild := false,
  scalafmtOnCompile in ThisBuild := true,
  testOptions in Test += Tests.Argument("-oD"),
  resolvers ++= Seq(
      Resolver.sonatypeRepo("releases"),
     Resolver.sonatypeRepo("snapshots")
  )
)

scalacOptions += "-Ypartial-unification"
scalacOptions += "-language:higherKinds"

lazy val cassandraFrame = project
  .in(file("."))
  .configs(IntegrationTest)
  .settings(commonSettings: _*)
  .settings(
    Defaults.itSettings,
    inThisBuild(
      List(
        scalaVersion := "2.12.8"
      )),
    name := "cassandra-migration",
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.6.0",
      "org.typelevel" %% "cats" % "0.9.0",
      "org.typelevel" %% "cats-effect" % "1.3.1",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
      "org.scalatest" %% "scalatest" % "3.0.8" % "it, test",
      "org.scalamock" %% "scalamock" % "4.2.0" % Test,
      "com.typesafe" % "config" % "1.3.4"
    )
  )
