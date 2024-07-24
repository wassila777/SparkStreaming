ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "TSapp"

  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.3"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.3"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "3.3.3"
)
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10"
