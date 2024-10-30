ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.5.0"

lazy val circeDependencies = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.11.2")

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.16.1",
  "org.scalatest" %% "scalatest" % "3.2.17",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "13.0.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "io.netty" % "netty-all" % "4.1.97.Final"
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkTestDemo",
    libraryDependencies ++= sparkDependencies ++ circeDependencies,
    javacOptions ++= Seq("-source", "16"),
    javaOptions ++= Seq( // Spark-specific JVM options
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    compileOrder := CompileOrder.JavaThenScala
  )





















