lazy val scalaV = "2.11.8"
lazy val circeVersion = "0.8.0"
lazy val flinkVersion = "1.3.2"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-generic" % circeVersion,
  "org.apache.flink" % "flink-core" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka-0.10_2.11" % flinkVersion
)

scalaVersion := scalaV
