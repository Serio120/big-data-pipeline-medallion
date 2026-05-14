name := "big-data-pipeline-medallion"
version := "1.0.0"
scalaVersion := "2.13.13"

// ✅ Versiones compatibles con tu Spark 4.1.1
val sparkVersion = "4.1.1"
val deltaVersion = "4.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "io.delta"         %% "delta-spark" % deltaVersion,
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}