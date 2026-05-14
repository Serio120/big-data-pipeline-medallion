name := "big-data-pipeline-medallion"
version := "1.0.0"
scalaVersion := "2.13.12"

// Versiones estables
val sparkVersion = "3.5.0"
val deltaVersion = "3.0.0"

libraryDependencies ++= Seq(
  // Spark Core y SQL
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  
  // Delta Lake
  "io.delta" %% "delta-spark" % deltaVersion,

  // Logging
  "org.slf4j" % "slf4j-api" % "2.0.9",
  "org.slf4j" % "slf4j-simple" % "2.0.9"
)

// Estrategia de mezcla para el JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}