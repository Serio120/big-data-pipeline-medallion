name := "synthetic-data-generator"
version := "1.0.0"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "4.1.1" % "provided"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
