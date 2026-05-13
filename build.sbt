name := "synthetic-data-generator"
version := "1.0.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.0",
  "org.scalatest" %% "scalatest" % "3.2.14" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
