name := "big-data-pipeline-medallion"
version := "1.0.0"
scalaVersion := "2.13.12"

// Versiones estables para evitar conflictos de dependencias
val sparkVersion = "3.5.0"
val deltaVersion = "3.0.0"

libraryDependencies ++= Seq(
  // Core de Spark para procesamiento distribuido
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  
  // Dependencia específica para Delta Lake (Capa Bronze/Silver/Gold)
  "io.delta" %% "delta-spark" % deltaVersion,

  // (Opcional) Logging para ver mejor qué pasa en la consola
  "org.slf4j" % "slf4j-api" % "2.0.9",
  "org.slf4j" % "slf4j-simple" % "2.0.9"
)

// Estrategia de mezcla para evitar errores al crear el JAR con sbt assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}