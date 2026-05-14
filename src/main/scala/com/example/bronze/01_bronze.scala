package com.example.bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables._

object BronzeIngestion {
  def main(args: Array[String]): Unit = {
    // 1. Configuración de Spark con Delta Lake
    val spark = SparkSession.builder()
      .appName("ETL-Bronze-Ingestion")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    println("🚀 Spark con Delta Lake inicializado correctamente.")

    /* 2. Definición de rutas (Cambiamos a rutas relativas que Spark entiende bien)
    val inputPath = "data"
    val outputPath = "delta/bronze"
    */

    // Sustituye las líneas de rutas por estas (Asegúrate de que la ruta sea idéntica a tu PC)
    val inputPath = "E:/modulo-final-big-data/big-data-pipeline-medallion/data"
    val outputPath = "E:/modulo-final-big-data/big-data-pipeline-medallion/delta/bronze"
    println(s"📥 Leyendo datos desde: $inputPath")

    try {
      // --- INGESTA DE SALES (CSV) ---
      // Leemos como String para no perder datos con errores de calidad
      val salesRawDF = spark.read
        .option("header", "true")
        .csv(s"$inputPath/sales")

      val salesBronze = salesRawDF
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit("sales.csv"))

      salesBronze.write
        .mode("overwrite")
        .format("delta")
        .save(s"$outputPath/sales")
      
      println(s"✅ Tabla Bronze SALES creada. Registros: ${salesBronze.count()}")

      // --- INGESTA DE PRODUCTS (JSON) ---
      val productsRawDF = spark.read
        .json(s"$inputPath/products")

      val productsBronze = productsRawDF
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit("products.json"))

      productsBronze.write
        .mode("overwrite")
        .format("delta")
        .save(s"$outputPath/products")

      println(s"✅ Tabla Bronze PRODUCTS creada. Registros: ${productsBronze.count()}")

      // --- INGESTA DE CUSTOMERS (PARQUET) ---
      val customersRawDF = spark.read
        .parquet(s"$inputPath/customers")

      val customersBronze = customersRawDF
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit("customers.parquet"))

      customersBronze.write
        .mode("overwrite")
        .format("delta")
        .save(s"$outputPath/customers")

      println(s"✅ Tabla Bronze CUSTOMERS creada. Registros: ${customersBronze.count()}")

      println("\n✨ ¡Capa Bronze completada con éxito en la carpeta /delta!")

    } catch {
      case e: Exception => 
        println(s"\n❌ ERROR DURANTE LA INGESTA: ${e.getMessage}")
        println("Asegúrate de haber ejecutado primero el generador de datos (Opción 4).")
    } finally {
      println(s"📂 Verificando carpeta de salida: ${new java.io.File(outputPath).getAbsolutePath}")
      spark.stop()
    }
  }
}