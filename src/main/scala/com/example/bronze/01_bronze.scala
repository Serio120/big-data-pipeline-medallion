package com.example.bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BronzeIngestion {
  def main(args: Array[String]): Unit = {

   
    val spark = SparkSession.builder()
      .appName("ETL-Bronze-Ingestion")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ─────────────────────────────────────────────
    // 2. Rutas relativas — funcionan en cualquier PC
    //    Spark las resuelve desde el directorio donde
    //    lanzas spark-submit (la raíz del proyecto)
    // ─────────────────────────────────────────────
    val inputPath  = "data"
    val outputPath = "delta/bronze"
    println(s"📥 Leyendo datos desde: $inputPath")

    try {

      // ── SALES (CSV con problemas de calidad) ───────
      // inferSchema=false → todo entra como String
      // Así Bronze conserva "no aplica", negativos, nulos
      val salesRawDF = spark.read
        .option("header",      "true")
        .option("inferSchema", "false")   // ← clave para Bronze
        .option("mode",        "PERMISSIVE")
        .csv(s"$inputPath/sales")

      val salesBronze = salesRawDF
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file",         lit("sales.csv"))

      salesBronze.write
        .mode("overwrite")
        .parquet(s"$outputPath/sales")

      val salesCount = salesBronze.count()
      println(s"✅ Bronze SALES    → $salesCount filas | delta/bronze/sales")
      salesBronze.printSchema()

      // ── PRODUCTS (JSON con problemas de calidad) ───
      val productsRawDF = spark.read
        .option("mode", "PERMISSIVE")
        .json(s"$inputPath/products")

      val productsBronze = productsRawDF
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file",         lit("products.json"))

      productsBronze.write
        .mode("overwrite")
        .parquet(s"$outputPath/products")

      val productsCount = productsBronze.count()
      println(s"✅ Bronze PRODUCTS → $productsCount filas | delta/bronze/products")
      productsBronze.printSchema()

      // ── CUSTOMERS (Parquet — datos limpios) ────────
      val customersRawDF = spark.read
        .parquet(s"$inputPath/customers")

      val customersBronze = customersRawDF
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file",         lit("customers.parquet"))

      customersBronze.write
        .mode("overwrite")
        .parquet(s"$outputPath/customers")

      val customersCount = customersBronze.count()
      println(s"✅ Bronze CUSTOMERS → $customersCount filas | delta/bronze/customers")
      customersBronze.printSchema()

      // ── Resumen ────────────────────────────────────
      println("\n" + "=" * 50)
      println("  📋 RESUMEN CAPA BRONZE")
      println("=" * 50)
      println(f"  bronze/sales      → $salesCount%,d filas")
      println(f"  bronze/products   → $productsCount%,d filas")
      println(f"  bronze/customers  → $customersCount%,d filas")
      println("=" * 50)
      println("✨ ¡Capa Bronze completada con éxito!")

    } catch {
      case e: Exception =>
        println(s"\n❌ ERROR: ${e.getMessage}")
        println("Asegúrate de haber ejecutado primero DataGeneratorWithQualityIssues.")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
