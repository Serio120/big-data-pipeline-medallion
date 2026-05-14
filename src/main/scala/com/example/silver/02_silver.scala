package com.example.silver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * CAPA SILVER — Limpieza y Normalización
 *
 * Lee las tablas Bronze (Parquet), aplica limpieza documentada
 * y guarda en silver/ (Parquet).
 *
 * Problemas tratados:
 *   SALES:
 *     - 100 registros duplicados       → dropDuplicates
 *     - 5% nulos en quantity           → imputar con 1
 *     - Precios inválidos ("no aplica", negativos) → eliminar fila
 *     - Fechas en DD/MM/YYYY y YYYY-MM-DD → normalizar a YYYY-MM-DD
 *
 *   PRODUCTS:
 *     - Espacios extra en name         → trim
 *     - 20% sin category               → imputar con "Sin Categoría"
 *     - Fechas inconsistentes          → normalizar a YYYY-MM-DD
 *
 *   CUSTOMERS:
 *     - Datos limpios de origen        → solo cast de tipos
 */
object SilverCleaning {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ETL-Silver-Cleaning")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val bronzePath = "delta/bronze"
    val silverPath = "delta/silver"

    println("=" * 55)
    println("  🥈 INICIANDO CAPA SILVER — Limpieza")
    println("=" * 55)

    // ─────────────────────────────────────────────
    // 1. SILVER SALES
    // ─────────────────────────────────────────────
    println("\n🧹 [1/3] Limpiando SALES...")

    val salesBronze = spark.read.parquet(s"$bronzePath/sales")
    val salesBronzeCount = salesBronze.count()

    // A) Eliminar duplicados exactos (los 100 registros iguales)
    val salesDedup = salesBronze.dropDuplicates(
      Seq("order_id", "customer_id", "product_id", "order_date")
    )
    val dupRemoved = salesBronzeCount - salesDedup.count()
    println(s"  ✂️  Duplicados eliminados: $dupRemoved")

    // B) Filtrar precios inválidos: no numéricos o negativos
    val salesValidPrice = salesDedup
      .filter(col("price").rlike("^[0-9]+(\\.[0-9]+)?$"))  // solo números positivos
    val invalidPrices = salesDedup.count() - salesValidPrice.count()
    println(s"  ❌ Precios inválidos eliminados: $invalidPrices")

    // C) Eliminar filas con order_id o customer_id nulos (claves primarias)
    val salesNoNullKeys = salesValidPrice
      .filter(col("order_id").isNotNull && col("customer_id").isNotNull)

    // D) Imputar quantity nula con 1 (venta mínima)
    val salesImputed = salesNoNullKeys
      .withColumn("quantity", when(col("quantity").isNull, lit("1")).otherwise(col("quantity")))
    val nullsImputed = salesNoNullKeys.filter(col("quantity").isNull).count()
    println(s"  🔧 Nulos en quantity imputados con 1: $nullsImputed")

    // E) Normalizar fechas: DD/MM/YYYY → YYYY-MM-DD
    val salesNormDate = salesImputed.withColumn("order_date",
      when(col("order_date").rlike("^\\d{2}/\\d{2}/\\d{4}$"),
        to_date(col("order_date"), "dd/MM/yyyy").cast("string")
      ).otherwise(col("order_date"))
    )

    // F) Cast de tipos ahora que los datos están limpios
    val salesSilver = salesNormDate
      .withColumn("order_id",    col("order_id").cast("int"))
      .withColumn("customer_id", col("customer_id").cast("int"))
      .withColumn("product_id",  col("product_id").cast("int"))
      .withColumn("quantity",    col("quantity").cast("int"))
      .withColumn("price",       col("price").cast("double"))
      .withColumn("order_date",  col("order_date").cast("date"))
      .withColumn("silver_layer", lit("silver"))

    salesSilver.write.mode("overwrite").parquet(s"$silverPath/sales")
    val salesSilverCount = salesSilver.count()
    println(s"  ✅ Silver SALES → $salesSilverCount filas | delta/silver/sales")
    salesSilver.printSchema()

    // ─────────────────────────────────────────────
    // 2. SILVER PRODUCTS
    // ─────────────────────────────────────────────
    println("\n🧹 [2/3] Limpiando PRODUCTS...")

    val productsBronze = spark.read.parquet(s"$bronzePath/products")

    // A) Trim de espacios extra en name
    val productsTrimmed = productsBronze
      .withColumn("name", trim(col("name")))

    // B) Imputar category nula con "Sin Categoría"
    val productsImputed = productsTrimmed
      .withColumn("category",
        when(col("category").isNull, lit("Sin Categoría"))
        .otherwise(col("category"))
      )
    val nullCats = productsBronze.filter(col("category").isNull).count()
    println(s"  🔧 Categorías nulas imputadas: $nullCats")

    // C) Normalizar fechas inconsistentes en created_date
    val productsNormDate = productsImputed.withColumn("created_date",
      when(col("created_date").rlike("^\\d{2}/\\d{2}/\\d{4}$"),
        to_date(col("created_date"), "dd/MM/yyyy").cast("string")
      ).otherwise(col("created_date"))
    )

    // D) Cast de tipos
    val productsSilver = productsNormDate
      .withColumn("product_id",   col("product_id").cast("int"))
      .withColumn("cost",         col("cost").cast("double"))
      .withColumn("price",        col("price").cast("double"))
      .withColumn("supplier_id",  col("supplier_id").cast("int"))
      .withColumn("created_date", col("created_date").cast("date"))
      .withColumn("silver_layer", lit("silver"))

    productsSilver.write.mode("overwrite").parquet(s"$silverPath/products")
    val productsSilverCount = productsSilver.count()
    println(s"  ✅ Silver PRODUCTS → $productsSilverCount filas | delta/silver/products")
    productsSilver.printSchema()

    // ─────────────────────────────────────────────
    // 3. SILVER CUSTOMERS
    // ─────────────────────────────────────────────
    println("\n🧹 [3/3] Limpiando CUSTOMERS...")

    val customersBronze = spark.read.parquet(s"$bronzePath/customers")

    // Customers llegan limpios — solo cast de tipos y añadir silver_layer
    val customersSilver = customersBronze
      .withColumn("customer_id",  col("customer_id").cast("int"))
      .withColumn("signup_date",  col("signup_date").cast("date"))
      .withColumn("silver_layer", lit("silver"))

    customersSilver.write.mode("overwrite").parquet(s"$silverPath/customers")
    val customersSilverCount = customersSilver.count()
    println(s"  ✅ Silver CUSTOMERS → $customersSilverCount filas | delta/silver/customers")
    customersSilver.printSchema()

    // ─────────────────────────────────────────────
    // 4. Resumen
    // ─────────────────────────────────────────────
    println("\n" + "=" * 55)
    println("  📋 RESUMEN CAPA SILVER")
    println("=" * 55)
    println(f"  Bronze SALES entrada  → $salesBronzeCount%,d filas")
    println(f"  Silver SALES salida   → $salesSilverCount%,d filas")
    println(f"  Filas eliminadas      → ${salesBronzeCount - salesSilverCount}%,d")
    println("-" * 55)
    println(f"  Silver PRODUCTS   → $productsSilverCount%,d filas")
    println(f"  Silver CUSTOMERS  → $customersSilverCount%,d filas")
    println("=" * 55)
    println("✨ ¡Capa Silver completada con éxito!")

    spark.stop()
  }
}