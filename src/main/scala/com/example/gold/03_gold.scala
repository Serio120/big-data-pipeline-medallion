package com.example.gold

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * CAPA GOLD — KPIs y Agregaciones
 *
 * Lee las tablas Silver y produce 4 tablas Gold que responden
 * las preguntas de negocio de TechZone:
 *
 *   Q1 → top_products   : Top 10 productos por ingreso total
 *   Q2 → category_kpis  : Margen bruto y ticket medio por categoría
 *   Q3 → monthly_kpis   : Evolución de ventas mes a mes
 *   Q4 → country_kpis   : Ingresos y clientes activos por país
 *
 * Salida: delta/gold/ (Parquet) + dashboard/ (CSV para Power BI)
 */
object GoldKPIs {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ETL-Gold-KPIs")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val silverPath    = "delta/silver"
    val goldPath      = "delta/gold"
    val dashboardPath = "dashboard"

    println("=" * 55)
    println("  🥇 INICIANDO CAPA GOLD — KPIs TechZone")
    println("=" * 55)

    // ─────────────────────────────────────────────
    // Leer tablas Silver
    // ─────────────────────────────────────────────
    val sales     = spark.read.parquet(s"$silverPath/sales")
    val products  = spark.read.parquet(s"$silverPath/products")
    val customers = spark.read.parquet(s"$silverPath/customers")

    // Join enriquecido: sales + products + customers
    val enriched = sales
      .join(products.select("product_id", "name", "category", "cost"),
            Seq("product_id"), "left")
      .join(customers.select("customer_id", "country"),
            Seq("customer_id"), "left")
      .withColumn("revenue",      col("quantity") * col("price"))
      .withColumn("total_cost",   col("quantity") * col("cost"))
      .withColumn("gross_margin", col("revenue") - col("total_cost"))
      .withColumn("year_month",
        date_format(col("order_date"), "yyyy-MM"))

    println(s"\n📊 Dataset enriquecido: ${enriched.count()} filas")

    // ─────────────────────────────────────────────
    // Q1 — Top 10 productos por ingreso total
    // ─────────────────────────────────────────────
    println("\n🏆 [1/4] Calculando TOP PRODUCTS...")

    val topProducts = enriched
      .groupBy("product_id", "name", "category")
      .agg(
        round(sum("revenue"),      2).as("total_revenue"),
        round(sum("gross_margin"), 2).as("total_margin"),
        sum("quantity").as("total_units"),
        count("order_id").as("total_orders"),
        round(avg("price"), 2).as("avg_price")
      )
      .orderBy(desc("total_revenue"))
      .limit(10)
      .withColumn("gold_layer", lit("gold"))

    topProducts.write.mode("overwrite").parquet(s"$goldPath/top_products")
    topProducts.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$dashboardPath/top_products")

    println(s"  ✅ top_products → ${topProducts.count()} filas")
    topProducts.show(10, truncate = false)

    // ─────────────────────────────────────────────
    // Q2 — Margen bruto y ticket medio por categoría
    // ─────────────────────────────────────────────
    println("\n📦 [2/4] Calculando CATEGORY KPIs...")

    val categoryKpis = enriched
      .groupBy("category")
      .agg(
        round(sum("revenue"),      2).as("total_revenue"),
        round(sum("total_cost"),   2).as("total_cost"),
        round(sum("gross_margin"), 2).as("total_margin"),
        round(
          (sum("gross_margin") / sum("revenue")) * 100, 2
        ).as("margin_pct"),
        round(avg("revenue"), 2).as("avg_ticket"),
        count("order_id").as("total_orders"),
        sum("quantity").as("total_units")
      )
      .orderBy(desc("total_revenue"))
      .withColumn("gold_layer", lit("gold"))

    categoryKpis.write.mode("overwrite").parquet(s"$goldPath/category_kpis")
    categoryKpis.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$dashboardPath/category_kpis")

    println(s"  ✅ category_kpis → ${categoryKpis.count()} filas")
    categoryKpis.show(truncate = false)

    // ─────────────────────────────────────────────
    // Q3 — Evolución mensual de ventas
    // ─────────────────────────────────────────────
    println("\n📅 [3/4] Calculando MONTHLY KPIs...")

    val monthlyKpis = enriched
      .groupBy("year_month")
      .agg(
        round(sum("revenue"),      2).as("total_revenue"),
        round(sum("gross_margin"), 2).as("total_margin"),
        count("order_id").as("total_orders"),
        sum("quantity").as("total_units"),
        round(avg("revenue"), 2).as("avg_ticket"),
        countDistinct("customer_id").as("unique_customers")
      )
      .orderBy("year_month")
      .withColumn("gold_layer", lit("gold"))

    monthlyKpis.write.mode("overwrite").parquet(s"$goldPath/monthly_kpis")
    monthlyKpis.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$dashboardPath/monthly_kpis")

    println(s"  ✅ monthly_kpis → ${monthlyKpis.count()} meses")
    monthlyKpis.show(24, truncate = false)

    // ─────────────────────────────────────────────
    // Q4 — Ingresos y clientes activos por país
    // ─────────────────────────────────────────────
    println("\n🌍 [4/4] Calculando COUNTRY KPIs...")

    val countryKpis = enriched
      .groupBy("country")
      .agg(
        round(sum("revenue"),      2).as("total_revenue"),
        round(sum("gross_margin"), 2).as("total_margin"),
        round(
          (sum("gross_margin") / sum("revenue")) * 100, 2
        ).as("margin_pct"),
        countDistinct("customer_id").as("active_customers"),
        count("order_id").as("total_orders"),
        round(avg("revenue"), 2).as("avg_ticket")
      )
      .orderBy(desc("total_revenue"))
      .withColumn("gold_layer", lit("gold"))

    countryKpis.write.mode("overwrite").parquet(s"$goldPath/country_kpis")
    countryKpis.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$dashboardPath/country_kpis")

    println(s"  ✅ country_kpis → ${countryKpis.count()} países")
    countryKpis.show(truncate = false)

    // ─────────────────────────────────────────────
    // Resumen final
    // ─────────────────────────────────────────────
    val totalRevenue = enriched.agg(round(sum("revenue"), 2)).first().getDouble(0)
    val totalMargin  = enriched.agg(round(sum("gross_margin"), 2)).first().getDouble(0)
    val marginPct    = (totalMargin / totalRevenue) * 100

    println("\n" + "=" * 55)
    println("  📋 RESUMEN CAPA GOLD — TechZone")
    println("=" * 55)
    println(f"  Ingresos totales   → $$$totalRevenue%,.2f")
    println(f"  Margen bruto total → $$$totalMargin%,.2f")
    println(f"  Margen bruto %%     → $marginPct%.1f%%")
    println("-" * 55)
    println("  Tablas Gold generadas:")
    println("    delta/gold/top_products   → dashboard/top_products/")
    println("    delta/gold/category_kpis  → dashboard/category_kpis/")
    println("    delta/gold/monthly_kpis   → dashboard/monthly_kpis/")
    println("    delta/gold/country_kpis   → dashboard/country_kpis/")
    println("=" * 55)
    println("✨ ¡Capa Gold completada con éxito!")

    spark.stop()
  }
}