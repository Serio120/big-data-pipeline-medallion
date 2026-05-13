package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AdvancedGenerator {
  val spark = SparkSession.builder()
    .appName("AdvancedDataGenerator")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val customers = spark.read.parquet("data/customers")
    val products = spark.read.json("data/products")
    val orders = spark.read.option("header", "true").csv("data/orders")

    println("=== 📊 ANÁLISIS DE DATOS SINTÉTICOS ===\n")

    println("🔝 Top 10 clientes por gasto:")
    orders.join(customers, $"customer_id" === $"customer_id")
      .groupBy("customer_id", "name")
      .agg(
        sum(col("quantity").cast("double") * col("price").cast("double")).as("total_spent"),
        count("*").as("orders_count")
      )
      .orderBy(desc("total_spent"))
      .limit(10)
      .show()

    println("\n📦 Top 5 productos más vendidos:")
    orders.join(products, $"product_id" === $"product_id")
      .groupBy("product_id", "name")
      .agg(
        sum("quantity").as("total_quantity"),
        avg("price").as("avg_price")
      )
      .orderBy(desc("total_quantity"))
      .limit(5)
      .show()

    spark.stop()
  }
}
