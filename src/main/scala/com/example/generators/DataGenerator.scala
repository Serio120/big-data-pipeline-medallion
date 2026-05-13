package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.Random
import java.time.LocalDate
import java.time.temporal.ChronoUnit

object DataGenerator {
  val spark = SparkSession.builder()
    .appName("SyntheticDataGenerator")
    .getOrCreate()

  import spark.implicits._

  val firstNames = Seq("Juan", "María", "Carlos", "Ana", "Luis", "Sofia", "Pedro", "Laura", "Miguel", "Elena")
  val lastNames = Seq("García", "López", "Martínez", "Rodríguez", "Pérez", "Sánchez", "González", "Hernández", "Díaz", "Moreno")
  val countries = Seq("España", "México", "Argentina", "Colombia", "Chile", "Perú", "Venezuela", "Brasil", "Estados Unidos", "Canadá")
  
  val productNames = Seq(
    "Laptop Pro", "Mouse Inalámbrico", "Teclado Mecánico", "Monitor 4K", "Webcam HD",
    "Auriculares Bluetooth", "Cargador Rápido", "Cable USB-C", "Dock Multifuncional", "SSD 1TB",
    "Router WiFi 6", "Pantalla Táctil", "Projector LED", "Micrófono Condensador", "Cable HDMI"
  )
  
  val categories = Seq("Electrónica", "Periféricos", "Accesorios", "Almacenamiento", "Redes")

  def generateCustomers(count: Int = 10000): Unit = {
    println(s"📊 Generando $count clientes...")
    
    val data = (1 to count).map { id =>
      val firstName = firstNames(Random.nextInt(firstNames.size))
      val lastName = lastNames(Random.nextInt(lastNames.size))
      val country = countries(Random.nextInt(countries.size))
      val signupDate = LocalDate.of(2020, 1, 1)
        .plusDays(Random.nextInt(365 * 6))
      
      (
        id,
        s"$firstName $lastName",
        s"${id}.${firstName.toLowerCase}.${lastName.toLowerCase}@email.com",
        country,
        signupDate.toString
      )
    }

    val df = data.toDF("customer_id", "name", "email", "country", "signup_date")
    df.write
      .mode("overwrite")
      .parquet("data/customers")
    
    println(s"✅ Clientes guardados en: data/customers")
    df.show(5)
  }

  def generateProducts(count: Int = 1000): Unit = {
    println(s"📦 Generando $count productos...")
    
    val data = (1 to count).map { id =>
      val name = productNames(Random.nextInt(productNames.size)) + s" (v${id % 10 + 1})"
      val category = categories(Random.nextInt(categories.size))
      val cost = 10 + Random.nextDouble() * 490
      val price = cost * (1 + Random.nextDouble() * 0.5)
      val supplierId = (id % 50) + 1
      
      (id, name, category, BigDecimal(cost).setScale(2, BigDecimal.RoundingMode.HALF_UP), 
       BigDecimal(price).setScale(2, BigDecimal.RoundingMode.HALF_UP), supplierId)
    }

    val df = data.toDF("product_id", "name", "category", "cost", "price", "supplier_id")
    df.write
      .mode("overwrite")
      .json("data/products")
    
    println(s"✅ Productos guardados en: data/products")
    df.show(5)
  }

  def generateOrders(count: Int = 100000, maxCustomerId: Int = 10000, maxProductId: Int = 1000): Unit = {
    println(s"💳 Generando $count transacciones...")
    
    val data = (1 to count).map { id =>
      val customerId = Random.nextInt(maxCustomerId) + 1
      val productId = Random.nextInt(maxProductId) + 1
      val quantity = Random.nextInt(20) + 1
      val basePrice = 50 + Random.nextDouble() * 450
      val discount = if (Random.nextDouble() < 0.2) Random.nextDouble() * 0.3 else 0.0
      val finalPrice = basePrice * (1 - discount)
      val orderDate = LocalDate.of(2023, 1, 1)
        .plusDays(Random.nextInt(365 * 2))
      
      (id, customerId, productId, quantity, BigDecimal(finalPrice).setScale(2, BigDecimal.RoundingMode.HALF_UP), orderDate.toString)
    }

    val df = data.toDF("order_id", "customer_id", "product_id", "quantity", "price", "order_date")
    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/orders")
    
    println(s"✅ Transacciones guardadas en: data/orders")
    df.show(5)
  }

  def main(args: Array[String]): Unit = {
    println("🚀 Iniciando generación de datos sintéticos...")
    
    generateCustomers(10000)
    println()
    generateProducts(1000)
    println()
    generateOrders(100000, 10000, 1000)
    
    println("\n✨ ¡Generación completada exitosamente!")
    println("📍 Ubicaciones:")
    println("   - Clientes: data/customers (Parquet)")
    println("   - Productos: data/products (JSON)")
    println("   - Órdenes: data/orders (CSV)")
    
    spark.stop()
  }
}
