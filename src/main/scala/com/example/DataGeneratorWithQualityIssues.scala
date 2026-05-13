package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DataGeneratorWithQualityIssues {
  val spark = SparkSession.builder()
    .appName("DataGeneratorWithQualityIssues")
    .getOrCreate()

  import spark.implicits._

  val firstNames = Seq("Juan", "María", "Carlos", "Ana", "Luis", "Sofia", "Pedro", "Laura", "Miguel", "Elena")
  val lastNames = Seq("García", "López", "Martínez", "Rodríguez", "Pérez", "Sánchez", "González", "Hernández", "Díaz", "Moreno")
  val countries = Seq("España", "México", "Argentina", "Colombia", "Chile", "Perú", "Venezuela", "Brasil", "Estados Unidos", "Canadá")
  
  val productNames = Seq(
    "Laptop Pro", "Mouse Inalámbrico", "Teclado Mecánico", "Monitor 4K", "Webcam HD",
    "Auriculares Bluetooth", "Cargador Rápido", "Cable USB-C", "Dock Multifuncional", "SSD 1TB"
  )
  
  val categories = Seq("Electrónica", "Periféricos", "Accesorios", "Almacenamiento", "Redes")

  // ===== GENERAR ÓRDENES CON PROBLEMAS DE CALIDAD =====
  def generateOrdersWithIssues(count: Int = 100000): Unit = {
    println(s"💳 Generando $count órdenes CON PROBLEMAS DE CALIDAD...")
    
    val data = (1 to count).map { id =>
      val customerId = Random.nextInt(10000) + 1
      val productId = Random.nextInt(1000) + 1
      
      // ✗ PROBLEMA 1: Valores nulos (5%)
      val quantity = if (Random.nextDouble() < 0.05) null else Random.nextInt(20) + 1
      
      // ✗ PROBLEMA 2: Precios negativos o no numéricos (3%)
      val price: Any = Random.nextInt(100) match {
        case x if x < 3 => "no aplica"  // Texto en columna numérica
        case x if x < 5 => -Math.abs(Random.nextDouble() * 100)  // Negativos
        case _ => BigDecimal(50 + Random.nextDouble() * 450).setScale(2, BigDecimal.RoundingMode.HALF_UP)
      }
      
      // ✗ PROBLEMA 3: Fechas inconsistentes (50% formato DD/MM/YYYY)
      val orderDate = if (Random.nextDouble() < 0.5) {
        val date = LocalDate.of(2023, 1, 1).plusDays(Random.nextInt(365 * 2))
        date.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))  // DD/MM/YYYY
      } else {
        LocalDate.of(2023, 1, 1).plusDays(Random.nextInt(365 * 2)).toString  // YYYY-MM-DD
      }
      
      (id, customerId, productId, quantity, price, orderDate)
    }

    val df = data.toDF("order_id", "customer_id", "product_id", "quantity", "price", "order_date")
    
    // ✗ PROBLEMA 4: Agregar 100 registros DUPLICADOS (líneas 1000-1050)
    val duplicateData = (1000 to 1099).map { _ =>
      (1001, 5555, 333, 42, "abc_invalid", "01/01/2023")
    }.toDF("order_id", "customer_id", "product_id", "quantity", "price", "order_date")
    
    val finalDf = df.union(duplicateData).coalesce(1)
    
    finalDf.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/sales")
    
    println(s"✅ Órdenes (CON PROBLEMAS) guardadas en: data/sales")
    finalDf.show(10)
  }

  // ===== GENERAR PRODUCTOS CON PROBLEMAS DE CALIDAD =====
  def generateProductsWithIssues(count: Int = 1000): Unit = {
    println(s"📦 Generando $count productos CON PROBLEMAS DE CALIDAD...")
    
    val data = (1 to count).map { id =>
      val name = productNames(Random.nextInt(productNames.size)) + s" (v${id % 10 + 1})"
      
      // ✗ PROBLEMA 1: Espacios extra en nombres (10%)
      val nameWithSpaces = if (Random.nextDouble() < 0.1) {
        "   " + name + "   "
      } else {
        name
      }
      
      // ✗ PROBLEMA 2: 20% sin categoría (nulos)
      val category = if (Random.nextDouble() < 0.2) null else categories(Random.nextInt(categories.size))
      
      // ✗ PROBLEMA 3: Fechas en formato DD/MM/YYYY en JSON
      val createdDate = if (Random.nextDouble() < 0.7) {
        LocalDate.of(2020, 1, 1).plusDays(Random.nextInt(365 * 4))
          .format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))
      } else {
        LocalDate.of(2020, 1, 1).plusDays(Random.nextInt(365 * 4)).toString
      }
      
      val cost = 10 + Random.nextDouble() * 490
      val price = cost * (1 + Random.nextDouble() * 0.5)
      val supplierId = (id % 50) + 1
      
      Map(
        "product_id" -> id,
        "name" -> nameWithSpaces,
        "category" -> category,
        "cost" -> BigDecimal(cost).setScale(2, BigDecimal.RoundingMode.HALF_UP),
        "price" -> BigDecimal(price).setScale(2, BigDecimal.RoundingMode.HALF_UP),
        "supplier_id" -> supplierId,
        "created_date" -> createdDate
      )
    }

    val df = spark.createDataFrame(data).coalesce(1)
    
    df.write
      .mode("overwrite")
      .json("data/products")
    
    println(s"✅ Productos (CON PROBLEMAS) guardados en: data/products")
    df.show(5)
  }

  // ===== GENERAR CLIENTES =====
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
  }

  def main(args: Array[String]): Unit = {
    println("🚀 Iniciando generación de datos CON PROBLEMAS DE CALIDAD...\n")
    
    generateCustomers(10000)
    println()
    generateProductsWithIssues(1000)
    println()
    generateOrdersWithIssues(100000)
    
    println("\n✨ ¡Generación completada!")
    println("📍 Ubicaciones:")
    println("   - Clientes: data/customers (Parquet)")
    println("   - Productos: data/products (JSON)")
    println("   - Órdenes: data/sales (CSV)")
    
    spark.stop()
  }
}
