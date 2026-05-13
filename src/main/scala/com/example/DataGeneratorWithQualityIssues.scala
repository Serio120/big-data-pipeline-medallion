package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
  val productNames = Seq("Laptop Pro", "Mouse", "Teclado", "Monitor 4K", "Webcam HD", "Auriculares", "Cargador", "Cable USB-C", "Dock", "SSD 1TB")
  val categories = Seq("Electrónica", "Periféricos", "Accesorios", "Almacenamiento", "Redes")

  def generateOrdersWithIssues(count: Int = 100000): Unit = {
    println(s"💳 Generando $count órdenes CON PROBLEMAS...")
    
    val data = (1 to count).map { id =>
      val customerId = Random.nextInt(10000) + 1
      val productId = Random.nextInt(1000) + 1
      
      // ✗ 5% valores nulos
      val quantity: String = if (Random.nextDouble() < 0.05) null else (Random.nextInt(20) + 1).toString
      
      // ✗ 3% precios inválidos
      val price: String = Random.nextInt(100) match {
        case x if x < 2 => "no aplica"
        case x if x < 4 => (-Random.nextInt(100)).toString
        case _ => (50 + Random.nextInt(450)).toString
      }
      
      // ✗ 50% fechas formato DD/MM/YYYY
      val orderDate = if (Random.nextDouble() < 0.5) {
        LocalDate.of(2023, 1, 1).plusDays(Random.nextInt(730))
          .format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))
      } else {
        LocalDate.of(2023, 1, 1).plusDays(Random.nextInt(730)).toString
      }
      
      (id.toString, customerId.toString, productId.toString, quantity, price, orderDate)
    }

    val df = data.toDF("order_id", "customer_id", "product_id", "quantity", "price", "order_date")
    
    // ✗ Agregar 100 registros DUPLICADOS
    val duplicates = (1000 to 1099).map { _ =>
      ("1001", "5555", "333", "42", "abc", "01/01/2023")
    }.toDF("order_id", "customer_id", "product_id", "quantity", "price", "order_date")
    
    val finalDf = df.union(duplicates).coalesce(1)
    finalDf.write.mode("overwrite").option("header", "true").csv("data/sales")
    
    println(s"✅ Órdenes (CON PROBLEMAS) guardadas")
  }

  def generateProductsWithIssues(count: Int = 1000): Unit = {
    println(s"📦 Generando $count productos CON PROBLEMAS...")
    
    val data = (1 to count).map { id =>
      val name = productNames(Random.nextInt(productNames.size))
      
      // ✗ Espacios extra
      val nameSpaces = if (Random.nextDouble() < 0.1) "   " + name + "   " else name
      
      // ✗ 20% sin categoría
      val category = if (Random.nextDouble() < 0.2) null else categories(Random.nextInt(categories.size))
      
      // ✗ Fechas inconsistentes
      val createdDate = if (Random.nextDouble() < 0.7) {
        LocalDate.of(2020, 1, 1).plusDays(Random.nextInt(1460))
          .format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))
      } else {
        LocalDate.of(2020, 1, 1).plusDays(Random.nextInt(1460)).toString
      }
      
      val cost = 10 + Random.nextInt(490)
      val price = (cost * 1.3).toInt
      val supplierId = (id % 50) + 1
      
      (id.toString, nameSpaces, category, cost.toString, price.toString, supplierId.toString, createdDate)
    }

    val df = data.toDF("product_id", "name", "category", "cost", "price", "supplier_id", "created_date")
    df.write.mode("overwrite").json("data/products")
    
    println(s"✅ Productos (CON PROBLEMAS) guardados")
  }

  def generateCustomers(count: Int = 10000): Unit = {
    println(s"📊 Generando $count clientes...")
    
    val data = (1 to count).map { id =>
      val firstName = firstNames(Random.nextInt(firstNames.size))
      val lastName = lastNames(Random.nextInt(lastNames.size))
      val country = countries(Random.nextInt(countries.size))
      val signupDate = LocalDate.of(2020, 1, 1).plusDays(Random.nextInt(2190))
      
      (id.toString, s"$firstName $lastName", s"${id}.${firstName.toLowerCase}@email.com", country, signupDate.toString)
    }

    val df = data.toDF("customer_id", "name", "email", "country", "signup_date")
    df.write.mode("overwrite").parquet("data/customers")
    println(s"✅ Clientes guardados")
  }

  def main(args: Array[String]): Unit = {
    println("🚀 Generando datos CON PROBLEMAS DE CALIDAD...\n")
    generateCustomers(10000)
    generateProductsWithIssues(1000)
    generateOrdersWithIssues(100000)
    println("\n✨ ¡Completado! Datos con problemas generados.")
    spark.stop()
  }
}
