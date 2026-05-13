import org.apache.spark.sql.SparkSession
import io.delta.tables._

object BronzeIngestion {
  def main(args: Array[String]): Unit = {
    // Configuración obligatoria para que Spark use el motor de Delta Lake
    val spark = SparkSession.builder()
      .appName("ETL-Bronze-Ingestion")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    println("🚀 Spark con Delta Lake inicializado correctamente.")
    
    // Aquí irá tu lógica de lectura de /data y escritura en /delta/bronze
    
    spark.stop()
  }
}