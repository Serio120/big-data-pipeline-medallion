# 🎯 Generador de Datos Sintéticos - Scala/Spark

## ¿Qué genera?

| Tabla | Formato | Registros | Campos |
|-------|---------|-----------|--------|
| **Clientes** | Parquet | 10,000 | customer_id, name, email, country, signup_date |
| **Productos** | JSON | 1,000 | product_id, name, category, cost, price, supplier_id |
| **Transacciones** | CSV | 100,000 | order_id, customer_id, product_id, quantity, price, order_date |

## 📋 Requisitos

- Java 11+
- Scala 2.12
- Spark 3.3.0+
- sbt 1.7+

## 🚀 Instalación y ejecución

### 1. Compilar
```bash
sbt clean assembly
```

### 2. Ejecutar generador básico
```bash
spark-submit --class com.example.DataGenerator \
  --master local[*] \
  target/scala-2.12/synthetic-data-generator-assembly-1.0.0.jar
```

### 3. Ejecutar análisis avanzado
```bash
spark-submit --class com.example.AdvancedGenerator \
  --master local[*] \
  target/scala-2.12/synthetic-data-generator-assembly-1.0.0.jar
```

## 📂 Estructura de directorios

```
synthetic-data-generator/
├── build.sbt
├── src/
│   └── main/
│       └── scala/
│           └── com/
│               └── example/
│                   ├── DataGenerator.scala
│                   └── AdvancedGenerator.scala
├── data/
│   ├── customers/   (Parquet)
│   ├── products/    (JSON)
│   └── orders/      (CSV)
└── README.md
```

## 🔧 Personalización

En `DataGenerator.scala`, modifica estas líneas:

```scala
generateCustomers(10000)      // Número de clientes
generateProducts(1000)        // Número de productos
generateOrders(100000, 10000, 1000)  // Órdenes, max_customer, max_product
```

## 📊 Consultas de ejemplo

### Leer datos en PySpark
```python
spark = SparkSession.builder.appName("ReadSynthetic").getOrCreate()

customers = spark.read.parquet("data/customers")
products = spark.read.json("data/products")
orders = spark.read.option("header", "true").csv("data/orders")

customers.show()
orders.printSchema()
```

### Con SQL
```sql
SELECT c.name, COUNT(o.order_id) as orders
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY orders DESC
LIMIT 10
```

## 🎨 Características de datos

✅ Nombres realistas en español  
✅ Países variados  
✅ Fechas distribuidas (2 años)  
✅ Descuentos aleatorios (20% de órdenes)  
✅ Márgenes de ganancia variables (0-50%)  
✅ Relaciones realistas entre tablas  

## 📈 Estadísticas

Con los parámetros por defecto:
- **Clientes únicos**: 10,000
- **Productos únicos**: 1,000
- **Órdenes**: 100,000
- **Tamaño aproximado**:
  - Parquet: ~50 MB
  - JSON: ~5 MB
  - CSV: ~15 MB

## 🐛 Troubleshooting

**Error: "data/customers not found"**
→ Asegúrate de ejecutar DataGenerator primero

**Error: java.lang.OutOfMemoryError**
→ Aumenta memoria: `spark-submit ... --driver-memory 4g`

**Error: Scala version mismatch**
→ Actualiza `scalaVersion` en `build.sbt` a tu versión

## 📝 Licencia

MIT - Libre para usar y modificar

## ✨ Próximas mejoras

- [ ] Generador incremental (agregar más datos)
- [ ] Datos con anomalías y ruido
- [ ] Exportación a base de datos
- [ ] Configuración por archivo properties