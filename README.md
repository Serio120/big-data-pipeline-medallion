# 🏗️ Big Data Pipeline Medallion — TechZone

> Pipeline ETL end-to-end con **Scala + Apache Spark 4.1.1** sobre arquitectura **Medallion (Bronze → Silver → Gold)**, desde datos sintéticos con problemas de calidad hasta KPIs listos para Power BI.

---

## 📋 Índice

- [Descripción del proyecto](#-descripción-del-proyecto)
- [Arquitectura del pipeline](#-arquitectura-del-pipeline)
- [Dominio de negocio](#-dominio-de-negocio)
- [Fuentes de datos](#-fuentes-de-datos)
- [Estructura del repositorio](#-estructura-del-repositorio)
- [Requisitos](#-requisitos)
- [Instalación y ejecución](#-instalación-y-ejecución)
- [Resultados](#-resultados)
- [Stack tecnológico](#-stack-tecnológico)

---

## 🎯 Descripción del proyecto

Pipeline ETL completo que simula el flujo de datos de una empresa de e-commerce (**TechZone**), procesando más de **100.000 transacciones** a través de tres capas de transformación y generando KPIs ejecutivos para toma de decisiones.

| Métrica | Valor |
|---------|-------|
| 💰 Ingresos totales procesados | $264.9M |
| 📦 Transacciones | 100.100 |
| 👥 Clientes | 10.000 |
| 🛍️ Productos | 1.000 |
| 🌍 Países | 10 |
| 📅 Periodo | 2023–2024 |

---

## 🏛 Arquitectura del pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                     FUENTES DE DATOS                        │
│   sales.csv (CSV)  │  products.json (JSON)  │  customers    │
│   100.100 filas    │  1.000 filas           │  (Parquet)    │
│   CON problemas    │  CON problemas         │  10.000 filas │
└────────┬───────────┴──────────┬─────────────┴──────┬────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│  🥉 CAPA BRONZE  —  Ingesta Raw (01_bronze.scala)           │
│  • Lee sin transformar  • inferSchema=false en CSV          │
│  • Añade: ingestion_timestamp, source_file                  │
│  • Formato: Parquet  │  Ruta: delta/bronze/                 │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  🥈 CAPA SILVER  —  Limpieza (02_silver.scala)              │
│  • 99 duplicados eliminados                                 │
│  • 3.853 precios inválidos filtrados                        │
│  • 4.753 nulos en quantity imputados (→ 1)                  │
│  • 200 categorías nulas imputadas (→ "Sin Categoría")       │
│  • Fechas normalizadas a YYYY-MM-DD                         │
│  • Cast de tipos correctos                                  │
│  • Formato: Parquet  │  Ruta: delta/silver/                 │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  🥇 CAPA GOLD  —  KPIs (03_gold.scala)                      │
│  • top_products   → Q1: Top 10 productos por ingreso        │
│  • category_kpis  → Q2: Margen bruto por categoría          │
│  • monthly_kpis   → Q3: Evolución de ventas mes a mes       │
│  • country_kpis   → Q4: Ingresos y clientes por país        │
│  • Formato: Parquet + CSV  │  Ruta: delta/gold/ + dashboard/│
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  📊 POWER BI DASHBOARD                                      │
│  4 CSVs → 4 visualizaciones → Dashboard ejecutivo TechZone │
└─────────────────────────────────────────────────────────────┘
```

---

## 🏢 Dominio de negocio

**TechZone** es un e-commerce de electrónica que vende en 10 países de habla hispana y América del Norte. Tiene 1.000 productos activos, 10.000 clientes registrados y procesa ~100.000 transacciones anuales.

**Problema:** No tienen visibilidad sobre qué productos y categorías generan más margen, en qué países concentrar inversión, ni cómo evoluciona la demanda a lo largo del año.

### ❓ Preguntas de negocio respondidas

| # | Pregunta | Tabla Gold |
|---|----------|------------|
| Q1 | ¿Cuáles son los 10 productos con mayor ingreso total? | `top_products` |
| Q2 | ¿Cuál es el margen bruto por categoría de producto? | `category_kpis` |
| Q3 | ¿Cómo evolucionan las ventas mes a mes? ¿Hay estacionalidad? | `monthly_kpis` |
| Q4 | ¿Qué países generan más ingresos y clientes activos? | `country_kpis` |
| Q5 | ¿Cuál es el ticket medio por país y categoría? | `category_kpis` + `country_kpis` |

---

## 📊 Fuentes de datos

Datos generados sintéticamente con **Scala/Spark** mediante `DataGeneratorWithQualityIssues.scala`.

| Fuente | Formato | Filas | Problemas introducidos |
|--------|---------|-------|----------------------|
| `data/sales` | CSV | 100.100 | 5% nulos en `quantity`, 3% precios inválidos ("no aplica", negativos), 50% fechas en DD/MM/YYYY, 100 duplicados |
| `data/products` | JSON | 1.000 | 20% sin categoría, 10% espacios extra en nombre, 70% fechas en DD/MM/YYYY |
| `data/customers` | Parquet | 10.000 | Datos limpios |

### 🔧 Decisiones de limpieza (Silver)

| Problema | Estrategia | Justificación |
|----------|-----------|---------------|
| 100 duplicados en sales | `dropDuplicates` | Registros idénticos sin valor analítico |
| Precios "no aplica" o negativos | Eliminar fila | No se puede imputar un precio de venta |
| Nulos en `quantity` (5%) | Imputar con `1` | Venta mínima posible, conserva el registro |
| Fechas DD/MM/YYYY | Normalizar a YYYY-MM-DD | Estándar ISO para análisis temporal |
| Espacios extra en nombres | `trim()` | Evita duplicados lógicos en agrupaciones |
| Categorías nulas (20%) | Imputar "Sin Categoría" | Conserva el producto para análisis de precio/coste |

---

## 📁 Estructura del repositorio

```
big-data-pipeline-medallion/
│
├── README.md
├── build.sbt                          ← Scala 2.13.13 + Spark 4.1.1
│
├── src/main/scala/com/example/
│   ├── generators/
│   │   ├── DataGenerator.scala        ← Genera datos limpios
│   │   ├── DataGeneratorWithQualityIssues.scala  ← Datos con problemas
│   │   └── AdvancedGenerator.scala    ← Consultas de análisis
│   ├── bronze/
│   │   └── 01_bronze.scala            ← Ingesta raw → delta/bronze/
│   ├── silver/
│   │   └── 02_silver.scala            ← Limpieza → delta/silver/
│   ├── gold/
│   │   └── 03_gold.scala              ← KPIs → delta/gold/ + dashboard/
│   └── utils/
│
├── data/                              ← Fuentes generadas (no versionadas)
│   ├── sales/                         (CSV)
│   ├── products/                      (JSON)
│   └── customers/                     (Parquet)
│
├── delta/                             ← Capas del pipeline (no versionadas)
│   ├── bronze/
│   │   ├── sales/
│   │   ├── products/
│   │   └── customers/
│   ├── silver/
│   │   ├── sales/
│   │   ├── products/
│   │   └── customers/
│   └── gold/
│       ├── top_products/
│       ├── category_kpis/
│       ├── monthly_kpis/
│       └── country_kpis/
│
└── dashboard/                         ← CSVs para Power BI
    ├── top_products/
    ├── category_kpis/
    ├── monthly_kpis/
    └── country_kpis/
```

---

## ⚙ Requisitos

| Herramienta | Versión |
|------------|---------|
| Java | 17+ |
| Scala | 2.13.13 |
| Apache Spark | 4.1.1 |
| sbt | 1.7+ |

> ⚠️ **Nota sobre Delta Lake:** Delta Lake no tiene soporte oficial para Spark 4.x todavía. Este proyecto usa **Parquet** como formato de almacenamiento para todas las capas, manteniendo la misma arquitectura medallion.

---

## 🚀 Instalación y ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/Serio120/big-data-pipeline-medallion.git
cd big-data-pipeline-medallion
```

### 2. Compilar

```bash
sbt clean assembly
```

### 3. Ejecutar el pipeline completo (en orden)

```powershell
# Paso 1 — Generar datos con problemas de calidad
spark-submit --class com.example.generators.DataGeneratorWithQualityIssues `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar

# Paso 2 — Capa Bronze (ingesta raw)
spark-submit --class com.example.bronze.BronzeIngestion `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar

# Paso 3 — Capa Silver (limpieza)
spark-submit --class com.example.silver.SilverCleaning `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar

# Paso 4 — Capa Gold (KPIs)
spark-submit --class com.example.gold.GoldKPIs `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar
```

---

## 📈 Resultados

### KPIs globales TechZone (2023–2024)

| KPI | Valor |
|-----|-------|
| Ingresos totales | $264.9M |
| Margen bruto total | $16.9M |
| Margen bruto % | 6.4% |
| Ticket medio | ~$2.760 |

### Margen por categoría

| Categoría | Ingresos | Margen % |
|-----------|----------|----------|
| Sin Categoría | $52.5M | 12.86% |
| Accesorios | $45.0M | 7.62% |
| Redes | $37.5M | 6.21% |
| Almacenamiento | $42.3M | 5.25% |
| Periféricos | $40.0M | 5.03% |
| Electrónica | $47.7M | 0.36% |

### Top países por ingreso

| País | Ingresos | Margen % |
|------|----------|----------|
| 🇵🇪 Perú | $27.9M | 5.01% |
| 🇦🇷 Argentina | $27.6M | 7.04% |
| 🇨🇴 Colombia | $27.6M | 5.74% |
| 🇨🇱 Chile | $25.8M | 7.80% ⬆️ mejor margen |

---

## 🛠 Stack tecnológico

![Scala](https://img.shields.io/badge/Scala-2.13.13-red?logo=scala)
![Spark](https://img.shields.io/badge/Apache%20Spark-4.1.1-orange?logo=apache-spark)
![Java](https://img.shields.io/badge/Java-17-blue?logo=java)
![sbt](https://img.shields.io/badge/sbt-1.12-lightgrey)
![Parquet](https://img.shields.io/badge/Format-Parquet-green)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=powerbi)

---

> Proyecto final del módulo de Big Data  
> by [@Serio120](https://github.com/Serio120)
