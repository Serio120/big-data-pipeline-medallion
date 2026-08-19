# 🏗️ Big Data Medallion Pipeline — TechZone

> End-to-end ETL pipeline built with **Scala + Apache Spark 4.1.1** using **Medallion Architecture (Bronze → Silver → Gold)**, from synthetic data with quality issues to business KPIs ready for Power BI.

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Pipeline Architecture](#-pipeline-architecture)
- [Business Domain](#-business-domain)
- [Data Sources](#-data-sources)
- [Repository Structure](#-repository-structure)
- [Requirements](#-requirements)
- [Installation & Execution](#-installation--execution)
- [Results](#-results)
- [Technology Stack](#-technology-stack)

---

## 🎯 Project Overview

A complete ETL pipeline that simulates the data flow of an e-commerce company (**TechZone**), processing more than **100,000 transactions** through three transformation layers and generating executive KPIs for business decision-making.

| Metric | Value |
|---------|-------|
| 💰 Total revenue processed | $264.9M |
| 📦 Transactions | 100,100 |
| 👥 Customers | 10,000 |
| 🛍️ Products | 1,000 |
| 🌍 Countries | 10 |
| 📅 Period | 2023–2024 |

---

## 🏛 Pipeline Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                       DATA SOURCES                          │
│   sales.csv (CSV)  │  products.json (JSON)  │  customers    │
│   100,100 rows     │  1,000 rows            │  (Parquet)    │
│   WITH issues      │  WITH issues           │  10,000 rows  │
└────────┬───────────┴──────────┬─────────────┴──────┬────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER — Raw Ingestion (01_bronze.scala)          │
│  • Reads raw data without transformation                     │
│  • inferSchema=false for CSV                                 │
│  • Adds: ingestion_timestamp, source_file                    │
│  • Format: Parquet  │  Path: delta/bronze/                   │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER — Data Cleaning (02_silver.scala)          │
│  • 99 duplicates removed                                     │
│  • 3,853 invalid prices filtered                             │
│  • 4,753 null quantities imputed (→ 1)                       │
│  • 200 null categories imputed (→ "Uncategorized")           │
│  • Dates normalized to YYYY-MM-DD                            │
│  • Correct data types applied                                │
│  • Format: Parquet  │  Path: delta/silver/                   │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER — KPIs (03_gold.scala)                       │
│  • top_products   → Q1: Top 10 products by revenue          │
│  • category_kpis  → Q2: Gross margin by category             │
│  • monthly_kpis   → Q3: Monthly sales evolution              │
│  • country_kpis   → Q4: Revenue and customers by country     │
│  • Format: Parquet + CSV  │  Path: delta/gold/ + dashboard/  │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  📊 POWER BI DASHBOARD                                      │
│  4 CSVs → 4 visualizations → Executive TechZone Dashboard  │
└─────────────────────────────────────────────────────────────┘
```

---

## 🏢 Business Domain

**TechZone** is an electronics e-commerce company operating across 10 Spanish-speaking and North American countries. It has 1,000 active products, 10,000 registered customers, and processes approximately 100,000 transactions per year.

**Business problem:** The company lacks visibility into which products and categories generate the highest margins, where investment should be focused, and how demand evolves throughout the year.

### ❓ Business Questions Answered

| # | Question | Gold Table |
|---|----------|------------|
| Q1 | Which 10 products generate the highest total revenue? | `top_products` |
| Q2 | What is the gross margin by product category? | `category_kpis` |
| Q3 | How do sales evolve month by month? Is there seasonality? | `monthly_kpis` |
| Q4 | Which countries generate the most revenue and active customers? | `country_kpis` |
| Q5 | What is the average order value by country and category? | `category_kpis` + `country_kpis` |

---

## 📊 Data Sources

Synthetic data is generated with **Scala/Spark** using `DataGeneratorWithQualityIssues.scala`.

| Source | Format | Rows | Injected Issues |
|--------|---------|-------|----------------|
| `data/sales` | CSV | 100,100 | 5% null `quantity` values, 3% invalid prices ("not applicable", negative values), 50% dates in DD/MM/YYYY, 100 duplicates |
| `data/products` | JSON | 1,000 | 20% missing categories, 10% extra spaces in names, 70% dates in DD/MM/YYYY |
| `data/customers` | Parquet | 10,000 | Clean data |

### 🔧 Silver Layer Cleaning Decisions

| Issue | Strategy | Rationale |
|----------|-----------|-----------|
| 100 duplicate sales records | `dropDuplicates` | Identical records provide no additional analytical value |
| "Not applicable" or negative prices | Remove row | A valid selling price cannot be reliably imputed |
| Null `quantity` values (5%) | Impute with `1` | Represents the minimum possible sale quantity while preserving the record |
| DD/MM/YYYY dates | Normalize to YYYY-MM-DD | ISO standard for time-based analysis |
| Extra spaces in names | `trim()` | Prevents logical duplicates during aggregations |
| Null categories (20%) | Impute "Uncategorized" | Preserves the product for price/cost analysis |

---

## 📁 Repository Structure

```text
big-data-pipeline-medallion/
│
├── README.md
├── build.sbt                          ← Scala 2.13.13 + Spark 4.1.1
│
├── src/main/scala/com/example/
│   ├── generators/
│   │   ├── DataGenerator.scala        ← Generates clean data
│   │   ├── DataGeneratorWithQualityIssues.scala  ← Generates data with issues
│   │   └── AdvancedGenerator.scala    ← Analysis queries
│   ├── bronze/
│   │   └── 01_bronze.scala            ← Raw ingestion → delta/bronze/
│   ├── silver/
│   │   └── 02_silver.scala             ← Cleaning → delta/silver/
│   ├── gold/
│   │   └── 03_gold.scala              ← KPIs → delta/gold/ + dashboard/
│   └── utils/
│
├── data/                              ← Generated sources (not versioned)
│   ├── sales/                         (CSV)
│   ├── products/                      (JSON)
│   └── customers/                     (Parquet)
│
├── delta/                             ← Pipeline layers (not versioned)
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
└── dashboard/                         ← CSV files for Power BI
    ├── top_products/
    ├── category_kpis/
    ├── monthly_kpis/
    └── country_kpis/
```

---

## ⚙ Requirements

| Tool | Version |
|------------|---------|
| Java | 17+ |
| Scala | 2.13.13 |
| Apache Spark | 4.1.1 |
| sbt | 1.7+ |

> ⚠️ **Delta Lake note:** Delta Lake does not yet officially support Spark 4.x. This project therefore uses **Parquet** as the storage format for all layers while preserving the same Medallion Architecture.

---

## 🚀 Installation & Execution

### 1. Clone the repository

```bash
git clone https://github.com/Serio120/big-data-pipeline-medallion.git
cd big-data-pipeline-medallion
```

### 2. Build the project

```bash
sbt clean assembly
```

### 3. Run the complete pipeline in order

```powershell
# Step 1 — Generate data with quality issues
spark-submit --class com.example.generators.DataGeneratorWithQualityIssues `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar

# Step 2 — Bronze layer (raw ingestion)
spark-submit --class com.example.bronze.BronzeIngestion `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar

# Step 3 — Silver layer (data cleaning)
spark-submit --class com.example.silver.SilverCleaning `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar

# Step 4 — Gold layer (KPIs)
spark-submit --class com.example.gold.GoldKPIs `
  --master local[*] `
  target/scala-2.13/big-data-pipeline-medallion-assembly-1.0.0.jar
```

---

## 📈 Results

### TechZone Global KPIs (2023–2024)

| KPI | Value |
|-----|-------|
| Total revenue | $264.9M |
| Total gross margin | $16.9M |
| Gross margin % | 6.4% |
| Average order value | ~$2,760 |

### Margin by Category

| Category | Revenue | Margin % |
|-----------|----------|----------|
| Uncategorized | $52.5M | 12.86% |
| Accessories | $45.0M | 7.62% |
| Networking | $37.5M | 6.21% |
| Storage | $42.3M | 5.25% |
| Peripherals | $40.0M | 5.03% |
| Electronics | $47.7M | 0.36% |

### Top Countries by Revenue

| Country | Revenue | Margin % |
|------|----------|----------|
| 🇵🇪 Peru | $27.9M | 5.01% |
| 🇦🇷 Argentina | $27.6M | 7.04% |
| 🇨🇴 Colombia | $27.6M | 5.74% |
| 🇨🇱 Chile | $25.8M | 7.80% ⬆️ highest margin |

---

## 🛠 Technology Stack

![Scala](https://img.shields.io/badge/Scala-2.13.13-red?logo=scala)
![Spark](https://img.shields.io/badge/Apache%20Spark-4.1.1-orange?logo=apache-spark)
![Java](https://img.shields.io/badge/Java-17-blue?logo=java)
![sbt](https://img.shields.io/badge/sbt-1.12-lightgrey)
![Parquet](https://img.shields.io/badge/Format-Parquet-green)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=powerbi)

---

> Project by [@Serio120](https://github.com/Serio120)
