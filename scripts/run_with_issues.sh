#!/bin/bash
set -e

echo "🔨 Compilando..."
sbt clean assembly

echo ""
echo "🚀 Ejecutando generador con problemas de calidad..."
spark-submit --class com.example.DataGeneratorWithQualityIssues \
  --master local[*] \
  --driver-memory 2g \
  target/scala-2.13/synthetic-data-generator-assembly-1.0.0.jar

echo ""
echo "✨ ¡Listo! Datos con problemas generados en 'data/'"
echo "📄 Ver PROBLEMAS_CALIDAD.txt para detalles"
