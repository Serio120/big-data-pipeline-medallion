#!/bin/bash
set -e

echo "🔨 Compilando..."
sbt clean assembly

echo ""
echo "🚀 Ejecutando generador de datos..."
spark-submit --class com.example.DataGenerator \
  --master local[*] \
  --driver-memory 2g \
  target/scala-2.12/synthetic-data-generator-assembly-1.0.0.jar

echo ""
echo "✨ ¡Listo! Los datos están en el directorio 'data/'"
