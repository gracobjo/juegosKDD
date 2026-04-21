-- Base de datos Hive para tablas del recomendador (Spark saveAsTable también puede crearla)
CREATE DATABASE IF NOT EXISTS gaming_recommender
COMMENT 'KDD Recomendador híbrido — behaviors, reviews, catálogo'
LOCATION 'hdfs:///user/hive/warehouse/gaming_recommender.db';

USE gaming_recommender;

-- Placeholders opcionales: las tablas reales se crean vía Spark Parquet + saveAsTable
-- en loader.py y kdd_pipeline.py.
