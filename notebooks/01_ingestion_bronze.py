"""
Ingestão de dados para camada Bronze
"""
# Notebook source
from pyspark.sql.functions import lit

# paths
workspace_path = "/Volumes/workspace/default/tec_poc/Telecom Customers Churn.csv"
bronze_path = "/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/bronze"

# Le o CSV
df_bronze = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(workspace_path)

# Salva o Delta
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)
# Resmo
print(f"Bronze layer criado: {df_bronze.count()} rows")