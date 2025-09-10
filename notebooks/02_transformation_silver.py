"""
Transformação de dados para camada Silver
"""
# Notebook source
from pyspark.sql.functions import col, when, lit
from config.paths_config import PathConfig

# Paths
bronze_path = PathConfig.BRONZE_PATH
silver_path = PathConfig.SILVER_PATH

# Le dados da camada Bronze
df_bronze = spark.read.format("delta").load(bronze_path)
print(f"Bronze data loaded: {df_bronze.count()} rows")

# Aplica transformações
df_silver = df_bronze.withColumn(
    "TotalCharges_clean",
    when(col("TotalCharges") == " ", lit(None))
      .otherwise(col("TotalCharges").cast("double"))
).withColumn(
    "ChurnFlag",
    when(col("Churn") == "Yes", 1).otherwise(0)
).withColumn(
    "TenureSegment",
    when(col("tenure") < 12, "Novo")
      .when(col("tenure") < 36, "Intermediário")
      .otherwise("Antigo")
).withColumn(
    "SeniorCitizenFlag",
    when(col("SeniorCitizen") == 1, "Sênior").otherwise("Adulto")
)

# Resumo
print("Transformações aplicadas:")
print(f"   - TotalCharges convertido para double")
print(f"   - ChurnFlag criado (0/1)")
print(f"   - TenureSegment criado (Novo/Intermediário/Antigo)")
print(f"   - SeniorCitizenFlag criado")

# Esquema
print("Esquema da Silver:")
df_silver.printSchema()

# Salvando camada Silver
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print(f"Silver layer saved: {df_silver.count()} rows")
print(f"Path: {silver_path}")

# Amostra 
print("Sample data:")
display(df_silver.limit(5))