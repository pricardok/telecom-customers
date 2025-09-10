"""
Análises business para camada Gold
"""
# Notebook source
from pyspark.sql.functions import col, when, sum, avg, count, round, max
from config.paths_config import PathConfig

# Paths
silver_path = PathConfig.SILVER_PATH
gold_clv_path = PathConfig.GOLD_CLV
gold_patterns_path = PathConfig.GOLD_PATTERNS
gold_payment_path = PathConfig.GOLD_PAYMENT

# Le dados da Silver
df_silver = spark.read.format("delta").load(silver_path)
print(f"Silver data loaded: {df_silver.count()} rows")

# Analise 1: Customer Lifetime Value
print("Calculando Customer Lifetime Value...")
df_clv = df_silver.groupBy("customerID").agg(
    round(avg("MonthlyCharges"), 2).alias("AvgMonthlySpend"),
    round(sum("MonthlyCharges"), 2).alias("TotalRevenue"),
    max("tenure").alias("TenureMonths"),
    round(avg("MonthlyCharges") * max("tenure"), 2).alias("EstimatedCLV"),
    round(avg("MonthlyCharges") * 12 * (1 - avg("ChurnFlag")), 2).alias("ProjectedYearlyValue")
).withColumn(
    "ValueSegment",
    when(col("EstimatedCLV") > 5000, "Premium")
      .when(col("EstimatedCLV") > 2000, "High")
      .when(col("EstimatedCLV") > 1000, "Medium")
      .otherwise("Standard")
).withColumn(
    "ChurnRisk",
    when(col("TenureMonths") < 6, "High")
      .when(col("TenureMonths") < 12, "Medium")
      .otherwise("Low")
)

df_clv.write.format("delta").mode("overwrite").save(gold_clv_path)
print(f"CLV analysis saved: {df_clv.count()} rows")

# Analise 2: Padrões de Serviço
print("Analisando padrões de serviço...")
from pyspark.sql.functions import concat_ws

df_patterns = df_silver.withColumn(
    "ServiceBundle",
    concat_ws("_",
        when(col("PhoneService") == "Yes", "Phone"),
        when(col("MultipleLines") == "Yes", "MultiLine"),
        when(col("InternetService") != "No", col("InternetService"))
    )
).groupBy("ServiceBundle", "Contract", "PaymentMethod").agg(
    count("customerID").alias("TotalCustomers"),
    round(avg("ChurnFlag"), 3).alias("ChurnRate"),
    round(avg("MonthlyCharges"), 2).alias("AvgMonthlyRevenue"),
    round(avg("tenure"), 1).alias("AvgTenureMonths")
).withColumn(
    "BundleProfitability",
    round(col("AvgMonthlyRevenue") * col("AvgTenureMonths") * (1 - col("ChurnRate")), 2)
).withColumn(
    "BundleSegment",
    when(col("AvgMonthlyRevenue") > 80, "PremiumBundle")
      .when(col("AvgMonthlyRevenue") > 50, "ValueBundle")
      .otherwise("BasicBundle")
).filter(col("TotalCustomers") > 10)

df_patterns.write.format("delta").mode("overwrite").save(gold_patterns_path)
print(f"Service patterns analysis saved: {df_patterns.count()} bundles")

# Analise 3: Métodos de Pagamento
print("Analisando métodos de pagamento...")
df_payment = df_silver.groupBy("PaymentMethod").agg(
    count("customerID").alias("TotalCustomers"),
    round(avg("MonthlyCharges"), 2).alias("AvgMonthlySpend"),
    round(avg("tenure"), 1).alias("AvgTenure"),
    round(avg("ChurnFlag"), 3).alias("ChurnRate"),
    count(when(col("ChurnFlag") == 1, True)).alias("ChurnedCustomers"),
    count(when(col("ChurnFlag") == 0, True)).alias("RetainedCustomers")
).withColumn(
    "RetentionRate", 
    round(col("RetainedCustomers") / col("TotalCustomers") * 100, 1)
).withColumn(
    "PaymentRisk",
    when(col("ChurnRate") > 0.3, "HighRisk")
      .when(col("ChurnRate") > 0.2, "MediumRisk")
      .otherwise("LowRisk")
).withColumn(
    "Recommendation",
    when(col("PaymentMethod") == "Electronic check", "Incentivize Automatic Payments")
      .when(col("ChurnRate") > 0.25, "Offer Payment Discount")
      .otherwise("Maintain Current Strategy")
)

df_payment.write.format("delta").mode("overwrite").save(gold_payment_path)
print(f"Payment analysis saved: {df_payment.count()} payment methods")

# Resumo
print("Resumo:")
print(f"   CLV Analysis: {df_clv.count()} customers")
print(f"   Service Patterns: {df_patterns.count()} bundles")  
print(f"   Payment Methods: {df_payment.count()} methods")
print(f"   Todos os dados salvos em: {PathConfig.GOLD_PATH}")

# Amostras
print("Amostra CLV:")
display(df_clv.limit(3))

print("Amostra Patterns:")
display(df_patterns.limit(3))

print("Amostra Payment:")
display(df_payment.limit(3))