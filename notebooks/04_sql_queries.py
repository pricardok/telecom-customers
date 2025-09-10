"""
Consultas SQL para análise dos dados
"""
# Notebook source
from config.paths_config import PathConfig

# Análise de Churn por Segmento
print("Análise de Churn por Segmento Demográfico")
churn_by_demo = spark.sql(f"""
SELECT 
    gender,
    SeniorCitizenFlag,
    TenureSegment,
    COUNT(*) as TotalCustomers,
    ROUND(AVG(ChurnFlag) * 100, 1) as ChurnRatePercent,
    ROUND(AVG(MonthlyCharges), 2) as AvgMonthlySpend
FROM delta.`{PathConfig.SILVER_PATH}`
GROUP BY gender, SeniorCitizenFlag, TenureSegment
ORDER BY ChurnRatePercent DESC
""")

display(churn_by_demo)
print("Análise demográfica concluída")

# Valor do Cliente por Segmento
print("Valor do Cliente por Segmento")
clv_by_segment = spark.sql(f"""
SELECT 
    ValueSegment,
    COUNT(*) as NumberOfCustomers,
    ROUND(AVG(EstimatedCLV), 2) as AvgCLV,
    ROUND(SUM(EstimatedCLV), 2) as TotalValue,
    ROUND(AVG(ChurnRisk = 'High') * 100, 1) as HighRiskPercent
FROM delta.`{PathConfig.GOLD_CLV}`
GROUP BY ValueSegment
ORDER BY AvgCLV DESC
""")

display(clv_by_segment)
print("Análise de valor concluída")

# Padrões de Serviço Mais Lucrativos
print("Padrões de Serviço Mais Lucrativos")
top_bundles = spark.sql(f"""
SELECT 
    ServiceBundle,
    Contract,
    TotalCustomers,
    AvgMonthlyRevenue,
    ROUND(ChurnRate * 100, 1) as ChurnRatePercent,
    BundleProfitability,
    BundleSegment
FROM delta.`{PathConfig.GOLD_PATTERNS}`
WHERE TotalCustomers > 50
ORDER BY BundleProfitability DESC
LIMIT 10
""")

display(top_bundles)
print("Análise de bundles concluída")

# Risco por Método de Pagamento
print("Risco por Método de Pagamento")
payment_risk = spark.sql(f"""
SELECT 
    PaymentMethod,
    TotalCustomers,
    AvgMonthlySpend,
    ROUND(ChurnRate * 100, 1) as ChurnRatePercent,
    RetentionRate,
    PaymentRisk,
    Recommendation
FROM delta.`{PathConfig.GOLD_PAYMENT}`
ORDER BY ChurnRatePercent DESC
""")

display(payment_risk)
print("Análise de pagamento concluída")

# Insights para Ação
print("Insights para Tomada de Ação")
action_insights = spark.sql(f"""
SELECT 
    'Churn por Internet' as InsightCategory,
    InternetService,
    ROUND(AVG(ChurnFlag) * 100, 1) as ChurnRatePercent,
    COUNT(*) as Customers,
    'Focar em retenção de clientes Fiber optic' as Recommendation
FROM delta.`{PathConfig.SILVER_PATH}`
GROUP BY InternetService
HAVING ChurnRatePercent > 30

UNION ALL

SELECT 
    'Oportunidade de Upsell' as InsightCategory,
    ValueSegment,
    NULL as ChurnRatePercent,
    COUNT(*) as Customers,
    'Desenvolver programas para clientes Premium' as Recommendation
FROM delta.`{PathConfig.GOLD_CLV}`
WHERE ValueSegment = 'Premium'
GROUP BY ValueSegment

UNION ALL

SELECT 
    'Risco de Pagamento' as InsightCategory,
    PaymentMethod,
    ROUND(ChurnRate * 100, 1) as ChurnRatePercent,
    TotalCustomers as Customers,
    Recommendation
FROM delta.`{PathConfig.GOLD_PAYMENT}`
WHERE PaymentRisk = 'HighRisk'
""")

display(action_insights)
print("Insights de ação concluídos")

# Análise de Oportunidades de Upsell
print("Oportunidades de Upsell/Downsell")
upsell_opportunities = spark.sql(f"""
SELECT 
    InternetService,
    Contract,
    CustomerCount,
    AvgMonthlySpend,
    UpsellPotential || '%' as UpsellPotential,
    '$' || RevenueOpportunity as RevenueOpportunity,
    UpsellPriority,
    RecommendedAction,
    ActionType
FROM delta.`{PathConfig.GOLD_PATH}/upsell_analysis`
WHERE UpsellPriority IN ('High', 'Medium')
ORDER BY RevenueOpportunity DESC
LIMIT 15
""")

display(upsell_opportunities)
print("Análise de upsell concluída")

# Dashboard Executivo de Oportunidades
print("Dashboard Executivo de Revenue Opportunities")
executive_dashboard = spark.sql(f"""
SELECT 
    ActionType,
    COUNT(*) as NumberOfSegments,
    SUM(CustomerCount) as TotalCustomers,
    ROUND(SUM(RevenueOpportunity), 2) as TotalOpportunity,
    ROUND(AVG(UpsellPotential), 1) as AvgUpsellPotential,
    ROUND(SUM(RevenueOpportunity) / SUM(CustomerCount), 2) as ValuePerCustomer
FROM delta.`{PathConfig.GOLD_PATH}/upsell_analysis`
GROUP BY ActionType
ORDER BY TotalOpportunity DESC
""")

display(executive_dashboard)
print("Dashboard executivo concluído")

# Estatísticas Finais
print("Estatísticas")

total_customers = spark.sql(f"""
SELECT COUNT(*) as TotalCustomers 
FROM delta.`{PathConfig.SILVER_PATH}`
""").first()['TotalCustomers']

avg_churn = spark.sql(f"""
SELECT ROUND(AVG(ChurnFlag) * 100, 1) as AvgChurnRate 
FROM delta.`{PathConfig.SILVER_PATH}`
""").first()['AvgChurnRate']

total_value = spark.sql(f"""
SELECT ROUND(SUM(EstimatedCLV), 2) as TotalBusinessValue 
FROM delta.`{PathConfig.GOLD_CLV}`
""").first()['TotalBusinessValue']

print(f"""
RESUMO:
   Total de Clientes: {total_customers:,}
   Taxa Média de Churn: {avg_churn}%
   Valor Total do Negócio: ${total_value:,.2f}
   Análises Concluídas: 5
   Camadas Implementadas: 3 (Bronze, Silver, Gold)
""")

print("LH implementado com sucesso")