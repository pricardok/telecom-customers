-- Taxa de churn por tipo de internet
SELECT InternetService, AVG(ChurnFlag) as ChurnRate
FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/silver`
GROUP BY InternetService
ORDER BY ChurnRate DESC;

-- CLV por segmento
SELECT ValueSegment, AVG(EstimatedCLV) as AvgCLV
FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/customer_value_analysis`
GROUP BY ValueSegment;

-- Melhores oportunidades de upsell
SELECT 
  InternetService,
  Contract,
  CustomerCount,
  AvgSpend,
  UpsellPotential || '%' as UpsellPotential,
  '$' || RevenueOpportunity as RevenueOpportunity,
  RecommendedAction
FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/upsell_analysis`
WHERE UpsellPriority = 'High'
ORDER BY RevenueOpportunity DESC
LIMIT 10;

-- Top clientes por valor
SELECT ValueSegment, COUNT(*) as Clientes, 
       AVG(EstimatedCLV) as CLV_Medio, 
       AVG(ChurnRisk = 'High') as Risco_Alto
FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/customer_value_analysis`
GROUP BY ValueSegment
ORDER BY CLV_Medio DESC;

-- Análise de métodos de pagamento
SELECT 
  PaymentMethod, TotalCustomers, ChurnRate, RetentionRate, Recommendation
FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/payment_analysis`
ORDER BY ChurnRate DESC;

-- Top 10 clientes com maior valor estimado
SELECT * FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/customer_value_analysis`
ORDER BY EstimatedCLV DESC 
LIMIT 10;

-- Bundles de serviço mais lucrativos
SELECT ServiceBundle, TotalCustomers, AvgMonthlyRevenue, ChurnRate, BundleProfitability
FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/service_patterns_analysis`
ORDER BY BundleProfitability DESC 
LIMIT 10;

-- Melhores oportunidades de upsell
SELECT InternetService, Contract, CustomerCount, AvgSpend, UpsellPotential, RecommendedAction
FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/upsell_analysis`
WHERE UpsellPriority = 'High'
ORDER BY UpsellPotential DESC;

-- Clientes Premium com baixo risco
SELECT * FROM delta.`/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/gold/customer_value_analysis`
WHERE ValueSegment = 'Premium' AND ChurnRisk = 'Low'
ORDER BY EstimatedCLV DESC
LIMIT 10;