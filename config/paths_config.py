"""
Configuração de paths do projeto
"""
class PathConfig:
    # Base paths
    BASE_VOLUME = "/Volumes/workspace/default/tec_poc"
    LAKEHOUSE_BASE = f"{BASE_VOLUME}/lakehouse_telecom_churn"
    
    # Layer paths
    BRONZE_PATH = f"{LAKEHOUSE_BASE}/bronze"
    SILVER_PATH = f"{LAKEHOUSE_BASE}/silver"
    GOLD_PATH = f"{LAKEHOUSE_BASE}/gold"
    
    # Gold analysis paths
    GOLD_CLV = f"{GOLD_PATH}/customer_value_analysis"
    GOLD_PATTERNS = f"{GOLD_PATH}/service_patterns_analysis"
    GOLD_PAYMENT = f"{GOLD_PATH}/payment_analysis"
    GOLD_UPSELL = f"{GOLD_PATH}/upsell_analysis"
    GOLD_CHURN = f"{GOLD_PATH}/churn_analysis" 