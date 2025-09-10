"""
Script para validação e qualidade dos dados
"""
# Notebook source
from config.paths_config import PathConfig

def validate_data_quality():
    """Executa checks de qualidade dos dados"""
    checks = []
    
    # Check 1
    bronze_count = spark.read.format("delta").load(PathConfig.BRONZE_PATH).count()
    silver_count = spark.read.format("delta").load(PathConfig.SILVER_PATH).count()
    
    checks.append({
        "check": "Row count consistency",
        "status": "PASS" if bronze_count == silver_count else "FAIL",
        "details": f"Bronze: {bronze_count}, Silver: {silver_count}"
    })
    
    # Check 2
    silver_df = spark.read.format("delta").load(PathConfig.SILVER_PATH)
    null_counts = {}
    for col_name in silver_df.columns:
        null_count = silver_df.filter(silver_df[col_name].isNull()).count()
        if null_count > 0:
            null_counts[col_name] = null_count
    
    checks.append({
        "check": "Null values in Silver",
        "status": "PASS" if not null_counts else "WARNING",
        "details": f"Columns with nulls: {null_counts}" if null_counts else "No null values found"
    })
    
    # Check 3
    bronze_schema = spark.read.format("delta").load(PathConfig.BRONZE_PATH).columns
    silver_schema = spark.read.format("delta").load(PathConfig.SILVER_PATH).columns
    new_columns = set(silver_schema) - set(bronze_schema)
    
    checks.append({
        "check": "Schema evolution",
        "status": "PASS" if new_columns else "INFO",
        "details": f"New columns in Silver: {list(new_columns)}" if new_columns else "No schema changes"
    })
    
    # Check 4
    try:
        gold_clv_count = spark.read.format("delta").load(PathConfig.GOLD_CLV).count()
        checks.append({
            "check": "Gold layer data",
            "status": "PASS" if gold_clv_count > 0 else "FAIL",
            "details": f"Gold records: {gold_clv_count}"
        })
    except:
        checks.append({
            "check": "Gold layer data",
            "status": "FAIL",
            "details": "Gold layer not available"
        })
    
    # Exibir resultados
    print("Resumo")
    print("=" * 50)
    for check in checks:
        status_icon = "Ok" if check["status"] == "PASS" else "Atenção " if check["status"] == "WARNING" else "ops"
        print(f"{status_icon} {check['check']}: {check['status']}")
        print(f"   Details: {check['details']}")
        print()
    
    return checks

# validação
if __name__ == "__main__":
    validate_data_quality()