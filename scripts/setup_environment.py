"""
Script para configurar ambiente do Lakehouse
"""
# Notebook source
from pyspark.sql.functions import lit

def create_lakehouse_structure():
    """Cria estrutura de pastas do Lakehouse"""
    base_path = "/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn"
    
    folders = [
        f"{base_path}/bronze",
        f"{base_path}/silver", 
        f"{base_path}/gold",
        f"{base_path}/gold/customer_value_analysis",
        f"{base_path}/gold/service_patterns_analysis",
        f"{base_path}/gold/payment_analysis",
        f"{base_path}/gold/upsell_analysis"
    ]
    
    for folder in folders:
        try:
            dbutils.fs.mkdirs(folder)
            print(f"Created: {folder}")
        except Exception as e:
            print(f"Already exists: {folder}")

def validate_environment():
    """Valida se o ambiente está configurado corretamente"""
    required_paths = [
        "/Volumes/workspace/default/tec_poc/",
        "/Volumes/workspace/default/tec_poc/lakehouse_telecom_churn/"
    ]
    
    print("Validating environment...")
    for path in required_paths:
        try:
            files = dbutils.fs.ls(path)
            print(f"Accessible: {path}")
        except Exception as e:
            print(f"Cannot access: {path}")
            print(f"   Error: {e}")
            return False
    
    print("Environment validation passed!")
    return True

if __name__ == "__main__":
    print("Setting up Lakehouse environment...")
    create_lakehouse_structure()
    validate_environment()