# Conjunto de dados:
https://www.kaggle.com/datasets/tarekmuhammed/telecom-customers

# Camadas implementadas
- Bronze: Dados brutos ingeridos do CSV
- Silver: Dados limpos com transformações de qualidade
- Gold: Análises business (CLV, padrões de serviço, métodos de pagamento)

# Funcionalidades
- Ingestão de dados para o Delta
- Transformações
- Arquitetura 
- Análises de Customer Lifetime Value
- Identificação de padrões de serviço
- Análise de risco por método de pagamento
- Análise de Upsell/Downsell opportunities
- Identificação de potencial de aumento de receita
- Recomendações de migração entre planos
- Priorização de ações comerciais
- Consultas SQL otimizadas

# Guia
telecom-lakehouse-poc/
│
├── notebooks/
│   ├── 01_ingestion_bronze.py
│   ├── 02_transformation_silver.py
│   ├── 03_analysis_gold.py
│   └── 04_sql_queries.py
│
├── data/
│   └── Telecom_Customers_Churn_sample.csv
│
├── scripts/
│   ├── consultas.sql
│   ├── setup_environment.py
│   └── validation_checks.py
│
├── config/
│   └── paths_config.py
│
├── requirements.txt
├── README.md
├── .gitignore
└── LICENSE

# Tecnologias
- Databricks Community Edition
- Apache Spark 3.5
- Delta Lake 3.1
- Python 3.12
- PySpark SQL

# Como Executar:

# 1.0 - Upload do Projeto (notebook)
%sh
git clone https://github.com/pricardok/telecom-lakehouse-poc.git
cp -r telecom-lakehouse-poc/notebooks /Workspace/Users/$USER/

# 1.1 - Upload do Arquivo de Dados
# OBS: Caso não exista, crie um diretório no Catalogo, ex: tec_poc
Método 1: Importe manual
Navegue até Workspace > .... > Import → File
Selecione o arquivo “Telecom_Customers_Churn.csv”
Método 2: Via Código (notebook)
dbutils.fs.cp("file:/path/to/Telecom_Customers_Churn.csv","/Volumes/workspace/default/tec_poc/")

# 1.2 - Execute o script de setup
%run /Workspace/Users/$USER/notebooks/scripts/setup_environment

# 1.3 - Execute em sequência:
%run /Workspace/Users/$USER/notebooks/01_ingestion_bronze
%run /Workspace/Users/$USER/notebooks/02_transformation_silver  
%run /Workspace/Users/$USER/notebooks/03_analysis_gold
%run /Workspace/Users/$USER/notebooks/03b_upsell_analysis
%run /Workspace/Users/$USER/notebooks/04_sql_queries

# Analistas de Dados abra o consultas.sql
├── scripts/
│   ├── consultas.sql

# Monitoramento - Verificar Qualidade dos Dados
%run /Workspace/Users/$USER/notebooks/scripts/validation_checks

# Estrutura pos implementação
/Volumes/workspace/default/tec_poc/
├── lakehouse_telecom_churn/
│   ├── bronze/          # Dados brutos Delta
│   ├── silver/          # Dados estruturados
│   └── gold/            # Análises 
│       ├── customer_value_analysis/
│       ├── service_patterns_analysis/
│       ├── upsell_analysis/
│       └── payment_analysis/
└── Telecom Customers Churn.csv

# OBS: O código é portável para as opções abaixo, porem não foi testado:
- AWS EMR + S3
- Azure Databricks + ADLS  
- GCP Dataproc + GCS
- Spark local
