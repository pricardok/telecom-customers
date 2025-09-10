\# Conjunto de dados:
https://www.kaggle.com/datasets/tarekmuhammed/telecom-customers

\
\# Camadas implementadas 
- Bronze: Dados brutos ingeridos do CSV
- Silver: Dados limpos com transformações
- Gold: Análises business (CLV, padrões de serviço, métodos de pagamento, etc)

\
\# Funcionalidades 
- Ingestão de dados para o Delta
- Transformações
- Análises de Customer Lifetime Value
- Identificação de padrões de serviço
- Análise de risco por método de pagamento
- Análise de Upsell/Downsell opportunities
- Identificação de potencial de aumento de receita
- Recomendações de migração entre planos
- Priorização de ações comerciais
- Consultas SQL otimizadas

\
\# Guia/Estrutura 

<img width="300" alt="image" src="https://github.com/user-attachments/assets/8f9b459a-1c89-421a-a294-e4497e8216e6" />


\
\# Tecnologias 
- Databricks Community Edition
- Apache Spark 3.5
- Delta Lake 3.1
- Python 3.12
- PySpark SQL

\
\# Como Executar:

\# 1.0 - Upload do Projeto (notebook) - ( OBS: Caso não exista, crie um diretório no Catalogo, ex: tec_poc )

```bash
%sh  
git clone https://github.com/pricardok/telecom-customers.git  
```

<img width="400" alt="image" src="https://github.com/user-attachments/assets/2219905e-b0cd-421c-b16a-05b3aacba819" />

\
\# 1.1 - Upload do Arquivo de Dados ( OBS: Caso não exista, crie um diretório no Catalogo, ex: tec_poc )
- Método 1: Importe manual  
Navegue até Workspace > .... > Import → File  
Selecione o arquivo “Telecom_Customers_Churn.csv”  
- Método 2: Via Código (notebook)  
```python
dbutils.fs.cp("file:/path/to/Telecom_Customers_Churn.csv","/Volumes/workspace/default/tec_poc/")
```

\
\# 1.2 - Execute o script de setup

```python
/Workspace/poc_tec/telecom-customers/notebooks/setup_environment.py
```



\
\# 1.3 - Execute em sequência  

```python

/Workspace/poc_tec/telecom-customers/notebooks/01_ingestion_bronze.py  
/Workspace/poc_tec/telecom-customers/notebooks/02_transformation_silver.py
/Workspace/poc_tec/telecom-customers/notebooks/03_analysis_gold.py  
/Workspace/poc_tec/telecom-customers/notebooks/03b_upsell_analysis.py
/Workspace/poc_tec/telecom-customers/notebooks/04_sql_queries.py
```

\
\# Para analises de Dados abra o consultas.sql (consultas basicas)  
scripts/consultas.sql

\
\# Monitoramento - Verificar Qualidade dos Dados

```python
/Workspace/poc_tec/telecom-customers/scripts/validation_checks.py
```

\
\# Estrutura pos implementação  

<img width="300" alt="image" src="https://github.com/user-attachments/assets/b990faaf-f32d-4db8-a3a2-9ce01d9ce6da" />

\
\# OBS: O código é portável para as opções abaixo, porem não foi testado
- AWS EMR + S3
- Azure Databricks + ADLS  
- Spark local
  


\
\# Screenshots  



- setup_environment.py

<img width="350" alt="image" src="https://github.com/user-attachments/assets/c975a857-74b0-467f-a4fe-908baec027da" />



- 01_ingestion_bronze.py

<img width="500" alt="image" src="https://github.com/user-attachments/assets/55a7bfc4-5994-417a-a6ff-332b35a67d24" />



- Dados parquet   


<img width="500" alt="image" src="https://github.com/user-attachments/assets/93f29b44-e140-4bab-924a-49705b5e5503" />




- Catalogo  


<img width="500" alt="image" src="https://github.com/user-attachments/assets/4c8c2998-96f5-44e0-b9cc-bfc5b3d6b44c" />




- Dados (Broze, silver e gold)  
  

<img width="500" alt="image" src="https://github.com/user-attachments/assets/5d01bfb3-b260-464a-8a85-c7e52903b218" />



- Analises... (scripts/consultas.sql)  
  

<img width="500" alt="image" src="https://github.com/user-attachments/assets/54301635-0759-428f-8c10-c2e2be0b5e2f" />


- Grafico 01

<img width="500" alt="image" src="https://github.com/user-attachments/assets/ee7910fe-f83a-4f91-801f-111ea466081c" />



- Grafico 02
  
<img width="400" alt="image" src="https://github.com/user-attachments/assets/605ed903-a9a1-45ac-91b6-920bdb1d61d4" />





























































