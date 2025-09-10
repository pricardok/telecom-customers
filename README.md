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

<img width="263" height="337" alt="image" src="https://github.com/user-attachments/assets/285f7d0c-cb45-4d23-a3c0-067d037d43ab" />

\
\# Tecnologias 
- Databricks Community Edition
- Apache Spark 3.5
- Delta Lake 3.1
- Python 3.12
- PySpark SQL

\
\# Como Executar:

\# 1.0 - Upload do Projeto (notebook)  

```bash
%sh  
git clone https://github.com/pricardok/telecom-customers.git  
cp -r telecom-customers/notebooks /Workspace/Users/$USER/  
```

<img width="600" height="163" alt="image" src="https://github.com/user-attachments/assets/2219905e-b0cd-421c-b16a-05b3aacba819" />

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
%run /Workspace/Users/\$USER/notebooks/scripts/setup_environment
``` 

\
\# 1.3 - Execute em sequência  

```python
%run /Workspace/Users/$USER/notebooks/01_ingestion_bronze  
%run /Workspace/Users/$USER/notebooks/02_transformation_silver    
%run /Workspace/Users/$USER/notebooks/03_analysis_gold  
%run /Workspace/Users/$USER/notebooks/03b_upsell_analysis  
%run /Workspace/Users/$USER/notebooks/04_sql_queries  
```

\
\# Para analises de Dados abra o consultas.sql (consultas basicas)  
scripts/consultas.sql

\
\# Monitoramento - Verificar Qualidade dos Dados

```python
%run /Workspace/Users/\$USER/notebooks/scripts/validation_checks
```

\
\# Estrutura pos implementação  
<img width="263" height="202" alt="image" src="https://github.com/user-attachments/assets/b990faaf-f32d-4db8-a3a2-9ce01d9ce6da" />

\
\# OBS: O código é portável para as opções abaixo, porem não foi testado
- AWS EMR + S3
- Azure Databricks + ADLS  
- Spark local


\
\# Screenshots  


- Dados parquet  


<img width="1015" height="360" alt="image" src="https://github.com/user-attachments/assets/228c3c17-6656-4fe7-9e0d-9c4ca9af6989" />



- Catalogo  

\
<img width="1061" height="443" alt="image" src="https://github.com/user-attachments/assets/4c8c2998-96f5-44e0-b9cc-bfc5b3d6b44c" />



- Dados (Broze, silver e gold)  

\
<img width="735" height="251" alt="image" src="https://github.com/user-attachments/assets/5d01bfb3-b260-464a-8a85-c7e52903b218" />



- Analises... (scripts/consultas.sql)
  

<img width="1055" height="393" alt="image" src="https://github.com/user-attachments/assets/54301635-0759-428f-8c10-c2e2be0b5e2f" />


































