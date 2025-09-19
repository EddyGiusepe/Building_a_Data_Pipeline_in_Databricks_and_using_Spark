"""
Senior Data Scientist.: Dr. Eddy Giusepe Chirinos Isidro

Script databricks_connection_test.py
====================================
Este script testa a conexão com o Databricks e carrega dados do Volume Databricks.

RUN
---
uv run databricks_connection_test.py
"""
import os
from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

# Carregar credenciais obtidas no Databricks:
databricks_host = os.environ['DATABRICKS_HOST']
databricks_token = os.environ['DATABRICKS_TOKEN']
databricks_cluster_id = os.environ['DATABRICKS_CLUSTER_ID']

print("🔗 Testando conexão com Databricks...")
print(f"📍 Host: {databricks_host}")
print(f"🆔 Cluster: {databricks_cluster_id}")

try:
    # Tentar conexão Databricks
    from databricks.connect import DatabricksSession
    
    spark = DatabricksSession.builder \
        .remote(
            host=databricks_host,
            token=databricks_token,
            cluster_id=databricks_cluster_id
        ) \
        .getOrCreate()
    
    print("✅ Conectado ao Databricks com sucesso!")
    
    # Teste básico de conexão
    test_df = spark.sql("SELECT 1 as test")
    test_df.show()
    
    # Carregar dados do Volume Databricks
    volume_path = "/Volumes/workspace/default_eddy/volumeeddy-tmp-sampledata/sample_data.csv"
    print(f"📂 Carregando dados de: {volume_path}")
    
    df = spark.read.csv(volume_path, header=True, inferSchema=True)
    print("📊 Dados carregados do Databricks:")
    df.show(5)
    print(f"📈 Shape: {df.count()} linhas, {len(df.columns)} colunas")
    
except Exception as e:
    print(f"❌ Erro Databricks: {e}")
    print("\n🔄 Usando PySpark local puro...")
    
    # Importar PySpark padrão (não Connect)
    from pyspark.sql import SparkSession
    
    # Criar sessão Spark local PURA
    spark = SparkSession.builder \
        .appName("LocalSparkTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    
    print("✅ Spark local inicializado!")
    
    # Testar com arquivo local
    csv_path = os.path.join(os.getcwd(), "sample_data.csv")
    print(f"📂 Carregando dados de: {csv_path}")
    
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    print("📊 Dados carregados localmente:")
    df.show(5)
    print(f"📈 Shape: {df.count()} linhas, {len(df.columns)} colunas")
    
    # Análise básica
    print("\n📊 Análise básica:")
    df.describe().show()

print("\n✅ Teste concluído!")
