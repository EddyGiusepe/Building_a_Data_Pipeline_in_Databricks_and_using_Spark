import os
from dotenv import load_dotenv, find_dotenv

# Carregar credenciais
_ = load_dotenv(find_dotenv())
databricks_host = os.environ['DATABRICKS_HOST']
databricks_token = os.environ['DATABRICKS_TOKEN']
databricks_cluster_id = os.environ['DATABRICKS_CLUSTER_ID']

print("🔗 Conectando ao Databricks...")
print(f"📍 Host: {databricks_host}")
print(f"🆔 Cluster: {databricks_cluster_id}")

# Conectar ao Databricks
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder \
    .remote(
        host=databricks_host,
        token=databricks_token,
        cluster_id=databricks_cluster_id
    ) \
    .getOrCreate()

print("✅ Conectado ao Databricks com sucesso!")
print(f"🔧 Versão Spark: {spark.version}")

# 📁 CARREGAR DADOS DO VOLUME DATABRICKS
volume_path = "/Volumes/workspace/default_eddy/volumeeddy-tmp-sampledata/sample_data.csv"
print(f"\n📂 Carregando dados do Volume: {volume_path}")

df_spark = spark.read.csv(volume_path, header=True, inferSchema=True)

print("🎉 DADOS DO VOLUME CARREGADOS COM SUCESSO!")
print("\n📊 Primeiros registros:")
df_spark.show()

print(f"\n📈 Informações do Dataset:")
print(f"   • Linhas: {df_spark.count()}")
print(f"   • Colunas: {len(df_spark.columns)}")
print(f"   • Nomes das Colunas: {df_spark.columns}")

print("\n📋 Schema do Dataset:")
df_spark.printSchema()

print("\n📊 Estatísticas Descritivas:")
df_spark.describe().show()

# Análises com Spark SQL
print("\n🔍 Análises com Spark SQL:")
df_spark.createOrReplaceTempView("pessoas")

# Maiores de idade
print("\n🔞 Pessoas maiores de idade:")
adults = spark.sql("SELECT * FROM pessoas WHERE Age >= 18")
adults.show()

# Análise por cidade
print("\n🏙️ Contagem por Cidade:")
city_analysis = spark.sql("""
    SELECT City, COUNT(*) as quantidade
    FROM pessoas 
    GROUP BY City 
    ORDER BY quantidade DESC
""")
city_analysis.show()

# Converter para Pandas para análises adicionais (opcional)
df_pandas = df_spark.toPandas()
print(f"\n🐼 Dados também disponíveis como Pandas: {df_pandas.shape[0]} linhas, {df_pandas.shape[1]} colunas")

print("\n✅ Pipeline ETL com Volume Databricks concluído com sucesso!")
print("   • 🌩️ Processamento: Cluster Databricks")
print("   • 📁 Fonte: Volume Databricks")
print(f"   • 📊 Registros: {df_spark.count()}")
print(f"   • 🚀 Engine: Spark {spark.version}")
