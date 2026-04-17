from delta import *
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import sys

"""
    ---------------------------------------------------
    Configurando o builder e o Spark para formato delta
    ---------------------------------------------------
"""

builder = SparkSession.builder.appName("processamento_silver_pedidos") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

"""
    --------------------------------------
    Pegando data do Airflow como argumento
    --------------------------------------
"""

if len(sys.argv) > 1:
    data = sys.argv[1]
else:
    data = "2017-05-01"

print(f"Dados a serem processados para data {data}")

# Definindo caminho dos dados bronze
home = os.path.expanduser("~")
caminho_bronze = f"{home}/Projects/pipeline-lakehouse-local/data_lake/bronze/pedidos/"
caminho_silver = f"{home}/Projects/pipeline-lakehouse-local/data_lake/silver/delta_pedidos/"

df = spark.read.parquet(caminho_bronze).filter(col("data_compra") == lit(data)) # Filtrando dados da data selecionada
# Pode ser feito com f"{home}/Projects/pipeline-lakehouse-local/data-lake/bronze/pedidos/data_compra={data}" mas eh menos seguro

"""
    ----------------------
    Limpeza de dados no df
    ----------------------
"""

print("Limpando Nulos e Duplicatas")
# Apagando dados sem order_id ou customer_id
df_sem_nulos = df.dropna(subset=["order_id", "customer_id"])

# Apagar duplicadas (nesse caso mantem a ultima que foi ingerida)
df_sem_duplicadas = df_sem_nulos.drop_duplicates(["order_id"])

"""
    -----------------------------------------------------
    Salvando dados, caso nao exista a tabela, sera criada
    -----------------------------------------------------
"""
# Caso ja exista uma tabela delta, devemos fazer um merge
if DeltaTable.isDeltaTable(spark, caminho_silver):
    print("Realizando MERGE com uma tabela delta ja existente")
    # Bota a tabela delta em memoria para manipulacao
    tabela_delta = DeltaTable.forPath(spark, caminho_silver)

    tabela_delta.alias("alvo") \
        .merge(
            df_sem_duplicadas.alias("fonte"),
            "alvo.order_id = fonte.order_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    print("Merge finalizado!")
# Caso nao exista caminho, cria e salva
else:
    print("Sem tabelas delta, criando uma nova...")
    df_sem_duplicadas.write \
        .format("delta") \
        .partitionBy("data_compra") \
        .mode("overwrite") \
        .save(caminho_silver)
    print("Tabela Criada!")

print("Finalizando a sessãos do Spark.")
spark.stop()