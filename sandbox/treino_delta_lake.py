import os
import shutil
from pyspark.sql import SparkSession
from delta import *

# Builder padrão para formato delta
builder = SparkSession.builder.appName("TreinoDelta") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# spark.sql.extensions adiciona uma extensão de comandos para o spark, como o merge (upsert), update ou delete ja q o spark nativo nao tem.
# a config do catalog diz onde as tabelas estao guardadas e suas descrições, alterar para o formato DeltaCatalog e melhor para time travel e tabelas deltado q usar o nativo do spark


spark = configure_spark_with_delta_pip(builder).getOrCreate() # Busca essas configs do builder e baixa os .jars aplicando no nosso spark.
spark.sparkContext.setLogLevel("WARN")

pasta_treino = os.path.expanduser("~/Projects/pipeline-lakehouse-local/sandbox/data")

# Limpar dados do local de treino caso exista
if os.path.exists(pasta_treino):
    shutil.rmtree(pasta_treino)

dados_dia_1 = [
    (1, "João", "Ativo"),
    (2, "Maria", "Inativo")
]

df_dia_1 = spark.createDataFrame(
    dados_dia_1,                        # Dados
    ["id_cliente", "nome", "status"]    # Cabecalho
)

print("Salvando dados do dia 1")
df_dia_1.show()

# Salva o primeiro delta table na pasta_treino
df_dia_1.write.format("delta") \
    .mode("overwrite") \
    .save(pasta_treino)

dados_dia_2 = [
    (1, "João", "Cancelado"),  # João mudou o status (precisa de UPDATE)
    (3, "Carlos", "Ativo")     # Carlos é novo (precisa de INSERT)
    # Maria não veio no arquivo hoje, ela deve continuar intacta lá no lake.
]

df_dia_2 = spark.createDataFrame(
    dados_dia_2,
    ["id_cliente", "nome", "status"] 
)

print("Dados do dia 2")
df_dia_2.show()

tabela_Delta = DeltaTable.forPath(spark, pasta_treino)

# Merge dos dados do dia 1 com os dados do dia 2
tabela_Delta.alias("alvo") \
    .merge(
        df_dia_2.alias("fonte"),
        "alvo.id_cliente = fonte.id_cliente"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
# whenMatchedUpdateAll: Se o ID bater, atualize todas as colunas.
# whenNotMatchedInsertAll: Se o ID da fonte não existir no alvo, insira a linha nova.

print("Tabelas com merged")
spark.read.format("delta").load(pasta_treino).orderBy("id_cliente").show()

print("Versao V0 da tabela (Apenas dia 1)")
spark.read.format("delta").option("versionAsOf", 0).load(pasta_treino).orderBy("id_cliente").show()

spark.stop()