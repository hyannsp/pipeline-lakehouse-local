"""
- Script dinamico , pega o nome da tabela e a coluna chave como argumento para remover duplicatas
- Por ser dinamico deve rodar para as 3 tabelas dimensoes
- Salvar como diferentes tabelas em Delta com overwrite
"""

import os
import sys
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

"""
    ------------------------------
    Criando sessao spark com Delta
    ------------------------------
"""

builder = SparkSession.builder.appName("processamento_silver_dimensoes") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


"""
    -------------------
    Variaveis dinamicas
    -------------------
"""

if len(sys.argv) < 3:
    print("Sem parâmetros o suficiente, deve haver nome_tabela e nome_coluna,nome_coluna2")
    sys.exit(1)

tabela_origem = sys.argv[1]             # ex: clientes
colunas_pk = sys.argv[2].split(",")      # ex: customer_id, order_id

home = os.path.expanduser("~")
caminho_bronze = f"{home}/Projects/pipeline-lakehouse-local/data_lake/bronze/{tabela_origem}/"
caminho_silver = f"{home}/Projects/pipeline-lakehouse-local/data_lake/silver/delta_{tabela_origem}/"

"""
    ---------------------------
    Dropando nulos e duplicatas
    ---------------------------
"""

df = spark.read.parquet(caminho_bronze)

df_limpo = df.dropna(subset=colunas_pk).dropDuplicates(colunas_pk)

"""
    --------------
    Salvando Dados
    --------------
"""

qtd_linhas = df_limpo.count()

if qtd_linhas > 0:
    print(f"Salvando {qtd_linhas} linhas na tabela delta_{tabela_origem}")

    df_limpo.write \
        .format("delta") \
        .mode("overwrite") \
        .save(caminho_silver)
    
else:
    print("Nenhum registro encontrado, abortando.")
    sys.exit(1)

print("Finalizando sessao do spark")
spark.stop()