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

builder = SparkSession.builder.appName("modelagem_gold_dimensoes") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

"""
    ------------------------------
    Definindo caminhos e variaveis
    ------------------------------
"""

home = os.path.expanduser("~")
produto_silver = f"{home}/Projects/pipeline-lakehouse-local/data_lake/silver/delta_produtos/"
clientes_silver = f"{home}/Projects/pipeline-lakehouse-local/data_lake/silver/delta_clientes/"

"""
    ---------------------
    Padronizando Clientes
    ---------------------
"""

df_clientes = spark.read.format("delta").load(clientes_silver)

df_clientes_final = df_clientes.select(
    df_clientes["customer_id"],
    df_clientes["customer_city"].alias("cidade"),
    df_clientes["customer_state"].alias("estado")
)

"""
    ---------------------
    Padronizando Produtos
    ---------------------
"""

df_produtos = spark.read.format("delta").load(produto_silver)

df_produtos_final = df_produtos.select(
    df_produtos["product_id"],
    df_produtos["product_category_name"].alias("categoria")
)

"""
    -----------------------------------------------------
    Salvando dados, caso nao exista a tabela, sera criada
    -----------------------------------------------------
"""

def salvar_delta_table(df, caminho, cruzamento):
    if DeltaTable.isDeltaTable(spark, caminho):
        print("Realizando MERGE com uma tabela delta ja existente")
        tabela_delta = DeltaTable.forPath(spark, caminho)

        tabela_delta.alias("alvo") \
            .merge(
                df.alias("fonte"),
                cruzamento
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        print("Merge finalizado!")

    else:
        print("Sem tabelas delta, criando uma nova...")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(caminho)
        print("Tabela Criada!")
    return


produtos_gold = f"{home}/Projects/pipeline-lakehouse-local/data_lake/gold/dim_produtos/"
clientes_gold = f"{home}/Projects/pipeline-lakehouse-local/data_lake/gold/dim_clientes/"

salvar_delta_table(df_produtos_final, produtos_gold, "alvo.product_id = fonte.product_id")
salvar_delta_table(df_clientes_final, clientes_gold, "alvo.customer_id = fonte.customer_id")

print("Finalizando o Spark.")
spark.stop()