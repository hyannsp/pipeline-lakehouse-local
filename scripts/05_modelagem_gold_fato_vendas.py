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

builder = SparkSession.builder.appName("modelagem_gold_fato_vendas") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

"""
    ----------------------
    Lendo as tabelas Delta
    ----------------------
"""

home = os.path.expanduser("~")
path_pedidos = f"{home}/Projects/pipeline-lakehouse-local/data_lake/silver/delta_pedidos"
path_itens_pedido = f"{home}/Projects/pipeline-lakehouse-local/data_lake/silver/delta_itens_pedido"
path_fato_vendas = f"{home}/Projects/pipeline-lakehouse-local/data_lake/gold/delta_fato_vendas"

df_pedidos = spark.read.format("delta").load(path_pedidos)
df_itens_pedido = spark.read.format("delta").load(path_itens_pedido)

"""
    -------------------
    Join e Select final
    -------------------
"""
# Funciona igualzinho um Join do SQL
df_fato = df_pedidos.join(
    df_itens_pedido,                                 # Tabela join
    df_pedidos.order_id == df_itens_pedido.order_id, # ON
    "inner"                                          # Tipo de JOIN
)

df_fato_final = df_fato.select(
    df_pedidos["order_id"],
    df_pedidos["customer_id"],
    df_itens_pedido["product_id"],
    df_pedidos["data_compra"],
    df_itens_pedido["price"].alias("valor_produto"),
    df_itens_pedido["freight_value"].alias("valor_frete")
)

"""
    --------------
    Salvando Dados
    --------------
"""

qtd_linhas = df_fato_final.count()

if qtd_linhas > 0:
    # Salvando a tabela fato
    print(f"Salvando {qtd_linhas} registros na tabela fato_vendas")
    
    if DeltaTable.isDeltaTable(spark, path_fato_vendas):
        # Caso ja exista, fazer um upsert
        tabela_delta = DeltaTable.forPath(spark, path_fato_vendas)

        tabela_delta.alias("alvo") \
            .merge(
                df_fato_final.alias("fonte"),
                "alvo.order_id = fonte.order_id AND alvo.product_id = fonte.product_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    
    else:
        df_fato_final.write \
            .format("delta") \
            .partitionBy("data_compra") \
            .mode("overwrite") \
            .save(path_fato_vendas)

else:
    print("Nenhum registro encontrado, finalizando.")
    sys.exit(1)

print("Finalizando o Spark.")
spark.stop()