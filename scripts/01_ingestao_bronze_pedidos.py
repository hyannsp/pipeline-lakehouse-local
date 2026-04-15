import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit

""" 
    -----------------------------
    Configuracoes de Data e Spark
    -----------------------------
""" 
if len(sys.argv) > 1:
    data_alvo = sys.argv[1]
else:
    data_alvo = "2017-05-01"

print(f"Data de processamento: {data_alvo}")

# Configuração do Spark
spark = SparkSession.builder \
    .appName("ingestao_bronze_pedidos") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Inicializado")

"""
    -----------------------------------------
    Transformando o CSV de pedidos em parquet
    -----------------------------------------
"""

# Definindo caminhos para criar o Parquet da tabela de pedidos

home = os.path.expanduser("~")
origem = f"{home}/Projects/pipeline-lakehouse-local/raw_source/olist_orders_dataset.csv"
destino = f"{home}/Projects/pipeline-lakehouse-local/data_lake/bronze/pedidos"

# Script de ingestão
df_pedidos = spark.read.csv(origem, header=True, inferSchema=True, sep=",")

df_transformado = df_pedidos.withColumn("data_compra", to_date(col("order_purchase_timestamp"))) # Apenas YYYY-MM-DD

df_filtrado = df_transformado.filter(
    col("data_compra") == lit(data_alvo) # lit() compara o valor literal, se não compararia o nome da coluna
)

df_final = df_filtrado.withColumn("_data_ingestao", current_timestamp())

linhas_processadas = df_final.count()

if linhas_processadas > 0:
    print(f"Salvando {linhas_processadas} registros na Bronze")

    df_final.write.mode("append") \
        .partitionBy("data_compra") \
        .parquet(destino)
    
    print("Ingestão finalizada com Exito.")
else:
    print("Nenhum registro encontrado para a data especificada.")


"""
    -----------
    Finalizando 
    -----------
"""

spark.stop()
print("Spark Finalizado")