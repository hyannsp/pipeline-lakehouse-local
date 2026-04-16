import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit

"""
    Ler o CSV correspondente ao raw_source (Nome passado como argumento)
"""

if len(sys.argv) > 0:
    csv_origem = sys.argv[1]
    parquet_destino = sys.argv[2] if len(sys.argv) > 2 else csv_origem
    print(f"Arquvio a ser processado: {csv_origem} ser'a salvo como : {parquet_destino}")
else :
    print("Nenhum arquivo passado como parâmetro.")
    sys.exit(1)

"""
    -------------------
    Configurações Spark
    -------------------
"""

spark = SparkSession.builder \
    .appName("ingestao_bronze_dimensoes") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark session iniciada")

"""
    Pegando os CSVs e criando arquivos parquet
"""

home = os.path.expanduser("~")
origem = f"{home}/Projects/pipeline-lakehouse-local/raw_source/{csv_origem}.csv"
destino = f"{home}/Projects/pipeline-lakehouse-local/data_lake/bronze/{parquet_destino}"

df = spark.read.csv(
    path=origem,
    header=True,
    inferSchema=True,
    sep=','
)

df_final = df.withColumn("_data_ingestao", current_timestamp())

linhas_processadas = df_final.count()

if linhas_processadas > 0:
    df_final.write.mode("overwrite") \
        .parquet(destino)
    print(f"Ingestão finalizada com Exito, {linhas_processadas} linhas.")
else:
    print("Não ha linhas a serem ingeridas.")

"""
    -----------
    Finalizando 
    -----------
"""

spark.stop()
print("Spark Finalizado")


