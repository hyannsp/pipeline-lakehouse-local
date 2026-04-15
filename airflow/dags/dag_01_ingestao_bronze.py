from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Hyann',
    'start_date': datetime(2017, 5, 1)
}

# Estrutura da DAG
with DAG(
    dag_id= 'ingestao_bronze_pedidos_diaria',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True,
    tags=['bronze', 'pedidos']
) as dag:
    
    # Utilizar a venv para nao dar erro
    comando_terminal = """
    /home/klovys/Projects/pipeline-lakehouse-local/venv/bin/python /home/klovys/Projects/pipeline-lakehouse-local/scripts/01_ingestao_bronze_pedidos.py {{ ds }}
    """

    task_ingestao_pedidos = BashOperator(
        task_id='extrair_e_salvar_em_parquet',
        bash_command=comando_terminal
    )