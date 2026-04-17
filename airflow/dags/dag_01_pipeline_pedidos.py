from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

PYTHON_BIN = "home/klovys/Projects/pipeline-lakehouse-local/venv/bin/python"
SCRIPTS_DIR = "/home/klovys/Projects/pipeline-lakehouse-local/scripts"

default_args = {
    'owner': 'Hyann',
    'start_date': datetime(2017, 5, 1)
}

# Estrutura da DAG
with DAG(
    dag_id= 'pipeline_pedidos_diaria',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True,
    tags=['bronze', 'silver', 'pedidos']
) as dag:
    
    # Utilizar a venv para nao dar erro
    comando_bronze = f"""
    {PYTHON_BIN} {SCRIPTS_DIR}/01_ingestao_bronze_pedidos.py {{ ds }}
    """

    task_ingestao_pedidos = BashOperator(
        task_id='ingerir_dados_bronze',
        bash_command=comando_bronze
    )


    comando_silver = f"""
    {PYTHON_BIN} {SCRIPTS_DIR}/03_processamento_silver_pedidos.py {{ ds }}
    """
    task_processamento_silver = BashOperator(
        task_id = "processamento_dados_silver",
        bash_command=comando_silver
    )

    task_ingestao_pedidos >> task_processamento_silver