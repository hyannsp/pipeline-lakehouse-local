from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

PYTHON_BIN = "/home/klovys/Projects/pipeline-lakehouse-local/venv/bin/python"
SCRIPTS_DIR = "/home/klovys/Projects/pipeline-lakehouse-local/scripts"

default_args = {
    'owner': 'Hyann',
    'start_date': datetime(2017, 5, 1)
}

with DAG(
    dag_id="pipeline_gold_star_schema",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    tags=['gold', 'star_schema', 'pedidos', 'dimensoes']
) as dag:

    esperar_pedidos = ExternalTaskSensor(
        task_id="esperar_silver_pedidos",
        external_dag_id="pipeline_pedidos_diaria",      # ID da DAG de pedidos
        external_task_id="processamento_dados_silver",  # Ultima task da DAG de pedidos
        allowed_states=['success'],
        poke_interval= 60,
        timeout=3600
    )

    esperar_dimensoes = ExternalTaskSensor(
        task_id="esperar_silver_dimensoes",
        external_dag_id="pipeline_dimensoes_diaria",
        external_task_id="fim_processamento_dimensoes",
        allowed_states=['success']
    )

    task_fato_vendas = BashOperator(
        task_id = "modelar_fato_vendas",
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/05_modelagem_gold_fato_vendas.py"
    )

    task_gold_dimensoes = BashOperator(
        task_id = "modelar_gold_dimensoes",
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/06_modelagem_gold_dimensoes.py"
    )

    # Espera ambas as DAGs finalizarem
    [esperar_dimensoes, esperar_pedidos] >> task_fato_vendas
    task_fato_vendas >> task_gold_dimensoes