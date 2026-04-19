from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PYTHON_BIN = "/home/klovys/Projects/pipeline-lakehouse-local/venv/bin/python"
SCRIPTS_DIR = "/home/klovys/Projects/pipeline-lakehouse-local/scripts"

default_args = {
    'owner': 'Hyann',
    'start_date': datetime(2017, 5, 1)
}

# Estrutura da DAG
with DAG(
    dag_id= 'pipeline_dimensoes_diaria',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['bronze', 'silver', 'dimensoes']
) as dag:
    
    # Tabelas clientes
    bronze_clientes = BashOperator(
        task_id = 'bronze_clientes',
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/02_ingestao_bronze_dimensoes.py olist_customers_dataset clientes"
    )

    silver_clientes = BashOperator(
        task_id = 'silver_clientes',
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/04_processamento_silver_dimensoes.py clientes customer_id"
    )

    # Tabelas produtos
    bronze_produtos = BashOperator(
        task_id = 'bronze_produtos',
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/02_ingestao_bronze_dimensoes.py olist_products_dataset produtos"
    )

    silver_produtos = BashOperator(
        task_id = 'silver_produtos',
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/04_processamento_silver_dimensoes.py produtos product_id"
    )

    # Tabelas Itens Produto
    bronze_itens = BashOperator(
        task_id = 'bronze_itens',
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/02_ingestao_bronze_dimensoes.py olist_order_items_dataset itens_pedido"
    )

    silver_itens = BashOperator(
        task_id = 'silver_itens',
        bash_command=f"{PYTHON_BIN} {SCRIPTS_DIR}/04_processamento_silver_dimensoes.py itens_pedido order_id,order_item_id"
    )

    # Task vazia para validacao de processamento
    fim_processamento = EmptyOperator(
        task_id="fim_processamento_dimensoes"
    )

    # Ordem de execução
    bronze_clientes >> silver_clientes >> fim_processamento
    bronze_produtos >> silver_produtos >> fim_processamento
    bronze_itens >> silver_itens >> fim_processamento