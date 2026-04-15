# Projeto - Pipeline Data Lakehouse Local

## ⚙️ Configuração do Ambiente (Ubuntu)

Este projeto requer **Python 3.12** e **Java 11** (necessário para a JVM do Spark).

### 1. Preparação do Sistema
Antes de tudo fiz as configurações do ambiente linux, baixando python, criando uma venv, conectando ao github e baixando o visual studio code como IDE.
```bash
sudo apt install python3-pip python3-venv default-jdk -y
```

### 2. Configuração do Python
Instalando dependências iniciais:
```bash
pip install pyspark delta-spark # Pyspark e delta lake
pip install "apache-airflow[celery,postgresql,spark]==2.10.5" --upgrade
```

### 3. Inicialização do Airflow
```bash
export AIRFLOW_HOME=~/projeto-engenharia-dados/airflow
mkdir -p $AIRFLOW_HOME

airflow db init # Tabela de metadados do airflow

# Usuario de acesso interface web
airlfow users create \
    --username {nome_usuario} \
    --firstname {seu_nome} \
    --lastname {sobrenome} \
    --role Admin \
    --email {seuemail@exemplo.com}
```

## Inicialização no dia a dia
Sempre que for rodar o projeto, modificar, etc. Deve-se rodar o seguinte:
```bash
# Ativar o ambiente virtual
source venv/bin/activate

# Apontar airflow para a pasta correta
export AIFLOW_HOME=~/projeto-engenharia-dados/airflow

# Iniciar o Webserver e o Scheduler do airflow
airflow standalone
```

## Fonte de Dados

Vamos utilizar a fonte de dados da  Kaggle https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce, os CSVs foram baixados e inseridos na pasta `/raw_source`, dela vamos implementar a captura em batch pegando dados diarios (para simular a ingestão).

## 🛠️ Fase 1: Ingestão de Dados (Landing ➡️ Bronze)

Para este projeto, estamos utilizando o **Brazilian E-Commerce Public Dataset by Olist** (Kaggle). O objetivo desta fase é simular um ambiente de produção diário (Batch Processing).v


### 1. O Motor de Processamento (Apache Spark)
O primeiro script PySpark (`scripts/01_ingestao_bronze_pedidos.py`) atua da seguinte forma:
* Recebe uma data alvo como parâmetro externo (feita para o airflow).
* Lê os dados brutos (CSV) da área de *Landing* (`raw_source`).
* Filtra os pedidos específicos daquela data.
* Adiciona metadados de governança (`_data_ingestao`).
* Salva os dados na camada **Bronze** em formato **Parquet**.
* Para otimização os dados são salvos utilizando particionamento dinâmico por data (`partitionBy("data_compra")`).

### 2. A Orquestração (Apache Airflow)
A orquestração é feita através da DAG `dag_01_ingestao_bronze.py`. 
* **Estratégia de Backfilling:** Com o conceito de *Catchup* do Airflow acoplado à variável Jinja `{{ ds }}` (Execution Date) simula-se a passagem do tempo, processando o histórico de 2017 dia a dia de forma sequencial e controlada, injetando a data dinamicamente no comando Bash que chama o Spark. 

**OBS**: Para que o python tenha acesso aos parâmetros do Airflow, devemos utilizar o args da lib nativa do python `sys`. Um exemplo de uso:
![Exemplo de uso do argv](<images/Screenshot from 2026-04-15 14-50-12.png>)