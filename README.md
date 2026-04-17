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
export AIRFLOW_HOME=~/Projects/pipeline-lakehouse-local/airflow

# Iniciar o Webserver e o Scheduler do airflow
airflow standalone
```

## Fonte de Dados

Vamos utilizar a fonte de dados da  Kaggle <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>, os CSVs foram baixados e inseridos na pasta `/raw_source`, dela vamos implementar a captura em batch pegando dados diarios (para simular a ingestão).

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

### 3. Ingestão de Dimensões (Full Load)

Além da carga incremental diária da tabela fato (Pedidos), o pipeline conta com um script dinâmico para ingestão das tabelas de dimensão (Clientes, Produtos, Itens).

* **Script:** `scripts/02_ingestao_bronze_dimensoes.py`
* **Estratégia:** Ao contrário dos pedidos (que são particionados por data), as dimensões sofrem uma **Carga Total (Full Load)**, sobrescrevendo a base anterior (`mode("overwrite")`) para garantir que os metadados mais recentes estejam sempre disponíveis para a Camada Silver.
* **Execução Dinâmica:** O script recebe os nomes dos arquivos a serem ingeridos e o novo nome para o formato parquet via parâmetros do terminal (`sys.argv`), permitindo a reutilização do mesmo código para múltiplas tabelas, aplicando o metadado de `_data_ingestao` automaticamente.

```bash
# Pega o CSV olist_products_dataset e transforma em um parquet 'produtos' com a coluna nova de '_data_ingestao'
python3 scripts/02_ingestao_bronze_dimensoes.py olist_products_dataset produtos
```

## 🥈 Fase 2: Processamento e Limpeza (Camada Silver)

A Camada Silver é responsável pela higienização dos dados, aplicando regras de qualidade e convertendo os arquivos para o formato **Delta Lake**, garantindo transações ACID e *Idempotência* (capacidade de rodar o pipeline múltiplas vezes sem duplicar dados).

### 1. Tabela Fato (Pedidos) - Estratégia de Upsert

* **Script:** `scripts/03_processamento_silver_pedidos.py`
* **Lógica:** O script lê a partição do dia correspondente da Bronze, remove registros com chaves nulas e remove dados duplicados.
* **Delta Merge:** Utiliza o método `whenMatchedUpdateAll` e `whenNotMatchedInsertAll` para realizar um *Upsert* (Update + Insert) na Silver, evitando duplicação de dados em caso de reprocessamento.

### 2. Tabelas de Dimensão - Script Dinâmico

* **Script:** `scripts/04_processamento_silver_dimensoes.py`
* **Lógica:** Um único script dinâmico capaz de limpar qualquer tabela de dimensão. Ele recebe o nome da tabela e sua respectiva chave primária (ou lista de chaves, como `order_id,order_item_id`) via `sys.argv`.
* **Full Load:** Os dados são limpos e salvos na Silver sobrescrevendo o histórico anterior (`mode("overwrite")`), visto que são tabelas de atualização integral.

### 3. Orquestração das Dimensões (Airflow)

Foi criada a DAG `dag_02_pipeline_dimensoes.py` que paraleliza a execução das camadas Bronze e Silver para Clientes, Produtos e Itens do Pedido de forma diária, configurada com `catchup=False`.
