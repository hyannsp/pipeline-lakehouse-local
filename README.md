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