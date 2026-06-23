# Desafio Final - Engenharia de Dados (Tesouro Direto)

Pipeline ETL completo no modelo **Bronze / Silver / Gold**: ingestao do CSV de
precos e taxas do Tesouro Direto no PostgreSQL, movimentacao para o Amazon S3 via
Kafka Connect (camada Bronze bruta) e processamento das camadas Silver e Gold com
Apache Spark SQL.

## Estrutura
- `postgres/`     : Docker Compose do banco relacional (fonte).
- `confluent/`    : Docker Compose da plataforma Kafka + Dockerfile da imagem custom do Connect + `.env_kafka_connect` (chaves AWS).
- `connectors/`   : Configs JSON dos conectores Source (Postgres->Kafka) e Sink (Kafka->S3).
- `notebooks/`    : Script de ingestao Bronze (Python/Pandas) - `importar_dados.py`.
- `spark/`        : Ambiente Spark via Docker (`docker-compose.yml` + Jupyter) e o ETL das camadas Silver/Gold (`etl-spark.ipynb` e `etl_spark.py`).

## Pre-requisitos
- Docker e docker-compose
- Conta AWS (free tier) com 1 bucket S3 criado: `desafio-pos-eng-dados-gabriel-2026` (regiao us-east-1)
- Python 3 com `pandas` e `sqlalchemy` para a ingestao Bronze (`pip install pandas sqlalchemy psycopg2-binary`)

## Passo a passo

### 0. Configurar credenciais e rede
Edite `confluent/.env_kafka_connect` com SUAS chaves da AWS:
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```
Crie a rede compartilhada (os dois compose usam a mesma rede externa para que o
Connect enxergue o host `postgres`):
```bash
docker network create desafio-network
```

### 1. Subir o PostgreSQL
```bash
cd postgres
docker-compose up -d
```

### 2. Ingestao Bronze (Postgres)
```bash
cd ../notebooks
python3 importar_dados.py
```
Confira no DBeaver as tabelas `public.dadostesouroipca` e `public.dadostesouropre`.

### 3. Buildar a imagem custom e subir o Kafka
```bash
cd ../confluent
docker-compose up -d --build
```
Servicos: zookeeper, broker, schema-registry e connect (porta 8083).

### 4. (Opcional) Criar os topicos manualmente
Os conectores Source criam os topicos automaticamente, mas, se preferir, dentro do broker:
```bash
docker exec -it broker bash
kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic postgres-dadostesouroipca
kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic postgres-dadostesouropre
kafka-topics --bootstrap-server localhost:9092 --list
```

### 5. Registrar os conectores
Os arquivos de sink ja estao configurados com o bucket `desafio-pos-eng-dados-gabriel-2026`
e `topics.dir: raw-data/kafka`, entao basta registrar. No PowerShell use `curl.exe`
(no Linux/Mac, `curl`):
```bash
# Source (Postgres -> Kafka)
curl -X POST -H "Content-Type: application/json" --data @connectors/source/connect_jdbc_postgres_ipca.config.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/source/connect_jdbc_postgres_pre.config.json  http://localhost:8083/connectors

# Sink (Kafka -> S3)
curl -X POST -H "Content-Type: application/json" --data @connectors/sink/connect_s3_sink_ipca.config.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/sink/connect_s3_sink_pre.config.json  http://localhost:8083/connectors
```
Verifique os logs: `docker logs -f connect`

### 6. Configurar permissoes no bucket (IAM)
No bucket, aplique a policy de acesso ao seu usuario IAM (Action: s3:GetObject,
s3:PutObject, s3:DeleteObject, s3:ListBucket). Como os conectores usam
`s3.object.tagging=true`, inclua tambem **`s3:PutObjectTagging`** na policy (senao o
sink falha). Alternativa: mudar `s3.object.tagging` para `false` nos configs do sink.

### 7. Processamento Spark (Silver & Gold) - via Docker + Jupyter
Edite `spark/.env_spark` com suas chaves AWS (as mesmas do `.env_kafka_connect`) e suba o ambiente:
```bash
cd ../spark
docker-compose up -d
```
Abra `http://localhost:8888` (token: `desafio`), entre na pasta `work` e abra o
`etl-spark.ipynb`. Rode as celulas de cima para baixo. O notebook le a camada Bronze
de `raw-data/kafka/<topico>/`, processa IPCA e PRE e grava Silver (`processed-data/`)
e Gold (`analytics/`) em Parquet, usando consultas Spark SQL na camada Gold.

Alternativa (script via `spark-submit`, sem o container, com Spark instalado localmente):
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 etl_spark.py desafio-pos-eng-dados-gabriel-2026
```
O `--packages` baixa `hadoop-aws-3.3.4` e o `aws-java-sdk-bundle` automaticamente.

## Entregaveis (o que capturar)
1. Prints das tabelas no Postgres (DBeaver).
2. Prints/logs do codigo Spark executando (camadas Silver/Gold + consultas Spark SQL).
3. Prints do S3 com os dados organizados e particionados:
   - Bronze : `raw-data/kafka/<topico>/partition=0/`
   - Silver : `processed-data/{ipca,pre}/silver/`
   - Gold   : `analytics/{ipca,pre}/gold/`
