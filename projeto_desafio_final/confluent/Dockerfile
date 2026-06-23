# Imagem customizada do Kafka Connect com os plugins necessarios para o desafio:
#   - JDBC (Source): le dados do Postgres
#   - S3  (Sink):    grava dados no S3
# Estende a imagem oficial Confluent e instala os connectors via confluent-hub.
FROM confluentinc/cp-server-connect:7.6.1

# Conector JDBC (source/sink) da Confluent, alinhado a stack 7.6.1.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.6.0

# Conector Amazon S3 (sink).
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.4.2

# Os plugins ficam em /usr/share/confluent-hub-components,
# que ja consta no CONNECT_PLUGIN_PATH definido no docker-compose.yml.
