"""
Camadas Silver e Gold - Apache Spark SQL API

Le a camada Bronze (JSON gravado no S3 pelo Kafka Connect em
raw-data/kafka/<topico>/), aplica limpeza e transformacoes (Silver) e gera
metricas agregadas (Gold), salvando ambas em Parquet no S3. Processa IPCA e PRE.

Credenciais para o S3 (escolha uma):
  - export AWS_ACCESS_KEY_ID=...  / export AWS_SECRET_ACCESS_KEY=...
  - ou ~/.aws/credentials
O conector s3a usa a cadeia padrao de credenciais da AWS.

Execucao:
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 etl_spark.py <bucket>
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime


def criar_spark():
    return (
        SparkSession.builder
        .appName("ETL Pipeline - Tesouro Direto")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def processar(spark, bucket, tipo):
    """tipo: 'ipca' ou 'pre'."""
    topico = (
        "postgres-dadostesouroipca" if tipo == "ipca" else "postgres-dadostesouropre"
    )
    # Caminho real onde o Kafka Connect gravou a camada Bronze:
    bronze_path = f"s3a://{bucket}/raw-data/kafka/{topico}/"
    silver_path = f"s3a://{bucket}/processed-data/{tipo}/silver/"
    gold_path = f"s3a://{bucket}/analytics/{tipo}/gold/"

    # ----------------- CAMADA BRONZE (leitura do bruto) -----------------
    print(f"[{tipo}] Lendo camada Bronze em: {bronze_path}")
    df_bronze = spark.read.json(bronze_path)
    df_bronze.show()

    # ----------------- CAMADA SILVER (limpeza e transformacao) -----------------
    print(f"[{tipo}] Processando camada Silver...")
    df_silver = (
        df_bronze.dropDuplicates()  # remove registros identicos (inclui as duplicatas dos conectores)
        .withColumn(
            "Data_Vencimento",
            from_unixtime(col("Data_Vencimento") / 1000, "yyyy-MM-dd"),
        )
        .withColumn(
            "Data_Base", from_unixtime(col("Data_Base") / 1000, "yyyy-MM-dd")
        )
        .withColumn(
            "dt_update",
            from_unixtime(col("dt_update") / 1000, "yyyy-MM-dd HH:mm:ss"),
        )
        .fillna({"PUCompraManha": 0, "PUVendaManha": 0, "PUBaseManha": 0})
    )
    df_silver.show()
    df_silver.write.mode("overwrite").parquet(silver_path)
    print(f"[{tipo}] Camada Silver salva em: {silver_path}")

    # ----------------- CAMADA GOLD (agregacao com Spark SQL) -----------------
    print(f"[{tipo}] Processando camada Gold...")
    df_silver.createOrReplaceTempView("silver")
    df_gold = spark.sql(
        """
        SELECT
            Tipo,
            AVG(PUCompraManha) AS Media_PUCompraManha,
            AVG(PUVendaManha)  AS Media_PUVendaManha,
            AVG(PUBaseManha)   AS Media_PUBaseManha,
            COUNT(*)           AS Total_Registros
        FROM silver
        GROUP BY Tipo
        """
    )
    df_gold.show()
    df_gold.write.mode("overwrite").parquet(gold_path)
    print(f"[{tipo}] Camada Gold salva em: {gold_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: spark-submit etl_spark.py <bucket>")
        sys.exit(1)

    bucket = sys.argv[1]
    spark = criar_spark()
    try:
        processar(spark, bucket, "ipca")
        processar(spark, bucket, "pre")
        print("ETL concluido com sucesso!")
    finally:
        spark.stop()
