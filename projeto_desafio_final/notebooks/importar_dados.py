"""
Camada Bronze - Ingestao Bruta (Tesouro Direto -> PostgreSQL)

Le o CSV de precos e taxas dos titulos publicos do portal de Dados Abertos do
Tesouro Nacional (CKAN), normaliza os nomes das colunas para o padrao usado no
restante do pipeline e grava as tabelas dadostesouroipca e dadostesouropre no
PostgreSQL. Essas tabelas sao a fonte dos conectores Kafka Source.
"""

import pandas as pd
from sqlalchemy import create_engine
import ssl

# Alguns ambientes Windows reclamam do certificado do portal do Tesouro.
ssl._create_default_https_context = ssl._create_unverified_context

URL_TESOURO = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "df56aa42-484a-4a59-8184-7676580c81e3/resource/"
    "796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"
)

# Mapeia os nomes originais do CSV (com espacos/acentos) para o padrao consistente
# usado pelo Kafka Connect, pelo JSON do S3 e pelo Spark (camadas Silver/Gold).
RENOMEAR_COLUNAS = {
    "Tipo Titulo": "Tipo",
    "Data Vencimento": "Data_Vencimento",
    "Data Base": "Data_Base",
    "Taxa Compra Manha": "CompraManha",
    "Taxa Venda Manha": "VendaManha",
    "PU Compra Manha": "PUCompraManha",
    "PU Venda Manha": "PUVendaManha",
    "PU Base Manha": "PUBaseManha",
}


def importar_dados():
    print("Baixando dados do Tesouro Direto...")
    # CSV do governo: separador ';' e decimal ','
    df = pd.read_csv(URL_TESOURO, sep=";", decimal=",")

    # 1) Normalizacao dos nomes de coluna (requisito da camada Silver: nomes consistentes)
    df = df.rename(columns=RENOMEAR_COLUNAS)

    # 2) Conversao das datas brasileiras (dd/mm/aaaa) para datetime.
    #    Gravadas como TIMESTAMP no Postgres, o Kafka Connect as serializa em epoch (ms),
    #    que e exatamente o que a camada Silver no Spark espera (from_unixtime(col/1000)).
    df["Data_Vencimento"] = pd.to_datetime(
        df["Data_Vencimento"], format="%d/%m/%Y", errors="coerce"
    )
    df["Data_Base"] = pd.to_datetime(
        df["Data_Base"], format="%d/%m/%Y", errors="coerce"
    )

    # 3) Coluna de controle exigida pelo conector JDBC (mode=timestamp).
    df["dt_update"] = pd.Timestamp.now()

    # 4) Separacao por tipo de titulo.
    df_ipca = df[df["Tipo"].str.contains("IPCA", case=False, na=False)].copy()
    df_pre = df[df["Tipo"].str.contains("Prefixado", case=False, na=False)].copy()

    # 5) Padroniza o valor da coluna Tipo (facilita o GROUP BY da camada Gold).
    df_ipca["Tipo"] = "IPCA"
    df_pre["Tipo"] = "PRE-FIXADOS"

    # Conexao com o Postgres local (parametros do postgres/docker-compose.yml).
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")

    print("Enviando dados para o PostgreSQL...")
    df_ipca.to_sql("dadostesouroipca", engine, if_exists="replace", index=False)
    df_pre.to_sql("dadostesouropre", engine, if_exists="replace", index=False)

    print(
        f"Importacao concluida! "
        f"IPCA: {len(df_ipca)} linhas | PRE-FIXADOS: {len(df_pre)} linhas"
    )


if __name__ == "__main__":
    try:
        importar_dados()
    except Exception as e:
        print(f"Erro ao importar: {e}")
