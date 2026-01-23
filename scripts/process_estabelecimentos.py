# Arquivo: scripts/process_estabelecimentos.py
import sys
import os
import glob
import shutil
from pyspark.sql.functions import col
from spark_utils import get_spark_session 

if len(sys.argv) < 3:
    print("Uso: process_estabelecimentos.py <mes_ano> <tipo_arquivo>")
    sys.exit(1)

MES_ANO = sys.argv[1]
TYPE = sys.argv[2]

spark = get_spark_session(f"Process_{TYPE}")

# Caminhos locais (Volume Docker)
BASE_DIR = "/opt/airflow/data"
INPUT_PATTERN = f"{BASE_DIR}/raw/{MES_ANO}/{TYPE}*.csv"
TEMP_OUTPUT_DIR = f"{BASE_DIR}/temp_output/{TYPE}"
FINAL_FILE = f"{BASE_DIR}/output/{TYPE}_{MES_ANO}.csv"

print(f"Lendo arquivos locais: {INPUT_PATTERN}")

try:
    df = spark.read.format("csv") \
        .option("header", "false") \
        .option("delimiter", ";") \
        .option("quote", "\u0000") \
        .option("encoding", "ISO-8859-1") \
        .load(INPUT_PATTERN)

    # Mapeamento Direto das 30 colunas
    df_final = df.select(
        col("_c0").alias("cnpj_basico"),
        col("_c1").alias("cnpj_ordem"),
        col("_c2").alias("cnpj_dv"),
        col("_c3").alias("identificador_matriz_filial"),
        col("_c4").alias("nome_fantasia"),
        col("_c5").alias("situacao_cadastral"),
        col("_c6").alias("data_situacao_cadastral"),
        col("_c7").alias("motivo_situacao_cadastral"),
        col("_c8").alias("nome_cidade_exterior"),
        col("_c9").alias("pais"),
        col("_c10").alias("data_inicio_atividade"),
        col("_c11").alias("cnae_fiscal_principal"),
        col("_c12").alias("cnae_fiscal_secundaria"),
        col("_c13").alias("tipo_logradouro"),
        col("_c14").alias("logradouro"),
        col("_c15").alias("numero"),
        col("_c16").alias("complemento"),
        col("_c17").alias("bairro"),
        col("_c18").alias("cep"),
        col("_c19").alias("uf"),
        col("_c20").alias("municipio"),
        col("_c21").alias("ddd_1"),
        col("_c22").alias("telefone_1"),
        col("_c23").alias("ddd_2"),
        col("_c24").alias("telefone_2"),
        col("_c25").alias("ddd_fax"),
        col("_c26").alias("fax"),
        col("_c27").alias("correio_eletronico"),
        col("_c28").alias("situacao_especial"),
        col("_c29").alias("data_situacao_especial")
    )

    print(f"Gerando CSV único em: {TEMP_OUTPUT_DIR}")
    
    # Grava como CSV único (coalesce(1))
    df_final.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("encoding", "ISO-8859-1") \
        .mode("overwrite") \
        .save(TEMP_OUTPUT_DIR)
    
    # Renomeia
    part_files = glob.glob(f"{TEMP_OUTPUT_DIR}/part-*.csv")
    if part_files:
        os.makedirs(os.path.dirname(FINAL_FILE), exist_ok=True)
        shutil.move(part_files[0], FINAL_FILE)
        print(f"Arquivo final gerado: {FINAL_FILE}")
    else:
        raise Exception("Arquivo de parte não encontrado.")

    shutil.rmtree(TEMP_OUTPUT_DIR, ignore_errors=True)

except Exception as e:
    print(f"ERRO: {e}")
    sys.exit(1)

spark.stop()