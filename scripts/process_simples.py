# Arquivo: scripts/process_simples.py
import sys
import os
import glob
import shutil
from pyspark.sql.functions import col, regexp_replace
from spark_utils import get_spark_session 

if len(sys.argv) < 3:
    print("Uso: process_simples.py <mes_ano> <tipo_arquivo>")
    sys.exit(1)

MES_ANO = sys.argv[1]
TYPE = sys.argv[2]
# Simples só tem 1 arquivo, mas vamos manter o padrão

spark = get_spark_session(f"Process_{TYPE}")

# Caminhos locais (Volume Docker)
BASE_DIR = "/opt/airflow/data"
INPUT_PATTERN = f"{BASE_DIR}/raw/{MES_ANO}/{TYPE}*.csv" # ex: Simples.csv
TEMP_OUTPUT_DIR = f"{BASE_DIR}/temp_output/{TYPE}"
FINAL_FILE = f"{BASE_DIR}/output/{TYPE}_{MES_ANO}.csv"

print(f"Lendo arquivos locais: {INPUT_PATTERN}")

try:
    df = spark.read.format("csv") \
        .option("header", "false") \
        .option("delimiter", ";") \
        .option("quote", '"') \
        .option("encoding", "ISO-8859-1") \
        .load(INPUT_PATTERN)

    # Mapeamento do Layout Simples Nacional
    # O arquivo tem 7 colunas
    df_final = df.select(
        regexp_replace(col("_c0"), '"', '').alias("cnpj_basico"),
        regexp_replace(col("_c1"), '"', '').alias("opcao_simples"),          # S ou N
        regexp_replace(col("_c2"), '"', '').alias("data_opcao_simples"),
        regexp_replace(col("_c3"), '"', '').alias("data_exclusao_simples"),
        regexp_replace(col("_c4"), '"', '').alias("opcao_mei"),              # S ou N
        regexp_replace(col("_c5"), '"', '').alias("data_opcao_mei"),
        regexp_replace(col("_c6"), '"', '').alias("data_exclusao_mei")
    )

    print(f"Gerando CSV único em: {TEMP_OUTPUT_DIR}")

    # Grava como CSV único (coalesce(1))
    df_final.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("quote", "\u0000") \
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