"""
DAG: receita_federal_csv_generator
Pipeline de dados da Receita Federal (CNPJ) - Airflow 3.x com TaskFlow API.
"""
from __future__ import annotations

import os
import re
import shutil
import zipfile

import pendulum
import requests
from airflow.sdk import DAG, dag, task
from airflow.providers.standard.operators.bash import BashOperator

# --- CONFIG ---
LOCAL_DATA_PATH = "/opt/airflow/data"

FILES_GROUP_MAP = {
    "Empresas": [f"Empresas{i}.zip" for i in range(10)],
    "Estabelecimentos": [f"Estabelecimentos{i}.zip" for i in range(10)],
    "Socios": [f"Socios{i}.zip" for i in range(10)],
    "Simples": ["Simples.zip"],
}


@task
def get_latest_date() -> str:
    """Busca a data mais recente disponivel no site da Receita Federal."""
    url = "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/"
    print(f"Fetching latest date from {url}...")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    matches = re.findall(r'href="(\d{4}-\d{2})/"', response.text)
    if not matches:
        matches = re.findall(r'>(\d{4}-\d{2})/<', response.text)
    if not matches:
        raise Exception("No date pattern found on the page.")

    latest_date = sorted(matches)[-1]
    print(f"Latest date found: {latest_date}")
    return latest_date


@task
def download_and_extract(file_name: str, mes_ano: str) -> str:
    """Baixa ZIP da Receita, extrai CSV e renomeia."""
    url = f"https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{mes_ano}/{file_name}"
    local_zip = os.path.join(LOCAL_DATA_PATH, file_name)

    raw_dest_dir = os.path.join(LOCAL_DATA_PATH, "raw", mes_ano)
    os.makedirs(raw_dest_dir, exist_ok=True)

    # 1. Download
    print(f"--- Downloading {url} ---")
    if not os.path.exists(local_zip):
        with requests.get(url, stream=True, timeout=6000) as r:
            r.raise_for_status()
            with open(local_zip, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download finished.")
    else:
        print("Zip already exists. Skipping download.")

    # 2. Extract
    print("--- Extracting ---")
    valid_extensions = (".csv", ".emprecsv", ".estabele", ".socio", ".simples", ".txt")

    try:
        with zipfile.ZipFile(local_zip, "r") as z:
            target_info = None
            for info in z.infolist():
                if info.filename.lower().endswith(valid_extensions):
                    target_info = info
                    break

            if not target_info:
                target_info = max(z.infolist(), key=lambda x: x.file_size)

            print(f"Extracting {target_info.filename}...")
            extracted_path = z.extract(target_info, raw_dest_dir)

            base_name = os.path.splitext(file_name)[0]
            new_name = f"{base_name}.csv"
            final_path = os.path.join(raw_dest_dir, new_name)

            print(f"Renaming {extracted_path} to {final_path}")
            shutil.move(extracted_path, final_path)

    except Exception as e:
        print(f"Extraction failed: {e}")
        raise

    # 3. Cleanup Zip
    try:
        os.remove(local_zip)
    except OSError:
        pass

    return final_path


@dag(
    dag_id="receita_federal_csv_generator",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"pool": "spark_pool"},
)
def receita_federal_pipeline():
    # 1. Obter data mais recente
    latest_date = get_latest_date()

    # 2. Para cada grupo: download em paralelo → processamento Spark
    for type_name, file_list in FILES_GROUP_MAP.items():
        script_name = f"process_{type_name.lower()}.py"

        # Downloads em paralelo (TaskFlow expande automaticamente)
        download_tasks = [
            download_and_extract.override(task_id=f"download_{os.path.splitext(f)[0]}")(
                file_name=f, mes_ano=latest_date
            )
            for f in file_list
        ]

        # Processamento Spark (roda apos todos downloads do grupo)
        process_cmd = f"""
        spark-submit \
        --packages io.delta:delta-spark_2.12:3.0.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        --master local[*] \
        --driver-memory 4g \
        /opt/airflow/scripts/{script_name} \
        "{{{{ ti.xcom_pull(task_ids='get_latest_date') }}}}" \
        "{type_name}"
        """

        task_process = BashOperator(
            task_id=f"process_{type_name}",
            bash_command=process_cmd,
        )

        # Ingestao no ClickHouse (graceful - nao falha se sem credenciais)
        load_cmd = f"""
        python /opt/airflow/scripts/load_to_clickhouse.py \
        "{{{{ ti.xcom_pull(task_ids='get_latest_date') }}}}" \
        "{type_name}"
        """

        task_load = BashOperator(
            task_id=f"load_{type_name}",
            bash_command=load_cmd,
        )

        # Dependencias: downloads → processamento → ingestao ClickHouse
        download_tasks >> task_process >> task_load


# Instanciar a DAG
receita_federal_pipeline()
