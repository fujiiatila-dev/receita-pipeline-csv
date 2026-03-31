from __future__ import annotations
import pendulum
import os
import sys
import requests
import zipfile
import re
import shutil
from xml.etree import ElementTree
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Add scripts to path to import helpers if needed, though we'll implement logic inline for simplicity/robustness
sys.path.append("/opt/airflow/scripts")

# --- CONFIG ---
LOCAL_DATA_PATH = "/opt/airflow/data"
NEXTCLOUD_BASE = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9"

def get_latest_exec_date(**kwargs):
    """
    Lista as pastas disponíveis via WebDAV PROPFIND e retorna a mais recente (YYYY-MM).
    """
    print(f"Fetching latest date via WebDAV from {NEXTCLOUD_BASE}/ ...")
    try:
        response = requests.request(
            "PROPFIND",
            f"{NEXTCLOUD_BASE}/",
            headers={"Depth": "1"},
            timeout=30,
        )
        response.raise_for_status()

        # Parse WebDAV XML para extrair nomes de pastas
        root = ElementTree.fromstring(response.content)
        ns = {"d": "DAV:"}
        hrefs = [el.text for el in root.findall(".//d:href", ns) if el.text]

        matches = []
        for href in hrefs:
            m = re.search(r'(\d{4}-\d{2})/?$', href)
            if m:
                matches.append(m.group(1))

        if not matches:
            raise Exception("No date pattern found in WebDAV listing.")

        latest_date = sorted(matches)[-1]
        print(f"Latest date found: {latest_date}")
        return latest_date
    except Exception as e:
        print(f"Error: {e}")
        raise

def download_and_extract(file_name, ti):
    """
    Downloads ZIP, extracts CSV to local raw folder, renames it to specific pattern.
    """
    # Get latest date from XCom
    mes_ano = ti.xcom_pull(task_ids='get_latest_date')
    if not mes_ano:
        raise ValueError("Date not found in XCom.")

    url = f"{NEXTCLOUD_BASE}/{mes_ano}/{file_name}"
    local_zip = os.path.join(LOCAL_DATA_PATH, file_name)
    
    # Raw destination: /opt/airflow/data/raw/{mes_ano}/
    raw_dest_dir = os.path.join(LOCAL_DATA_PATH, "raw", mes_ano)
    os.makedirs(raw_dest_dir, exist_ok=True)
    
    # 1. Download
    print(f"--- Downloading {url} ---")
    if not os.path.exists(local_zip):
        with requests.get(url, stream=True, timeout=6000) as r: # Extended timeout for big files
            r.raise_for_status()
            with open(local_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download finished.")
    else:
        print("Zip already exists. Skipping download.")

    # 2. Extract
    print("--- Extracting ---")
    valid_extensions = ('.csv', '.emprecsv', '.estabele', '.socio', '.simples', '.txt')
    
    try:
        with zipfile.ZipFile(local_zip, 'r') as z:
            target_info = None
            for info in z.infolist():
                if info.filename.lower().endswith(valid_extensions):
                    target_info = info
                    break
            
            if not target_info:
                # Fallback: largest file
                target_info = max(z.infolist(), key=lambda x: x.file_size)
            
            print(f"Extracting {target_info.filename}...")
            
            # Extract to a temp location first or directly
            # We want to rename it immediately to {file_name_stem}.csv
            # e.g. Empresas0.zip -> Empresas0.csv inside raw dir
            
            extracted_path = z.extract(target_info, raw_dest_dir)
            
            # Determine new name
            base_name = os.path.splitext(file_name)[0] # Empresas0
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
    except:
        pass


FILES_GROUP_MAP = {
    "Empresas": [f"Empresas{i}.zip" for i in range(10)],
    "Estabelecimentos": [f"Estabelecimentos{i}.zip" for i in range(10)],
    "Socios": [f"Socios{i}.zip" for i in range(10)],
    "Simples": ["Simples.zip"]
}

with DAG(
    dag_id="receita_federal_csv_generator",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={'pool': 'spark_pool'}, 
) as dag:

    # 1. Get Date
    task_date = PythonOperator(
        task_id="get_latest_date",
        python_callable=get_latest_exec_date
    )

    # 2. Process Groups (Download -> Spark)
    # We iterate over types (Empresas, Socios...)
    
    for type_name, file_list in FILES_GROUP_MAP.items():
        
        # Determine script name
        script_name = f"process_{type_name.lower()}.py"
        
        # Download Tasks
        download_tasks = []
        for f_name in file_list:
            t_down = PythonOperator(
                task_id=f"download_{os.path.splitext(f_name)[0]}",
                python_callable=download_and_extract,
                op_kwargs={'file_name': f_name}
            )
            download_tasks.append(t_down)
        
        # Process Task (Runs once per Type, after ALL downloads for that type are done)
        # Passes the Date and Type to the script
        # Script expects: <DATE> <TYPE>
        # e.g. 2025-05 Empresas
        
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
        # Note: increased driver memory to 4g for merging
        
        task_process = BashOperator(
            task_id=f"process_{type_name}",
            bash_command=process_cmd
        )
        
        # Link: Get Date -> Downloads -> Process
        task_date >> download_tasks >> task_process
