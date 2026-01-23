
import requests
import re
from datetime import datetime

def get_latest_date_from_url():
    url = "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Regex to find YYYY-MM patterns in links
        # Looking for href="2024-05/" or similar
        matches = re.findall(r'href="(\d{4}-\d{2})/"', response.text)
        
        if not matches:
            # Fallback if the format is different (e.g. text content)
            matches = re.findall(r'>(\d{4}-\d{2})/<', response.text)

        if not matches:
            raise Exception("Could not find any date pattern YYYY-MM in the URL.")

        # Sort and get the latest
        # Filter strictly valid dates if needed, but lexicographical sort works for YYYY-MM
        latest_date = sorted(matches)[-1]
        return latest_date

    except Exception as e:
        print(f"Error fetching date: {e}")
        raise

if __name__ == "__main__":
    print(get_latest_date_from_url())
