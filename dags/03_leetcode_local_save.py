import requests
import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 


LEETCODE_USERNAME = 'Bartbox'

def pobierz_i_zapisz_lokalnie():
    url = f"https://leetcode-stats-api.herokuapp.com/{LEETCODE_USERNAME}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        # Dodajemy timestamp pobrania, żeby wiedzieć, kiedy zrobiliśmy "snapshot"
        data['captured_at'] = datetime.now().isoformat()
        
        # Scieżka wewnątrz kontenera Astro (folder include jest zmapowany)
        file_path = "/usr/local/airflow/include/leetcode_history.json"

        history = []
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                try:
                    history = json.load(f)
                except json.JSONDecodeError:
                    history = []
        
        history.append(data)
        
        with open(file_path, "w") as f:
            json.dump(history, f, indent=4)
            
        print(f"Sukces! Dane zapisane w {file_path}")
        print(f"Aktualna liczba wpisów w historii: {len(history)}")
    else:
        raise Exception(f"Błąd API: {response.status_code}")

with DAG(
    dag_id="03_leetcode_local_save",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task_save_data = PythonOperator(
        task_id="pobierz_i_zapisz_json",
        python_callable=pobierz_i_zapisz_lokalnie
    )