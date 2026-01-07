import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 


LEETCODE_USERNAME = 'Bartbox'

def pobierz_statystyki_leetcode():
    url = "https://leetcode-stats-api.herokuapp.com/" + LEETCODE_USERNAME
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        total_solved = data.get('totalSolved')
        print(f"Bartosz Pudlo zrobil {total_solved} zadan")
        easy = data.get('easySolved')
        medium = data.get('mediumSolved')
        hard = data.get('hardSolved')
        print(f"Bartosz Pudlo zrobil {easy} latwe, {medium} srednie i {hard} trudne zadania")

        return data
    else:
        raise Exception(f"Error during data fetching: {response.status_code}")

with DAG(
    dag_id="02_pobierz_statystyki_leetcode",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    zadanie_1 = PythonOperator(
        task_id="zadanie_1",
        python_callable=pobierz_statystyki_leetcode,
    )


### first part of the script 
# def powitanie():
#     print("Hej Bart. Twoje srodowisko dziala. Czas na LeetCode!")

# with DAG(
#     dag_id="01_test_powitanie",
#     start_date=datetime(2025, 1, 1),
#     schedule="@daily",
#     catchup=False,
# ) as dag:

#     zadanie_1 = PythonOperator(
#         task_id="zadanie_1",
#         python_callable=powitanie,
#     )
