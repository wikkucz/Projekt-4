from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


# Funkcja do przetwarzania danych
def process_data():
    file_path = '/opt/airflow/dags/stroke.csv'  # Ścieżka do pliku CSV
    df = pd.read_csv(file_path)

    # Zamiana płci na wartości liczbowe
    df = df[df['gender'] != 'Other']  # Usuń przypadki 'Other'
    df['gender'] = df['gender'].map({'Female': 0, 'Male': 1})

    # Zamiana kolumny 'ever_married' na liczby
    df['ever_married'] = df['ever_married'].map({'No': 0, 'Yes': 1})

    # Kodowanie kolumn kategorycznych
    df = pd.get_dummies(df, columns=['work_type', 'Residence_type', 'smoking_status'], drop_first=True)

    # Konwersja kolumn numerycznych
    df['bmi'] = pd.to_numeric(df['bmi'], errors='coerce')
    df['avg_glucose_level'] = pd.to_numeric(df['avg_glucose_level'], errors='coerce')

    # Usuwanie brakujących danych
    df.dropna(inplace=True)

    # Zapis przetworzonych danych do nowego pliku
    output_path = '/opt/airflow/dags/processed_stroke.csv'
    df.to_csv(output_path, index=False)
    print(f"Dane zostały przetworzone i zapisane do {output_path}")

# Definicja DAG-a
with DAG(
    dag_id='data_processing_dag',
    start_date=datetime(2023, 11, 28),  # Data startowa
    schedule_interval='@daily',         # Harmonogram
    catchup=False                       # Nie uruchamiaj zaległych DAG-ów
) as dag:

    # Zadanie PythonOperator
    process_data_task = PythonOperator(
        task_id='process_data_task',   # Identyfikator zadania
        python_callable=process_data  # Wywoływana funkcja
    )
