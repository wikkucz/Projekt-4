from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.model_selection import train_test_split
from tpot import TPOTClassifier
from sklearn.metrics import accuracy_score
import joblib
import logging


def train_model():
    try:
        # Wczytaj dane
        df = pd.read_csv('/opt/airflow/dags/processed_stroke.csv')
        X = df.drop('stroke', axis=1)
        y = df['stroke']

        # Podziel dane
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        # Trenuj model za pomocą TPOT
        model = TPOTClassifier(generations=5, population_size=20, random_state=42)
        model.fit(X_train, y_train)

        # Ewaluacja
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)

        # Zapisz model
        model_file = '/opt/airflow/models/model_tpot.pkl'
        joblib.dump(model, model_file)

        # Zapisz raport
        report_file = '/opt/airflow/reports/evaluation_report.txt'
        with open(report_file, 'w') as f:
            f.write(f'Accuracy: {accuracy}\n')

        logging.info(f"Model trained successfully with accuracy: {accuracy}")

    except Exception as e:
        logging.error(f"Error in training model: {e}")
        raise


with DAG(
    dag_id='model_training_dag',
    start_date=datetime(2024, 10, 1),
    schedule_interval=None
) as dag:
    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )

