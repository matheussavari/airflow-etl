from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sqlite3
import os
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'users_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for users data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 22),
    catchup=False,
)


def setup_database():
    db_path = os.path.join(os.path.dirname(__file__), 'etl_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            gender TEXT,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('DELETE FROM users')
    conn.commit()
    conn.close()


def extract_data():
    print("Extracting data from users.csv...")
    csv_path = os.path.join(os.path.dirname(__file__), 'users.csv')
    df = pd.read_csv(csv_path)
    return df.to_dict('records')


def transform_data(**context):
    print("Transforming data...")
    data = context['task_instance'].xcom_pull(task_ids='extract')

    return data


def load_data(**context):
    data = context['task_instance'].xcom_pull(task_ids='transform')
    
    db_path = os.path.join(os.path.dirname(__file__), 'etl_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for record in data:
        cursor.execute('''
            INSERT INTO users (id, first_name, last_name, email, gender, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            record['id'],
            record['first_name'],
            record['last_name'],
            record['email'],
            record['gender'],
            record['ip_address']
        ))
    
    conn.commit()
    conn.close()
    print(f"Successfully loaded {len(data)} user records into the database")

start_task = EmptyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Add database setup task
setup_db_task = PythonOperator(
    task_id='setup_database',
    python_callable=setup_database,
    dag=dag,
)

# Define the task dependencies
start_task >> setup_db_task >> extract_task >> transform_task >> load_task >> end_task
