from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract import fetch_prices
from load import load_prices

default_args = {
    'owner': 'teemu',
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'email_on_failure': False,
}

with DAG(
    dag_id = 'crypto_price_pipeline',
    description = '',
    schedule_interval = '@hourly',
    start_date = datetime(2024, 1, 1),
    catchup = False,
    default_args = default_args,
    tags = ['crypto', 'ingestion']
) as dag:
    
    def extract_task(**context):
        df = fetch_prices()
        context['ti'].xcom_push(key='prices', value= df.to_json())
        print(f"Extracted {len(df)} rows")

    def load_task(**context):
        import pandas as pd
        json_data = context['ti'].xcom_pull(key = 'prices', task_ids = 'extract')
        df = pd.read_json(json_data)
        load_prices(df)
        print(f"Loaded {len(df)} rows")

    def check_task(**context):
        from sqlalchemy import text
        from load import get_engine
        engine = get_engine()
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw_prices"))
            count = result.scalar()
            print(f"Total rows in raw_prices: {count}")

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
        provide_context=True,
    )

    health_check = PythonOperator(
        task_id="health_check",
        python_callable=check_task,
        provide_context=True,
    )

    extract >> load >> health_check