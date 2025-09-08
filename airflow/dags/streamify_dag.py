from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.clickhouse_utils import create_staging_tables
from etl.load_data_clickhouse import load_minio_to_clickhouse

EVENTS = ["listen_events", "page_view_events", "auth_events"]

with DAG(
    dag_id="Load_data_to_clickhouse",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["clickhouse", "test"],
) as dag:


    create_tables = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables,
    )


    load_tasks = []
    for event in EVENTS:
        task = PythonOperator(
            task_id=f"load_{event}",
            python_callable=load_minio_to_clickhouse,
            op_kwargs={"event_type": event},  
        )
        load_tasks.append(task)

        create_tables >> task
