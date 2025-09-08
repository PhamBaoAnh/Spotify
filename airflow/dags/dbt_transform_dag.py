from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='dbt_transform_dag',
    default_args=default_args,
    description='Test dbt with Airflow (prod)',
    schedule_interval="@once", 
    start_date=datetime(2022, 3, 20),
    catchup=False,   
    tags=['streamify', 'dbt']
) as dag:         

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /dbt && dbt deps --profiles-dir ."
    )

    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command="cd /dbt && dbt compile --profiles-dir . --target prod"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /dbt && dbt run --profiles-dir . --target prod"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /dbt && dbt test --profiles-dir . --target prod"
    )

    dbt_deps >> dbt_compile >> dbt_run >> dbt_test
