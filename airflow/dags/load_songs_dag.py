import os
import pyarrow.csv as pc
import pyarrow.parquet as pq
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from etl.clickhouse_utils import load_data_to_clickhouse, create_staging_tables

# -------------------
# Airflow default args
# -------------------
default_args = {
    'owner': 'airflow'
}


LOCAL_CSV_FOLDER = '/data'
CSV_FILES = ['songs.csv', 'state_codes.csv']
PARQUET_FOLDER = '/data/parquet'
os.makedirs(PARQUET_FOLDER, exist_ok=True)


MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'streamify')


def convert_to_parquet(csv_file):
    """Convert CSV file to Parquet"""
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"{csv_file} does not exist!")
    parquet_file = os.path.join(PARQUET_FOLDER, os.path.basename(csv_file).replace('.csv', '.parquet'))
    table = pc.read_csv(csv_file)
    pq.write_table(table, parquet_file)
    print(f"[SUCCESS] Converted {csv_file} to {parquet_file}")
    return parquet_file

def upload_to_minio(file_path):
    """Upload file lên MinIO"""
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    buckets = [b['Name'] for b in s3_client.list_buckets().get('Buckets', [])]
    if MINIO_BUCKET not in buckets:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)

    object_name = f"{os.path.basename(file_path)}"
    s3_client.upload_file(file_path, MINIO_BUCKET, object_name)
    print(f"[SUCCESS] Uploaded {file_path} to s3://{MINIO_BUCKET}/{object_name}")

def load_parquet_to_clickhouse(parquet_file, table_name):
    """Load Parquet vào ClickHouse"""
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"{parquet_file} does not exist!")
    load_data_to_clickhouse(file_path=parquet_file, table_name=table_name)
    print(f"[SUCCESS] Loaded {parquet_file} into ClickHouse table {table_name}")


with DAG(
    dag_id='load_songs_dag',
    default_args=default_args,
    description='Load CSVs from local folder into MinIO and ClickHouse',
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['streamify']
) as dag:

    create_tables_task = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables,
    )

    previous_task = create_tables_task
    for csv_file in CSV_FILES:
        csv_path = os.path.join(LOCAL_CSV_FOLDER, csv_file)
        parquet_path = os.path.join(PARQUET_FOLDER, csv_file.replace('.csv', '.parquet'))
        table_name = os.path.splitext(csv_file)[0]  

        convert_task = PythonOperator(
            task_id=f'convert_{table_name}',
            python_callable=convert_to_parquet,
            op_kwargs={'csv_file': csv_path},
        )

        upload_task = PythonOperator(
            task_id=f'upload_{table_name}',
            python_callable=upload_to_minio,
            op_kwargs={'file_path': parquet_path},
        )

        load_task = PythonOperator(
            task_id=f'load_{table_name}_to_clickhouse',
            python_callable=load_parquet_to_clickhouse,
            op_kwargs={'parquet_file': parquet_path, 'table_name': table_name},
        )

      
        previous_task >> convert_task >> upload_task >> load_task
        previous_task = load_task
