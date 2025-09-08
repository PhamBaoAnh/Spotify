import os
import io
import boto3
import pandas as pd
import json
from .clickhouse_utils import get_clickhouse_client

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "streamify")


def list_parquet_files(event_type: str, year: int = None, month: int = None, day: int = None, hour: int = None):
    """
    Lấy danh sách file parquet trong MinIO theo event_type.
    - Nếu có year/month/day/hour -> lọc theo partition Hive
    - Nếu không truyền -> lấy tất cả file parquet
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    prefix = f"{event_type}/"

    if year is not None:
        prefix += f"year={year}/"
        if month is not None:
            prefix += f"month={month}/"
            if day is not None:
                prefix += f"day={day}/"
                if hour is not None:
                    prefix += f"hour={hour}/"

    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"].endswith(".parquet"):
                    files.append(obj["Key"])

    files = sorted(files)

 
    if files:
        sample_key = files[0]
        print(f"📑 Inspect schema from {sample_key}")

        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=sample_key)
        df_sample = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        schema_info = {col: str(df_sample[col].dtype) for col in df_sample.columns}
        print("🔎 Schema detected:", schema_info)

    return files


def load_minio_to_clickhouse(
    event_type: str,
    year: int = None,
    month: int = None,
    day: int = None,
    hour: int = None,
    **context,
):
    """
    Load parquet từ MinIO (theo event_type) vào ClickHouse staging
    - Nếu truyền year/month/day/hour: load đúng partition
    - Nếu không truyền: load toàn bộ file
    - Gom tất cả file vào 1 DataFrame trước khi insert (tránh too many parts)
    """
    files = list_parquet_files(event_type, year, month, day, hour)

    if not files:
        print(f"⚠️ No files found for {event_type} {year}-{month}-{day} {hour}")
        return

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    client = get_clickhouse_client()

    schema_file = os.path.join(os.path.dirname(__file__), "schema.json")
    with open(schema_file, "r") as f:
        SCHEMA = json.load(f)

    valid_columns = [c["name"] for c in SCHEMA[event_type]]

    dfs = []
    for f in files:
        print(f"📥 Reading {f} ...")
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=f)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        if df.empty:
            print(f"⚠️ File {f} is empty, skipping.")
            continue

        # Giữ lại các cột hợp lệ
        df = df[[c for c in df.columns if c in valid_columns]]

        dfs.append(df)

    if not dfs:
        print(f"⚠️ No valid data for {event_type}")
        return

    # Gộp toàn bộ vào 1 DataFrame
    big_df = pd.concat(dfs, ignore_index=True)

    # Xử lý NULL cho các cột non-nullable (fillna rỗng)
    for col in valid_columns:
        if col in big_df.columns and big_df[col].dtype == object:
            big_df[col] = big_df[col].fillna("")

    data = [tuple(x) for x in big_df.to_numpy()]
    client.insert(
        f"{event_type}_staging",
        data,
        column_names=big_df.columns.tolist(),
    )

    print(f"✅ Inserted {len(big_df)} rows into {event_type}_staging")
