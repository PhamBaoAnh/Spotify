# clickhouse_utils.py
import os
import json
import pandas as pd
from clickhouse_connect import get_client

def map_schema_type(schema_type: str) -> str:
    mapping = {
        "object": "String",
        "string": "String",
        "STRING": "String",
        "int32": "Int32",
        "int64": "Int64",
        "INTEGER": "Int32",
        "float32": "Float32",
        "float64": "Float64",
        "FLOAT64": "Float64",
        "boolean": "UInt8",
        "BOOLEAN": "UInt8",
        "datetime64[ns]": "DateTime",
        "TIMESTAMP": "DateTime",
        "bool": "UInt8",
    }
    return mapping.get(schema_type.lower(), "String")

def generate_staging_sql(schema: dict, table_name: str) -> str:
    columns = schema[table_name]
    column_defs = [f'{col["name"]} {map_schema_type(col["type"])}' for col in columns]
    columns_sql = ",\n    ".join(column_defs)

    order_by = {
        "listen_events": "(userId, ts)",
        "page_view_events": "(sessionId, ts)",
        "auth_events": "(sessionId, ts)",
        "songs": "song_id"
    }.get(table_name, "tuple()")

    return f"""
        CREATE TABLE IF NOT EXISTS {table_name}_staging (
            {columns_sql}
        ) ENGINE = MergeTree()
        ORDER BY {order_by};
    """

def get_clickhouse_client():
    return get_client(
        host=os.getenv("CLICKHOUSE_HOST", "host.docker.internal"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "")
    )

def create_staging_tables():
    client = get_clickhouse_client()
    schema_file = os.path.join(os.path.dirname(__file__), "schema.json")
    with open(schema_file, "r", encoding="utf-8") as f:
        schema = json.load(f)

    for table_name in schema.keys():
        sql = generate_staging_sql(schema, table_name)
        print(f"[INFO] Creating table {table_name}_staging...")
        client.command(sql)

    print("[SUCCESS] All staging tables created successfully!")

def load_data_to_clickhouse(file_path: str, table_name: str):
    client = get_clickhouse_client()
    df = pd.read_parquet(file_path)
    print(f"[INFO] Reading {len(df)} rows from {file_path}")

    
    schema_file = os.path.join(os.path.dirname(__file__), "schema.json")
    with open(schema_file, "r", encoding="utf-8") as f:
        schema = json.load(f)[table_name]

    
    for col in schema:
        name, typ = col["name"], col["type"].lower()
        if name not in df.columns:
            
            df[name] = "" if typ in ["object", "string"] else pd.NA
        elif typ.startswith("float"):
            df[name] = df[name].astype(float)
        elif typ.startswith("int"):
            df[name] = df[name].astype("Int64")
        elif typ.startswith("string") or typ == "object":
            df[name] = df[name].fillna("").astype(str)
        elif typ.startswith("datetime"):
            df[name] = pd.to_datetime(df[name], errors="coerce")

  
    rows = [tuple(x) for x in df.to_numpy()]
    staging_table = f"{table_name}_staging"
    print(f"[INFO] Inserting {len(df)} rows into {staging_table} ...")
    client.insert(staging_table, rows, column_names=list(df.columns))
    print(f"[SUCCESS] Done: {len(df)} rows")
