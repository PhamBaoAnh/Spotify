FROM apache/superset:latest

USER root

# Fix version SQLAlchemy để tránh lỗi eagerload
RUN pip install --no-cache-dir "SQLAlchemy<2.0" "alembic<1.8"

# Cài driver cho ClickHouse, Postgres, Redis
RUN pip install --no-cache-dir clickhouse-connect psycopg2-binary redis

USER superset
