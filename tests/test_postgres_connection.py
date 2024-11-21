# tests/test_postgres_connection.py

import pytest
import psycopg2
from infrastructure.config import Config

def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname=Config.PG_DATABASE,
            user=Config.PG_USER,
            password=Config.PG_PASSWORD,
            host=Config.PG_SERVER,
            port=Config.PG_PORT
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        assert result[0] == 1
        cur.close()
        conn.close()
    except Exception as e:
        pytest.fail(f"Conexión a PostgreSQL falló: {e}")
