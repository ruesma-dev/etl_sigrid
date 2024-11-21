# test_sql_connection.py

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from infrastructure.config import Config

def test_connection():
    try:
        engine = create_engine(Config.SQL_CONNECTION_STRING)
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            print("Conexión exitosa:", result.fetchone())
    except SQLAlchemyError as e:
        print("Error al conectar a SQL Server:", e)

if __name__ == "__main__":
    test_connection()
