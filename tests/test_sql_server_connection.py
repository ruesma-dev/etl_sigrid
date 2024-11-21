# test_sql_server_connection.py

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
from infrastructure import config

# Cargar variables de entorno
load_dotenv()


def test_sql_server_connection():
    try:
        # Crear una cadena de conexión específica para el test, apuntando a la base de datos 'master'
        test_connection_string = (
            f"mssql+pyodbc://@{config.SQL_SERVER}/master"
            f"?driver={config.SQL_DRIVER}&trusted_connection=yes"
        )
        # Crear el motor de SQLAlchemy con la cadena de conexión del test
        engine = create_engine(test_connection_string)
        # Crear una conexión
        with engine.connect() as connection:
            # Ejecutar una consulta simple
            result = connection.execute(text("SELECT 1"))
            assert result.scalar() == 1, "La consulta de prueba en SQL Server no devolvió el resultado esperado."
            print("Test de conexión a SQL Server pasado.")
    except OperationalError as e:
        pytest.fail(f"Test de conexión a SQL Server fallido: {e}")
    finally:
        engine.dispose()
