# tests/test_sql_conn.py

import logging
import pytest
from sqlalchemy.exc import SQLAlchemyError
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.config import Config
from domain.entities import Database

def test_connection():
    # Configuración de la base de datos SQL Server
    sql_server_db = Database(
        name=Config.SQL_DATABASE,
        host=Config.SQL_SERVER,
        port=int(Config.SQL_PORT),
        # user y password omitidos para Autenticación de Windows
        driver=Config.SQL_DRIVER,
        data_path=Config.SQL_DATA_PATH,
        log_path=Config.SQL_LOG_PATH
    )

    sql_server_repo = None

    # Inicializar el repositorio
    try:
        sql_server_repo = SQLServerRepository(sql_server_db)
        tables = sql_server_repo.get_table_names()
        logging.info(f"Tablas disponibles en SQL Server: {tables}")
        assert tables is not None  # Verifica que se hayan obtenido tablas
    except SQLAlchemyError as e:
        logging.error(f"Error al conectar a SQL Server: {e}")
        pytest.fail(f"Error al conectar a SQL Server: {e}")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        pytest.fail(f"Error inesperado: {e}")
    finally:
        if sql_server_repo:
            sql_server_repo.close_connection()
