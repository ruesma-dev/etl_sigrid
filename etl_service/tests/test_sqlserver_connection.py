# etl_service/tests/test_sqlserver_connection.py

import pytest
import logging
from etl_service.infrastructure.sql_server_repository import SQLServerRepository
from etl_service.domain.entities import Database
from etl_service.infrastructure.config import Config
from sqlalchemy.exc import OperationalError


def test_sqlserver_connection_and_table_exists():
    # Configurar el nivel de logging
    logging.basicConfig(level=logging.INFO)

    # Configurar la base de datos SQL Server utilizando los valores de Config
    sql_server_db = Database(
        name=Config.SQL_DATABASE,
        host=Config.SQL_SERVER,
        driver=Config.SQL_DRIVER,
        port=int(Config.SQL_PORT),
    )

    # Intentar inicializar el repositorio (establecer conexión)
    try:
        sql_server_repo = SQLServerRepository(sql_server_db)
        logging.info(
            f"Conexión exitosa a la base de datos SQL Server '{sql_server_db.name}' en '{sql_server_db.host}'.")

        # Obtener la lista de tablas
        tables = sql_server_repo.get_table_names()
        logging.info(f"Tablas en SQL Server: {tables}")

        # Verificar que la tabla 'prv' existe
        assert 'prv' in tables, "La tabla 'prv' no existe en la base de datos SQL Server."
        logging.info("La tabla 'prv' existe en la base de datos SQL Server.")

        connected = True
    except OperationalError as e:
        logging.error(f"No se pudo conectar a la base de datos SQL Server: {e}")
        connected = False
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado: {e}")
        connected = False
    finally:
        # Asegurarse de cerrar la conexión si se estableció
        if 'sql_server_repo' in locals():
            sql_server_repo.close_connection()

    # Verificar que la conexión se estableció exitosamente
    assert connected, "No se pudo establecer conexión con la base de datos SQL Server."
