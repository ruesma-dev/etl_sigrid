# etl_service/main.py

import logging
import sys
from infrastructure.config import Config
from domain.entities import Database
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository
from application.extract_use_case import ExtractUseCase
from application.load_use_case import LoadUseCase
from application.etl_process_use_case import ETLProcessUseCase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main(tables_to_transfer=None):
    logging.info("=== Iniciando el proceso ETL ===")

    # Configuración de la base de datos SQL Server
    sql_server_db = Database(
        name=Config.SQL_DATABASE,
        host=Config.SQL_SERVER,
        driver=Config.SQL_DRIVER,
        port=int(Config.SQL_PORT),
    )

    # Configuración de la base de datos PostgreSQL
    postgres_db = Database(
        name=Config.PG_DATABASE,
        host=Config.PG_SERVER,
        port=int(Config.PG_PORT),
        user=Config.PG_USER,
        password=Config.PG_PASSWORD,
    )

    # Inicializar repositorios y casos de uso
    try:
        sql_server_repo = SQLServerRepository(sql_server_db)
        postgres_repo = PostgresRepository(postgres_db)

        extract_use_case = ExtractUseCase(sql_server_repo)
        load_use_case = LoadUseCase(postgres_repo)

        etl_process_use_case = ETLProcessUseCase(extract_use_case, load_use_case)
    except Exception as e:
        logging.error(f"Error al inicializar los componentes: {e}")
        return

    try:
        # Obtener la lista de tablas a transferir
        if not tables_to_transfer:
            tables_to_transfer = sql_server_repo.get_table_names()
            logging.info(f"Se transferirán todas las tablas: {tables_to_transfer}")
        else:
            logging.info(f"Tablas especificadas para transferir: {tables_to_transfer}")

        # Ejecutar el proceso ETL
        etl_process_use_case.execute(tables_to_transfer)

    except Exception as e:
        logging.error(f"El proceso ETL falló: {e}")
    finally:
        # Cerrar conexiones
        sql_server_repo.close_connection()
        postgres_repo.close_connection()
        logging.info("=== Proceso ETL completado ===")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        tables_to_transfer = sys.argv[1:]
    else:
        tables_to_transfer = []
    main(tables_to_transfer)
