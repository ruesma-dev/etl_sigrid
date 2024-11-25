# etl_service/main.py

import logging
from infrastructure.config import Config
from domain.entities import Database
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository
from application.extract_use_case import ExtractUseCase
from application.load_use_case import LoadUseCase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main(tables_to_transfer=None):
    logging.info("=== Iniciando el proceso ETL ===")

    # Si no se proporciona una lista de tablas, se transferirán todas
    if tables_to_transfer is None:
        tables_to_transfer = []
        logging.info("No se proporcionaron tablas; se transferirán todas las tablas.")
    else:
        logging.info(f"Tablas especificadas para transferir: {tables_to_transfer}")

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

    # Inicializar repositorios
    try:
        logging.info("Inicializando el repositorio SQL Server...")
        sql_server_repo = SQLServerRepository(sql_server_db)
        logging.info("Repositorio SQL Server inicializado correctamente.")

        logging.info("Inicializando el repositorio PostgreSQL...")
        postgres_repo = PostgresRepository(postgres_db)
        logging.info("Repositorio PostgreSQL inicializado correctamente.")
    except Exception as e:
        logging.error(f"Error al inicializar los repositorios: {e}")
        return

    # Inicializar casos de uso
    extract_use_case = ExtractUseCase(sql_server_repo)
    load_use_case = LoadUseCase(postgres_repo)

    try:
        # Extraer datos de SQL Server
        extracted_data = extract_use_case.execute(tables_to_transfer)

        # Cargar datos en PostgreSQL
        load_use_case.execute(extracted_data)

    except Exception as e:
        logging.error(f"El proceso ETL falló: {e}")

    finally:
        # Cerrar las conexiones después de terminar
        try:
            sql_server_repo.close_connection()
            logging.info("Conexión a SQL Server cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a SQL Server: {e}")

        try:
            postgres_repo.close_connection()
            logging.info("Conexión a PostgreSQL cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a PostgreSQL: {e}")

        logging.info("=== Proceso ETL completado ===")

# Si deseas ejecutar el script desde la línea de comandos
if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        tables_to_transfer = sys.argv[1:]
    else:
        tables_to_transfer = []
    main(tables_to_transfer)
