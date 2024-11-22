# test_sync_table.py

import logging
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository
from infrastructure.config import Config
from domain.entities import Database
from application.sync_sql_to_postgres import SyncSQLToPostgres

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def test_sync():
    # Configuración de las bases de datos
    sql_server_db = Database(
        name=Config.SQL_DATABASE,
        host=Config.SQL_SERVER,
        port=int(Config.SQL_PORT),
        # user y password omitidos para Autenticación de Windows
        driver=Config.SQL_DRIVER,
        data_path=Config.SQL_DATA_PATH,
        log_path=Config.SQL_LOG_PATH
    )

    postgres_db = Database(
        name=Config.PG_DATABASE,
        host=Config.PG_SERVER,
        port=int(Config.PG_PORT),
        user=Config.PG_USER,
        password=Config.PG_PASSWORD,
        data_path='',  # PostgreSQL no necesita paths específicos aquí
        log_path=''
    )

    # Inicializar repositorios
    try:
        sql_server_repo = SQLServerRepository(sql_server_db)
        postgres_repo = PostgresRepository(postgres_db)
    except Exception as e:
        logging.error(f"Error al inicializar los repositorios: {e}")
        return

    # Sincronizar una tabla específica
    try:
        sync_use_case = SyncSQLToPostgres(sql_server_repo, postgres_repo, tables=['prv'])
        sync_use_case.execute()
        logging.info("Sincronización de la tabla 'prv' completada correctamente.")
    except Exception as e:
        logging.error(f"Error durante la sincronización: {e}")
    finally:
        # Cerrar conexiones
        try:
            sql_server_repo.close_connection()
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a SQL Server: {e}")
        try:
            postgres_repo.close_connection()
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a PostgreSQL: {e}")

if __name__ == "__main__":
    test_sync()
