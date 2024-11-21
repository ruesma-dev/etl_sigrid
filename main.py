# main.py

import os
import sys
import logging
from application.restore_sql_database_use_case import RestoreSQLDatabaseUseCase
from application.sync_sql_to_postgres import SyncSQLToPostgresUseCase
from application.delete_sql_database_use_case import DeleteSQLDatabaseUseCase
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository
from infrastructure.config import Config
from domain.entities import Database

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    # Configuración de las bases de datos
    sql_server_db = Database(
        name=Config.SQL_DATABASE,
        host=Config.SQL_SERVER,
        port=int(Config.SQL_PORT),
        user=Config.SQL_USER,
        password=Config.SQL_PASSWORD,
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
    sql_server_repo = SQLServerRepository(sql_server_db)
    postgres_repo = PostgresRepository(postgres_db)

    # Ruta del archivo .bak ya extraído en la nueva ubicación
    bak_file_name = "ruesma202411070030.bak"  # Actualiza este nombre según corresponda
    bak_file_path = os.path.join(Config.LOCAL_BAK_FOLDER, bak_file_name)

    try:
        # Paso 1: Validar que el archivo .bak exista
        if not os.path.exists(bak_file_path):
            raise FileNotFoundError(f"El archivo .bak especificado no existe: {bak_file_path}")

        # Paso 2: Restaurar la base de datos en SQL Server desde el archivo .bak
        logging.info(f"Iniciando restauración de la base de datos desde: {bak_file_path}...")
        restore_use_case = RestoreSQLDatabaseUseCase(sql_server_repo, bak_file_path, sql_server_db)
        restored = restore_use_case.execute()
        if not restored:
            raise Exception("La restauración de la base de datos falló.")
        logging.info("Base de datos restaurada correctamente en SQL Server.")

        # Paso 3: Sincronizar la base de datos restaurada a PostgreSQL
        logging.info("Iniciando sincronización de datos de SQL Server a PostgreSQL...")
        sync_use_case = SyncSQLToPostgresUseCase(sql_server_repo, postgres_repo, tables=['prv', 'age', 'cli'])
        sync_use_case.execute()
        logging.info("Sincronización de datos completada correctamente.")

        # Paso 4: Eliminar la base de datos temporal en SQL Server
        logging.info("Iniciando eliminación de la base de datos temporal en SQL Server...")
        delete_use_case = DeleteSQLDatabaseUseCase(sql_server_repo)
        delete_use_case.execute()
        logging.info("Eliminación de la base de datos temporal completada correctamente.")

    except FileNotFoundError as fnf_error:
        logging.error(fnf_error)
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error en el proceso: {e}")
        sys.exit(1)
    finally:
        # Cerrar conexiones
        sql_server_repo.close_connection()
        postgres_repo.close_connection()
        logging.info("Conexiones a las bases de datos cerradas.")

if __name__ == "__main__":
    main()
