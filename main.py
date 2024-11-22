# main.py

import os
import sys
import logging
from application.restore_sql_database_use_case import RestoreSQLDatabaseUseCase
from application.sync_sql_to_postgres import SyncSQLToPostgres
from application.delete_sql_database_use_case import DeleteSQLDatabaseUseCase
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository
from infrastructure.config import Config
from domain.entities import Database

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("=== Iniciando el proceso de restauración y sincronización de la base de datos ===")

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

    # Imprimir configuraciones
    logging.info(f"Configuración de SQL Server: {sql_server_db}")
    logging.info(f"Configuración de PostgreSQL: {postgres_db}")

    # Inicializar repositorios
    try:
        logging.info("Inicializando repositorios...")
        sql_server_repo = SQLServerRepository(sql_server_db)
        postgres_repo = PostgresRepository(postgres_db)
        logging.info("Repositorios inicializados correctamente.")
    except Exception as e:
        logging.error(f"Error al inicializar los repositorios: {e}")
        sys.exit(1)

    # Ruta del archivo .bak ya extraído en la nueva ubicación
    bak_file_name = "ruesma202411070030.bak"  # Actualiza este nombre según corresponda
    bak_file_path = os.path.join(Config.LOCAL_BAK_FOLDER, bak_file_name)
    logging.info(f"Ruta del archivo .bak: {bak_file_path}")

    try:
        # Paso 1: Validar que el archivo .bak exista
        logging.info("Validando la existencia del archivo .bak...")
        if not os.path.exists(bak_file_path):
            raise FileNotFoundError(f"El archivo .bak especificado no existe: {bak_file_path}")
        logging.info("Archivo .bak encontrado.")

        # Paso 2: Restaurar la base de datos en SQL Server desde el archivo .bak
        logging.info(f"Iniciando restauración de la base de datos desde: {bak_file_path}...")
        restore_use_case = RestoreSQLDatabaseUseCase(sql_server_repo, bak_file_path, sql_server_db.name)
        restored = restore_use_case.execute()
        if not restored:
            raise Exception("La restauración de la base de datos falló.")
        logging.info("Base de datos restaurada correctamente en SQL Server.")

        # Paso 2a: Reconectar después de la restauración
        logging.info("Reconectando a la base de datos restaurada...")
        sql_server_repo.reconnect()

        # Paso 2b: Verificar que la base de datos está ONLINE
        logging.info("Verificando que la base de datos está ONLINE...")
        # Este paso ya se realiza dentro de `reconnect`, por lo que no es necesario duplicarlo aquí.

        # Paso 3: Sincronizar la base de datos restaurada a PostgreSQL
        logging.info("Iniciando sincronización de datos de SQL Server a PostgreSQL...")
        sync_use_case = SyncSQLToPostgres(sql_server_repo, postgres_repo, tables=['prv', 'age', 'cli'])
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
        logging.info("Cerrando conexiones a las bases de datos...")
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
        logging.info("=== Proceso completado ===")

if __name__ == "__main__":
    main()
