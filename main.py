# main.py

import os
import sys
import logging
from application.restore_sql_database_use_case import RestoreSQLDatabaseUseCase
from application.sync_sql_to_postgres import SyncSQLToPostgres
from application.delete_sql_database_use_case import DeleteSQLDatabaseUseCase  # Importar el nuevo caso de uso
from infrastructure.config import Config

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    # Ruta del archivo .bak ya extraído en la nueva ubicación
    bak_file_path = r"C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\Backup\ruesma202411070030.bak"

    try:
        # Validar que el archivo .bak exista
        if not os.path.exists(bak_file_path):
            raise FileNotFoundError(f"El archivo .bak especificado no existe: {bak_file_path}")

        # Paso 1: Restaurar la base de datos en SQL Server desde el archivo .bak
        logging.info(f"Iniciando restauración de la base de datos desde: {bak_file_path}...")
        restore_sql_use_case = RestoreSQLDatabaseUseCase(bak_file_path)
        restore_sql_use_case.execute()
        logging.info("Base de datos restaurada correctamente en SQL Server.")

        # Paso 2: Sincronizar la base de datos restaurada a PostgreSQL
        logging.info("Iniciando sincronización de datos de SQL Server a PostgreSQL...")
        sync_process = SyncSQLToPostgres(
            sql_server_config={
                "server": Config.SQL_SERVER,
                "database": Config.SQL_DATABASE,
                "driver": Config.SQL_DRIVER
            },
            postgres_config={
                "dbname": Config.PG_DATABASE,
                "user": Config.PG_USER,
                "password": Config.PG_PASSWORD,
                "host": Config.PG_SERVER,
                "port": Config.PG_PORT
            },
            tables=['prv', 'age', 'cli']  # Define aquí las tablas que deseas sincronizar
        )
        sync_process.execute()
        logging.info("Sincronización de datos completada correctamente.")

        # Paso 3: Eliminar la base de datos temporal en SQL Server
        logging.info("Iniciando eliminación de la base de datos temporal en SQL Server...")
        delete_sql_use_case = DeleteSQLDatabaseUseCase()
        delete_sql_use_case.execute()
        logging.info("Eliminación de la base de datos temporal completada correctamente.")

    except Exception as e:
        logging.error(f"Error en el proceso: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
