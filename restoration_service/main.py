# restoration_service/main.py

import os
import sys
import logging
from restoration_service.domain.entities import Database
from restoration_service.infrastructure.config import Config
from restoration_service.infrastructure.sql_server_repository import SQLServerRepository
from restoration_service.application.restore_database_use_case import RestoreSQLDatabaseUseCase

def main():
    # Configuración de Logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info("=== Iniciando el Servicio de Restauración ===")

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

        # Inicializar repositorio
        sql_server_db = Database(
            name=Config.SQL_DATABASE,
            host=Config.SQL_SERVER,
            port=int(Config.SQL_PORT),
            driver=Config.SQL_DRIVER,
            data_path=Config.SQL_DATA_PATH,
            log_path=Config.SQL_LOG_PATH
        )
        sql_server_repo = SQLServerRepository(sql_server_db)

        # Inicializar caso de uso
        restore_use_case = RestoreSQLDatabaseUseCase(sql_server_repo, bak_file_path, sql_server_db.name)

        # Ejecutar restauración
        restored = restore_use_case.execute()
        if restored:
            logging.info("Base de datos restaurada exitosamente.")
        else:
            logging.error("La restauración de la base de datos falló.")
            sys.exit(1)

    except FileNotFoundError as fnf_error:
        logging.error(fnf_error)
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error en el proceso de restauración: {e}")
        sys.exit(1)
    finally:
        # Cerrar conexiones
        logging.info("Cerrando conexiones a las bases de datos...")
        try:
            sql_server_repo.close_connection()
            logging.info("Conexión a SQL Server cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a SQL Server: {e}")
        logging.info("=== Proceso de Restauración Completado ===")

if __name__ == "__main__":
    main()
