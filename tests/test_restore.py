# test_restore.py

import logging
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.config import Config
from domain.entities import Database
from application.restore_sql_database_use_case import RestoreSQLDatabaseUseCase

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def test_restore():
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

    # Inicializar el repositorio
    sql_server_repo = SQLServerRepository(sql_server_db)

    # Ruta del archivo .bak ya extraído en la nueva ubicación
    bak_file_name = "ruesma202411070030.bak"  # Actualiza este nombre según corresponda
    bak_file_path = os.path.join(Config.LOCAL_BAK_FOLDER, bak_file_name)

    try:
        # Restaurar la base de datos
        restore_use_case = RestoreSQLDatabaseUseCase(sql_server_repo, bak_file_path, sql_server_db.name)
        restored = restore_use_case.execute()
        if restored:
            logging.info("Base de datos restaurada correctamente.")
        else:
            logging.error("La restauración de la base de datos falló.")
    except Exception as e:
        logging.error(f"Error durante la restauración: {e}")
    finally:
        sql_server_repo.close_connection()

if __name__ == "__main__":
    test_restore()
