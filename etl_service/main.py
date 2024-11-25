import logging
from infrastructure.config import Config
from domain.entities import Database
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository  # Nuevo repositorio para PostgreSQL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("=== Iniciando el proceso ===")

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
        data_path='',  # PostgreSQL no necesita paths específicos aquí
        log_path=''
    )


    # Inicializar el repositorio de SQL Server
    try:
        logging.info("Inicializando el repositorio SQL Server...")
        sql_server_repo = SQLServerRepository(sql_server_db)
        logging.info("Repositorio SQL Server inicializado correctamente.")
    except Exception as e:
        logging.error(f"Error al inicializar el repositorio SQL Server: {e}")
        return

    # Inicializar el repositorio de PostgreSQL
    try:
        logging.info("Inicializando el repositorio PostgreSQL...")
        postgres_repo = PostgresRepository(postgres_db)  # Nuevo repositorio para PostgreSQL
        logging.info("Repositorio PostgreSQL inicializado correctamente.")
    except Exception as e:
        logging.error(f"Error al inicializar el repositorio PostgreSQL: {e}")
        return

    # Paso 1: Obtener los nombres de las tablas de SQL Server
    try:
        logging.info("Obteniendo los nombres de las tablas de SQL Server...")
        sql_server_tables = sql_server_repo.get_table_names()
        logging.info(f"Tablas encontradas en SQL Server: {sql_server_tables}")
    except Exception as e:
        logging.error(f"Error al obtener los nombres de las tablas de SQL Server: {e}")

    # Paso 2: Obtener los nombres de las tablas de PostgreSQL
    try:
        logging.info("Obteniendo los nombres de las tablas de PostgreSQL...")
        postgres_tables = postgres_repo.get_table_names()
        logging.info(f"Tablas encontradas en PostgreSQL: {postgres_tables}")
    except Exception as e:
        logging.error(f"Error al obtener los nombres de las tablas de PostgreSQL: {e}")

    # Cerrar la conexión de SQL Server después de terminar
    try:
        sql_server_repo.close_connection()
        logging.info("Conexión a SQL Server cerrada correctamente.")
    except Exception as e:
        logging.error(f"Error al cerrar la conexión a SQL Server: {e}")

    # Cerrar la conexión de PostgreSQL después de terminar
    try:
        postgres_repo.close_connection()
        logging.info("Conexión a PostgreSQL cerrada correctamente.")
    except Exception as e:
        logging.error(f"Error al cerrar la conexión a PostgreSQL: {e}")

    logging.info("=== Proceso completado ===")

if __name__ == "__main__":
    main()
