import logging
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from infrastructure.config import Config
from domain.entities import Database

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class SQLServerRepository:
    def __init__(self, database: Database):
        self.database = database
        self.engine = None
        self.reconnect()  # Reconectar al iniciar la clase

    def reconnect(self):
        """
        Reconecta a la base de datos principal después de realizar operaciones administrativas.
        """
        try:
            # Desconectar el motor de conexión si está activo
            if self.engine:
                self.engine.dispose()
                logging.info(f"Motor de conexión a SQL Server para '{self.database.name}' eliminado.")

            # Reconstruir la cadena de conexión sin el puerto (tal como se requiere)
            connection_url = f"mssql+pyodbc://{self.database.host}/{self.database.name}?driver={self.database.driver}&trusted_connection=yes"

            # Reconectar usando la cadena de conexión URL
            self.engine = create_engine(connection_url, echo=True)
            logging.info(f"Motor de conexión a SQL Server para '{self.database.name}' reconectado exitosamente.")

            # Verificar que la base de datos esté en línea
            self.verify_database_online()

        except Exception as e:
            logging.error(f"Error al reconectar el motor de SQL Server: {e}")
            raise

    def verify_database_online(self):
        """
        Verifica que la base de datos esté en estado ONLINE.
        """
        try:
            # Cambiar la base de datos antes de ejecutar la consulta
            query = f"SELECT state_desc FROM sys.databases WHERE name = '{self.database.name}'"

            with self.engine.connect() as connection:
                # Ejecutar la consulta para obtener el estado de la base de datos
                result = connection.execute(text(query))
                state = result.fetchone()[0]

                # Verificar si el estado es ONLINE
                if state != "ONLINE":
                    raise Exception(f"La base de datos '{self.database.name}' no está ONLINE. Estado actual: {state}")

                logging.info(f"La base de datos '{self.database.name}' está en estado ONLINE.")
        except Exception as e:
            logging.error(f"Error al verificar el estado de la base de datos: {e}")
            raise

    def list_table_schema(self, table_name: str):
        """
        Lista el esquema de una tabla específica.
        """
        try:
            # Inspector para obtener información de la tabla
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)

            # Mostrar el esquema de la tabla
            logging.info(f"Esquema de la tabla '{table_name}':")
            for column in columns:
                logging.info(f"Columna: {column['name']}, Tipo: {column['type']}")

        except SQLAlchemyError as e:
            logging.error(f"Error al obtener el esquema de la tabla '{table_name}': {e}")
            raise


def main():
    logging.info("=== Iniciando el proceso de obtener el esquema de la tabla 'prv' ===")

    # Configuración de la base de datos SQL Server
    sql_server_db = Database(
        name=Config.SQL_DATABASE,
        host=Config.SQL_SERVER,
        port=int(Config.SQL_PORT),  # El puerto no se usará en la cadena de conexión
        driver=Config.SQL_DRIVER,
        data_path=Config.SQL_DATA_PATH,
        log_path=Config.SQL_LOG_PATH
    )

    # Inicializar el repositorio de SQL Server
    try:
        logging.info("Inicializando el repositorio SQL Server...")
        sql_server_repo = SQLServerRepository(sql_server_db)
        logging.info("Repositorio SQL Server inicializado correctamente.")
    except Exception as e:
        logging.error(f"Error al inicializar el repositorio SQL Server: {e}")
        return

    # Paso 1: Listar el esquema de la tabla 'prv'
    try:
        logging.info("Obteniendo el esquema de la tabla 'prv'...")
        sql_server_repo.list_table_schema('prv')
    except Exception as e:
        logging.error(f"Error al obtener el esquema de la tabla 'prv': {e}")

    # Cerrar la conexión después de terminar
    try:
        sql_server_repo.engine.dispose()
        logging.info("Conexión a SQL Server cerrada correctamente.")
    except Exception as e:
        logging.error(f"Error al cerrar la conexión a SQL Server: {e}")

    logging.info("=== Proceso completado ===")


if __name__ == "__main__":
    main()
