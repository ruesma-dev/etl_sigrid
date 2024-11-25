# etl_service/infrastructure/sql_server_repository.py

import urllib
import logging
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from domain.repositories_interfaces import SQLServerRepositoryInterface
from domain.entities import Database
import pandas as pd
from typing import List
from sqlalchemy.exc import SQLAlchemyError


class SQLServerRepository(SQLServerRepositoryInterface):
    def __init__(self, database: Database):
        self.database = database

        # Construir la cadena de conexión para la base de datos principal
        if self.database.user and self.database.password:
            # Autenticación SQL
            connection_string = (
                f"Driver={{{self.database.driver}}};"
                f"Server={self.database.host};"  # Eliminamos el puerto
                f"Database={self.database.name};"
                f"UID={self.database.user};"
                f"PWD={self.database.password};"
            )
        else:
            # Autenticación de Windows
            connection_string = (
                f"Driver={{{self.database.driver}}};"
                f"Server={self.database.host};"  # Eliminamos el puerto
                f"Database={self.database.name};"
                f"Trusted_Connection=yes;"
            )

        # Imprimir la cadena de conexión (sin exponer contraseñas)
        if self.database.user and self.database.password:
            masked_connection_string = (
                f"Driver={self.database.driver};"
                f"Server={self.database.host};"  # Eliminamos el puerto
                f"Database={self.database.name};"
                f"UID={self.database.user};"
                f"PWD=***"
            )
        else:
            masked_connection_string = (
                f"Driver={self.database.driver};"
                f"Server={self.database.host};"  # Eliminamos el puerto
                f"Database={self.database.name};"
                f"Trusted_Connection=yes;"
            )

        logging.info(f"Cadena de conexión principal: {masked_connection_string}")

        # Codificar la cadena de conexión
        params = urllib.parse.quote_plus(connection_string)
        connection_url = f"mssql+pyodbc:///?odbc_connect={params}"

        try:
            self.engine: Engine = create_engine(connection_url, echo=True)  # echo=True para depuración
            logging.info("Motor de conexión a SQL Server para la base de datos principal creado exitosamente.")
        except Exception as e:
            logging.error(f"Error al crear el motor de conexión: {e}")
            raise

    def get_table_names(self) -> List[str]:
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logging.info(f"Tablas encontradas en SQL Server: {tables}")
            return tables
        except SQLAlchemyError as e:
            logging.warning(f"Error al obtener nombres de tablas: {e}")
            raise

    def read_table(self, table_name: str, columns: List[str] = None) -> pd.DataFrame:
        try:
            logging.info(f"Intentando leer la tabla '{table_name}' de la base de datos '{self.database.name}'...")
            df = pd.read_sql_table(table_name, self.engine, columns=columns)
            logging.info(f"Tabla '{table_name}' leída exitosamente con {len(df)} registros.")
            return df
        except SQLAlchemyError as e:
            logging.warning(f"Error al leer la tabla '{table_name}': {e}")
            raise
        except Exception as e:
            logging.error(f"Error al leer la tabla '{table_name}': {e}")
            raise

    def reconnect(self):
        """
        Reconecta a la base de datos principal después de realizar operaciones administrativas.
        """
        try:
            self.engine.dispose()
            logging.info(f"Motor de conexión a SQL Server para '{self.database.name}' eliminado.")

            # Reconstruir la cadena de conexión sin puerto
            connection_url = f"mssql+pyodbc://{self.database.host}/{self.database.name}?driver={self.database.driver}&trusted_connection=yes"

            # Reconectar usando la cadena de conexión URL
            self.engine = create_engine(connection_url, echo=True)
            logging.info(f"Motor de conexión a SQL Server para '{self.database.name}' reconectado exitosamente.")
            # Verificar que la base de datos está en línea
            self.verify_database_online()

        except Exception as e:
            logging.error(f"Error al reconectar el motor de SQL Server: {e}")
            raise

    def verify_database_online(self):
        """
        Verifica que la base de datos esté en estado ONLINE.
        """
        try:
            query = f"SELECT state_desc FROM sys.databases WHERE name = '{self.database.name}'"
            with self.engine.connect() as connection:
                result = connection.execute(text(query))  # Usar text() aquí
                state = result.fetchone()[0]
                if state != "ONLINE":
                    raise Exception(f"La base de datos '{self.database.name}' no está ONLINE. Estado actual: {state}")
                logging.info(f"La base de datos '{self.database.name}' está en estado ONLINE.")
        except Exception as e:
            logging.error(f"Error al verificar el estado de la base de datos: {e}")
            raise

    def close_connection(self):
        """
        Cierra las conexiones abiertas.
        """
        try:
            self.engine.dispose()
            logging.info("Conexión a SQL Server cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a SQL Server: {e}")
            raise

    def get_table_row_count(self, table_name: str) -> int:
        """Obtiene el número de filas de una tabla específica."""
        try:
            with self.engine.connect() as connection:
                query = text(f"SELECT COUNT(*) FROM [{table_name}]")
                result = connection.execute(query)
                row_count = result.scalar()
                return row_count
        except Exception as e:
            logging.error(f"Error al obtener el número de filas de la tabla '{table_name}': {e}")
            raise