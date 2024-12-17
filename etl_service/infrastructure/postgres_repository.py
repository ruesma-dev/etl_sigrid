# etl_service/infrastructure/postgres_repository.py

import logging
from sqlalchemy import create_engine, inspect, text, Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from etl_service.domain.repositories_interfaces import PostgresRepositoryInterface
from etl_service.domain.entities import Database
from typing import List
import pandas as pd
# Eliminada la importación de PRIMARY_KEY_MAPPING

class PostgresRepository(PostgresRepositoryInterface):
    def __init__(self, database: Database):
        self.database = database
        self.engine = None  # Inicialmente None
        self.create_database_if_not_exists()
        self._connect_to_database()
        self.metadata = MetaData()  # Removed bind=self.engine

    def _connect_to_database(self):
        """Conecta al motor de la base de datos especificada."""
        # Construir la cadena de conexión para PostgreSQL
        connection_string = (
            f"postgresql+psycopg2://{self.database.user}:{self.database.password}"
            f"@{self.database.host}:{self.database.port}/{self.database.name}"
        )

        # Crear la conexión a la base de datos PostgreSQL
        try:
            self.engine: Engine = create_engine(connection_string)
            logging.info(f"Conectado a la base de datos PostgreSQL '{self.database.name}'.")
        except Exception as e:
            logging.error(f"Error al conectar a la base de datos PostgreSQL '{self.database.name}': {e}")
            raise

    def create_database_if_not_exists(self):
        """Crea la base de datos si no existe."""
        from sqlalchemy import create_engine, exc
        from sqlalchemy.engine import Engine
        from sqlalchemy.sql import text

        # Conectar a la base de datos 'postgres' para tener permisos de crear bases de datos
        default_connection_string = (
            f"postgresql+psycopg2://{self.database.user}:{self.database.password}"
            f"@{self.database.host}:{self.database.port}/postgres"
        )

        try:
            # Establecer el nivel de aislamiento a AUTOCOMMIT
            temp_engine = create_engine(default_connection_string, isolation_level='AUTOCOMMIT')
            with temp_engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT 1 FROM pg_database WHERE datname='{self.database.name}'")
                )
                exists = result.scalar() is not None
                if not exists:
                    conn.execute(text(f"CREATE DATABASE {self.database.name}"))
                    logging.info(f"Base de datos PostgreSQL '{self.database.name}' creada exitosamente.")
                else:
                    logging.info(f"La base de datos PostgreSQL '{self.database.name}' ya existe.")
            temp_engine.dispose()
        except Exception as e:
            logging.error(f"Error al verificar o crear la base de datos PostgreSQL: {e}")
            raise

    def get_table_names(self) -> List[str]:
        """Obtiene los nombres de las tablas de PostgreSQL."""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logging.info(f"Tablas encontradas en PostgreSQL: {tables}")
            return tables
        except SQLAlchemyError as e:
            logging.error(f"Error al obtener nombres de tablas: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe en PostgreSQL."""
        try:
            inspector = inspect(self.engine)
            exists = inspector.has_table(table_name)
            logging.info(f"Verificación de existencia de la tabla '{table_name}': {'Existe' if exists else 'No existe'}.")
            return exists
        except SQLAlchemyError as e:
            logging.error(f"Error al verificar la existencia de la tabla '{table_name}': {e}")
            raise

    def create_table(self, table_name: str, df: pd.DataFrame, primary_key: str = None):
        """
        Crea una tabla en PostgreSQL basada en un DataFrame, asignando una clave primaria si se especifica.

        :param table_name: Nombre de la tabla a crear.
        :param df: DataFrame con los datos y estructura de la tabla.
        :param primary_key: Nombre de la columna que actuará como clave primaria.
        """
        try:
            columns = []
            for column_name, dtype in zip(df.columns, df.dtypes):
                if pd.api.types.is_integer_dtype(dtype):
                    col_type = Integer
                elif pd.api.types.is_float_dtype(dtype):
                    col_type = Float
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    col_type = DateTime
                else:
                    col_type = String

                if column_name == primary_key:
                    columns.append(Column(column_name, col_type, primary_key=True))
                else:
                    columns.append(Column(column_name, col_type))

            table = Table(table_name, self.metadata, *columns)

            # Crear la tabla en la base de datos
            table.create(self.engine)
            logging.info(f"Tabla '{table_name}' creada exitosamente con clave primaria '{primary_key}'.")
        except SQLAlchemyError as e:
            logging.error(f"Error al crear la tabla '{table_name}': {e}")
            raise

    def insert_table(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """
        Inserta un DataFrame en una tabla de PostgreSQL.

        :param df: DataFrame a insertar.
        :param table_name: Nombre de la tabla de destino en PostgreSQL.
        :param if_exists: Acción a realizar si la tabla existe. Por defecto 'append'.
                          Opciones: 'fail', 'replace', 'append'.
        """
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logging.info(f"Datos insertados en la tabla '{table_name}' de PostgreSQL con if_exists='{if_exists}'.")
        except SQLAlchemyError as e:
            logging.error(f"Error al insertar datos en la tabla '{table_name}': {e}")
            raise

    def close_connection(self):
        """Cierra la conexión con PostgreSQL."""
        try:
            if self.engine:
                self.engine.dispose()
                logging.info("Conexión a PostgreSQL cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a PostgreSQL: {e}")
            raise

    def query(self, query: str) -> list:
        """
        Ejecuta una consulta SQL y devuelve los resultados.

        :param query: Consulta SQL a ejecutar.
        :return: Lista de resultados.
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                return result.fetchall()
        except SQLAlchemyError as e:
            logging.error(f"Error al ejecutar la consulta '{query}': {e}")
            raise
