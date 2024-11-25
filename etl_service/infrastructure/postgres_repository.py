# etl_service/infrastructure/postgres_repository.py

import logging
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from domain.repositories_interfaces import PostgresRepositoryInterface
from domain.entities import Database
from typing import List
import pandas as pd

class PostgresRepository(PostgresRepositoryInterface):
    def __init__(self, database: Database):
        self.database = database
        self.engine = None  # Inicialmente None
        self.create_database_if_not_exists()
        self._connect_to_database()

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

    def insert_table(self, df: pd.DataFrame, table_name: str):
        """Inserta un DataFrame en PostgreSQL."""
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            logging.info(f"Datos insertados en la tabla '{table_name}' de PostgreSQL.")
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
