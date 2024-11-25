import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from domain.repositories_interfaces import PostgresRepositoryInterface
from domain.entities import Database
from typing import List
from sqlalchemy.exc import SQLAlchemyError

class PostgresRepository(PostgresRepositoryInterface):
    def __init__(self, database: Database):
        self.database = database

        # Construir la cadena de conexión para PostgreSQL
        connection_string = (
            f"postgresql+psycopg2://{self.database.user}:{self.database.password}@{self.database.host}:{self.database.port}/{self.database.name}"
        )

        # Crear la conexión a la base de datos PostgreSQL
        try:
            self.engine: Engine = create_engine(connection_string)
            logging.info("Motor de conexión a PostgreSQL creado exitosamente.")
        except Exception as e:
            logging.error(f"Error al crear el motor de conexión: {e}")
            raise

    def get_table_names(self) -> List[str]:
        """Obtiene los nombres de las tablas de la base de datos PostgreSQL"""
        try:
            # Usar SQLAlchemy's inspector para obtener los nombres de las tablas
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()  # Obtenemos los nombres de las tablas
            logging.info(f"Tablas encontradas en PostgreSQL: {tables}")
            return tables
        except SQLAlchemyError as e:
            logging.error(f"Error al obtener nombres de tablas: {e}")
            raise

    def read_table(self, table_name: str, columns: List[str] = None):
        """Lee los datos de una tabla específica"""
        try:
            import pandas as pd
            logging.info(f"Intentando leer la tabla '{table_name}' de PostgreSQL...")
            df = pd.read_sql_table(table_name, self.engine, columns=columns)
            logging.info(f"Tabla '{table_name}' leída exitosamente con {len(df)} registros.")
            return df
        except SQLAlchemyError as e:
            logging.warning(f"Error al leer la tabla '{table_name}': {e}")
            raise
        except Exception as e:
            logging.error(f"Error al leer la tabla '{table_name}': {e}")
            raise

    def close_connection(self):
        """Cierra la conexión con PostgreSQL"""
        try:
            self.engine.dispose()  # Cierra la conexión a la base de datos
            logging.info("Conexión a PostgreSQL cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a PostgreSQL: {e}")
            raise
