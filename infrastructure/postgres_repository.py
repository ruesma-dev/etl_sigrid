# infrastructure/postgres_repository.py

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from domain.repositories_interfaces import PostgresRepositoryInterface
from domain.entities import Database
import pandas as pd
import logging


class PostgresRepository(PostgresRepositoryInterface):
    def __init__(self, database: Database):
        self.database = database
        self.engine: Engine = create_engine(
            f"postgresql+psycopg2://{database.user}:{database.password}@{database.host}:{database.port}/{database.name}"
        )

    def write_table(self, df: pd.DataFrame, table_name: str) -> None:
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)
        logging.info(f"Tabla '{table_name}' escrita en PostgreSQL.")

    def create_database_if_not_exists(self, database: Database) -> None:
        """
        Crea la base de datos PostgreSQL si no existe.
        """
        try:
            # Conectar a la base de datos 'postgres' para crear una nueva base de datos
            admin_engine = create_engine(
                f"postgresql+psycopg2://{database.user}:{database.password}@{database.host}:{database.port}/postgres"
            )
            with admin_engine.connect() as conn:
                # Configurar la conexión para autocommit
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                # Ejecutar el comando CREATE DATABASE
                conn.execute(text(f"CREATE DATABASE {database.name};"))
                logging.info(f"Base de datos '{database.name}' creada en PostgreSQL.")
        except ProgrammingError as e:
            # Acceder al código de error SQLSTATE desde la excepción original
            pgcode = getattr(e.orig, 'pgcode', None)
            if pgcode == '42P04':
                # Código SQLSTATE para 'DuplicateDatabase'
                logging.info(f"La base de datos '{database.name}' ya existe en PostgreSQL.")
            else:
                logging.error(f"Error al crear la base de datos PostgreSQL: {e}")
                raise
        finally:
            admin_engine.dispose()

    def close_connection(self):
        self.engine.dispose()
