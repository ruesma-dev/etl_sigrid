# application/sync_sql_to_postgres.py

import logging
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from infrastructure.config import Config
from typing import List
import pandas as pd
import urllib
import traceback

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class SyncSQLToPostgres:
    def __init__(self, sql_server_config, postgres_config, tables: List[str]):
        self.sql_server_config = sql_server_config
        self.postgres_config = postgres_config
        self.tables = tables

    def create_postgres_database_if_not_exists(self):
        """
        Crea la base de datos PostgreSQL si no existe.
        """
        try:
            # Conectar a la base de datos 'postgres' para crear la nueva base de datos
            con = psycopg2.connect(
                dbname='postgres',
                user=self.postgres_config['user'],
                password=self.postgres_config['password'],
                host=self.postgres_config['host'],
                port=self.postgres_config['port']
            )
            con.autocommit = True
            cur = con.cursor()
            # Verificar si la base de datos existe
            cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.postgres_config['dbname']}'")
            exists = cur.fetchone()
            if not exists:
                cur.execute(f'CREATE DATABASE "{self.postgres_config["dbname"]}";')
                logging.info(f"Base de datos '{self.postgres_config['dbname']}' creada exitosamente en PostgreSQL.")
            else:
                logging.info(f"La base de datos '{self.postgres_config['dbname']}' ya existe en PostgreSQL.")
            cur.close()
            con.close()
        except psycopg2.Error as e:
            logging.error(f"Error al crear la base de datos PostgreSQL: {e}")
            raise

    def create_postgres_engine(self):
        """
        Crea el motor de conexión para PostgreSQL.
        """
        connection_string = (
            f"postgresql://{self.postgres_config['user']}:{self.postgres_config['password']}@"
            f"{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['dbname']}"
        )
        try:
            pg_engine = create_engine(connection_string)
            logging.info("Motor de conexión a PostgreSQL creado exitosamente.")
            return pg_engine
        except SQLAlchemyError as e:
            logging.error(f"Error al crear el motor de PostgreSQL: {e}")
            raise

    def create_sql_engine(self):
        """
        Crea el motor de conexión para SQL Server usando odbc_connect.
        """
        connection_string = (
            f"DRIVER={{{self.sql_server_config['driver']}}};"
            f"SERVER={self.sql_server_config['server']};"
            f"DATABASE={self.sql_server_config['database']};"
            f"Trusted_Connection=yes;"
        )
        params = urllib.parse.quote_plus(connection_string)
        engine_url = f"mssql+pyodbc:///?odbc_connect={params}"
        try:
            sql_engine = create_engine(engine_url)
            logging.info("Motor de conexión a SQL Server creado exitosamente.")
            return sql_engine
        except SQLAlchemyError as e:
            logging.error(f"Error al crear el motor de SQL Server: {e}")
            raise

    def process_table(self, sql_engine, pg_engine, table_name: str):
        """
        Lee una tabla completa de SQL Server y la guarda en PostgreSQL sin mapeo de columnas.
        """
        logging.info(f"\nProcesando la tabla '{table_name}'...")

        try:
            # Leer toda la tabla de SQL Server
            df = pd.read_sql_table(table_name, sql_engine)
            logging.info(f"Tabla '{table_name}' leída desde SQL Server con {len(df)} registros.")
            logging.debug(f"Tipos de datos antes de la sincronización:\n{df.dtypes}")

            if df.empty:
                logging.info(f"La tabla '{table_name}' no tiene datos. No se transferirá.")
                return

            # Guardar el DataFrame en PostgreSQL
            target_table_name = table_name  # Puedes ajustar esto si deseas renombrar tablas
            logging.info(f"Guardando la tabla '{target_table_name}' en PostgreSQL...")
            df.to_sql(target_table_name, pg_engine, if_exists='replace', index=False)
            logging.info(f"Tabla '{target_table_name}' guardada en PostgreSQL con éxito.")

        except Exception as e:
            logging.error(f"Error al transferir la tabla '{table_name}': {e}")
            logging.error("Detalles del error:")
            traceback_str = ''.join(traceback.format_exception(None, e, e.__traceback__))
            logging.error(traceback_str)
            raise

    def execute(self):
        """
        Función principal que ejecuta la transferencia de tablas desde SQL Server a PostgreSQL.
        """
        try:
            # Crear la base de datos PostgreSQL si no existe
            self.create_postgres_database_if_not_exists()

            # Crear motores de conexión
            sql_engine = self.create_sql_engine()
            pg_engine = self.create_postgres_engine()
            logging.info('Conexión exitosa a SQL Server y PostgreSQL.')

            # Procesar cada tabla y transferirla
            for table_name in self.tables:
                self.process_table(sql_engine, pg_engine, table_name)

            # Cerrar las conexiones
            sql_engine.dispose()
            pg_engine.dispose()
            logging.info('\nConexiones cerradas.')

        except Exception as e:
            logging.error(f"Error durante la sincronización: {e}")
            raise
