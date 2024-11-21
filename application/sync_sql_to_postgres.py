# application/sync_sql_to_postgres.py

from domain.repositories_interfaces import SQLServerRepositoryInterface, PostgresRepositoryInterface
from typing import List
import logging
import pandas as pd
import traceback


class SyncSQLToPostgresUseCase:
    def __init__(self, sql_server_repo: SQLServerRepositoryInterface, postgres_repo: PostgresRepositoryInterface, tables: List[str]):
        self.sql_server_repo = sql_server_repo
        self.postgres_repo = postgres_repo
        self.tables = tables

    def execute(self):
        """
        Sincroniza las tablas especificadas desde SQL Server a PostgreSQL.
        """
        try:
            # Crear la base de datos PostgreSQL si no existe
            self.postgres_repo.create_database_if_not_exists(self.postgres_repo.database)

            # Procesar cada tabla y transferirla
            for table_name in self.tables:
                logging.info(f"\nProcesando la tabla '{table_name}'...")

                # Leer la tabla desde SQL Server
                df = self.sql_server_repo.read_table(table_name)
                logging.info(f"Tabla '{table_name}' leída desde SQL Server con {len(df)} registros.")

                if df.empty:
                    logging.info(f"La tabla '{table_name}' no tiene datos. No se transferirá.")
                    continue

                # Escribir la tabla en PostgreSQL
                self.postgres_repo.write_table(df, table_name)
                logging.info(f"Tabla '{table_name}' sincronizada correctamente en PostgreSQL.")

        except Exception as e:
            logging.error(f"Error durante la sincronización de datos: {e}")
            logging.error("Detalles del error:")
            traceback_str = ''.join(traceback.format_exception(None, e, e.__traceback__))
            logging.error(traceback_str)
            raise
