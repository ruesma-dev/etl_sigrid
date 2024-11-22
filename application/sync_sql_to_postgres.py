# application/sync_sql_to_postgres.py

import logging
import traceback
from typing import List
import pandas as pd

class SyncSQLToPostgres:
    def __init__(self, sql_server_repo, postgres_repo, tables: List[str]):
        self.sql_server_repo = sql_server_repo
        self.postgres_repo = postgres_repo
        self.tables = tables

    def process_table(self, table_name: str):
        """
        Lee una tabla completa de SQL Server y la guarda en PostgreSQL sin mapeo de columnas.
        """
        logging.info(f"\nProcesando la tabla '{table_name}'...")

        try:
            # Leer toda la tabla de SQL Server
            df = self.sql_server_repo.read_table(table_name)
            logging.info(f"Tabla '{table_name}' leída desde SQL Server con {len(df)} registros.")
            logging.debug(f"Tipos de datos antes de la sincronización:\n{df.dtypes}")

            if df.empty:
                logging.info(f"La tabla '{table_name}' no tiene datos. No se transferirá.")
                return

            # Guardar el DataFrame en PostgreSQL
            target_table_name = table_name  # Puedes ajustar esto si deseas renombrar tablas
            logging.info(f"Guardando la tabla '{target_table_name}' en PostgreSQL...")
            df.to_sql(target_table_name, self.postgres_repo.engine, if_exists='replace', index=False)
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
            self.postgres_repo.create_database_if_not_exists()

            logging.info('Conexión exitosa a SQL Server y PostgreSQL.')

            # Procesar cada tabla y transferirla
            for table_name in self.tables:
                self.process_table(table_name)

            logging.info("\nSincronización de datos completada correctamente.")

        except Exception as e:
            logging.error(f"Error durante la sincronización de datos: {e}")
            raise
