# etl_service/application/extract_use_case.py

from etl_service.domain.repositories_interfaces import SQLServerRepositoryInterface
from typing import List
import pandas as pd
import logging

class ExtractUseCase:
    def __init__(self, sql_server_repo: SQLServerRepositoryInterface):
        self.sql_server_repo = sql_server_repo

    def execute(self, tables_to_extract: List[str]) -> dict:
        extracted_data = {}
        skipped_tables = []
        try:
            if not tables_to_extract:
                # logging.info("No se proporcionaron tablas específicas; extrayendo todas las tablas.")
                tables_to_extract = self.sql_server_repo.get_table_names()
            for table_name in tables_to_extract:
                # logging.info(f"Verificando si la tabla '{table_name}' tiene datos.")
                row_count = self.sql_server_repo.get_table_row_count(table_name)
                if row_count == 0:
                    # logging.info(f"La tabla '{table_name}' está vacía y no se cargará.")
                    skipped_tables.append(table_name)
                    continue
                # logging.info(f"Extrayendo tabla '{table_name}' de SQL Server.")
                df = self.sql_server_repo.read_table(table_name)
                extracted_data[table_name] = df
                # logging.info(f"Tabla '{table_name}' extraída con {len(df)} registros.")
            if skipped_tables:
                logging.info(f"Las siguientes tablas estaban vacías y se omitieron: {skipped_tables}")
            return extracted_data
        except Exception as e:
            logging.error(f"Error durante la extracción: {e}")
            raise
