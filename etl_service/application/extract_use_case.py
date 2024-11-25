# etl_service/application/extract_use_case.py

from domain.repositories_interfaces import SQLServerRepositoryInterface
from typing import List
import pandas as pd
import logging


class ExtractUseCase:
    def __init__(self, sql_server_repo: SQLServerRepositoryInterface):
        self.sql_server_repo = sql_server_repo

    def execute(self, tables_to_extract: List[str] = None) -> dict:
        """Extrae tablas de SQL Server.

        Args:
            tables_to_extract (List[str], opcional): Lista de nombres de tablas a extraer.
                Si es None, extrae todas las tablas.

        Returns:
            dict: Un diccionario donde las claves son los nombres de las tablas y los valores son DataFrames.
        """
        extracted_data = {}
        try:
            if not tables_to_extract:
                logging.info("No se proporcionaron tablas específicas, extrayendo todas las tablas.")
                tables_to_extract = self.sql_server_repo.get_table_names()
            for table_name in tables_to_extract:
                logging.info(f"Extrayendo tabla '{table_name}' de SQL Server.")
                df = self.sql_server_repo.read_table(table_name)
                extracted_data[table_name] = df
            return extracted_data
        except Exception as e:
            logging.error(f"Error durante la extracción: {e}")
            raise
