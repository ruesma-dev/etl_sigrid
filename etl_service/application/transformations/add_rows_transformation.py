# etl_service/application/transformations/add_rows_transformation.py

import logging
import pandas as pd
from etl_service.application.transformations.base_transformation import BaseTransformation


class AddRowsTransformation(BaseTransformation):
    def __init__(self, rows_to_add: list):
        """
        :param rows_to_add: Lista de diccionarios que representan las filas a añadir.
        """
        self.rows_to_add = rows_to_add

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Añade filas al DataFrame.

        :param df: DataFrame original.
        :return: DataFrame con las filas añadidas.
        """
        try:
            if self.rows_to_add:
                new_rows_df = pd.DataFrame(self.rows_to_add)
                df = pd.concat([df, new_rows_df], ignore_index=True)
                logging.info(f"Añadidas {len(self.rows_to_add)} filas.")
            return df
        except Exception as e:
            logging.error(f"Error al añadir filas: {e}")
            raise
