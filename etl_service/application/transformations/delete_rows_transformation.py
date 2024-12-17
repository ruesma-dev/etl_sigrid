# etl_service/application/transformations/delete_rows_transformation.py

import logging
import pandas as pd
from etl_service.application.transformations.base_transformation import BaseTransformation


class DeleteRowsTransformation(BaseTransformation):
    def __init__(self, criteria: list):
        """
        :param criteria: Lista de diccionarios que representan las condiciones para eliminar filas.
                         Cada diccionario representa una condición (e.g., {"ide": 0}).
        """
        self.criteria = criteria

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas del DataFrame basándose en las condiciones proporcionadas.

        :param df: DataFrame original.
        :return: DataFrame con las filas eliminadas.
        """
        try:
            initial_count = len(df)
            for condition in self.criteria:
                for column, value in condition.items():
                    df = df[df[column] != value]
                    logging.info(f"Eliminadas filas donde {column} == {value}.")
            final_count = len(df)
            deleted_count = initial_count - final_count
            logging.info(f"Total de filas eliminadas: {deleted_count}.")
            return df
        except Exception as e:
            logging.error(f"Error al eliminar filas: {e}")
            raise
