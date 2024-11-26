# etl_service/application/transformations/rename_columns_transformation.py

import pandas as pd
import logging
from etl_service.application.transformations.base_transformation import BaseTransformation

class RenameColumnsTransformation(BaseTransformation):
    def __init__(self, rename_mapping: dict):
        """
        :param rename_mapping: Diccionario que mapea nombres de columnas originales a nuevos nombres.
        """
        self.rename_mapping = rename_mapping

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Renombrar las columnas según el mapping proporcionado
            df = df.rename(columns=self.rename_mapping)
            logging.info(f"Columnas renombradas según mapping: {self.rename_mapping}")
            return df
        except Exception as e:
            logging.error(f"Error en RenameColumnsTransformation: {e}")
            raise
