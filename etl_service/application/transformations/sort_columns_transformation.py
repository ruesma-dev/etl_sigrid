# etl_service/application/transformations/sort_columns_transformation.py

import pandas as pd
import logging
from etl_service.application.transformations.base_transformation import BaseTransformation

class SortColumnsTransformation(BaseTransformation):
    def __init__(self, ascending: bool = True):
        """
        Inicializa la transformación para ordenar las columnas alfabéticamente.

        :param ascending: Si es True, ordena en orden ascendente. Si es False, descendente.
        """
        self.ascending = ascending

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Ordenar las columnas alfabéticamente
            sorted_columns = sorted(df.columns, reverse=not self.ascending)
            df = df[sorted_columns]
            logging.info(f"Columnas ordenadas alfabéticamente en orden {'ascendente' if self.ascending else 'descendente'}.")
            return df
        except Exception as e:
            logging.error(f"Error en SortColumnsTransformation: {e}")
            raise
