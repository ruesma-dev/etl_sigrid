# etl_service/application/transformations/date_transformation.py

import pandas as pd
import logging
from etl_service.domain.repositories_interfaces import BaseTransformation


class DateTransformation(BaseTransformation):
    def __init__(self, column_mapping: dict, null_if_zero: bool = True):
        """
        :param column_mapping: Diccionario que mapea nombres de columnas originales a nuevos nombres.
        :param null_if_zero: Si es True, convierte valores 0 a NULL.
        """
        self.column_mapping = column_mapping
        self.null_if_zero = null_if_zero

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Renombrar las columnas
            df = df.rename(columns=self.column_mapping)
            logging.info(f"Columnas renombradas según mapping: {self.column_mapping}")

            # Lista de columnas de fecha después de renombrar
            date_columns = list(self.column_mapping.values())

            for col in date_columns:
                if self.null_if_zero:
                    # Reemplazar 0 con NaN (NULL)
                    df[col] = df[col].replace(0, pd.NA)
                    logging.info(f"Valores 0 en la columna '{col}' reemplazados por NaN.")

                df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d', errors='coerce')

                logging.info(f"Columna '{col}' convertida a datetime con formato '%Y%m%d'.")

            return df
        except Exception as e:
            logging.error(f"Error en DateTransformation: {e}")
            raise