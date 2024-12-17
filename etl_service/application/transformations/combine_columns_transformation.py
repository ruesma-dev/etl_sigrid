# etl_service/application/transformations/combine_columns_transformation.py

import pandas as pd
import logging
from etl_service.domain.repositories_interfaces import BaseTransformation

class CombineColumnsTransformation(BaseTransformation):
    def __init__(self, new_column_name: str, columns_to_combine: list, separator: str = '_'):
        """
        :param new_column_name: Nombre de la nueva columna a crear.
        :param columns_to_combine: Lista de columnas cuyas valores se combinarán.
        :param separator: Separador entre los valores combinados.
        """
        self.new_column_name = new_column_name
        self.columns_to_combine = columns_to_combine
        self.separator = separator

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Asegurar que las columnas existen
            for col in self.columns_to_combine:
                if col not in df.columns:
                    logging.warning(f"La columna '{col}' no existe en el DataFrame. No se puede combinar.")
                    return df

            # Convertir a string las columnas a combinar y luego unirlas
            df[self.new_column_name] = df[self.columns_to_combine].astype(str).agg(self.separator.join, axis=1)
            logging.info(f"Columna '{self.new_column_name}' creada combinando {self.columns_to_combine}.")
            return df
        except Exception as e:
            logging.error(f"Error en CombineColumnsTransformation: {e}")
            raise
