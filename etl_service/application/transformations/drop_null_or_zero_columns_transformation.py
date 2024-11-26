# etl_service/application/transformations/drop_null_or_zero_columns_transformation.py

import pandas as pd
import logging
from etl_service.application.transformations.base_transformation import BaseTransformation

class DropNullOrZeroColumnsTransformation(BaseTransformation):
    def __init__(self):
        """
        Inicializa la transformación para eliminar columnas con todos los valores como NULL, 0 o cadenas en blanco.
        """
        pass

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Identificar columnas donde todos los valores son NULL, 0 o cadenas en blanco
            cols_to_drop = []
            for col in df.columns:
                # Obtener la serie de la columna
                series = df[col]

                # Verificar si todos los valores son NULL
                all_null = series.isnull().all()

                # Verificar si todos los valores son 0 (ignorando NULL)
                if series.dtype in ['int64', 'float64']:
                    all_zero = (series.dropna() == 0).all() if not series.dropna().empty else False
                else:
                    all_zero = False

                # Verificar si todos los valores son cadenas en blanco (ignorando NULL)
                if series.dtype == 'object':
                    all_blank = (series.dropna().str.strip() == '').all() if not series.dropna().empty else False
                else:
                    all_blank = False

                # Si alguna de las condiciones se cumple, marcar la columna para eliminar
                if all_null or all_zero or all_blank:
                    cols_to_drop.append(col)
                    logging.info(f"Columna '{col}' marcada para ser eliminada (todos los valores son NULL, 0 o cadenas en blanco).")

            # Eliminar las columnas identificadas
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
                logging.info(f"Columnas eliminadas: {cols_to_drop}")
            else:
                logging.info("No se encontraron columnas para eliminar.")

            return df
        except Exception as e:
            logging.error(f"Error en DropNullOrZeroColumnsTransformation: {e}")
            raise
