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
            cols_to_drop = []
            for col in df.columns:
                series = df[col]

                # Si la columna es de tipo objeto, intentar manejar bytes y strings
                if pd.api.types.is_object_dtype(series):
                    # Intentar decodificar valores bytes a str
                    def to_str(val):
                        if isinstance(val, bytes):
                            try:
                                return val.decode('utf-8', errors='replace')  # Reemplazar caracteres inválidos
                            except Exception as decode_err:
                                logging.warning(f"No se pudo decodificar un valor en la columna '{col}': {decode_err}")
                                # Si no se puede decodificar, forzar a string
                                return str(val)
                        # Si no es bytes, convertir a string directamente
                        return str(val) if val is not None else val

                    series = series.apply(to_str)
                    df[col] = series

                    # Ahora que la columna es string, podemos aplicar strip()
                    series = series.fillna('').apply(lambda x: x.strip() if isinstance(x, str) else x)
                    df[col] = series

                # Verificar si todos los valores son NULL
                all_null = series.isnull().all()

                # Verificar si todos los valores son 0 (ignorando NULL)
                if pd.api.types.is_numeric_dtype(series):
                    non_null_series = series.dropna()
                    all_zero = (non_null_series == 0).all() if not non_null_series.empty else False
                else:
                    all_zero = False

                # Verificar si todos los valores son cadenas en blanco (ignorando NULL)
                if pd.api.types.is_string_dtype(series):
                    non_null_series = series.dropna()
                    all_blank = (non_null_series == '').all() if not non_null_series.empty else False
                else:
                    all_blank = False

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
