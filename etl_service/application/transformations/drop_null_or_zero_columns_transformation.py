# etl_service/application/transformations/drop_null_or_zero_columns_transformation.py

import pandas as pd
import logging
from etl_service.application.transformations.base_transformation import BaseTransformation

class DropNullOrZeroColumnsTransformation(BaseTransformation):
    def __init__(self):
        pass

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            cols_to_drop = []
            for col in df.columns:
                series = df[col]

                # Convertir objetos a cadenas decodificadas
                if pd.api.types.is_object_dtype(series):
                    def to_str(val):
                        if isinstance(val, bytes):
                            try:
                                return val.decode('utf-8', errors='replace')
                            except Exception as decode_err:
                                logging.warning(f"No se pudo decodificar valor en '{col}': {decode_err}")
                                return str(val)
                        return str(val) if val is not None else val

                    series = series.apply(to_str)
                    df[col] = series

                    # Aplicar strip si es string
                    # Primero llenar NaN con '' para no causar error
                    series = series.fillna('')
                    # Asegurar que ahora todo es string
                    series = series.astype(str)
                    series = series.apply(lambda x: x.strip() if isinstance(x, str) else x)
                    df[col] = series

                # Verificar valores nulos
                all_null = bool(series.isnull().all())

                # Verificar todos ceros si es numérico
                if pd.api.types.is_numeric_dtype(series):
                    non_null_series = series.dropna()
                    all_zero = bool((non_null_series == 0).all() if not non_null_series.empty else False)
                else:
                    all_zero = False

                # Verificar todos en blanco si es string
                if pd.api.types.is_string_dtype(series):
                    non_null_series = series.dropna()
                    # Convertir a str para evitar problemas
                    non_null_series = non_null_series.astype(str)
                    all_blank = bool((non_null_series == '').all() if not non_null_series.empty else False)
                else:
                    all_blank = False

                # Ahora all_null, all_zero y all_blank deberían ser booleanos puros
                if all_null or all_zero or all_blank:
                    cols_to_drop.append(col)
                    logging.info(f"Columna '{col}' marcada para eliminar (todos NULL, 0 o blanco).")

            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
                logging.info(f"Columnas eliminadas: {cols_to_drop}")
            else:
                logging.info("No se encontraron columnas para eliminar.")

            return df
        except Exception as e:
            logging.error(f"Error en DropNullOrZeroColumnsTransformation: {e}")
            raise
