# etl_service/application/transformations/clean_null_chars_transformation.py

import pandas as pd
import logging
from etl_service.application.transformations.base_transformation import BaseTransformation

class CleanNullCharsTransformation(BaseTransformation):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            for col in df.columns:
                if pd.api.types.is_string_dtype(df[col]):
                    # Reemplazar caracteres NUL '\x00'
                    df[col] = df[col].astype(str).apply(lambda x: x.replace('\x00', '') if x is not None else x)
            logging.info("Caracteres NUL removidos de las columnas tipo texto.")
            return df
        except Exception as e:
            logging.error(f"Error en CleanNullCharsTransformation: {e}")
            raise
