# etl_service/application/transformations/join_with_con_transformation.py

import pandas as pd
import logging
from etl_service.application.transformations.base_transformation import BaseTransformation

class JoinWithConTransformation(BaseTransformation):
    def __init__(self, con_df: pd.DataFrame, join_column: str):
        """
        :param con_df: DataFrame con la tabla 'con' cargada.
        :param join_column: Nombre de la columna en la tabla actual que se unirá con 'con.ide'.
        """
        self.join_column = join_column

        # Renombrar todas las columnas de con_df con prefijo con_, excepto 'ide' que será 'con_ide'
        renamed_cols = {}
        for c in con_df.columns:
            if c == 'ide':
                renamed_cols[c] = 'con_ide'
            else:
                renamed_cols[c] = f'con_{c}'
        self.con_df = con_df.rename(columns=renamed_cols)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Hacemos el merge (join izquierdo) ahora sobre con_ide
            merged_df = df.merge(self.con_df, how='left', left_on=self.join_column, right_on='con_ide')

            logging.info("Join con tabla 'con' realizado exitosamente.")
            return merged_df
        except Exception as e:
            logging.error(f"Error en JoinWithConTransformation: {e}")
            raise
