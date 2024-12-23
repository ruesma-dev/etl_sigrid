# etl_service/application/transformations/specific/obrparpre_transformations.py

import logging
import pandas as pd
from typing import List
from etl_service.domain.interfaces import SpecificTableTransformations
from etl_service.domain.repositories_interfaces import BaseTransformation


class ObrParPreCompositeKeyTransformation(BaseTransformation):
    """
    Crea una columna 'composite_key' en la tabla obrparpre que une obride y fas.
    Resultado: 'composite_key' = '{obride}_{fas}'
    """

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            if 'obride' in df.columns and 'fas' in df.columns:
                df['composite_key'] = df.apply(
                    lambda row: f"{row['obride']}_{row['fas']}"
                    if pd.notnull(row['obride']) and pd.notnull(row['fas'])
                    else None,
                    axis=1
                )
                logging.info("Columna 'composite_key' creada exitosamente en la tabla 'obrparpre'.")
            else:
                logging.warning("No se encontraron las columnas 'obride' y/o 'fas' en 'obrparpre'.")
            return df
        except Exception as e:
            logging.error(f"Error en ObrParPreCompositeKeyTransformation: {e}")
            raise


class ObrparpreTransformations(SpecificTableTransformations):
    """
    Transforms específicas para la tabla obrparpre.
    """

    def get_table_transformations(self) -> List[BaseTransformation]:
        return [
            ObrParPreCompositeKeyTransformation()
        ]
