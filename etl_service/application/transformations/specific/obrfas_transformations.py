# etl_service/application/transformations/specific/obrfas_transformations.py

import logging
import pandas as pd
from typing import List
from etl_service.domain.interfaces import SpecificTableTransformations
from etl_service.domain.repositories_interfaces import BaseTransformation


class ObrFasCompositeKeyTransformation(BaseTransformation):
    """
    Crea una columna 'composite_key' en la tabla obrfas que une obride y fasnum.
    Resultado: 'composite_key' = '{obride}_{fasnum}'
    """

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Verificamos si existen las columnas
            if 'obride' in df.columns and 'fasnum' in df.columns:
                df['composite_key'] = df.apply(
                    lambda row: f"{row['obride']}_{row['fasnum']}"
                    if pd.notnull(row['obride']) and pd.notnull(row['fasnum'])
                    else None,
                    axis=1
                )
                logging.info("Columna 'composite_key' creada exitosamente en la tabla 'obrfas'.")
            else:
                logging.warning("No se encontraron las columnas 'obride' y/o 'fasnum' en 'obrfas'.")
            return df
        except Exception as e:
            logging.error(f"Error en ObrFasCompositeKeyTransformation: {e}")
            raise


class ObrfasTransformations(SpecificTableTransformations):
    """
    Transforms específicas para la tabla obrfas.
    Puedes añadir más transformaciones en la lista si es necesario.
    """

    def get_table_transformations(self) -> List[BaseTransformation]:
        return [
            ObrFasCompositeKeyTransformation()
        ]
