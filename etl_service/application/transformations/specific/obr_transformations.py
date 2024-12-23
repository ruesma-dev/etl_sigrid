# etl_service/application/transformations/specific/obr_transformations.py

import logging
import pandas as pd
from typing import List
from dateutil.relativedelta import relativedelta  # Importamos relativedelta
from etl_service.domain.interfaces import SpecificTableTransformations
from etl_service.domain.repositories_interfaces import BaseTransformation

class ObrDateDifferenceTransformation(BaseTransformation):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Asumiendo que fecinirea y fecfinpre ya son columnas de tipo datetime
            if 'fecinirea' in df.columns and 'fecfinpre' in df.columns:
                # Calcular la diferencia en meses
                df['plazo_ejecucion'] = df.apply(
                    lambda row: relativedelta(row['fecfinpre'], row['fecinirea']).months +
                                (relativedelta(row['fecfinpre'], row['fecinirea']).years * 12)
                                if pd.notnull(row['fecfinpre']) and pd.notnull(row['fecinirea']) else None,
                    axis=1
                )
                logging.info("Columna 'plazo_ejecucion' creada exitosamente en la tabla 'obr'.")
            else:
                logging.warning("No se encontraron las columnas 'fecinirea' y/o 'fecfinpre' en la tabla 'obr'.")

            return df
        except Exception as e:
            logging.error(f"Error en ObrDateDifferenceTransformation: {e}")
            raise

class ObrTransformations(SpecificTableTransformations):
    def __init__(self):
        # Si no se necesitan dependencias externas, no hace falta init con args
        pass

    def get_table_transformations(self) -> List[BaseTransformation]:
        return [ObrDateDifferenceTransformation()]
