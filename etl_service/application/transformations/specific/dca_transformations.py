# etl_service/application/transformations/specific/dca_transformations.py

import logging
import pandas as pd
from typing import List
from etl_service.domain.interfaces import SpecificTableTransformations
from etl_service.domain.repositories_interfaces import BaseTransformation
from etl_service.infrastructure.memory_repos import CtrMemoryRepo, ConMemoryRepo, ObrMemoryRepo

class DcaCustomTransformation(BaseTransformation):
    def __init__(self, ctr_repo, con_repo, obr_repo):
        """
        Recibe repositorios o servicios que permitan acceder a datos de ctr, con y obr.
        """
        self.ctr_repo = ctr_repo
        self.con_repo = con_repo
        self.obr_repo = obr_repo

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina las filas donde 'ctride', 'entide' y 'obride' son 0, "0" o nulos.

        :param df: DataFrame de entrada.
        :return: DataFrame después de eliminar las filas especificadas.
        """
        try:
            # Definir la condición: 'ctride', 'entide' y 'obride' son 0, "0" o nulos
            condition = (
                    (df['ctride'].isna() | (df['ctride'] == 0) | (df['ctride'] == "0")) &
                    (df['entide'].isna() | (df['entide'] == 0) | (df['entide'] == "0")) &
                    (df['obride'].isna() | (df['obride'] == 0) | (df['obride'] == "0"))
            )

            # Imprimir 'ide' de las filas a eliminar para depuración
            ids_to_remove = df.loc[condition, 'ide']
            print("IDs a eliminar:", ids_to_remove.tolist())

            # Contar filas antes de la eliminación
            initial_count = len(df)

            # Eliminar las filas que cumplen la condición
            df = df.loc[~condition].copy()

            # Contar filas después de la eliminación
            final_count = len(df)
            deleted_count = initial_count - final_count

            logging.info(f"Filas eliminadas: {deleted_count}")

            return df
        except KeyError as e:
            logging.error(f"Columna faltante en DataFrame: {e}")
            raise
        except Exception as e:
            logging.error(f"Error durante la transformación de la tabla 'dca': {e}")
            raise

class DcaTransformations(SpecificTableTransformations):
    def __init__(self, extract_use_case):
        self.extract_use_case = extract_use_case
        self.ctr_repo = None
        self.con_repo = None
        self.obr_repo = None

    def _ensure_dependencies_loaded(self):
        if self.ctr_repo is None or self.con_repo is None or self.obr_repo is None:
            needed_tables = ['ctr', 'con', 'obr']
            extracted_data = {}
            for t in needed_tables:
                logging.info(f"Cargando tabla '{t}' para dependencias de DCA...")
                df_map = self.extract_use_case.execute([t])
                if df_map and t in df_map:
                    extracted_data[t] = df_map[t]
                    # logging.info(f"Tabla '{t}' cargada con {len(df_map[t])} registros para dependencias de DCA.")
                else:
                    logging.warning(f"No se pudo cargar la tabla '{t}' para dependencias de DCA. Podría haber errores.")

            ctr_df = extracted_data.get('ctr')
            con_df = extracted_data.get('con')
            obr_df = extracted_data.get('obr')

            self.ctr_repo = CtrMemoryRepo(ctr_df) if ctr_df is not None else None
            self.con_repo = ConMemoryRepo(con_df) if con_df is not None else None
            self.obr_repo = ObrMemoryRepo(obr_df) if obr_df is not None else None

    def get_table_transformations(self) -> List[BaseTransformation]:
        self._ensure_dependencies_loaded()
        return [DcaCustomTransformation(self.ctr_repo, self.con_repo, self.obr_repo)]
