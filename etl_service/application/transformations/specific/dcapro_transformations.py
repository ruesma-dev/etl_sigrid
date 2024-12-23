# etl_service/application/transformations/specific/dcapro_transformations.py

import logging
import pandas as pd
from typing import List
from etl_service.domain.interfaces import SpecificTableTransformations
from etl_service.domain.repositories_interfaces import BaseTransformation

class DcaproJoinObrparTransformation(BaseTransformation):
    """
    Realiza un left join entre dcapro(paride) y obrparpar(ide),
    añadiendo un sufijo en las columnas de obrparpar.
    """
    def __init__(self, obrparpar_df: pd.DataFrame):
        """
        :param obrparpar_df: DataFrame con la tabla 'obrparpar' (ya cargada en memoria).
        """
        self.obrparpar_df = obrparpar_df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            if self.obrparpar_df is None or self.obrparpar_df.empty:
                logging.warning("No se pudo realizar el join con 'obrparpar' porque obrparpar_df está vacío/None.")
                return df

            if 'paride' not in df.columns:
                logging.warning("No existe la columna 'paride' en dcapro; no se hará el join con obrparpar.")
                return df
            print(list(self.obrparpar_df.columns))
            # Realizamos un merge (left join) con sufijos:
            merged_df = df.merge(
                self.obrparpar_df,
                how='left',
                left_on='paride',
                right_on='ide',
                suffixes=('', '_obrpar')  # Sufijo para las columnas de obrparpar
            )
            print(list(df.columns))
            print(list(merged_df.columns))
            # Opcional: si quieres renombrar todas las columnas que llegaron de obrparpar
            # para que tengan un prefijo "obrpar_":
            #
            # Ejemplo:
            # obrpar_cols = [c for c in self.obrparpar_df.columns if c in merged_df.columns]
            # for col in obrpar_cols:
            #     if col != 'ide':  # si no quieres volver a chocar con 'paride'
            #         merged_df.rename(columns={col: f"obrpar_{col}"}, inplace=True)
            #
            # (Ya no es estrictamente necesario, pues suffixes=('', '_obrpar') se encargó
            #  de todas las colisiones. Pero si deseas un *prefijo*, puedes hacerlo.)

            logging.info("Join (dcapro.paride -> obrparpar.ide) realizado con sufijo '_obrpar'.")
            return merged_df

        except Exception as e:
            logging.error(f"Error en DcaproJoinObrparTransformation: {e}")
            raise


class DcaproTransformations(SpecificTableTransformations):
    """
    Transformaciones específicas para la tabla 'dcapro'.
    """
    def __init__(self, extract_use_case):
        """
        Recibe el extract_use_case para cargar 'obrparpar' desde SQL Server
        en caso de que deseemos hacer el join en memoria.
        """
        self.extract_use_case = extract_use_case
        self.obrparpar_df = None

    def _ensure_dependencies_loaded(self):
        """
        Carga la tabla 'obrparpar' si aún no está en memoria.
        """
        if self.obrparpar_df is None:
            logging.info("Cargando tabla 'obrparpar' para dependencias de dcapro (join)...")
            df_map = self.extract_use_case.execute(['obrparpar'])
            if df_map and 'obrparpar' in df_map:
                self.obrparpar_df = df_map['obrparpar']
                # print(list(self.obrparpar_df.columns))
                logging.info(f"Tabla 'obrparpar' cargada con {len(self.obrparpar_df)} registros.")
            else:
                logging.warning("No se pudo cargar 'obrparpar'. El join quedará incompleto.")

    def get_table_transformations(self) -> List[BaseTransformation]:
        """
        Devuelve la lista de transformaciones específicas para 'dcapro'.
        """
        self._ensure_dependencies_loaded()

        transformations: List[BaseTransformation] = []
        if self.obrparpar_df is not None:
            transformations.append(DcaproJoinObrparTransformation(self.obrparpar_df))

        # Aquí podrías añadir otras transformaciones específicas de dcapro
        # transformations.append(AlgoAdicional(...))

        return transformations
