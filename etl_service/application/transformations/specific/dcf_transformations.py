# etl_service/application/transformations/specific/dcf_transformations.py

import logging
import pandas as pd
from typing import List
from etl_service.domain.interfaces import SpecificTableTransformations
from etl_service.domain.repositories_interfaces import BaseTransformation

class DcfJoinDcfproTransformation(BaseTransformation):
    """
    Toma la tabla dcf y le asigna el primer valor no nulo de 'docoricod'
    encontrado en dcfpro (dcf.ide == dcfpro.docide).
    """

    def __init__(self, dcfpro_df: pd.DataFrame):
        """
        :param dcfpro_df: DataFrame de la tabla dcfpro con la que haremos la búsqueda.
        """
        self.dcfpro_df = dcfpro_df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Para cada fila de dcf (df), busca en dcfpro las filas donde docide == dcf.ide,
        y copia el primer 'docoricod' no nulo en la columna 'docoricod_dcfpro' (o el nombre deseado).
        """
        try:
            if self.dcfpro_df is None or self.dcfpro_df.empty:
                logging.warning(
                    "El DataFrame de dcfpro está vacío o es None. "
                    "No se podrá completar la unión para docoricod."
                )
                return df

            # Agrupamos dcfpro por docide y extraemos el primer docoricod no nulo
            #   1) eliminamos filas con docoricod nulo
            #   2) hacemos groupby('docide')
            #   3) tomamos la primera ocurrencia ('first') de docoricod
            #   4) renombramos docide -> ide para facilitar el merge con dcf
            grouped = (
                self.dcfpro_df
                  .dropna(subset=["docoricod"])  # solo las filas con docoricod no nulo
                  .groupby("docide", as_index=False)
                  .agg({"docoricod": "first"})   # primer valor no nulo
                  .rename(columns={"docide": "ide", "docoricod": "docoricod_dcfpro"})
            )

            # Hacemos un merge left:
            #   - df  => la tabla dcf
            #   - grouped => la tabla con docoricod resumido por docide
            merged = df.merge(grouped, how="left", on="ide")

            logging.info(
                "Se ha copiado el primer 'docoricod' de dcfpro como 'docoricod_dcfpro' en la tabla 'dcf'."
            )
            return merged

        except Exception as e:
            logging.error(f"Error en DcfJoinDcfproTransformation: {e}")
            raise


class DcfTransformations(SpecificTableTransformations):
    """
    Transformaciones específicas para la tabla dcf.
    """

    def __init__(self, dcfpro_df: pd.DataFrame):
        """
        :param dcfpro_df: DataFrame con los registros de dcfpro, usado para
                          buscar el primer docoricod no nulo.
        """
        self.dcfpro_df = dcfpro_df

    def get_table_transformations(self) -> List[BaseTransformation]:
        """
        Retorna la lista de transformaciones específicas para la tabla dcf.
        """
        transformations = []
        # Añadimos la transformación que realiza el join y copia docoricod
        transformations.append(DcfJoinDcfproTransformation(self.dcfpro_df))
        return transformations
