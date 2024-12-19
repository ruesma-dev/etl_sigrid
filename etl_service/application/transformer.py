# etl_service/application/transformer.py

import pandas as pd
import logging
from etl_service.application.transformations.transformation_factory import TransformationFactory


class Transformer:
    @staticmethod
    def transform(table_key: str, df, extract_use_case=None) -> pd.DataFrame:
        """
        Aplica las transformaciones necesarias al DataFrame de la tabla especificada.

        :param table_key: Clave de la tabla.
        :param df: DataFrame a transformar.
        :param extract_use_case: Instancia de ExtractUseCase para cargar dependencias (opcional).
        :return: DataFrame transformado.
        """
        try:
            # Obtener las transformaciones desde el factory, pasando extract_use_case
            transformations = TransformationFactory.get_transformations(table_key, extract_use_case)

            # Aplicar cada transformación en orden
            for transformation in transformations:
                df = transformation.transform(df)

            logging.info(f"Transformaciones aplicadas exitosamente a la tabla '{table_key}'.")
            return df
        except Exception as e:
            logging.error(f"Error durante la transformación de la tabla '{table_key}': {e}")
            raise
