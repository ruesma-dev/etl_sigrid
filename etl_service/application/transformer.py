# etl_service/application/transformer.py

import pandas as pd
import logging
from etl_service.application.transformations.transformation_factory import TransformationFactory

class Transformer:
    @staticmethod
    def transform(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica las transformaciones necesarias al DataFrame de la tabla especificada.
        :param table_name: Nombre de la tabla.
        :param df: DataFrame a transformar.
        :return: DataFrame transformado.
        """
        try:
            # Obtener las transformaciones desde el factory
            transformations = TransformationFactory.get_transformations(table_name)

            # Aplicar cada transformación en orden
            for transformation in transformations:
                df = transformation.transform(df)

            logging.info(f"Transformaciones aplicadas exitosamente a la tabla '{table_name}'.")
            return df
        except Exception as e:
            logging.error(f"Error durante la transformación de la tabla '{table_name}': {e}")
            raise
