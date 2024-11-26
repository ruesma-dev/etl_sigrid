# etl_service/application/transformations/base_transformation.py

from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformation(ABC):
    """
    Clase abstracta base para todas las transformaciones de datos.
    Todas las transformaciones específicas deben heredar de esta clase e implementar el método `transform`.
    """

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica una transformación al DataFrame proporcionado.

        :param df: DataFrame a transformar.
        :return: DataFrame transformado.
        """
        pass
