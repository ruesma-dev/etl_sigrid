from abc import ABC, abstractmethod
from typing import List
from domain.entities import Database  # Asegúrate de importar la clase Database correctamente
import pandas as pd


class SQLServerRepositoryInterface(ABC):
    @abstractmethod
    def get_table_names(self) -> List[str]:
        pass

    @abstractmethod
    def read_table(self, table_name: str, columns: List[str] = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def close_connection(self):
        pass


class PostgresRepositoryInterface(ABC):
    @abstractmethod
    def get_table_names(self) -> List[str]:
        pass

    @abstractmethod
    def read_table(self, table_name: str, columns: List[str] = None):
        pass

    @abstractmethod
    def close_connection(self):
        pass
