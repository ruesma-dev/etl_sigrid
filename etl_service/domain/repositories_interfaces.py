# etl_service/domain/repositories_interfaces.py

from abc import ABC, abstractmethod
from typing import List
from domain.entities import Database
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
    def create_database_if_not_exists(self):
        pass

    @abstractmethod
    def insert_table(self, df: pd.DataFrame, table_name: str):
        pass

    @abstractmethod
    def get_table_names(self) -> List[str]:
        pass

    @abstractmethod
    def close_connection(self):
        pass

class BaseTransformation(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass