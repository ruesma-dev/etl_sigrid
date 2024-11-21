# domain/repositories_interfaces.py

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
    def restore_database(self, bak_file_path: str, database: Database) -> bool:
        pass

    @abstractmethod
    def delete_database(self, database_name: str) -> bool:
        pass

    @abstractmethod
    def close_connection(self):
        pass


class PostgresRepositoryInterface(ABC):
    @abstractmethod
    def write_table(self, df: pd.DataFrame, table_name: str) -> None:
        pass

    @abstractmethod
    def create_database_if_not_exists(self, database: Database) -> None:
        pass

    @abstractmethod
    def close_connection(self):
        pass
