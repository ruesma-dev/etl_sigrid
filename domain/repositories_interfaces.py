# repositories_interfaces.py

from abc import ABC, abstractmethod
from typing import List


class SQLServerRepositoryInterface(ABC):
    @abstractmethod
    def get_table_names(self) -> List[str]:
        pass

    @abstractmethod
    def read_table(self, table_name: str, columns: List[str] = None):
        pass

    @abstractmethod
    def close_connection(self):
        pass


class PostgresRepositoryInterface(ABC):
    @abstractmethod
    def write_table(self, df, table_name: str):
        pass

    @abstractmethod
    def close_connection(self):
        pass
