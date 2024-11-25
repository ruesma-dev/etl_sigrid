# restoration_service/domain/repositories_interfaces.py

from abc import ABC, abstractmethod
from typing import List
from restoration_service.domain.entities import Database

class SQLServerRepositoryInterface(ABC):
    @abstractmethod
    def restore_database(self, bak_file_path: str, database_name: str) -> bool:
        pass

    @abstractmethod
    def delete_database(self, database_name: str) -> bool:
        pass

    @abstractmethod
    def reconnect(self):
        pass

    @abstractmethod
    def verify_database_online(self, database_name: str) -> bool:
        pass

    @abstractmethod
    def close_connection(self):
        pass
