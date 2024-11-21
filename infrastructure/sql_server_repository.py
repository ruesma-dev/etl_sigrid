# infrastructure/sql_server_repository.py

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from domain.repositories_interfaces import SQLServerRepositoryInterface
from infrastructure import config
import pandas as pd
from typing import List


class SQLServerRepository(SQLServerRepositoryInterface):
    def __init__(self):
        self.engine: Engine = create_engine(config.SQL_CONNECTION_STRING)

    def get_table_names(self) -> List[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def read_table(self, table_name: str, columns: List[str] = None):
        return pd.read_sql_table(table_name, self.engine, columns=columns)

    def close_connection(self):
        self.engine.dispose()
