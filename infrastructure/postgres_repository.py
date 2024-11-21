# infrastructure/postgres_repository.py

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from domain.repositories_interfaces import PostgresRepositoryInterface
from infrastructure import config


class PostgresRepository(PostgresRepositoryInterface):
    def __init__(self):
        self.engine: Engine = create_engine(config.PG_CONNECTION_STRING)

    def write_table(self, df, table_name: str):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

    def close_connection(self):
        self.engine.dispose()
