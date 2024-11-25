# application/extract_use_case.py

from infrastructure.sql_server_repository import SQLServerRepository

class ExtractUseCase:
    def __init__(self, repository: SQLServerRepository):
        self.repository = repository

    def execute(self, table_name: str):
        return self.repository.read_table(table_name)
