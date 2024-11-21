# application/restore_sql_database_use_case.py

from domain.repositories_interfaces import SQLServerRepositoryInterface
from domain.entities import Database
import logging


class RestoreSQLDatabaseUseCase:
    """
    Caso de uso para restaurar una base de datos en SQL Server desde un archivo .bak.
    """
    def __init__(self, sql_server_repo: SQLServerRepositoryInterface, bak_file_path: str, database: Database):
        self.sql_server_repo = sql_server_repo
        self.bak_file_path = bak_file_path
        self.database = database

    def execute(self) -> bool:
        """
        Restaura la base de datos desde el archivo .bak.
        """
        try:
            success = self.sql_server_repo.restore_database(self.bak_file_path, self.database)
            if success:
                logging.info(f"Base de datos '{self.database.name}' restaurada correctamente en SQL Server.")
            else:
                logging.error(f"No se pudo restaurar la base de datos '{self.database.name}'.")
            return success
        except Exception as e:
            logging.error(f"Error durante la restauración de la base de datos: {e}")
            raise
