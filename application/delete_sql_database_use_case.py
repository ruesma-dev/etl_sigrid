# application/delete_sql_database_use_case.py

from domain.repositories_interfaces import SQLServerRepositoryInterface
import logging


class DeleteSQLDatabaseUseCase:
    def __init__(self, sql_server_repo: SQLServerRepositoryInterface):
        self.sql_server_repo = sql_server_repo

    def execute(self):
        try:
            # Eliminar la base de datos temporal en SQL Server
            success = self.sql_server_repo.delete_database(self.sql_server_repo.database.name)
            if success:
                logging.info("Base de datos temporal eliminada correctamente en SQL Server.")
            else:
                logging.error("No se pudo eliminar la base de datos temporal en SQL Server.")
        except Exception as e:
            logging.error(f"Error al eliminar la base de datos temporal: {e}")
            raise
