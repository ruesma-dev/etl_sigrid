# application/delete_sql_database_use_case.py

from infrastructure.database_utilities import DatabaseUtilities
import logging

class DeleteSQLDatabaseUseCase:
    def execute(self):
        try:
            # Eliminar la base de datos temporal en SQL Server
            DatabaseUtilities.delete_sql_server_database()
            logging.info("Base de datos temporal eliminada correctamente en SQL Server.")
        except Exception as e:
            logging.error(f"Error al eliminar la base de datos temporal: {e}")
            raise
