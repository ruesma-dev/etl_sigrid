# infrastructure/sql_server_repository.py

import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from domain.repositories_interfaces import SQLServerRepositoryInterface
from domain.entities import Database
import pandas as pd
import subprocess
import logging
from typing import List



class SQLServerRepository(SQLServerRepositoryInterface):
    def __init__(self, database: Database):
        self.database = database
        self.engine: Engine = create_engine(
            f"mssql+pyodbc://@{database.host}:{database.port}/{database.name}"
            f"?driver={database.driver}&trusted_connection=yes"
        )

    def get_table_names(self) -> List[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def read_table(self, table_name: str, columns: List[str] = None) -> pd.DataFrame:
        return pd.read_sql_table(table_name, self.engine, columns=columns)

    def restore_database(self, bak_file_path: str, database: Database) -> bool:
        try:
            # Obtener nombres lógicos desde FILELISTONLY
            filelist_cmd = [
                'sqlcmd',
                '-S', database.host,
                '-E',
                '-Q', f"RESTORE FILELISTONLY FROM DISK = '{bak_file_path}'"
            ]
            result = subprocess.run(filelist_cmd, capture_output=True, text=True, check=True)
            filelist_output = result.stdout
            logging.info("Salida de FILELISTONLY obtenida correctamente.")

            # Parsear nombres lógicos
            logical_names = self.parse_logical_names(filelist_output)
            logging.info(f"Nombres lógicos detectados: {logical_names}")

            # Preparar paths
            data_path = os.path.join(database.data_path, f"{database.name}.mdf")
            log_path = os.path.join(database.log_path, f"{database.name}_Log.ldf")

            # Generar script de restauración
            restore_script = (
                f"RESTORE DATABASE [{database.name}] FROM DISK = '{bak_file_path}' "
                f"WITH MOVE '{logical_names['data']}' TO '{data_path}', "
                f"MOVE '{logical_names['log']}' TO '{log_path}', "
                f"REPLACE, STATS = 10;"
            )

            # Ejecutar restauración
            restore_cmd = [
                'sqlcmd',
                '-S', database.host,
                '-E',
                '-Q', restore_script
            ]
            logging.info("Iniciando restauración de la base de datos...")
            subprocess.run(restore_cmd, check=True)
            logging.info(f"Base de datos '{database.name}' restaurada correctamente.")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al restaurar la base de datos: {e}")
            return False
        except Exception as e:
            logging.error(f"Error inesperado al restaurar la base de datos: {e}")
            return False

    def parse_logical_names(self, filelist_output: str) -> dict:
        """
        Parsear los nombres lógicos desde la salida de RESTORE FILELISTONLY.
        """
        import re
        logical_names = {"data": None, "log": None}
        lines = filelist_output.splitlines()

        for line in lines:
            match = re.match(r"^\s*(\S+)\s+([A-Za-z]:\\.+?\.\w+)\s+([DL])", line)
            if match:
                logical_name, physical_name, file_type = match.groups()
                if file_type == "D":  # Archivo de datos
                    logical_names["data"] = logical_name
                elif file_type == "L":  # Archivo de log
                    logical_names["log"] = logical_name

        if not logical_names["data"] or not logical_names["log"]:
            raise ValueError("No se pudieron determinar los nombres lógicos del archivo .bak.")

        return logical_names

    def delete_database(self, database_name: str) -> bool:
        try:
            delete_script = (
                f"ALTER DATABASE [{database_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE [{database_name}];"
            )
            delete_cmd = [
                'sqlcmd',
                '-S', self.database.host,
                '-E',
                '-Q', delete_script
            ]
            subprocess.run(delete_cmd, check=True)
            logging.info(f"Base de datos '{database_name}' eliminada correctamente.")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al eliminar la base de datos: {e}")
            return False

    def close_connection(self):
        self.engine.dispose()
