# infrastructure/sql_server_repository.py

import urllib
import logging
from sqlalchemy import create_engine, inspect, text  # Asegúrate de importar text
from sqlalchemy.engine import Engine
from domain.repositories_interfaces import SQLServerRepositoryInterface
from domain.entities import Database
import pandas as pd
from typing import List
from sqlalchemy.exc import SQLAlchemyError
import subprocess
import os
import re

class SQLServerRepository(SQLServerRepositoryInterface):
    def __init__(self, database: Database):
        self.database = database

        # Construir la cadena de conexión para la base de datos principal (TemporaryDB)
        if self.database.user and self.database.password:
            # Autenticación SQL
            connection_string = (
                f"Driver={{{self.database.driver}}};"
                f"Server={self.database.host},{self.database.port};"
                f"Database={self.database.name};"
                f"UID={self.database.user};"
                f"PWD={self.database.password};"
            )
        else:
            # Autenticación de Windows
            connection_string = (
                f"Driver={{{self.database.driver}}};"
                f"Server={self.database.host},{self.database.port};"
                f"Database={self.database.name};"
                f"Trusted_Connection=yes;"
            )

        # Imprimir la cadena de conexión (sin exponer contraseñas)
        if self.database.user and self.database.password:
            masked_connection_string = (
                f"Driver={self.database.driver};"
                f"Server={self.database.host},{self.database.port};"
                f"Database={self.database.name};"
                f"UID={self.database.user};"
                f"PWD=***"
            )
        else:
            masked_connection_string = (
                f"Driver={self.database.driver};"
                f"Server={self.database.host},{self.database.port};"
                f"Database={self.database.name};"
                f"Trusted_Connection=yes;"
            )
        print(f"Cadena de conexión principal: {masked_connection_string}")

        # Codificar la cadena de conexión para TemporaryDB
        params = urllib.parse.quote_plus(connection_string)
        connection_url = f"mssql+pyodbc:///?odbc_connect={params}"

        try:
            self.engine: Engine = create_engine(connection_url, echo=True)  # echo=True para depuración
            logging.info("Motor de conexión a SQL Server para la base de datos principal creado exitosamente.")
        except Exception as e:
            logging.error(f"Error al crear el motor de SQL Server para la base de datos principal: {e}")
            raise

        # Construir la cadena de conexión para la base de datos 'master' (admin)
        if self.database.user and self.database.password:
            # Autenticación SQL
            admin_connection_string = (
                f"Driver={{{self.database.driver}}};"
                f"Server={self.database.host},{self.database.port};"
                f"Database=master;"
                f"UID={self.database.user};"
                f"PWD={self.database.password};"
            )
        else:
            # Autenticación de Windows
            admin_connection_string = (
                f"Driver={{{self.database.driver}}};"
                f"Server={self.database.host},{self.database.port};"
                f"Database=master;"
                f"Trusted_Connection=yes;"
            )

        # Imprimir la cadena de conexión de 'master' (sin exponer contraseñas)
        if self.database.user and self.database.password:
            masked_admin_connection_string = (
                f"Driver={self.database.driver};"
                f"Server={self.database.host},{self.database.port};"
                f"Database=master;"
                f"UID={self.database.user};"
                f"PWD=***"
            )
        else:
            masked_admin_connection_string = (
                f"Driver={self.database.driver};"
                f"Server={self.database.host},{self.database.port};"
                f"Database=master;"
                f"Trusted_Connection=yes;"
            )
        print(f"Cadena de conexión administrativa: {masked_admin_connection_string}")

        # Codificar la cadena de conexión para 'master'
        admin_params = urllib.parse.quote_plus(admin_connection_string)
        admin_connection_url = f"mssql+pyodbc:///?odbc_connect={admin_params}"

        try:
            self.admin_engine: Engine = create_engine(admin_connection_url, echo=True)  # echo=True para depuración
            logging.info("Motor de conexión a SQL Server para 'master' creado exitosamente.")
        except Exception as e:
            logging.error(f"Error al crear el motor de SQL Server para 'master': {e}")
            raise

    def get_table_names(self) -> List[str]:
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logging.info(f"Tablas encontradas en SQL Server: {tables}")
            return tables
        except SQLAlchemyError as e:
            logging.warning(f"Error al obtener nombres de tablas: {e}")
            raise

    def read_table(self, table_name: str, columns: List[str] = None) -> pd.DataFrame:
        try:
            logging.info(f"Intentando leer la tabla '{table_name}' de la base de datos '{self.database.name}'...")
            df = pd.read_sql_table(table_name, self.engine, columns=columns)
            logging.info(f"Tabla '{table_name}' leída exitosamente con {len(df)} registros.")
            return df
        except SQLAlchemyError as e:
            logging.warning(f"Error al leer la tabla '{table_name}': {e}")
            raise
        except Exception as e:
            logging.error(f"Error al leer la tabla '{table_name}': {e}")
            raise

    def delete_database(self):
        """
        Elimina la base de datos si ya existe.
        """
        try:
            logging.info(f"Eliminando la base de datos '{self.database.name}' si existe...")
            delete_cmd = f"DROP DATABASE {self.database.name}"
            cmd = [
                'sqlcmd',
                '-S', self.database.host,
                '-E',
                '-Q', delete_cmd
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            if result.returncode == 0:
                logging.info(f"La base de datos '{self.database.name}' se eliminó correctamente.")
            else:
                logging.warning(f"Error al eliminar la base de datos: {result.stderr}")
        except Exception as e:
            logging.error(f"Error durante la eliminación de la base de datos: {e}")

    def restore_database(self, bak_file_path: str):
        """
        Restaura la base de datos desde un archivo .bak.
        """
        try:
            logging.info(f"Iniciando restauración de la base de datos '{self.database.name}' desde '{bak_file_path}'...")
            # Obtener los nombres lógicos del archivo .bak
            filelist_cmd = f"RESTORE FILELISTONLY FROM DISK = '{bak_file_path}'"
            cmd = [
                'sqlcmd',
                '-S', self.database.host,
                '-E',
                '-d', 'master',  # Conectarse a master
                '-Q', filelist_cmd
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=True)
            filelist_output = result.stdout
            logging.info("Salida de FILELISTONLY obtenida correctamente:")
            logging.info(filelist_output)

            # Parsear los nombres lógicos
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

            # Definir rutas de archivos restaurados
            data_path = os.path.join(self.database.data_path, f"{self.database.name}.mdf")
            log_path = os.path.join(self.database.log_path, f"{self.database.name}_Log.ldf")

            # Preparar comando de restauración
            restore_cmd = f"""
            RESTORE DATABASE [{self.database.name}]
            FROM DISK = '{bak_file_path}'
            WITH 
            MOVE '{logical_names["data"]}' TO '{data_path}',
            MOVE '{logical_names["log"]}' TO '{log_path}',
            REPLACE, STATS = 10;
            """
            logging.info("Comando RESTORE generado:")
            logging.info(restore_cmd)

            temp_sql_file = os.path.join(self.database.data_path, "restore_command.sql")
            with open(temp_sql_file, "w") as f:
                f.write(restore_cmd)

            restore_cmd_exec = [
                'sqlcmd',
                '-S', self.database.host,
                '-E',
                '-d', 'master',  # Conectarse a master
                '-i', temp_sql_file
            ]

            logging.info(f"Iniciando restauración de la base de datos como '{self.database.name}'...")
            restore_result = subprocess.run(restore_cmd_exec, capture_output=True, text=True, shell=True)
            logging.info(f"Salida del comando de restauración:")
            logging.info(restore_result.stdout)

            if restore_result.returncode != 0:
                logging.error(f"Error durante la restauración: {restore_result.stderr}")
                raise Exception("Error durante la restauración de la base de datos.")

            logging.info(f"La base de datos '{self.database.name}' se restauró correctamente.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al restaurar la base de datos: {e}")
            raise
        except Exception as e:
            logging.error(f"Error inesperado durante la restauración: {e}")
            raise

    def reconnect(self):
        """
        Reconecta a la base de datos principal después de realizar operaciones administrativas.
        """
        try:
            self.engine.dispose()
            logging.info(f"Motor de conexión a SQL Server para '{self.database.name}' eliminado.")

            # Reconstruir la cadena de conexión siguiendo el formato que funciona
            connection_url = f"mssql+pyodbc://localhost/{self.database.name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

            # Imprimir la cadena de conexión reconectada (sin exponer contraseñas)
            masked_connection_url = f"mssql+pyodbc://localhost/{self.database.name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            print(f"Cadena de conexión reconectada: {masked_connection_url}")

            # Reconectar usando la cadena de conexión URL
            self.engine = create_engine(connection_url, echo=True)
            logging.info(f"Motor de conexión a SQL Server para '{self.database.name}' reconectado exitosamente.")

            # Verificar que la base de datos está en línea
            self.verify_database_online()

        except Exception as e:
            logging.error(f"Error al reconectar el motor de SQL Server: {e}")
            raise

    def verify_database_online(self):
        """
        Verifica que la base de datos esté en estado ONLINE.
        """
        try:
            query = f"SELECT state_desc FROM sys.databases WHERE name = '{self.database.name}'"
            with self.engine.connect() as connection:
                result = connection.execute(text(query))  # Usar text() aquí
                state = result.fetchone()[0]
                if state != "ONLINE":
                    raise Exception(f"La base de datos '{self.database.name}' no está ONLINE. Estado actual: {state}")
                logging.info(f"La base de datos '{self.database.name}' está en estado ONLINE.")
        except Exception as e:
            logging.error(f"Error al verificar el estado de la base de datos: {e}")
            raise

    def close_connection(self):
        try:
            self.engine.dispose()
            self.admin_engine.dispose()
            logging.info("Conexión a SQL Server cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a SQL Server: {e}")
