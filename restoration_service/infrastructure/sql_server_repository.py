# restoration_service/infrastructure/sql_server_repository.py

import urllib
import logging
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from restoration_service.domain.repositories_interfaces import SQLServerRepositoryInterface
from restoration_service.domain.entities import Database
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

    def restore_database(self, bak_file_path: str, database_name: str) -> bool:
        """
        Restaura la base de datos desde el archivo .bak.
        """
        try:
            logging.info(f"Iniciando restauración de la base de datos '{database_name}' desde '{bak_file_path}'...")

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

            logical_names = self.parse_logical_names(filelist_output)
            logging.info(f"Nombres lógicos detectados: {logical_names}")

            # Definir rutas de archivos restaurados
            data_path = os.path.join(self.database.data_path, f"{database_name}.mdf")
            log_path = os.path.join(self.database.log_path, f"{database_name}_Log.ldf")

            # Preparar comando de restauración
            restore_cmd = f"""
            RESTORE DATABASE [{database_name}]
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

            logging.info(f"Iniciando restauración de la base de datos como '{database_name}'...")
            restore_result = subprocess.run(restore_cmd_exec, capture_output=True, text=True, shell=True)
            logging.info(f"Salida del comando de restauración:")
            logging.info(restore_result.stdout)

            if restore_result.returncode != 0:
                logging.error(f"Error durante la restauración: {restore_result.stderr}")
                return False

            # Verificar que la base de datos está ONLINE
            if self.verify_database_online(database_name):
                logging.info(f"La base de datos '{database_name}' está ONLINE y funcional.")
                return True
            else:
                logging.error(f"Error: La base de datos '{database_name}' no se encuentra en la lista de bases de datos.")
                return False

        except subprocess.CalledProcessError as e:
            logging.error(f"Error al restaurar la base de datos: {e}")
            return False
        except Exception as e:
            logging.error(f"Error inesperado durante la restauración: {e}")
            return False

    def delete_database(self, database_name: str) -> bool:
        """
        Elimina la base de datos si ya existe.
        """
        try:
            logging.info(f"Eliminando la base de datos '{database_name}' si existe...")
            delete_cmd = f"DROP DATABASE {database_name}"
            cmd = [
                'sqlcmd',
                '-S', self.database.host,
                '-E',
                '-Q', delete_cmd
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            if result.returncode == 0:
                logging.info(f"La base de datos '{database_name}' se eliminó correctamente.")
                return True
            else:
                logging.warning(f"Error al eliminar la base de datos: {result.stderr}")
                return False
        except Exception as e:
            logging.error(f"Error durante la eliminación de la base de datos: {e}")
            return False

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
            self.verify_database_online(database_name=self.database.name)

        except Exception as e:
            logging.error(f"Error al reconectar el motor de SQL Server: {e}")
            raise

    def verify_database_online(self, database_name: str) -> bool:
        """
        Verifica que la base de datos esté en estado ONLINE.
        """
        try:
            query = f"SELECT state_desc FROM sys.databases WHERE name = '{database_name}'"
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                state = result.fetchone()[0]
                if state != "ONLINE":
                    raise Exception(f"La base de datos '{database_name}' no está ONLINE. Estado actual: {state}")
                logging.info(f"La base de datos '{database_name}' está en estado ONLINE.")
                return True
        except Exception as e:
            logging.error(f"Error al verificar el estado de la base de datos: {e}")
            return False

    def parse_logical_names(self, filelist_output: str) -> dict:
        """
        Parsear los nombres lógicos desde la salida de RESTORE FILELISTONLY.
        """
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

    def close_connection(self):
        try:
            self.engine.dispose()
            self.admin_engine.dispose()
            logging.info("Conexión a SQL Server cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión a SQL Server: {e}")
