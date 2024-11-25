# restoration_service/application/restore_database_use_case.py

import logging
import subprocess
import os

class RestoreSQLDatabaseUseCase:
    """
    Caso de uso para restaurar una base de datos en SQL Server desde un archivo .bak.
    """

    def __init__(self, sql_server_repo, bak_file_path, database_name="TemporaryDB"):
        self.sql_server_repo = sql_server_repo
        self.bak_file_path = bak_file_path
        self.database_name = database_name
        self.base_path = r"C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\Backup"
        self.subdirectories = ["Data", "Logs"]

    def ensure_directories_exist(self):
        """
        Crea las subcarpetas necesarias si no existen.
        """
        for subdirectory in self.subdirectories:
            dir_path = os.path.join(self.base_path, subdirectory)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                logging.info(f"Carpeta creada: {dir_path}")
            else:
                logging.info(f"Carpeta ya existe: {dir_path}")

    def delete_database(self):
        """
        Elimina la base de datos si ya existe.
        """
        try:
            logging.info(f"Eliminando la base de datos '{self.database_name}' si existe...")
            delete_cmd = f"DROP DATABASE {self.database_name}"
            cmd = [
                'sqlcmd',
                '-S', 'localhost',
                '-E',
                '-Q', delete_cmd
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            if result.returncode == 0:
                logging.info(f"La base de datos '{self.database_name}' se eliminó correctamente.")
            else:
                logging.warning(f"Error al eliminar la base de datos: {result.stderr}")
        except Exception as e:
            logging.error(f"Error durante la eliminación de la base de datos: {e}")

    def parse_logical_names(self, filelist_output):
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

    def verify_database_status(self):
        """
        Verifica si la base de datos está ONLINE en SQL Server.
        """
        try:
            logging.info(f"Verificando si la base de datos '{self.database_name}' está ONLINE...")
            query_cmd = f"SELECT name, state_desc FROM sys.databases WHERE name = '{self.database_name}'"
            cmd = [
                'sqlcmd',
                '-S', 'localhost',
                '-E',
                '-Q', query_cmd
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=True)
            if self.database_name in result.stdout and "ONLINE" in result.stdout:
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Error al verificar el estado de la base de datos: {e}")
            return False

    def execute(self):
        """
        Restaura la base de datos desde el archivo .bak.
        """
        self.ensure_directories_exist()

        data_path = os.path.join(self.base_path, "Data", f"{self.database_name}.mdf")
        log_path = os.path.join(self.base_path, "Logs", f"{self.database_name}_Log.ldf")

        if not os.path.exists(self.bak_file_path):
            logging.error(f"Error: El archivo .bak no existe en la ruta especificada: {self.bak_file_path}")
            return False

        try:
            # Obtener los nombres lógicos del archivo .bak
            logging.info("Obteniendo los nombres lógicos del archivo .bak...")
            filelist_cmd = f"RESTORE FILELISTONLY FROM DISK = '{self.bak_file_path}'"
            cmd = [
                'sqlcmd',
                '-S', 'localhost',
                '-E',
                '-Q', filelist_cmd
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=True)
            filelist_output = result.stdout
            logging.info("Salida de FILELISTONLY obtenida correctamente:")
            logging.info(filelist_output)

            logical_names = self.parse_logical_names(filelist_output)
            logging.info(f"Nombres lógicos detectados: {logical_names}")

            # Preparar comando de restauración
            restore_cmd = f"""
            RESTORE DATABASE [{self.database_name}]
            FROM DISK = '{self.bak_file_path}'
            WITH 
            MOVE '{logical_names["data"]}' TO '{data_path}',
            MOVE '{logical_names["log"]}' TO '{log_path}',
            REPLACE, STATS = 10;
            """
            logging.info("Comando RESTORE generado:")
            logging.info(restore_cmd)

            temp_sql_file = os.path.join(self.base_path, "restore_command.sql")
            with open(temp_sql_file, "w") as f:
                f.write(restore_cmd)

            restore_cmd_exec = [
                'sqlcmd',
                '-S', 'localhost',
                '-E',
                '-i', temp_sql_file
            ]

            logging.info(f"Iniciando restauración de la base de datos como '{self.database_name}'...")
            restore_result = subprocess.run(restore_cmd_exec, capture_output=True, text=True, shell=True)
            logging.info(f"Salida del comando de restauración:")
            logging.info(restore_result.stdout)

            if restore_result.returncode != 0:
                logging.error(f"Error durante la restauración: {restore_result.stderr}")
                return False

            if self.verify_database_status():
                logging.info(f"La base de datos '{self.database_name}' está ONLINE y funcional.")
                return True
            else:
                logging.error(f"Error: La base de datos '{self.database_name}' no se encuentra en la lista de bases de datos.")
                return False

        except subprocess.CalledProcessError as e:
            logging.error(f"Error al restaurar la base de datos: {e}")
            return False