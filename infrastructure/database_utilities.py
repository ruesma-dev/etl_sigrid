# infrastructure/database_utilities.py

import logging
import subprocess
from infrastructure.config import Config  # Importar solo la clase Config
import os
import glob
import zipfile


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DatabaseUtilities:
    @staticmethod
    def map_network_drive():
        """
        Mapea la unidad de red para acceder al archivo ZIP.
        Si existe una conexión previa, intenta desconectarla.
        """
        network_path = r'\\192.168.14.243\tecnologia'
        disconnect_cmd = ['net', 'use', network_path, '/delete']
        try:
            subprocess.run(disconnect_cmd, check=True, shell=False)
            logging.info("Conexión existente eliminada.")
        except subprocess.CalledProcessError as e:
            if '2250' in str(e):
                logging.info("No había conexión previa para desmapear.")
            else:
                logging.warning(f"No se pudo eliminar la conexión existente: {e}")

        connect_cmd = [
            'net', 'use', network_path,
            f'/user:{Config.NETWORK_SHARE_USER}', Config.NETWORK_SHARE_PASSWORD
        ]
        try:
            subprocess.run(connect_cmd, check=True, shell=False)
            logging.info("Unidad de red mapeada correctamente.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al mapear la unidad de red: {e}")
            raise

    @staticmethod
    def unmap_network_drive():
        """
        Desmapea la unidad de red.
        """
        network_path = r'\\192.168.14.243\tecnologia'
        cmd = ['net', 'use', network_path, '/delete']
        try:
            subprocess.run(cmd, check=True, shell=False)
            logging.info("Unidad de red desmapeada correctamente.")
        except subprocess.CalledProcessError as e:
            logging.warning(f"Error al desmapear la unidad de red: {e}")

    @staticmethod
    def get_latest_zip_file():
        """
        Encuentra el archivo ZIP más reciente en la unidad de red.
        """
        zip_files = glob.glob(r'\\192.168.14.243\tecnologia\*.zip')
        if not zip_files:
            raise FileNotFoundError("No se encontraron archivos ZIP en la unidad de red.")
        latest_file = max(zip_files, key=os.path.getmtime)
        logging.info(f"Archivo ZIP más reciente encontrado: {latest_file}")
        return latest_file

    @staticmethod
    def extract_bak_from_zip(zip_file_path):
        """
        Extrae el archivo .bak del archivo ZIP especificado.
        """
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            bak_files = [f for f in zip_ref.namelist() if f.lower().endswith('.bak')]
            if not bak_files:
                raise FileNotFoundError("No se encontraron archivos .bak en el archivo ZIP.")
            zip_ref.extractall(members=bak_files)
            logging.info(f"Archivos .bak extraídos: {bak_files}")
            extracted_bak_files = [os.path.abspath(f) for f in bak_files]
            return extracted_bak_files

    @staticmethod
    def restore_database_from_bak(bak_file_path):
        """
        Restaura la base de datos SQL Server a partir de un archivo .bak.
        """
        if not os.path.exists(bak_file_path):
            raise FileNotFoundError(f"El archivo .bak no existe en la ruta especificada: {bak_file_path}")

        restore_script = f"""
        RESTORE DATABASE [{Config.SQL_DATABASE}] 
        FROM DISK = N'{bak_file_path}' 
        WITH FILE = 1, NOUNLOAD, REPLACE, STATS = 5;
        """

        cmd = [
            'sqlcmd',
            '-S', Config.SQL_SERVER,
            '-E',  # Usar autenticación de Windows
            '-Q', restore_script
        ]

        try:
            subprocess.run(cmd, check=True, shell=False)
            logging.info(f"Base de datos {Config.SQL_DATABASE} restaurada correctamente.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al restaurar la base de datos: {e}")
            raise

    @staticmethod
    def delete_bak_files(bak_files):
        """
        Elimina los archivos .bak especificados.
        """
        for bak_file in bak_files:
            try:
                os.remove(bak_file)
                logging.info(f"Archivo .bak eliminado: {bak_file}")
            except Exception as e:
                logging.error(f"Error al eliminar el archivo .bak {bak_file}: {e}")

    @staticmethod
    def clean_up(bak_files):
        """
        Limpia los archivos .bak y desmapea la unidad de red.
        """
        if bak_files:
            DatabaseUtilities.delete_bak_files(bak_files)
        DatabaseUtilities.unmap_network_drive()

    @staticmethod
    def delete_sql_server_database():
        """
        Elimina la base de datos temporal de SQL Server.
        """
        delete_script = (
            f"ALTER DATABASE [{Config.SQL_DATABASE}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            f"DROP DATABASE [{Config.SQL_DATABASE}];"
        )

        cmd = [
            'sqlcmd',
            '-S', Config.SQL_SERVER,
            '-E',  # Usar autenticación de Windows
            '-Q', delete_script
        ]

        try:
            # Ejecutar el comando sin pasarlo como una sola cadena con comillas
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(f"Base de datos {Config.SQL_DATABASE} eliminada correctamente.")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error al eliminar la base de datos {Config.SQL_DATABASE}: {e}")
            print("Salida estándar:", e.stdout)
            print("Salida de error:", e.stderr)
            raise

    @staticmethod
    def validate_bak_file(bak_file_path):
        """
        Valida que el archivo .bak exista.
        """
        return os.path.exists(bak_file_path)
