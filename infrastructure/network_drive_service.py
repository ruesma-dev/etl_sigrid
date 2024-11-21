# infrastructure/network_drive_service.py

import subprocess
import logging
from infrastructure.config import Config


class NetworkDriveService:
    def __init__(self):
        self.network_path = r'\\192.168.14.243\tecnologia'

    def map_network_drive(self):
        """
        Mapea la unidad de red para acceder al archivo ZIP.
        Si existe una conexión previa, intenta desconectarla.
        """
        disconnect_cmd = ['net', 'use', self.network_path, '/delete']
        try:
            subprocess.run(disconnect_cmd, check=True, shell=False)
            logging.info("Conexión existente eliminada.")
        except subprocess.CalledProcessError as e:
            if '2250' in str(e):
                logging.info("No había conexión previa para desmapear.")
            else:
                logging.warning(f"No se pudo eliminar la conexión existente: {e}")

        connect_cmd = [
            'net', 'use', self.network_path,
            f'/user:{Config.NETWORK_SHARE_USER}', Config.NETWORK_SHARE_PASSWORD
        ]
        try:
            subprocess.run(connect_cmd, check=True, shell=False)
            logging.info("Unidad de red mapeada correctamente.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error al mapear la unidad de red: {e}")
            raise

    def unmap_network_drive(self):
        """
        Desmapea la unidad de red.
        """
        cmd = ['net', 'use', self.network_path, '/delete']
        try:
            subprocess.run(cmd, check=True, shell=False)
            logging.info("Unidad de red desmapeada correctamente.")
        except subprocess.CalledProcessError as e:
            logging.warning(f"Error al desmapear la unidad de red: {e}")
