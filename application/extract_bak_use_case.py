# application/extract_bak_use_case.py

from infrastructure.network_drive_service import NetworkDriveService
from infrastructure.file_service import FileService
import logging
from typing import List
import os


class ExtractBakUseCase:
    """
    Caso de uso para extraer el archivo .bak más reciente desde la carpeta compartida.
    """
    def __init__(self, network_service: NetworkDriveService, file_service: FileService, local_folder: str):
        self.network_service = network_service
        self.file_service = file_service
        self.local_folder = local_folder

    def execute(self) -> List[str]:
        """
        Extrae el archivo .bak desde un ZIP en la carpeta compartida y lo mueve a la carpeta local.
        Devuelve la lista de archivos .bak extraídos.
        """
        # Crear carpeta local si no existe
        if not os.path.exists(self.local_folder):
            os.makedirs(self.local_folder)
            logging.info(f"Carpeta creada: {self.local_folder}")

        try:
            # Mapear la unidad de red
            self.network_service.map_network_drive()

            # Encontrar el archivo ZIP más reciente
            latest_zip = self.file_service.get_latest_zip_file(self.network_service.network_path)
            logging.info(f"Último archivo ZIP encontrado: {latest_zip}")

            # Extraer el archivo .bak del ZIP
            bak_files = self.file_service.extract_bak_from_zip(latest_zip, self.local_folder)
            logging.info(f"Archivos .bak extraídos: {bak_files}")

            # Opcional: Eliminar el archivo ZIP si no es necesario
            # os.remove(latest_zip)

            logging.info(f"Todos los archivos .bak se extrajeron correctamente a {self.local_folder}")
            return bak_files

        except Exception as e:
            logging.error(f"Error durante la extracción del archivo .bak: {e}")
            raise
        finally:
            # Desmapear la unidad de red
            self.network_service.unmap_network_drive()
