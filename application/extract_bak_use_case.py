# application/extract_bak_use_case.py

import os
import logging
from infrastructure.database_utilities import DatabaseUtilities

class ExtractBakUseCase:
    """
    Caso de uso para extraer el archivo .bak más reciente desde la carpeta compartida.
    """
    def __init__(self, shared_folder: str, local_folder: str):
        self.shared_folder = shared_folder
        self.local_folder = local_folder  # Ahora se ajusta a la nueva carpeta de destino

    def execute(self):
        """
        Extrae el archivo .bak desde un ZIP en la carpeta compartida y lo mueve a la carpeta local.
        Devuelve el nombre del archivo .bak extraído.
        """
        # Crear carpeta local si no existe
        if not os.path.exists(self.local_folder):
            os.makedirs(self.local_folder)  # Crear carpeta de destino si no existe
            logging.info(f"Carpeta creada: {self.local_folder}")

        try:
            # Mapear la unidad de red
            DatabaseUtilities.map_network_drive()

            # Comprobar si la carpeta compartida es accesible
            if not os.path.exists(self.shared_folder):
                raise FileNotFoundError(f"No se puede acceder a la carpeta compartida en {self.shared_folder}")

            # Encontrar el archivo ZIP más reciente
            latest_zip = DatabaseUtilities.get_latest_zip_file()
            if not latest_zip:
                raise FileNotFoundError("No se encontró ningún archivo ZIP en la carpeta compartida.")
            logging.info(f"Último archivo ZIP encontrado: {latest_zip}")

            # Extraer el archivo .bak del ZIP
            bak_files = DatabaseUtilities.extract_bak_from_zip(latest_zip)
            if not bak_files:
                raise FileNotFoundError(f"No se encontró ningún archivo .bak en el archivo ZIP {latest_zip}")

            # Mover el archivo .bak extraído a la carpeta local
            extracted_bak_files = []
            for bak_file in bak_files:
                bak_name = os.path.basename(bak_file)
                destination = os.path.join(self.local_folder, bak_name)

                # Si el archivo ya existe, lo eliminamos para sobrescribir
                if os.path.exists(destination):
                    logging.warning(f"El archivo ya existe en {destination}. Será sobrescrito.")
                    os.remove(destination)

                os.rename(bak_file, destination)
                logging.info(f"Archivo .bak movido a {destination}")
                extracted_bak_files.append(destination)

            logging.info(f"Todos los archivos .bak se extrajeron correctamente a {self.local_folder}")
            return extracted_bak_files  # Devolver la lista de archivos extraídos

        except Exception as e:
            logging.error(f"Error durante la extracción del archivo .bak: {e}")
            raise
        finally:
            # Desmapear la unidad de red
            DatabaseUtilities.unmap_network_drive()
