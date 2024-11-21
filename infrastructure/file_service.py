# infrastructure/file_service.py

import glob
import os
import zipfile
import tempfile
import logging
from typing import List


class FileService:
    def get_latest_zip_file(self, directory: str) -> str:
        """
        Encuentra el archivo ZIP más reciente en el directorio especificado.
        """
        zip_files = glob.glob(os.path.join(directory, '*.zip'))
        if not zip_files:
            raise FileNotFoundError("No se encontraron archivos ZIP en la unidad de red.")
        latest_file = max(zip_files, key=os.path.getmtime)
        logging.info(f"Archivo ZIP más reciente encontrado: {latest_file}")
        return latest_file

    def extract_bak_from_zip(self, zip_file_path: str, extract_to: str) -> List[str]:
        """
        Extrae el archivo .bak del archivo ZIP especificado.
        """
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            bak_files = [f for f in zip_ref.namelist() if f.lower().endswith('.bak')]
            if not bak_files:
                raise FileNotFoundError(f"No se encontraron archivos .bak en el archivo ZIP {zip_file_path}")
            zip_ref.extractall(path=extract_to, members=bak_files)
            logging.info(f"Archivos .bak extraídos: {bak_files}")
            extracted_bak_files = [os.path.join(extract_to, f) for f in bak_files]
            return extracted_bak_files
