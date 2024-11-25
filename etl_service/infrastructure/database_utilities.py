# infrastructure/database_utilities.py

import os
import logging


class DatabaseUtilities:
    @staticmethod
    def validate_bak_file(bak_file_path: str) -> bool:
        """
        Valida que el archivo .bak exista.
        """
        exists = os.path.exists(bak_file_path)
        if exists:
            logging.info(f"El archivo .bak existe: {bak_file_path}")
        else:
            logging.error(f"El archivo .bak no existe: {bak_file_path}")
        return exists
