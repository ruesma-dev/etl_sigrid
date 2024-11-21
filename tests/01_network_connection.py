# 01_network_connection.py

import os
import pytest
from infrastructure.database_utilities import DatabaseUtilities
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()


def test_network_share_connection():
    # Mapear la unidad de red
    try:
        DatabaseUtilities.map_network_drive()
        # Especificar la ruta de la carpeta compartida
        shared_folder = r'\\192.168.14.243\tecnologia'
        # Comprobar si podemos acceder a la carpeta
        assert os.path.exists(shared_folder), f"No se puede acceder a la carpeta compartida en {shared_folder}"
        # Listar los archivos en la carpeta
        files_in_folder = os.listdir(shared_folder)
        assert len(files_in_folder) > 0, f"La carpeta compartida en {shared_folder} está vacía."
        print(f"Archivos en la carpeta compartida: {files_in_folder}")
        print("Test de acceso a la carpeta de red pasado.")
    except Exception as e:
        pytest.fail(f"Test de acceso a la carpeta de red fallido: {e}")
    finally:
        # Desmapear la unidad de red
        DatabaseUtilities.unmap_network_drive()
