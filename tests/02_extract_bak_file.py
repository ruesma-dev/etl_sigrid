# 02_extract_bak_file.py

import os
import pytest
from application.extract_bak_use_case import ExtractBakUseCase
from infrastructure.database_utilities import DatabaseUtilities

@pytest.fixture
def mock_environment(mocker):
    """
    Configura los mocks necesarios para simular el entorno
    """
    # Mock para mapear y desmapear la unidad de red
    mocker.patch.object(DatabaseUtilities, "map_network_drive")
    mocker.patch.object(DatabaseUtilities, "unmap_network_drive")

    # Mock para obtener el último archivo ZIP
    mocker.patch.object(DatabaseUtilities, "get_latest_zip_file", return_value="test.zip")

    # Mock para extraer archivos .bak
    mocker.patch.object(DatabaseUtilities, "extract_bak_from_zip", return_value=["test.bak"])

    # Mock para mover archivos extraídos
    mocker.patch("os.rename")

    # Mock para crear directorios
    mocker.patch("os.makedirs")

    # Simular la existencia de carpetas
    mocker.patch("os.path.exists", side_effect=lambda path: "database" in path)

@pytest.mark.network
def test_extract_bak_use_case(mock_environment):
    """
    Prueba el caso de uso de extracción del archivo .bak
    """
    # Configurar rutas
    shared_folder = r'\\192.168.14.243\tecnologia'
    local_folder = "test_database"

    # Instanciar el caso de uso
    use_case = ExtractBakUseCase(shared_folder, local_folder)

    # Ejecutar el caso de uso
    use_case.execute()

    # Verificar que las funciones de infraestructura fueron llamadas
    DatabaseUtilities.map_network_drive.assert_called_once()
    DatabaseUtilities.unmap_network_drive.assert_called_once()
    DatabaseUtilities.get_latest_zip_file.assert_called_once()
    DatabaseUtilities.extract_bak_from_zip.assert_called_once_with("test.zip")
    os.rename.assert_called_once_with("test.bak", os.path.join(local_folder, "test.bak"))

    print("Test del caso de uso 'ExtractBakUseCase' completado con éxito.")
