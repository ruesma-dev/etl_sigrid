# test_restore_database_use_case.py

import pytest
from application.restore_sql_database_use_case import RestoreDatabaseUseCase
from infrastructure.database_utilities import DatabaseUtilities
from unittest.mock import patch


def test_restore_database_use_case():
    with patch.object(DatabaseUtilities, 'map_network_drive') as mock_map, \
         patch.object(DatabaseUtilities, 'get_latest_zip_file') as mock_get_zip, \
         patch.object(DatabaseUtilities, 'extract_bak_from_zip') as mock_extract_bak, \
         patch.object(DatabaseUtilities, 'restore_database_from_bak') as mock_restore_db, \
         patch.object(DatabaseUtilities, 'delete_bak_files') as mock_delete_bak, \
         patch.object(DatabaseUtilities, 'unmap_network_drive') as mock_unmap:

        # Configurar los mocks
        mock_get_zip.return_value = r'C:\path\to\latest.zip'
        mock_extract_bak.return_value = [r'C:\path\to\latest.bak']
        mock_restore_db.return_value = None

        # Ejecutar el caso de uso
        use_case = RestoreDatabaseUseCase()
        use_case.execute()

        # Verificar que los métodos fueron llamados
        mock_map.assert_called_once()
        mock_get_zip.assert_called_once()
        mock_extract_bak.assert_called_once_with(mock_get_zip.return_value)
        mock_restore_db.assert_called_once_with(mock_extract_bak.return_value[0])
        mock_delete_bak.assert_called_once_with(mock_extract_bak.return_value)
        mock_unmap.assert_called_once()
