# tests/test_restore_and_copy_use_case.py

import pytest
from unittest.mock import patch, MagicMock
from application.restore_and_copy_use_case import RestoreAndCopyUseCase
from infrastructure.database_utilities import DatabaseUtilities
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository


def test_restore_and_copy_use_case():
    with patch.object(DatabaseUtilities, 'map_network_drive') as mock_map, \
         patch.object(DatabaseUtilities, 'get_latest_zip_file') as mock_get_zip, \
         patch.object(DatabaseUtilities, 'extract_bak_from_zip') as mock_extract_bak, \
         patch.object(DatabaseUtilities, 'restore_database_from_bak') as mock_restore_db, \
         patch.object(DatabaseUtilities, 'delete_bak_files') as mock_delete_bak, \
         patch.object(DatabaseUtilities, 'unmap_network_drive') as mock_unmap, \
         patch.object(SQLServerRepository, 'copy_table_auxban') as mock_copy_table:

        # Configurar los mocks
        mock_map.return_value = None
        mock_get_zip.return_value = r'C:\path\to\latest.zip'
        mock_extract_bak.return_value = [r'C:\path\to\latest.bak']
        mock_restore_db.return_value = None
        mock_delete_bak.return_value = None
        mock_unmap.return_value = None
        mock_copy_table.return_value = None

        # Crear una instancia del caso de uso
        use_case = RestoreAndCopyUseCase()

        # Ejecutar el caso de uso
        use_case.execute()

        # Verificar que los métodos de DatabaseUtilities fueron llamados correctamente
        mock_map.assert_called_once()
        mock_get_zip.assert_called_once()
        mock_extract_bak.assert_called_once_with(mock_get_zip.return_value)
        mock_restore_db.assert_called_once_with(mock_extract_bak.return_value[0])
        mock_delete_bak.assert_called_once_with(mock_extract_bak.return_value)
        mock_unmap.assert_called_once()

        # Verificar que el método de copia de la tabla fue llamado
        mock_copy_table.assert_called_once_with(use_case.pg_repo)
