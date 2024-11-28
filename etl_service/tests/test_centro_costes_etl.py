# etl_service/tests/test_etl_process/test_centro_costes_etl.py

import pytest
from unittest.mock import Mock
import pandas as pd
from etl_service.application.extract_use_case import ExtractUseCase
from etl_service.application.load_use_case import LoadUseCase
from etl_service.application.etl_process_use_case import ETLProcessUseCase

def test_etl_process_centro_costes_transformation():
    # Crear un DataFrame de ejemplo que simula los datos extraídos de SQL Server para 'cen'
    data = {
        'id': [1, 2, 3],
        'nombre': ['Centro 1', 'Centro 2', 'Centro 3'],
        'fecfinpre': [20240101, 0, 20240315],
        'fecfinrea': [0, 20240701, 0],
        'fecbloqueo': [20240110, 20240210, 0],
        'fecfincie': [20240610, 0, 20240715],
        # Otros campos...
    }
    df_cen = pd.DataFrame(data)

    # Crear un mock del caso de uso de extracción que retorna el DataFrame
    extract_use_case_mock = Mock(spec=ExtractUseCase)
    extract_use_case_mock.execute.return_value = {'cen': df_cen}

    # Crear un mock del caso de uso de carga
    load_use_case_mock = Mock(spec=LoadUseCase)

    # Instanciar el caso de uso ETL
    etl_process_use_case = ETLProcessUseCase(
        extract_use_case=extract_use_case_mock,
        load_use_case=load_use_case_mock
    )

    # Ejecutar el proceso ETL para la tabla 'cen'
    etl_process_use_case.execute(['cen'])

    # Verificar que se llamó al caso de uso de extracción con la tabla 'cen'
    extract_use_case_mock.execute.assert_called_with(['cen'])

    # Definir el DataFrame transformado esperado
    expected_data = {
        'id': [1, 2, 3],
        'nombre': ['Centro 1', 'Centro 2', 'Centro 3'],
        'fecha_fin_previsto': pd.to_datetime(['2024-01-01', pd.NaT, '2024-03-15']),
        'fecha_fin_real': pd.to_datetime([pd.NaT, '2024-07-01', pd.NaT]),
        'fecha_bloqueo': pd.to_datetime(['2024-01-10', '2024-02-10', pd.NaT]),
        'fecha_fin_cierre': pd.to_datetime(['2024-06-10', pd.NaT, '2024-07-15']),
        # Otros campos...
    }
    df_expected = pd.DataFrame(expected_data)

    # Verificar que el DataFrame transformado coincide con el esperado
    load_use_case_mock.execute.assert_called_once()
    args, kwargs = load_use_case_mock.execute.call_args
    assert 'centro_costes' in args[0]
    loaded_df = args[0]['centro_costes']

    # Asegurarse de que los DataFrames sean iguales
    pd.testing.assert_frame_equal(loaded_df.reset_index(drop=True), df_expected.reset_index(drop=True))
