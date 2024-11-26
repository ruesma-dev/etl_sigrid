# etl_service/tests/test_transformations/test_rename_columns_transformation.py

import pytest
import pandas as pd
from etl_service.application.transformations.rename_columns_transformation import RenameColumnsTransformation

def test_rename_columns_transformation():
    # Datos de ejemplo con la columna 'res' que debe ser renombrada
    data = {
        'id': [1, 2, 3],
        'res': ['Proyecto A', 'Proyecto B', 'Proyecto C'],
        'otra_columna': [10, 20, 30]
    }

    df_input = pd.DataFrame(data)

    # Definir el mapeo de renombrado
    rename_mapping = {
        'res': 'nombre_obra'
    }

    # Instanciar la transformación
    rename_transformation = RenameColumnsTransformation(rename_mapping=rename_mapping)

    # Aplicar la transformación
    df_transformed = rename_transformation.transform(df_input)

    # Definir el DataFrame transformado esperado
    expected_data = {
        'id': [1, 2, 3],
        'nombre_obra': ['Proyecto A', 'Proyecto B', 'Proyecto C'],
        'otra_columna': [10, 20, 30]
    }

    df_expected = pd.DataFrame(expected_data)

    # Asegurarse de que los DataFrames tengan el mismo orden de columnas
    df_transformed = df_transformed[['id', 'nombre_obra', 'otra_columna']]
    df_expected = df_expected[['id', 'nombre_obra', 'otra_columna']]

    # Comparar los DataFrames
    pd.testing.assert_frame_equal(df_transformed.reset_index(drop=True), df_expected.reset_index(drop=True))
