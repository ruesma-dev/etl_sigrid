# etl_service/tests/test_transformations/test_sort_columns_transformation.py

import pytest
import pandas as pd
from etl_service.application.transformations.sort_columns_transformation import SortColumnsTransformation

def test_sort_columns_transformation_ascending():
    # Datos de ejemplo con columnas desordenadas
    data = {
        'z_columna': [1, 2, 3],
        'a_columna': ['A', 'B', 'C'],
        'm_columna': [10.5, 20.5, 30.5],
        'b_columna': ['X', 'Y', 'Z']
    }

    df_input = pd.DataFrame(data)

    # Definir el orden esperado de columnas (ascendente)
    expected_columns = ['a_columna', 'b_columna', 'm_columna', 'z_columna']
    df_expected = df_input[expected_columns]

    # Instanciar la transformación
    sort_transformation = SortColumnsTransformation(ascending=True)

    # Aplicar la transformación
    df_transformed = sort_transformation.transform(df_input)

    # Verificar que las columnas están ordenadas correctamente
    assert list(df_transformed.columns) == expected_columns

def test_sort_columns_transformation_descending():
    # Datos de ejemplo con columnas desordenadas
    data = {
        'z_columna': [1, 2, 3],
        'a_columna': ['A', 'B', 'C'],
        'm_columna': [10.5, 20.5, 30.5],
        'b_columna': ['X', 'Y', 'Z']
    }

    df_input = pd.DataFrame(data)

    # Definir el orden esperado de columnas (descendente)
    expected_columns = ['z_columna', 'm_columna', 'b_columna', 'a_columna']
    df_expected = df_input[expected_columns]

    # Instanciar la transformación
    sort_transformation = SortColumnsTransformation(ascending=False)

    # Aplicar la transformación
    df_transformed = sort_transformation.transform(df_input)

    # Verificar que las columnas están ordenadas correctamente
    assert list(df_transformed.columns) == expected_columns

def test_sort_columns_transformation_with_empty_dataframe():
    # Datos de ejemplo con un DataFrame vacío
    df_input = pd.DataFrame()

    # Instanciar la transformación
    sort_transformation = SortColumnsTransformation(ascending=True)

    # Aplicar la transformación
    df_transformed = sort_transformation.transform(df_input)

    # Verificar que el DataFrame sigue estando vacío
    assert df_transformed.empty

def test_sort_columns_transformation_with_single_column():
    # Datos de ejemplo con una sola columna
    data = {
        'unique_columna': [100, 200, 300]
    }

    df_input = pd.DataFrame(data)

    # Definir el orden esperado de columnas (no cambia)
    expected_columns = ['unique_columna']
    df_expected = df_input[expected_columns]

    # Instanciar la transformación
    sort_transformation = SortColumnsTransformation(ascending=True)

    # Aplicar la transformación
    df_transformed = sort_transformation.transform(df_input)

    # Verificar que las columnas siguen siendo las mismas
    assert list(df_transformed.columns) == expected_columns
