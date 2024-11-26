# etl_service/tests/test_transformations/test_date_transformation.py

import pytest
import pandas as pd
from etl_service.application.transformations.date_transformation import DateTransformation

def test_date_transformation():
    # Datos de ejemplo que incluyen valores 0, enteros y valores no conformes
    data = {
        'id': [1, 2, 3, 4, 5],
        'nombre': ['Obra 1', 'Obra 2', 'Obra 3', 'Obra 4', 'Obra 5'],
        'fecinipre': [20230101, 0, 20230315, 'invalid', 20230520],
        'fecfinpre': [0, 20230701, 'invalid', 20230830, 0],
        'fecinirea': [20230110, 20230210, 0, 20230425, 'invalid'],
        'fecfinrea': [20230610, 0, 20230715, 'invalid', 20230905],
        'feccieest': [20230601, 0, 20230815, 0, 20231020],
        'fecadj': [20230701, 20230801, 0, 'invalid', 20231105],
        'fecfincie': [20230801, 0, 20230915, 0, 'invalid'],
        'fecciepre': [20230901, 'invalid', 0, 20231025, 0],
        'fecapelic': [0, 20231101, 'invalid', 0, 20231220],
    }

    df_input = pd.DataFrame(data)

    # Definir el mapeo de columnas
    column_mapping = {
        'fecinipre': 'fecha_inicio_prevista',
        'fecfinpre': 'fecha_fin_prevista',
        'fecinirea': 'fecha_inicio_real',
        'fecfinrea': 'fecha_fin_real',
        'feccieest': 'fecha_cierre_estudio',
        'fecadj': 'fecha_adjudicacion',
        'fecfincie': 'fecha_fin_cierre',
        'fecciepre': 'fecha_cierre_previsto',
        'fecapelic': 'fecha_apertura_licitacion'
    }

    # Instanciar la transformación
    date_transformation = DateTransformation(column_mapping=column_mapping, null_if_zero=True)

    # Aplicar la transformación
    df_transformed = date_transformation.transform(df_input)

    # Definir el DataFrame transformado esperado
    expected_data = {
        'id': [1, 2, 3, 4, 5],
        'nombre': ['Obra 1', 'Obra 2', 'Obra 3', 'Obra 4', 'Obra 5'],
        'fecha_inicio_prevista': pd.to_datetime(['2023-01-01', pd.NaT, '2023-03-15', pd.NaT, '2023-05-20']),
        'fecha_fin_prevista': pd.to_datetime([pd.NaT, '2023-07-01', pd.NaT, '2023-08-30', pd.NaT]),
        'fecha_inicio_real': pd.to_datetime(['2023-01-10', '2023-02-10', pd.NaT, '2023-04-25', pd.NaT]),
        'fecha_fin_real': pd.to_datetime(['2023-06-10', pd.NaT, '2023-07-15', pd.NaT, '2023-09-05']),
        'fecha_cierre_estudio': pd.to_datetime(['2023-06-01', pd.NaT, '2023-08-15', pd.NaT, '2023-10-20']),
        'fecha_adjudicacion': pd.to_datetime(['2023-07-01', '2023-08-01', pd.NaT, pd.NaT, '2023-11-05']),
        'fecha_fin_cierre': pd.to_datetime(['2023-08-01', pd.NaT, '2023-09-15', pd.NaT, pd.NaT]),
        'fecha_cierre_previsto': pd.to_datetime(['2023-09-01', pd.NaT, pd.NaT, '2023-10-25', pd.NaT]),
        'fecha_apertura_licitacion': pd.to_datetime([pd.NaT, '2023-11-01', pd.NaT, pd.NaT, '2023-12-20']),
    }

    df_expected = pd.DataFrame(expected_data)

    # Asegurarse de que los DataFrames tengan el mismo orden de columnas
    df_transformed = df_transformed[['id', 'nombre', 'fecha_inicio_prevista', 'fecha_fin_prevista',
                                     'fecha_inicio_real', 'fecha_fin_real',
                                     'fecha_cierre_estudio', 'fecha_adjudicacion', 'fecha_fin_cierre',
                                     'fecha_cierre_previsto', 'fecha_apertura_licitacion']]
    df_expected = df_expected[['id', 'nombre', 'fecha_inicio_prevista', 'fecha_fin_prevista',
                               'fecha_inicio_real', 'fecha_fin_real',
                               'fecha_cierre_estudio', 'fecha_adjudicacion', 'fecha_fin_cierre',
                               'fecha_cierre_previsto', 'fecha_apertura_licitacion']]

    # Comparar los DataFrames
    pd.testing.assert_frame_equal(df_transformed.reset_index(drop=True), df_expected.reset_index(drop=True))
