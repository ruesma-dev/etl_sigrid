# test_planif_read_postgres.py

import pytest
import logging
import pandas as pd
import os
import sys

# Agregar el directorio raíz al PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.insert(0, root_dir)

from infrastructure.config import Config
from etl_service.domain.entities import Database
from infrastructure.postgres_repository import PostgresRepository


"""
Este test realiza la conexión a PostgreSQL de forma análoga a cómo se hace en 
'second_phase_main.py', usando la clase 'Database', la configuración de la
infraestructura y el 'PostgresRepository'. Luego, lee la tabla 'obrparpre'
y realiza el split en la columna 'planif', descartando filas donde 'planif'
esté en blanco.

Ejecutar con:
    pytest test_planif_read_postgres.py
"""


@pytest.mark.parametrize("table_name", ["DimPresupuestoMediciones"])
def test_planif_read_postgres(table_name):
    logging.info("=== Iniciando test de split de 'planif' leyendo desde Postgres ===")

    # 1) Configurar la base de datos PostgreSQL (similar a second_phase_main)
    postgres_db = Database(
        name=Config.PG_DATABASE,
        host=Config.PG_SERVER,
        port=int(Config.PG_PORT),
        user=Config.PG_USER,
        password=Config.PG_PASSWORD,
    )

    # 2) Inicializar repositorio de PostgreSQL
    try:
        logging.info("Inicializando PostgresRepository (test) ...")
        postgres_repo = PostgresRepository(postgres_db)
        logging.info("Repositorio PostgreSQL inicializado correctamente (test).")
    except Exception as e:
        pytest.fail(f"Error al inicializar PostgresRepository: {e}")

    # 3) Leer la tabla obrparpre y realizar la transformación
    try:
        with postgres_repo.engine.connect() as conn:
            query = f'SELECT * FROM "{table_name}"'
            df = pd.read_sql(query, conn)
            logging.info(f"Leída tabla '{table_name}' con {len(df)} filas.")

        # Verificar que existen columnas esenciales
        assert 'planif' in df.columns, f"No existe la columna 'planif' en '{table_name}'."
        assert 'obride' in df.columns, f"No existe la columna 'obride' en '{table_name}'."

        # Convertir 'planif' a string
        df['planif'] = df['planif'].astype(str)

        # Descartar filas en blanco o solo espacios
        df['planif'] = df['planif'].str.strip().replace('nan', '', regex=False)
        df = df[df['planif'] != '']
        logging.info(f"Filas tras descartar 'planif' vacíos: {len(df)}")

        if df.empty:
            logging.warning("No hay filas con 'planif' válido en la tabla. Abortando test.")
            return

        # Hacer split por '|'
        df['planif_split'] = df['planif'].str.split('|')

        # Explode
        exploded_df = df.explode('planif_split').reset_index(drop=True)

        # Limpiar espacios y convertir a float
        exploded_df['planif_split'] = exploded_df['planif_split'].fillna('').str.strip()
        exploded_df['porcentaje'] = pd.to_numeric(exploded_df['planif_split'], errors='coerce')

        # Crear 'fasnum' según la posición de cada split
        exploded_df['fasnum'] = exploded_df.groupby('obride').cumcount() + 1

        # Construir el dataframe final
        final_df = exploded_df[['obride', 'fasnum', 'porcentaje']].copy()

        # Impresiones de control
        print("=== DataFrame original (HEAD) ===")
        print(df.head(10))
        print("=== Exploded DataFrame (HEAD) ===")
        print(exploded_df.head(10))
        print("=== DataFrame final (HEAD) ===")
        print(final_df.head(10))

        # Aserciones básicas
        assert len(final_df) > 0, "El dataframe final está vacío tras la transformación."
        for col in ('obride', 'fasnum', 'porcentaje'):
            assert col in final_df.columns, f"Falta la columna '{col}' en el resultado final."

        logging.info(f"Test completado. Filas finales: {len(final_df)}")

    except Exception as ex:
        pytest.fail(f"Error en la lógica de split de planif: {ex}")

    finally:
        # Cerrar la conexión
        logging.info("Cerrando conexión a PostgreSQL (test).")
        postgres_repo.close_connection()
        logging.info("=== Test finalizado ===")
