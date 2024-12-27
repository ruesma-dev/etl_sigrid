#!/usr/bin/env python
# coding: utf-8

import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # 1) Cadena de conexión a tu PostgreSQL (ajusta según tu entorno)
    user = "postgres"
    password = "admin"
    host = "localhost"
    port = 5432
    database_name = "clone_sigrid"
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_name}"

    try:
        # 2) Crear engine de conexión
        engine = create_engine(connection_string)
        logging.info("Conexión establecida a PostgreSQL.")

        # 3) Leer tablas dca y dcfpro
        dca_query = 'SELECT * FROM "DimAlbaranCompra";'        # Ajusta si tu tabla se llama distinto
        dcfpro_query = 'SELECT * FROM "DimFacturaCompraProductos";'
        logging.info("Leyendo tabla 'dca' desde PostgreSQL...")
        dca_df = pd.read_sql(dca_query, con=engine)

        logging.info("Leyendo tabla 'dcfpro' desde PostgreSQL...")
        dcfpro_df = pd.read_sql(dcfpro_query, con=engine)

        logging.info(f"Registros dca_df={len(dca_df)}, dcfpro_df={len(dcfpro_df)}.")

        # 4) Subconjunto de dcfpro: solo docide, ide, docoriide
        subset_dcfpro = dcfpro_df[['docide', 'ide', 'docoriide']].copy()

        # 5) Renombrar con sufijo _dcfpro
        subset_dcfpro.rename(columns={
            'docide': 'docide_dcfpro',
            'ide': 'ide_dcfpro',
            'docoriide': 'docoriide_dcfpro'
        }, inplace=True)

        # 6) Realizar el left join:
        #    - El dca_df se left-join con subset_dcfpro usando
        #      dca_df.ide == subset_dcfpro.docoriide_dcfpro
        merged_df = dca_df.merge(
            subset_dcfpro,
            how="left",
            left_on="ide",             # Columna en DCA
            right_on="docoriide_dcfpro"   # Columna en subset_dcfpro
        )
        logging.info(f"Join completado. Filas resultantes: {len(merged_df)}.")

        # 7) Contar cuántas veces se repite docide_dcfpro por cada ide de DCA
        #    Si tu 'ide' de DCA sigue siendo 'ide', puedes agrupar directamente.
        count_df = (
            merged_df
            .groupby("ide")['docide_dcfpro']
            # Ojo: count() vs nunique():
            # count() te dará cuántas filas hay, nunique() cuántos valores distintos.
            # Asumiendo quieres TODAS las repeticiones (no solo distintas):
            .count()
            .reset_index()
            .rename(columns={'docide_dcfpro': 'docide_count'})
        )

        # Ordenar de mayor a menor
        count_df.sort_values(by='docide_count', ascending=False, inplace=True)

        logging.info("=== EJEMPLO de conteo docide_dcfpro por ide ===")
        print(count_df.head(20))

        # 8) Guardar el merged_df y el conteo en CSV para inspección
        merged_df.to_csv("resultado_dca_dcfpro.csv", index=False, sep=';', encoding='utf-8')
        count_df.to_csv("conteo_docide_por_ide.csv", index=False, sep=';', encoding='utf-8')

        logging.info("Archivos CSV generados: 'resultado_dca_dcfpro.csv' y 'conteo_docide_por_ide.csv'")

    except SQLAlchemyError as e:
        logging.error(f"Error con SQLAlchemy: {e}")
    except Exception as ex:
        logging.error(f"Error inesperado: {ex}")
    finally:
        # Cerrar conexión
        if 'engine' in locals():
            engine.dispose()
            logging.info("Conexión a PostgreSQL cerrada.")

if __name__ == "__main__":
    main()
