#!/usr/bin/env python3
# eda_presupuestos_partidas.py

import os
import logging
import pandas as pd
from sqlalchemy import create_engine

# Configuración de conexión a PostgreSQL
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")
PG_SERVER = os.getenv("PG_SERVER", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "clone_sigrid")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Conectar a PostgreSQL
connection_string = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_SERVER}:{PG_PORT}/{PG_DATABASE}"
)
engine = create_engine(connection_string)

try:
    # Leer tablas
    logging.info("Leyendo tablas desde la base de datos...")
    presupuestos = pd.read_sql_table("DimPresupuestoMediciones", con=engine, schema="public")
    partidas = pd.read_sql_table("DimPartidasObra", con=engine, schema="public")
    ambitos = pd.read_sql_table("DimAmbitoObra", con=engine, schema="public")

    # Realizar joins
    logging.info("Realizando joins entre las tablas...")
    merged_df = pd.merge(presupuestos, partidas, left_on="paride", right_on="ide", how="left", suffixes=("", "_partida"))
    merged_df = pd.merge(merged_df, ambitos, left_on="amb", right_on="ide", how="left", suffixes=("", "_ambito"))
    print(presupuestos.shape)
    print(merged_df.shape)

    # Filtrar por obride = 2098292
    logging.info("Filtrando datos con obride = 2098292...")
    filtered_df = merged_df[merged_df['obride'] == 2128229]

    # Crear un DataFrame por cada valor único de 'amb'
    output_folder = "./eda_results"
    os.makedirs(output_folder, exist_ok=True)

    for amb in filtered_df['amb'].unique():
        amb_df = filtered_df[filtered_df['amb'] == amb]
        csv_path = os.path.join(output_folder, f"amb_{amb}.csv")
        amb_df.to_csv(csv_path, index=False, encoding='utf-8')
        logging.info(f"Exportado DataFrame para amb={amb} a {csv_path}")

        # Analizar repeticiones de paride para amb=3
        if amb == 7:
            paride_counts = amb_df['paride'].value_counts()
            repeated_paride = paride_counts[paride_counts > 1]
            logging.info(f"Valores repetidos de paride en amb=2:\n{repeated_paride}")

    logging.info("Proceso completado correctamente.")

except Exception as e:
    logging.error(f"Error durante la ejecución: {e}")

finally:
    engine.dispose()
    logging.info("Conexión cerrada.")
