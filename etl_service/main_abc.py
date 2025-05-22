# etl_service/main_abc.py

import logging
import sys
from infrastructure.config import Config
from domain.entities import Database
from infrastructure.postgres_repository import PostgresRepository

from etl_service.application.postprocess.abc_analysis_use_case import AbcAnalysisUseCase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main_abc():
    logging.info("=== Iniciando proceso de ABC analysis (Post-processing) ===")

    # 1) Configuración de la base de datos PostgreSQL
    postgres_db = Database(
        name=Config.PG_DATABASE,
        host=Config.PG_SERVER,
        port=int(Config.PG_PORT),
        user=Config.PG_USER,
        password=Config.PG_PASSWORD,
    )

    # 2) Inicializar repositorio de PostgreSQL
    try:
        logging.info("Inicializando PostgresRepository para ABC analysis...")
        postgres_repo = PostgresRepository(postgres_db)
        logging.info("Repositorio PostgreSQL inicializado correctamente (ABC).")
    except Exception as e:
        logging.error(f"Error al inicializar PostgresRepository (ABC): {e}")
        return

    try:
        abc_use_case = AbcAnalysisUseCase(postgres_repo)

        # Lee version por sys.argv, default=1
        if len(sys.argv) >= 2:
            version_value = int(sys.argv[1])
        else:
            version_value = 1  # default

        logging.info(f"Ejecutando ABC analysis con version={version_value} ...")

        # Llamar la lógica principal
        abc_use_case.execute_abc_analysis(
            version_value=version_value,
            cost_table="obrparpre_master_coste",
            sale_table="obrparpre_master_venta",
            part_table="DimPartidasObra",
            cost_col="importe_fase_diff",
            sale_col="importe_fase_diff"
        )

    except Exception as e:
        logging.error(f"Error en ABC analysis: {e}")
    finally:
        logging.info("Cerrando conexión a PostgreSQL (ABC).")
        postgres_repo.close_connection()
        logging.info("=== ABC analysis completado ===")


if __name__ == "__main__":
    main_abc()
