# etl_service/second_phase_main.py

import logging
import sys

from infrastructure.config import Config
from domain.entities import Database
from infrastructure.postgres_repository import PostgresRepository
from application.transformations.second_phase_use_case import SecondPhaseUseCase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main_second_phase():
    logging.info("=== Iniciando segunda fase ETL (solo en PostgreSQL) ===")

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
        logging.info("Inicializando PostgresRepository para la segunda fase...")
        postgres_repo = PostgresRepository(postgres_db)
        logging.info("Repositorio PostgreSQL inicializado correctamente (segunda fase).")
    except Exception as e:
        logging.error(f"Error al inicializar PostgresRepository (segunda fase): {e}")
        return

    # 3) Instanciar SecondPhaseUseCase
    second_phase_use_case = SecondPhaseUseCase(postgres_repo)

    try:
        # 4) Llamar la función de join DCA <-> DCFPRO (DimAlbaranCompra / DimFacturaCompraProductos)
        dca_table = "DimAlbaranCompra"
        dcfpro_table = "DimFacturaCompraProductos"

        logging.info(f"=== [Segunda fase] Procesando tablas '{dca_table}' y '{dcfpro_table}' ===")
        second_phase_use_case.execute_dca_dcfpro_join(
            dca_table=dca_table,
            dcfpro_table=dcfpro_table
        )

    except Exception as e:
        logging.error(f"Error en el proceso de segunda fase: {e}")
    finally:
        # 5) Cerrar conexión PostgreSQL
        logging.info("Cerrando conexión a PostgreSQL (segunda fase).")
        postgres_repo.close_connection()
        logging.info("=== Segunda fase completada ===")


if __name__ == "__main__":
    # Puedes pasar parámetros o personalizar si deseas.
    main_second_phase()
