# etl_service/second_phase_main.py

import logging
from infrastructure.config import Config
from domain.entities import Database
from infrastructure.postgres_repository import PostgresRepository

# Importamos cada caso de uso por separado
from application.transformations.second_phase_use_case_dca import SecondPhaseUseCaseDcaDcfpro
from application.transformations.second_phase_use_case_obrparpar import SecondPhaseUseCaseConObraObrparpar
from application.transformations.second_phase_use_case_dca_con import SecondPhaseUseCaseConObraDca
from application.transformations.second_phase_use_case_dcapro import SecondPhaseUseCaseDcaproObrparpar

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main_second_phase(process="all"):
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

    try:
        # Instanciar cada caso de uso por separado
        dca_dcfpro_use_case = SecondPhaseUseCaseDcaDcfpro(postgres_repo)
        con_obra_obrparpar_use_case = SecondPhaseUseCaseConObraObrparpar(postgres_repo)
        con_obra_dca_use_case = SecondPhaseUseCaseConObraDca(postgres_repo)
        second_phase_use_case_dcapro_par = SecondPhaseUseCaseDcaproObrparpar(postgres_repo)

        if process == "dca" or process == "all":
            # Llamar la función de join DCA <-> DCFPRO
            dca_table = "dca"
            dcfpro_table = "dcfpro"
            logging.info("=== [Segunda fase] Llamando execute_dca_dcfpro_join ===")
            dca_dcfpro_use_case.execute_dca_dcfpro_join(
                dca_key=dca_table,
                dcfpro_key=dcfpro_table
            )

        if process == "obrparpar" or process == "all":
            # Llamar la función para con->obr->obrparpar => creacion de 'codigo_obra'
            con_table = "con"      # Ajusta según tu caso
            obr_table = "obr"
            obrparpar_table = "obrparpar"
            logging.info("=== [Segunda fase] Llamando execute_con_obra_obrparpar_codigo_obra ===")
            con_obra_obrparpar_use_case.execute_con_obra_obrparpar_codigo_obra(
                con_key=con_table,
                obr_key=obr_table,
                obrparpar_key=obrparpar_table
            )

        if process == "dca_con" or process == "all":
            # Llamar la función para con->obr->dca => creación de 'codigo_obra'
            con_table = "con"  # Ajusta según tu caso
            obr_table = "obr"
            dca_table = "dca"
            logging.info("=== [Segunda fase] Llamando execute_con_obra_dca_codigo_obra ===")
            con_obra_dca_use_case.execute_con_obra_dca_codigo_obra(
                con_key=con_table,
                obr_key=obr_table,
                dca_key=dca_table
            )

        if process == "dcapro" or process == "all":
            dcapro_key = "dcapro"
            obrparpar_key = "obrparpar"
            logging.info("=== [Segunda fase] Llamando execute_dcapro_obrparpar_codigo_partida ===")
            second_phase_use_case_dcapro_par.execute_dcapro_obrparpar_codigo_partida(
                dcapro_key=dcapro_key,
                obrparpar_key=obrparpar_key
            )

    except Exception as e:
        logging.error(f"Error en la segunda fase: {e}")
    finally:
        logging.info("Cerrando conexión a PostgreSQL (segunda fase).")
        postgres_repo.close_connection()
        logging.info("=== Segunda fase completada ===")


if __name__ == "__main__":
    """
      Puedes pasar un argumento (ej: 'dca' / 'obrparpar' / 'dca_con' / 'all')
      python second_phase_main.py dca
      python second_phase_main.py obrparpar
      python second_phase_main.py dca_con
      python second_phase_main.py all
    """
    import sys
    if len(sys.argv) > 1:
        process_flag = sys.argv[1]
    else:
        process_flag = 'all'  # Ejecuta todo por defecto

    main_second_phase(process=process_flag)
