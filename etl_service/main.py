# etl_service/main.py

import logging
import sys
from infrastructure.config import Config
from domain.entities import Database
from infrastructure.sql_server_repository import SQLServerRepository
from infrastructure.postgres_repository import PostgresRepository
from application.extract_use_case import ExtractUseCase
from application.load_use_case import LoadUseCase
from application.etl_process_use_case import ETLProcessUseCase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main(tables_to_transfer=None):
    logging.info("=== Iniciando el proceso ETL ===")

    # Configuración de la base de datos SQL Server
    sql_server_db = Database(
        name=Config.SQL_DATABASE,
        host=Config.SQL_SERVER,
        driver=Config.SQL_DRIVER,
        port=int(Config.SQL_PORT),
    )

    # Configuración de la base de datos PostgreSQL
    postgres_db = Database(
        name=Config.PG_DATABASE,
        host=Config.PG_SERVER,
        port=int(Config.PG_PORT),
        user=Config.PG_USER,
        password=Config.PG_PASSWORD,
    )

    # Inicializar repositorios y casos de uso
    try:
        sql_server_repo = SQLServerRepository(sql_server_db)
        postgres_repo = PostgresRepository(postgres_db)

        extract_use_case = ExtractUseCase(sql_server_repo)
        # print("ExtractUseCase instance:", extract_use_case)  # Verificar que no es None

        load_use_case = LoadUseCase(postgres_repo)

        etl_process_use_case = ETLProcessUseCase(extract_use_case, load_use_case)
    except Exception as e:
        logging.error(f"Error al inicializar los componentes: {e}")
        return

    try:
        # Obtener la lista de tablas a transferir
        if not tables_to_transfer:
            tables_to_transfer = sql_server_repo.get_table_names()
            # logging.info(f"Se transferirán todas las tablas: {tables_to_transfer}")
        else:
            logging.info(f"Tablas especificadas para transferir: {tables_to_transfer}")

        # Ejecutar el proceso ETL
        etl_process_use_case.execute(tables_to_transfer)
    except Exception as e:
        logging.error(f"El proceso ETL falló: {e}")
    finally:
        # Cerrar conexiones
        sql_server_repo.close_connection()
        postgres_repo.close_connection()
        logging.info("=== Proceso ETL completado ===")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        tables_to_transfer = sys.argv[1:]
    else:
        tables_to_transfer = ['obrfasamb' , 'hmo', 'hmores', 'obrfas', 'auxobramb', 'obrparpre', 'tar', 'dcf', 'dcfpro', 'cli', 'pro', 'cob', 'dvf', 'dvfpro', 'obr', 'obrctr', 'obrparpar', 'cen', 'con', 'auxobrtip', 'auxobrcla', 'conest', 'dca', 'ctr', 'dcapro', 'dcaproana', 'dcaprodes', 'dcapropar', 'dcaproser', 'dcarec', 'cer', 'cerpro']
        # tables_to_transfer = ['obrfasamb' ]
        # tables_to_transfer = ['obr']
    main(tables_to_transfer)


# TODO: falta el total cobrado
# TODO: partes de horas
# TODO: falta tema de OEPC como ingresos.
# TODO: albaranes sin codigo?
# TODO: almacen
# TODO: reglas para desglose de partidas en CD, CI, y GG
# TODO: excel con datos adicionales como presu, ODC aprobadas, pendientes, etc...

# TODO: incluir excel cierre en base de datos, crea tabla con bbdd de proyecto en postgres
# TODO: estudiar fases para planificacion temporal
# TODO: visual coste y planificacion a partir de obrparpre

# TODO: partidas con CI son Coste indirecto, CP, son generales y deben salir ddel coste del proyecto y NTC van contra retencion del proveedor, asi que tambien salen
# TODO: coste por factura, y alabranes sin facturas, esos costes si se meten por albaran

# TODO: las facturas que no tengan partida, ver el centro de coste y su codigo para asignar. mirar el csv que sale d ela EDA. al hacer el join se multiplicadn las lineas

# TODO: en obrparpre hay que hacer columna nueva que sea pre * can