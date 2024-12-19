# etl_service/application/load_use_case.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from etl_service.application.transformations.table_config import TABLE_CONFIG
from etl_service.application.transformations.row_modifications_mapping import ROW_INSERTION_MAPPING


class LoadUseCase:
    def __init__(self, postgres_repo):
        """
        :param postgres_repo: Instancia del repositorio para interactuar con PostgreSQL.
        """
        self.postgres_repo = postgres_repo

    def execute(self, data_frames: dict):
        """
        Carga los DataFrames transformados en PostgreSQL, asignando claves primarias si es necesario
        y validando la inserción de filas adicionales.

        :param data_frames: Diccionario donde la llave es el nombre de la tabla destino y el valor es el DataFrame a cargar.
        """
        try:
            for table_name, df in data_frames.items():
                # logging.info(f"Cargando datos en la tabla '{table_name}'.")

                # Buscar la configuración correspondiente al target_table_name
                config = next((config for config in TABLE_CONFIG.values() if config['target_table'] == table_name), None)

                if not config:
                    logging.error(
                        f"No existe una configuración para la tabla de destino '{table_name}'. Saltando esta tabla.")
                    continue

                primary_key = config.get('primary_key')

                # Verificar si la tabla ya existe en PostgreSQL
                if self.postgres_repo.table_exists(table_name):
                    # logging.info(f"La tabla '{table_name}' ya existe. Insertando datos.")
                    self.postgres_repo.insert_table(df, table_name, if_exists='append')
                else:
                    # logging.info(f"La tabla '{table_name}' no existe. Creando tabla y cargando datos.")
                    self.postgres_repo.create_table(table_name, df, primary_key)
                    self.postgres_repo.insert_table(df, table_name, if_exists='append')

                # Validar inserciones si hay filas adicionales en el mapeo
                rows_to_insert = ROW_INSERTION_MAPPING.get(table_name, [])
                if rows_to_insert:
                    for row in rows_to_insert:
                        # Construir la consulta de validación
                        conditions = " AND ".join([
                            f"{col} = '{value}'" if isinstance(value, str) else f"{col} = {value}"
                            for col, value in row.items()
                        ])
                        query = f"SELECT 1 FROM {table_name} WHERE {conditions} LIMIT 1"
                        result = self.postgres_repo.query(query)
                        if result:
                            logging.info(f"Fila añadida correctamente en '{table_name}': {row}")
                        else:
                            logging.error(f"Fila no encontrada en '{table_name}': {row}")

                # logging.info(f"Datos cargados exitosamente en la tabla '{table_name}'.")

        except SQLAlchemyError as e:
            logging.error(f"Error al cargar datos en PostgreSQL: {e}")
            raise
        except Exception as e:
            logging.error(f"Error inesperado durante la carga de datos: {e}")
            raise
