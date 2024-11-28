# etl_service/application/load_use_case.py

import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.exc import SQLAlchemyError
from etl_service.application.transformations.table_config import TABLE_CONFIG


class LoadUseCase:
    def __init__(self, postgres_repo):
        """
        :param postgres_repo: Instancia del repositorio para interactuar con PostgreSQL.
        """
        self.postgres_repo = postgres_repo

    def execute(self, data_frames: dict):
        """
        Carga los DataFrames transformados en PostgreSQL, asignando claves primarias si es necesario.

        :param data_frames: Diccionario donde la llave es el nombre de la tabla destino y el valor es el DataFrame a cargar.
        """
        try:
            for table_name, df in data_frames.items():
                logging.info(f"Cargando datos en la tabla '{table_name}'.")

                # Buscar la configuración correspondiente al target_table_name
                config = next((config for config in TABLE_CONFIG.values() if config['target_table'] == table_name),
                              None)

                if not config:
                    logging.error(
                        f"No existe una configuración para la tabla de destino '{table_name}'. Saltando esta tabla.")
                    continue

                primary_key = config.get('primary_key')

                # Verificar si la tabla ya existe
                if self.postgres_repo.table_exists(table_name):
                    logging.info(f"La tabla '{table_name}' ya existe. Insertando datos.")
                    self.postgres_repo.insert_table(df, table_name, if_exists='append')
                else:
                    logging.info(f"La tabla '{table_name}' no existe. Creando tabla y cargando datos.")

                    # Crear la tabla con la clave primaria si está definida
                    self.postgres_repo.create_table(table_name, df, primary_key)

                    # Insertar los datos
                    self.postgres_repo.insert_table(df, table_name, if_exists='append')

                logging.info(f"Datos cargados exitosamente en la tabla '{table_name}'.")

        except SQLAlchemyError as e:
            logging.error(f"Error al cargar datos en PostgreSQL: {e}")
            raise
        except Exception as e:
            logging.error(f"Error inesperado durante la carga de datos: {e}")
            raise
