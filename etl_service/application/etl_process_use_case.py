# etl_service/application/etl_process_use_case.py

import logging
from etl_service.application.extract_use_case import ExtractUseCase
from etl_service.application.load_use_case import LoadUseCase
from etl_service.application.transformer import Transformer
from etl_service.application.transformations.table_config import TABLE_CONFIG

class ETLProcessUseCase:
    def __init__(self, extract_use_case: ExtractUseCase, load_use_case: LoadUseCase):
        self.extract_use_case = extract_use_case
        self.load_use_case = load_use_case

    def execute(self, tables_to_transfer):
        try:
            # Procesar cada tabla individualmente
            for table_key in tables_to_transfer:
                try:
                    logging.info(f"Procesando la tabla clave '{table_key}'.")

                    if table_key not in TABLE_CONFIG:
                        logging.error(f"No existe una configuración para la tabla clave '{table_key}'. Saltando esta tabla.")
                        continue

                    config = TABLE_CONFIG[table_key]
                    source_table = config['source_table']
                    target_table_name = config['target_table']
                    primary_key = config.get('primary_key')

                    logging.info(f"Nombre de la tabla de origen: '{source_table}'.")
                    logging.info(f"Nombre de la tabla de destino: '{target_table_name}'.")

                    # Extraer datos de la tabla
                    extracted_data = self.extract_use_case.execute([source_table])
                    if not extracted_data or source_table not in extracted_data:
                        logging.info(f"La tabla de origen '{source_table}' está vacía o no se pudo extraer.")
                        continue  # Pasar a la siguiente tabla

                    df = extracted_data[source_table]

                    # Aplicar transformaciones
                    transformed_df = Transformer.transform(table_key, df)
                    logging.info(f"Tabla clave '{table_key}' transformada exitosamente.")

                    # Verificar si la tabla ya existe en PostgreSQL
                    if self.load_use_case.postgres_repo.table_exists(target_table_name):
                        logging.info(f"La tabla '{target_table_name}' ya existe. Insertando datos.")
                        self.load_use_case.postgres_repo.insert_table(transformed_df, target_table_name, if_exists='append')
                    else:
                        logging.info(f"La tabla '{target_table_name}' no existe. Creando tabla y cargando datos.")
                        self.load_use_case.postgres_repo.create_table(target_table_name, transformed_df, primary_key)
                        self.load_use_case.postgres_repo.insert_table(transformed_df, target_table_name, if_exists='append')

                    logging.info(f"Tabla clave '{table_key}' cargada exitosamente en PostgreSQL como '{target_table_name}'.")

                except Exception as e:
                    logging.error(f"Error al procesar la tabla clave '{table_key}': {e}")
                    continue  # Continúa con la siguiente tabla
        except Exception as e:
            logging.error(f"El proceso ETL falló: {e}")
            raise
