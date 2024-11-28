# etl_service/application/etl_process_use_case.py

import logging
from etl_service.application.extract_use_case import ExtractUseCase
from etl_service.application.load_use_case import LoadUseCase
from etl_service.application.transformer import Transformer
from etl_service.application.transformations.primary_key_mapping import PRIMARY_KEY_MAPPING
from etl_service.application.transformations.column_mappings import TABLE_NAME_MAPPINGS

class ETLProcessUseCase:
    def __init__(self, extract_use_case: ExtractUseCase, load_use_case: LoadUseCase):
        self.extract_use_case = extract_use_case
        self.load_use_case = load_use_case

    def execute(self, tables_to_transfer):
        try:
            # Procesar cada tabla individualmente
            for table_name in tables_to_transfer:
                try:
                    logging.info(f"Procesando la tabla '{table_name}'.")

                    # Extraer datos de la tabla
                    extracted_data = self.extract_use_case.execute([table_name])
                    if not extracted_data or table_name not in extracted_data:
                        logging.info(f"La tabla '{table_name}' está vacía o no se pudo extraer.")
                        continue  # Pasar a la siguiente tabla

                    df = extracted_data[table_name]

                    # Aplicar transformaciones
                    transformed_df = Transformer.transform(table_name, df)
                    logging.info(f"Tabla '{table_name}' transformada exitosamente.")

                    # Determinar el nombre de la tabla de destino usando TABLE_NAME_MAPPINGS si está definido
                    target_table_name = TABLE_NAME_MAPPINGS.get(table_name.lower(), table_name)

                    # Obtener la clave primaria para la tabla destino si existe
                    primary_key = PRIMARY_KEY_MAPPING.get(target_table_name)

                    # Verificar si la tabla ya existe
                    if self.load_use_case.postgres_repo.table_exists(target_table_name):
                        logging.info(f"La tabla '{target_table_name}' ya existe. Insertando datos.")
                        self.load_use_case.postgres_repo.insert_table(transformed_df, target_table_name, if_exists='append')
                    else:
                        logging.info(f"La tabla '{target_table_name}' no existe. Creando tabla y cargando datos.")
                        self.load_use_case.postgres_repo.create_table(target_table_name, transformed_df, primary_key)
                        self.load_use_case.postgres_repo.insert_table(transformed_df, target_table_name, if_exists='append')

                    logging.info(f"Tabla '{table_name}' cargada exitosamente en PostgreSQL como '{target_table_name}'.")

                except Exception as e:
                    logging.error(f"Error al procesar la tabla '{table_name}': {e}")
                    continue  # Continúa con la siguiente tabla
        except Exception as e:
            logging.error(f"El proceso ETL falló: {e}")
            raise
