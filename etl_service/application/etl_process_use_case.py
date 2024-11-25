# etl_service/application/etl_process_use_case.py

import logging
from application.extract_use_case import ExtractUseCase
from application.load_use_case import LoadUseCase


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
                    if not extracted_data:
                        logging.info(f"La tabla '{table_name}' está vacía o no se pudo extraer.")
                        continue  # Pasar a la siguiente tabla

                    # Cargar datos en PostgreSQL
                    self.load_use_case.execute(extracted_data)
                    logging.info(f"Tabla '{table_name}' procesada exitosamente.")
                except Exception as e:
                    logging.error(f"Error al procesar la tabla '{table_name}': {e}")
                    continue  # Continúa con la siguiente tabla
        except Exception as e:
            logging.error(f"El proceso ETL falló: {e}")
            raise
