# etl_service/application/etl_process_use_case.py

import logging
from etl_service.application.extract_use_case import ExtractUseCase
from etl_service.application.load_use_case import LoadUseCase
from etl_service.application.transformer import Transformer

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

                    df = extracted_data[table_name]

                    # Aplicar transformaciones
                    transformed_df = Transformer.transform(table_name, df)
                    logging.info(f"Tabla '{table_name}' transformada exitosamente.")

                    # Determinar el nombre de la tabla de destino
                    if table_name.lower() == 'obr':
                        target_table_name = 'FacObra'
                    elif table_name.lower() == 'cen':
                        target_table_name = 'centro_costes'
                    else:
                        target_table_name = table_name

                    # Cargar datos en PostgreSQL
                    self.load_use_case.execute({target_table_name: transformed_df})
                    logging.info(f"Tabla '{table_name}' cargada exitosamente en PostgreSQL como '{target_table_name}'.")

                except Exception as e:
                    logging.error(f"Error al procesar la tabla '{table_name}': {e}")
                    continue  # Continúa con la siguiente tabla
        except Exception as e:
            logging.error(f"El proceso ETL falló: {e}")
            raise
