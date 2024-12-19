# etl_service/application/transformations/transformation_factory.py

import logging
import importlib.util
import importlib
from etl_service.application.transformations.date_transformation import DateTransformation
from etl_service.application.transformations.drop_null_or_zero_columns_transformation import DropNullOrZeroColumnsTransformation
from etl_service.application.transformations.rename_columns_transformation import RenameColumnsTransformation
from etl_service.application.transformations.sort_columns_transformation import SortColumnsTransformation
from etl_service.application.transformations.table_config import TABLE_CONFIG
from etl_service.application.transformations.row_modifications_mapping import ROW_INSERTION_MAPPING, ROW_DELETION_MAPPING
from etl_service.application.transformations.add_rows_transformation import AddRowsTransformation
from etl_service.application.transformations.delete_rows_transformation import DeleteRowsTransformation
from etl_service.application.transformations.combine_columns_transformation import CombineColumnsTransformation
from etl_service.domain.interfaces import SpecificTableTransformations

class TransformationFactory:
    @staticmethod
    def get_transformations(table_key: str, extract_use_case=None) -> list:
        """
        Retorna una lista de transformaciones aplicables a la tabla especificada, incluyendo la adición y eliminación de filas,
        transformación de fechas, eliminación de columnas vacías, renombrado de columnas, orden alfabético, y la creación de
        nuevas columnas combinando columnas existentes.
        """
        transformations = []

        if table_key in TABLE_CONFIG:
            config = TABLE_CONFIG[table_key]
            rename_mapping = config.get('rename_columns', {})
            date_columns = config.get('date_columns', [])
            target_table_name = config.get('target_table')

            # Obtener configuraciones de columnas a combinar
            combine_columns_config = config.get('combine_columns', [])

            # Añadir AddRowsTransformation
            rows_to_add = ROW_INSERTION_MAPPING.get(target_table_name, [])
            if rows_to_add:
                transformations.append(AddRowsTransformation(rows_to_add))
                logging.info(f"Añadido AddRowsTransformation para la tabla '{table_key}'.")

            # Añadir DeleteRowsTransformation
            rows_to_delete = ROW_DELETION_MAPPING.get(target_table_name, [])
            if rows_to_delete:
                transformations.append(DeleteRowsTransformation(rows_to_delete))
                logging.info(f"Añadido DeleteRowsTransformation para la tabla '{table_key}'.")

            # Añadir DateTransformation
            if date_columns:
                date_column_mapping = {col: col for col in date_columns}
                transformations.append(DateTransformation(column_mapping=date_column_mapping, null_if_zero=True))

            # Añadir CombineColumnsTransformation
            for combo_config in combine_columns_config:
                new_col = combo_config.get('new_column_name')
                cols_to_combine = combo_config.get('columns_to_combine', [])
                sep = combo_config.get('separator', '_')
                if new_col and cols_to_combine:
                    transformations.append(CombineColumnsTransformation(new_column_name=new_col, columns_to_combine=cols_to_combine, separator=sep))
                    logging.info(f"Añadido CombineColumnsTransformation para la tabla '{table_key}' creando '{new_col}'.")

            # Añadir DropNullOrZeroColumnsTransformation
            transformations.append(DropNullOrZeroColumnsTransformation())

            # Añadir RenameColumnsTransformation
            if rename_mapping:
                transformations.append(RenameColumnsTransformation(rename_mapping=rename_mapping))

            # Añadir SortColumnsTransformation
            transformations.append(SortColumnsTransformation(ascending=True))

            # Cargar transformaciones específicas
            module_name = f"etl_service.application.transformations.specific.{table_key}_transformations"
            class_name = f"{table_key.capitalize()}Transformations"

            spec = importlib.util.find_spec(module_name)
            if spec is not None:
                mod = importlib.import_module(module_name)
                TransformClass = getattr(mod, class_name, None)
                print(f'TransformClass es {TransformClass}')
                if TransformClass and issubclass(TransformClass, SpecificTableTransformations):
                    if table_key == 'dca':
                        # Pasar extract_use_case como parámetro
                        print('por aqui vamos')
                        print(extract_use_case)
                        specific_instance = TransformClass(extract_use_case)
                    else:
                        # Para otras tablas específicas que no necesitan extract_use_case
                        specific_instance = TransformClass()

                    specific_transformations = specific_instance.get_table_transformations()
                    transformations.extend(specific_transformations)

        else:
            logging.warning(f"No se encontraron mapeos de columnas para la tabla '{table_key}'. No se aplicarán transformaciones específicas.")

        return transformations
