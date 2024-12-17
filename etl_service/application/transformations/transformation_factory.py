# etl_service/application/transformations/transformation_factory.py

import logging
from etl_service.application.transformations.date_transformation import DateTransformation
from etl_service.application.transformations.drop_null_or_zero_columns_transformation import DropNullOrZeroColumnsTransformation
from etl_service.application.transformations.rename_columns_transformation import RenameColumnsTransformation
from etl_service.application.transformations.sort_columns_transformation import SortColumnsTransformation
from etl_service.application.transformations.table_config import TABLE_CONFIG
from etl_service.application.transformations.row_modifications_mapping import ROW_INSERTION_MAPPING, ROW_DELETION_MAPPING
from etl_service.application.transformations.add_rows_transformation import AddRowsTransformation
from etl_service.application.transformations.delete_rows_transformation import DeleteRowsTransformation


class TransformationFactory:
    @staticmethod
    def get_transformations(table_key: str) -> list:
        """
        Retorna una lista de transformaciones aplicables a la tabla especificada, incluyendo la adición y eliminación de filas.

        :param table_key: Clave de la tabla en TABLE_CONFIG.
        :return: Lista de instancias de transformaciones.
        """
        transformations = []

        if table_key in TABLE_CONFIG:
            config = TABLE_CONFIG[table_key]
            rename_mapping = config.get('rename_columns', {})
            date_columns = config.get('date_columns', [])
            target_table_name = config.get('target_table')

            # Añadir la transformación para añadir filas si hay filas definidas en ROW_INSERTION_MAPPING
            rows_to_add = ROW_INSERTION_MAPPING.get(target_table_name, [])
            if rows_to_add:
                transformations.append(
                    AddRowsTransformation(rows_to_add)
                )
                logging.info(f"Añadido AddRowsTransformation para la tabla '{table_key}'.")

            # Añadir la transformación para eliminar filas si hay filas definidas en ROW_DELETION_MAPPING
            rows_to_delete = ROW_DELETION_MAPPING.get(target_table_name, [])
            if rows_to_delete:
                transformations.append(
                    DeleteRowsTransformation(rows_to_delete)
                )
                logging.info(f"Añadido DeleteRowsTransformation para la tabla '{table_key}'.")

            # Añadir la transformación de fechas si hay columnas de fecha
            if date_columns:
                date_column_mapping = {col: col for col in date_columns}
                transformations.append(
                    DateTransformation(
                        column_mapping=date_column_mapping,
                        null_if_zero=True
                    )
                )

            # Añadir la transformación para eliminar columnas con todos NULL, 0 o cadenas en blanco
            transformations.append(DropNullOrZeroColumnsTransformation())

            # Añadir la transformación para renombrar columnas si hay mapeos de renombrado
            if rename_mapping:
                transformations.append(
                    RenameColumnsTransformation(rename_mapping=rename_mapping)
                )

            # Añadir la transformación para ordenar columnas alfabéticamente
            transformations.append(
                SortColumnsTransformation(ascending=True)  # Cambia a False si deseas orden descendente
            )
        else:
            logging.warning(
                f"No se encontraron mapeos de columnas para la tabla '{table_key}'. No se aplicarán transformaciones específicas.")

        return transformations
