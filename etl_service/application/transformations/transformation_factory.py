# etl_service/application/transformations/transformation_factory.py

import logging
from etl_service.application.transformations.date_transformation import DateTransformation
from etl_service.application.transformations.drop_null_or_zero_columns_transformation import \
    DropNullOrZeroColumnsTransformation
from etl_service.application.transformations.rename_columns_transformation import RenameColumnsTransformation
from etl_service.application.transformations.sort_columns_transformation import SortColumnsTransformation
from etl_service.application.transformations.table_config import TABLE_CONFIG


class TransformationFactory:
    @staticmethod
    def get_transformations(table_key: str) -> list:
        """
        Retorna una lista de transformaciones aplicables a la tabla especificada.

        :param table_key: Clave de la tabla en TABLE_CONFIG.
        :return: Lista de instancias de transformaciones.
        """
        transformations = []

        if table_key in TABLE_CONFIG:
            config = TABLE_CONFIG[table_key]
            rename_mapping = config.get('rename_columns', {})
            date_columns = config.get('date_columns', [])

            # Añadir la transformación de fechas si hay columnas de fecha
            if date_columns:
                # Asumiendo que los nombres de las columnas ya están correctos en el DataFrame después del renombrado
                # Si los nombres de las columnas cambian, deberás ajustar el mapeo en 'column_mapping'
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
