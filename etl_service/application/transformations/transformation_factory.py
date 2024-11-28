# etl_service/application/transformations/transformation_factory.py

from etl_service.application.transformations.date_transformation import DateTransformation
from etl_service.application.transformations.drop_null_or_zero_columns_transformation import \
    DropNullOrZeroColumnsTransformation
from etl_service.application.transformations.rename_columns_transformation import RenameColumnsTransformation
from etl_service.application.transformations.sort_columns_transformation import SortColumnsTransformation
from etl_service.application.transformations.column_mappings import COLUMN_MAPPINGS


class TransformationFactory:
    @staticmethod
    def get_transformations(table_name: str) -> list:
        """
        Retorna una lista de transformaciones aplicables a la tabla especificada.

        :param table_name: Nombre de la tabla.
        :return: Lista de instancias de transformaciones.
        """
        transformations = []
        table_key = table_name.lower()

        if table_key in COLUMN_MAPPINGS:
            mappings = COLUMN_MAPPINGS[table_key]

            # Añadir la transformación de fechas si hay columnas de fecha
            if 'date_columns' in mappings and mappings['date_columns']:
                transformations.append(
                    DateTransformation(
                        column_mapping={col: col for col in mappings['date_columns']},
                        # Asumiendo que los nombres ya están correctos
                        null_if_zero=True
                    )
                )

            # Añadir la transformación para eliminar columnas con todos NULL, 0 o cadenas en blanco
            transformations.append(DropNullOrZeroColumnsTransformation())

            # Añadir la transformación para renombrar columnas si hay mapeos de renombrado
            if 'rename' in mappings and mappings['rename']:
                transformations.append(
                    RenameColumnsTransformation(rename_mapping=mappings['rename'])
                )

            # Añadir la transformación para ordenar columnas alfabéticamente
            transformations.append(
                SortColumnsTransformation(ascending=True)  # Cambia a False si deseas orden descendente
            )
        else:
            logging.warning(
                f"No se encontraron mapeos de columnas para la tabla '{table_name}'. No se aplicarán transformaciones específicas.")

        return transformations
