# etl_service/application/transformations/transformation_factory.py

from etl_service.application.transformations.date_transformation import DateTransformation

class TransformationFactory:
    @staticmethod
    def get_transformations(table_name: str) -> list:
        """
        Retorna una lista de transformaciones aplicables a la tabla especificada.
        :param table_name: Nombre de la tabla.
        :return: Lista de instancias de transformaciones.
        """
        transformations = []

        if table_name.lower() == 'obr':
            # Definir el mapeo de columnas para la tabla 'obr'
            column_mapping = {
                'fecinipre': 'fecha_inicio_prevista',
                'fecfinpre': 'fecha_fin_prevista',
                'fecinirea': 'fecha_inicio_real',
                'fecfinrea': 'fecha_fin_real'
            }
            # Añadir la transformación de fechas
            transformations.append(DateTransformation(column_mapping=column_mapping, null_if_zero=True))

        # Aquí puedes añadir más transformaciones para otras tablas

        return transformations
