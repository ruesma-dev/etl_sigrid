# etl_service/application/transformations/transformation_factory.py

from etl_service.application.transformations.date_transformation import DateTransformation
from etl_service.application.transformations.drop_null_or_zero_columns_transformation import DropNullOrZeroColumnsTransformation
from etl_service.application.transformations.rename_columns_transformation import RenameColumnsTransformation
from etl_service.application.transformations.sort_columns_transformation import SortColumnsTransformation

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
            # Definir el mapeo de columnas de fecha para la tabla 'obr'
            column_mapping = {
                'fecinipre': 'fecha_inicio_prevista',
                'fecfinpre': 'fecha_fin_prevista',
                'fecinirea': 'fecha_inicio_real',
                'fecfinrea': 'fecha_fin_real',
                'feccieest': 'fecha_cierre_estudio',
                'fecadj': 'fecha_adjudicacion',
                'fecfincie': 'fecha_fin_cierre',
                'fecciepre': 'fecha_cierre_previsto',
                'fecapelic': 'fecha_apertura_licitacion',
                'feclic': 'fecha_licitacion',
                'fecofe': 'fecha_oferta'
            }
            # Añadir la transformación de fechas
            transformations.append(DateTransformation(column_mapping=column_mapping, null_if_zero=True))
            # Añadir la transformación para eliminar columnas con todos NULL, 0 o cadenas en blanco
            transformations.append(DropNullOrZeroColumnsTransformation())
            # Definir el mapeo para renombrar 'res' a 'nombre_obra'
            rename_mapping = {
                'res': 'nombre_obra'
            }
            # Añadir la transformación para renombrar columnas
            transformations.append(RenameColumnsTransformation(rename_mapping=rename_mapping))
            # Añadir la transformación para ordenar columnas alfabéticamente
            transformations.append(SortColumnsTransformation(ascending=True))  # Cambia a False si deseas orden descendente

        elif table_name.lower() == 'cen':
            # Definir el mapeo de columnas de fecha para la tabla 'cen' (centro_costes)
            column_mapping_cen = {
                'fecfinpre': 'fecha_fin_previsto',
                'fecfinrea': 'fecha_fin_real',
                'fecbloqueo': 'fecha_bloqueo',
                'fecfincie': 'fecha_fin_cierre'
            }
            # Añadir la transformación de fechas para 'cen'
            transformations.append(DateTransformation(column_mapping=column_mapping_cen, null_if_zero=True))
            # Añadir la transformación para eliminar columnas con todos NULL, 0 o cadenas en blanco
            transformations.append(DropNullOrZeroColumnsTransformation())
            # Definir el mapeo para renombrar la tabla 'cen' a 'centro_costes'
            # Suponiendo que deseas renombrar la tabla al cargar, manejarás esto en el caso de uso de carga
            # Sin embargo, si hay columnas específicas que necesiten renombrarse, añade aquí
            # Por ejemplo, si hay alguna columna 'res' similar a 'obr', podrías añadir:
            # rename_mapping_cen = {
            #     'res': 'nombre_obra_centro'
            # }
            # transformations.append(RenameColumnsTransformation(rename_mapping=rename_mapping_cen))
            # Añadir la transformación para ordenar columnas alfabéticamente
            transformations.append(
                SortColumnsTransformation(ascending=True))  # Cambia a False si deseas orden descendente

            # Aquí puedes añadir más transformaciones para otras tablas

        return transformations

        return transformations
