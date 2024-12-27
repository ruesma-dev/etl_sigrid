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
from etl_service.application.transformations.join_with_con_transformation import JoinWithConTransformation
from etl_service.application.transformations.clean_null_chars_transformation import CleanNullCharsTransformation
from etl_service.domain.interfaces import SpecificTableTransformations

class TransformationFactory:
    """
    Factoría que genera la lista de transformaciones a aplicar a cada tabla,
    incluyendo la posibilidad de cargar y unir la tabla `con`, y también
    la tabla `dcfpro` en caso de que la tabla 'dcf' necesite combinar datos.
    """

    # DataFrame cache estática para 'con'
    con_df = None
    # DataFrame cache estática para 'dcfpro'
    dcfpro_df = None

    # Guardamos aquí la referencia al ExtractUseCase para poder cargar tablas
    extract_use_case = None

    @staticmethod
    def load_con_df_if_needed():
        """
        Carga la tabla 'con' en self.con_df si no estaba ya cargada.
        Requiere que extract_use_case no sea None.
        """
        if TransformationFactory.con_df is None:
            logging.info("Cargando la tabla 'con' en memoria para joins.")
            if TransformationFactory.extract_use_case is None:
                raise ValueError("extract_use_case no está configurado en TransformationFactory (para 'con').")
            df_map = TransformationFactory.extract_use_case.execute(['con'])
            if df_map and 'con' in df_map:
                TransformationFactory.con_df = df_map['con']
                logging.info(f"Tabla 'con' cargada con {len(TransformationFactory.con_df)} registros.")
            else:
                logging.warning("No se pudo cargar la tabla 'con'. Los joins con 'con' no funcionarán correctamente.")

    @staticmethod
    def load_dcfpro_df_if_needed():
        """
        Carga la tabla 'dcfpro' en dcfpro_df si no estaba ya cargada.
        Requiere que extract_use_case no sea None.
        """
        if TransformationFactory.dcfpro_df is None:
            logging.info("Cargando la tabla 'dcfpro' en memoria para transformaciones de DCF.")
            if TransformationFactory.extract_use_case is None:
                raise ValueError("extract_use_case no está configurado en TransformationFactory (para 'dcfpro').")
            df_map = TransformationFactory.extract_use_case.execute(['dcfpro'])
            if df_map and 'dcfpro' in df_map:
                TransformationFactory.dcfpro_df = df_map['dcfpro']
                logging.info(f"Tabla 'dcfpro' cargada con {len(TransformationFactory.dcfpro_df)} registros.")
            else:
                logging.warning("No se pudo cargar la tabla 'dcfpro'. La lógica específica de 'dcf' que usa 'dcfpro' no funcionará.")

    @staticmethod
    def get_transformations(table_key: str, extract_use_case=None) -> list:
        """
        Retorna una lista de transformaciones aplicables a la tabla especificada.

        :param table_key: Clave (nombre) de la tabla a procesar (p.e. 'obr', 'cen', etc.)
        :param extract_use_case: para poder cargar tablas auxiliares si es necesario.
        """
        transformations = []

        # Si no se pasa extract_use_case en la llamada, pero la factoría ya tiene uno, lo seguimos usando.
        # En cambio, si la factoría no tiene extract_use_case y se pasa uno, lo guardamos.
        if extract_use_case and TransformationFactory.extract_use_case is None:
            TransformationFactory.extract_use_case = extract_use_case

        # Ver si está en la config
        if table_key in TABLE_CONFIG:
            config = TABLE_CONFIG[table_key]
            rename_mapping = config.get('rename_columns', {})
            date_columns = config.get('date_columns', [])
            target_table_name = config.get('target_table')
            combine_columns_config = config.get('combine_columns', [])

            # 1) JOINS CON 'con'
            join_with_con_config = config.get('join_with_con', None)
            if join_with_con_config:
                join_column = join_with_con_config.get('join_column')
                if join_column:
                    # Se requiere extract_use_case para cargar 'con'
                    if TransformationFactory.extract_use_case is not None:
                        TransformationFactory.load_con_df_if_needed()
                        if TransformationFactory.con_df is not None:
                            transformations.append(
                                JoinWithConTransformation(TransformationFactory.con_df, join_column)
                            )
                            logging.info(f"Se ha insertado JoinWithConTransformation para '{table_key}' usando '{join_column}'.")
                        else:
                            logging.warning("No se pudo realizar el join con 'con' ya que con_df es None.")
                    else:
                        logging.warning("No extract_use_case provided, no se pudo cargar 'con' para el join_with_con.")

            # 2) Insertar filas
            rows_to_add = ROW_INSERTION_MAPPING.get(target_table_name, [])
            if rows_to_add:
                transformations.append(AddRowsTransformation(rows_to_add))
                logging.info(f"Añadido AddRowsTransformation para la tabla '{table_key}'.")

            # 3) Eliminar filas
            rows_to_delete = ROW_DELETION_MAPPING.get(target_table_name, [])
            if rows_to_delete:
                transformations.append(DeleteRowsTransformation(rows_to_delete))
                logging.info(f"Añadido DeleteRowsTransformation para la tabla '{table_key}'.")

            # 4) Transformaciones de fecha
            if date_columns:
                date_column_mapping = {col: col for col in date_columns}
                transformations.append(DateTransformation(column_mapping=date_column_mapping, null_if_zero=True))

            # 5) Combinar columnas
            for combo_config in combine_columns_config:
                new_col = combo_config.get('new_column_name')
                cols_to_combine = combo_config.get('columns_to_combine', [])
                sep = combo_config.get('separator', '_')
                if new_col and cols_to_combine:
                    transformations.append(
                        CombineColumnsTransformation(
                            new_column_name=new_col,
                            columns_to_combine=cols_to_combine,
                            separator=sep
                        )
                    )
                    logging.info(f"Añadido CombineColumnsTransformation para la tabla '{table_key}' creando '{new_col}'.")

            # 6) Eliminar columnas vacías
            transformations.append(DropNullOrZeroColumnsTransformation())

            # 7) Renombrar columnas
            if rename_mapping:
                transformations.append(RenameColumnsTransformation(rename_mapping=rename_mapping))

            # 8) Ordenar columnas
            transformations.append(SortColumnsTransformation(ascending=True))

            # 9) Transformaciones ESPECÍFICAS
            module_name = f"etl_service.application.transformations.specific.{table_key}_transformations"
            class_name = f"{table_key.capitalize()}Transformations"
            spec = importlib.util.find_spec(module_name)

            if spec is not None:
                mod = importlib.import_module(module_name)
                TransformClass = getattr(mod, class_name, None)

                if TransformClass and issubclass(TransformClass, SpecificTableTransformations):
                    # Caso especial: la tabla 'dcf' necesita cargar 'dcfpro'
                    if table_key == 'dcf':
                        # Cargamos la df de 'dcfpro'
                        if TransformationFactory.extract_use_case is None:
                            logging.warning("No hay extract_use_case para cargar 'dcfpro'. Transformación dcf incompleta.")
                        else:
                            TransformationFactory.load_dcfpro_df_if_needed()

                        if TransformationFactory.dcfpro_df is not None:
                            specific_instance = TransformClass(TransformationFactory.dcfpro_df)
                        else:
                            logging.warning("dcfpro_df es None, no se podrá hacer la lógica de DcfTransformations.")
                            # Si el constructor exige dcfpro_df, podríamos pasar un DF vacío o None.
                            # O no instanciar en absoluto:
                            specific_instance = None

                    # Caso especial: la tabla 'dcapro' (otra con extract_use_case)
                    elif table_key in ['dca', 'dcapro']:
                        specific_instance = TransformClass(extract_use_case)
                    else:
                        # Otras tablas
                        specific_instance = TransformClass()

                    if specific_instance is not None:
                        specific_transformations = specific_instance.get_table_transformations()
                        transformations.extend(specific_transformations)

            # 10) Limpieza de caracteres NUL
            transformations.append(CleanNullCharsTransformation())

        else:
            logging.warning(
                f"No se encontraron mapeos de columnas para la tabla '{table_key}'. No se aplicarán transformaciones específicas."
            )

        return transformations
