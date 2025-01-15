# etl_service/application/transformations/second_phase_use_case_dca.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text


class SecondPhaseUseCaseDcaDcfpro:
    """
    Clase para la segunda fase de ETL en PostgreSQL.
    Se asume que solo trabaja con la base de datos en PostgreSQL.
    """
    def __init__(self, postgres_repo):
        """
        :param postgres_repo: Instancia de PostgresRepository para interactuar con PostgreSQL.
        """
        self.postgres_repo = postgres_repo

    def execute_dca_dcfpro_join(self,
                                dca_key="dca",
                                dcfpro_key="dcfpro"):
        """
        Mantener EXACTAMENTE el número de filas de DCA.
        1) Leer la tabla DCA y la tabla DCFPRO.
        2) De DCFPRO, quedarnos solo con UNA fila por cada docoriide (p.e. la primera).
        3) Hacer un left join con dca.ide == subset_dcfpro.docoriide.
        4) Copiar la columna dcfpro.ide como 'ide_factura'.
        5) Crear columna booleana 'tiene_factura'.
        6) Sobrescribir la tabla DCA con las columnas nuevas.
        """
        try:
            logging.info(
                f"=== [con → obr → dca] Creando 'codigo_obra' a partir de {dca_key}, {dcfpro_key} ===")

            # 1) Obtener los nombres REALES de las tablas en la DB
            dca_table = self._get_target_table_name(dca_key)
            dcfpro_table = self._get_target_table_name(dcfpro_key)

            logging.info(
                f"Mapeo: {dca_key} => {dca_table}, {dcfpro_key} => {dcfpro_table}")

            # 1. Leer DCA
            dca_df = self._read_table_from_postgres(dca_table)
            # 2. Leer DCFPRO
            dcfpro_df = self._read_table_from_postgres(dcfpro_table)

            if dca_df.empty:
                logging.warning(f"La tabla '{dca_table}' está vacía. No se realizará proceso.")
                return

            if dcfpro_df.empty:
                logging.warning(f"La tabla '{dcfpro_table}' está vacía. Se marcará 'tiene_factura' = False a todo.")
                dca_df['ide_factura'] = None
                dca_df['tiene_factura'] = False
                self._overwrite_table(dca_table, dca_df)
                return

            logging.info(f"Registros en DCA: {len(dca_df)} | Registros en DCFPRO: {len(dcfpro_df)}")

            # 2.1) Quedarnos con UNA fila por cada 'docoriide'
            # p.e. la "primera" según orden ascendente de 'ide'
            # (puede usarse otra lógica)
            # docoriide: la col en dcfpro que referencia a la dca.ide
            dcfpro_subset = (
                dcfpro_df
                .sort_values(by=["docoriide", "ide"])   # Ordenar por docoriide y luego ide (p.e.)
                .drop_duplicates(subset=["docoriide"], keep="first")  # Quedarnos la 1ra fila
            )

            # Solo necesitaremos docoriide + ide, renombrando 'ide' a 'ide_factura'
            dcfpro_subset = dcfpro_subset[['docoriide', 'ide']].copy()
            dcfpro_subset.rename(columns={'ide': 'ide_factura'}, inplace=True)

            # 3. Merge (LEFT JOIN) => # filas = # filas DCA
            merged_df = dca_df.merge(
                dcfpro_subset,
                how='left',
                left_on='ide',        # en DCA
                right_on='docoriide'  # en DCFPRO subset
            )

            # 4. Crear columna booleana 'tiene_factura'
            merged_df['tiene_factura'] = merged_df['ide_factura'].apply(
                lambda x: (not pd.isna(x)) and x not in [0, '0']
            )

            # Eliminar la col 'docoriide' si no la necesitamos ya
            if 'docoriide' in merged_df.columns:
                merged_df.drop(columns=['docoriide'], inplace=True)

            logging.info("=== Muestra de datos resultantes (head) ===")
            logging.info(merged_df.head(5))

            logging.info(
                f"Filas en 'merged_df': {len(merged_df)} (debe coincidir con # filas de DCA original={len(dca_df)})"
            )

            # 5. Sobrescribir la tabla DCA
            self._overwrite_table(dca_table, merged_df)

            logging.info(
                f"Tabla '{dca_table}' sobrescrita con 'ide_factura' y 'tiene_factura' (SIN duplicar filas)."
            )

        except SQLAlchemyError as e:
            logging.error(f"Error SQLAlchemy durante la segunda fase: {e}")
        except Exception as ex:
            logging.error(f"Error inesperado en segunda fase ETL: {ex}")

    # --------------------------------------------------------------------------
    # Métodos de apoyo
    # --------------------------------------------------------------------------
    def _get_target_table_name(self, table_key: str) -> str:
        """
        Retorna el 'target_table' real definido en table_config
        (si no se encuentra, retorna el key tal cual).
        """
        from etl_service.application.transformations.table_config import TABLE_CONFIG

        if table_key in TABLE_CONFIG:
            return TABLE_CONFIG[table_key].get('target_table', table_key)
        else:
            return table_key

    def _read_table_from_postgres(self, table_name: str) -> pd.DataFrame:
        """
        Lee una tabla completa de PostgreSQL en un DataFrame.
        """
        query = f'SELECT * FROM "{table_name}"'
        try:
            with self.postgres_repo.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except SQLAlchemyError as e:
            logging.error(f"No se pudo leer la tabla '{table_name}' desde PostgreSQL: {e}")
            return pd.DataFrame()

    def _overwrite_table(self, table_name: str, df: pd.DataFrame):
        """
        Sobrescribe la tabla en PostgreSQL, eliminándola primero (DROP) y luego
        creando la tabla nueva con df.to_sql(...).
        """
        if df.empty:
            logging.warning(f"DataFrame vacío para la tabla '{table_name}'. No se crea nada.")
            return

        # Convertir a object para evitar problemas con NaT
        df = df.astype(object)
        # Reemplazar NaN/NaT con None => en Postgres será NULL
        df = df.where(pd.notnull(df), None)

        try:
            with self.postgres_repo.engine.begin() as conn:
                # Borrar la tabla si existe
                logging.info(f"Eliminando tabla '{table_name}' (si existe) para sobrescribirla...")
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

            # Crear la tabla con la nueva estructura
            df.to_sql(table_name, self.postgres_repo.engine, if_exists='fail', index=False)
            logging.info(f"Tabla '{table_name}' creada exitosamente con la nueva data (sobrescritura).")

        except Exception as e:
            logging.error(f"Error al sobrescribir la tabla '{table_name}': {e}")
            raise


